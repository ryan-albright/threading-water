use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    process::{self, Child, ChildStderr, ChildStdin, ChildStdout, ExitStatus, Stdio},
    sync::mpsc::{self, RecvTimeoutError},
    thread::{self, sleep},
    time::{Duration, Instant},
};

struct ServerProcess {
    child: Child,
    address: SocketAddr,
    stdin: ChildStdin,
    stdout: Option<BufReader<ChildStdout>>,
    stderr: ChildStderr,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const TIME_LIMIT: Duration = Duration::from_secs(3);
const ANY_FREE_PORT: u16 = 0;

fn start_server() -> ServerProcess {
    let placeholder_address = SocketAddr::from(([127, 0, 0, 1], ANY_FREE_PORT));
    let mut child = process::Command::new(env!("CARGO_BIN_EXE_server"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("SERVER_PORT", ANY_FREE_PORT.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Server binary should open");

    let stdin = child.stdin.take().expect("stdin should pipe");
    let stdout = child.stdout.take().expect("stdout should pipe");
    let stderr = child.stderr.take().expect("stderr should pipe");
    let mut process = ServerProcess {
        child,
        address: placeholder_address,
        stdin,
        stdout: Some(BufReader::new(stdout)),
        stderr,
    };

    let (tx, rx) = mpsc::channel::<(BufReader<ChildStdout>, String)>();

    let mut reader = process
        .stdout
        .take()
        .expect("Server stdout should not already be taken");

    thread::spawn(move || {
        let mut server_output = String::new();

        reader
            .read_line(&mut server_output)
            .expect("Should read server's first stdout line");

        let _ = tx.send((reader, server_output));
    });

    let (reader, bind_confirmation) = match rx.recv_timeout(TIME_LIMIT) {
        Ok(t) => t,
        Err(RecvTimeoutError::Timeout) => {
            panic!("Server did not print anything within {TIME_LIMIT:?}")
        }
        Err(RecvTimeoutError::Disconnected) => panic!("Reader thread ended without sending"),
    };

    process.address = parse_listening_line(&bind_confirmation);
    process.stdout = Some(reader);
    process
}

fn parse_listening_line(input: &str) -> SocketAddr {
    input
        .trim()
        .strip_prefix("Listening on ")
        .unwrap_or_else(|| panic!("Prefix should match {input:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("Failed to parse {input:?}"))
}

impl ServerProcess {
    fn send(&self, path: &str) -> TcpStream {
        let mut connection = TcpStream::connect(self.address)
            .unwrap_or_else(|e| panic!("Server should accept connection at {}: {e}", self.address));

        let request = format!("GET {path} HTTP/1.1\r\n\r\n");
        connection
            .write_all(request.as_bytes())
            .expect("Should write request to server");

        connection
    }

    fn get(&self, path: &str) -> String {
        let mut connection = self.send(path);

        let mut response = String::new();
        connection
            .read_to_string(&mut response)
            .expect("Should read full response");

        response
    }

    fn wait_for_exit(&mut self, time_limit: Duration) -> ExitStatus {
        let deadline = Instant::now() + time_limit;
        loop {
            match self
                .child
                .try_wait()
                .expect("Polling server process should succeed")
            {
                Some(status) => return status,
                None => {
                    if Instant::now() > deadline {
                        panic!("Server failed to exit within {time_limit:?}")
                    }
                    sleep(Duration::from_millis(25));
                }
            }
        }
    }

    fn write_stdin(&mut self, input: &str) {
        self.stdin
            .write_all(input.as_bytes())
            .expect("Write to server's stdin should succeed");
    }

    fn read_stdout(&mut self) -> String {
        let mut output = String::new();
        self.stdout
            .as_mut()
            .expect("Server stdout should not already be taken")
            .read_to_string(&mut output)
            .expect("Should read stdout to EOF");
        output
    }

    fn read_stderr(&mut self) -> String {
        let mut output = String::new();
        self.stderr
            .read_to_string(&mut output)
            .expect("Should read stderr to EOF");
        output
    }
}

#[test]
fn get_root_returns_hello_page() {
    let server_process = start_server();
    let response = server_process.get("/");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("<p>Hi from Rust</p>"), "{response}");
}

#[test]
fn bad_get_returns_404_page() {
    let server_process = start_server();
    let response = server_process.get("/nope");

    assert!(response.starts_with("HTTP/1.1 404 NOT FOUND"), "{response}");
    assert!(
        response.contains("<p>Sorry, I don't know what you're asking for.</p>"),
        "{response}"
    );
}

#[test]
fn q_input_shuts_down_server() {
    let mut server_process = start_server();

    server_process.write_stdin("q\n");
    let status = server_process.wait_for_exit(TIME_LIMIT);
    assert_eq!(status.code(), Some(0), "{status}");

    let output = server_process.read_stdout();
    assert!(output.contains("Shutdown complete."), "{output}");
}

#[test]
fn shutdown_abandons_in_flight_sleep_request() {
    let mut server_process = start_server();

    let _slow_response = server_process.send("/sleep");
    // Send a fast request to confirm the sleep request was received
    let fast_response = server_process.get("/");
    assert!(
        fast_response.starts_with("HTTP/1.1 200 OK"),
        "{fast_response}"
    );

    server_process.write_stdin("q\n");
    // set to 3 seconds as the shutdown is limited to 2 seconds and the sleep request takes 5 seconds
    let status = server_process.wait_for_exit(Duration::from_secs(3));
    assert_eq!(status.code(), Some(1), "{status}");

    let stderr_output = server_process.read_stderr();
    assert!(
        stderr_output.contains("timed out; job abandoned"),
        "{stderr_output}"
    );
}
