use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{self, Child, ChildStdin, ChildStdout, ExitStatus, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

struct ServerProcess {
    child: Child,
    address: SocketAddr,
    stdin: ChildStdin,
    stdout: ChildStdout,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const TIME_LIMIT: Duration = Duration::from_secs(3);

fn start_server(time_limit: Duration) -> ServerProcess {
    let target_address = "127.0.0.1:0";

    let listener = TcpListener::bind(target_address).expect("Listener should bind");

    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut child = process::Command::new(env!("CARGO_BIN_EXE_server"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("SERVER_PORT", address.port().to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Server start should succeed");

    let stdin = child.stdin.take().expect("stdin should pipe");
    let stdout = child.stdout.take().expect("stdout should pipe");
    let process = ServerProcess {
        child,
        address,
        stdin,
        stdout,
    };
    let deadline = Instant::now() + time_limit;

    while TcpStream::connect(process.address).is_err() {
        if Instant::now() > deadline {
            panic!("Server did not accept connections within {time_limit:?}")
        }
        sleep(Duration::from_millis(50))
    }

    process
}

impl ServerProcess {
    fn get(&self, path: &str) -> String {
        let mut connection =
            TcpStream::connect(self.address).expect("Server should accept connection");

        let request = format!("GET {path} HTTP/1.1\r\n\r\n");
        connection
            .write_all(request.as_bytes())
            .expect("Request should be accepted");

        let mut response = String::new();
        connection
            .read_to_string(&mut response)
            .expect("Read should succeed");

        response
    }

    fn wait_for_exit(&mut self, time_limit: Duration) -> ExitStatus {
        let deadline = Instant::now() + time_limit;
        loop {
            match self.child.try_wait().expect("try wait should succeed") {
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
            .expect("stdin write should succeed");
    }

    fn read_stdout(&mut self) -> String {
        let mut output = String::new();
        self.stdout
            .read_to_string(&mut output)
            .expect("stdout should be readable");
        output
    }
}

#[test]
fn get_root_returns_hello_page() {
    let server_process = start_server(TIME_LIMIT);
    let response = server_process.get("/");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("<p>Hi from Rust</p>"), "{response}");
}

#[test]
fn bad_get_returns_404_page() {
    let server_process = start_server(TIME_LIMIT);
    let response = server_process.get("/nope");

    assert!(response.starts_with("HTTP/1.1 404 NOT FOUND"), "{response}");
    assert!(
        response.contains("<p>Sorry, I don't know what you're asking for.</p>"),
        "{response}"
    );
}

#[test]
fn q_input_shutsdown_server() {
    let mut server_process = start_server(TIME_LIMIT);

    server_process.write_stdin("q\n");
    let status = server_process.wait_for_exit(TIME_LIMIT);
    assert_eq!(status.code(), Some(0), "{status}");

    let output = server_process.read_stdout();
    assert!(output.contains("Shutdown complete."), "{output}");
}
