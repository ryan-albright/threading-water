use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{self, Child, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

struct ServerProcess {
    child: Child,
    address: SocketAddr,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server(time_limit: Duration) -> ServerProcess {
    let target_address = "127.0.0.1:0";

    let listener = TcpListener::bind(target_address).expect("Listener should bind");

    let address = listener.local_addr().unwrap();
    drop(listener);
    let child = process::Command::new(env!("CARGO_BIN_EXE_server"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("SERVER_PORT", address.port().to_string())
        .stdin(Stdio::piped())
        .spawn()
        .expect("Server start should succeed");

    let process = ServerProcess { child, address };
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
}

#[test]
fn get_root_returns_hello_page() {
    let server_process = start_server(Duration::from_secs(3));
    let response = server_process.get("/");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("<p>Hi from Rust</p>"), "{response}");
}

#[test]
fn bad_get_returns_404_page() {
    let server_process = start_server(Duration::from_secs(3));
    let response = server_process.get("/nope");

    assert!(response.starts_with("HTTP/1.1 404 NOT FOUND"), "{response}");
    assert!(
        response.contains("<p>Sorry, I don't know what you're asking for.</p>"),
        "{response}"
    );
}
