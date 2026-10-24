use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{self, Child, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

struct ServerGuard {
    child: Child,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn get_root_returns_hello_page() {
    let target_address = "127.0.0.1:0";

    let listener = TcpListener::bind(target_address).expect("Listener should bind");

    let bound_address = listener.local_addr().unwrap();
    drop(listener);
    let child = process::Command::new(env!("CARGO_BIN_EXE_server"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("SERVER_PORT", bound_address.port().to_string())
        .stdin(Stdio::piped())
        .spawn()
        .expect("Server start should succeed");

    let _process = ServerGuard { child };

    let time_limit = Duration::from_secs(3);
    let deadline = Instant::now() + time_limit;

    let mut stream = None;

    while stream.is_none() && Instant::now() < deadline {
        let connection_attempt = TcpStream::connect(bound_address);
        stream = connection_attempt.ok();
        if stream.is_none() {
            sleep(Duration::from_millis(50))
        }
    }

    let mut connection = stream.expect("Server did not accept connections within 3s");

    connection
        .write_all(b"GET / HTTP/1.1\r\n\r\n")
        .expect("Request should be accepted");

    let mut response = String::new();
    connection
        .read_to_string(&mut response)
        .expect("Read should be successful");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("<p>Hi from Rust</p>"), "{response}");
}

#[test]
fn bad_get_returns_404_page() {
    let target_address = "127.0.0.1:0";

    let listener = TcpListener::bind(target_address).expect("Listener should bind");

    let bound_address = listener.local_addr().unwrap();
    drop(listener);
    let child = process::Command::new(env!("CARGO_BIN_EXE_server"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("SERVER_PORT", bound_address.port().to_string())
        .stdin(Stdio::piped())
        .spawn()
        .expect("Server start should succeed");

    let _process = ServerGuard { child };

    let time_limit = Duration::from_secs(3);
    let deadline = Instant::now() + time_limit;

    let mut stream = None;

    while stream.is_none() && Instant::now() < deadline {
        let connection_attempt = TcpStream::connect(bound_address);
        stream = connection_attempt.ok();
        if stream.is_none() {
            sleep(Duration::from_millis(50))
        }
    }

    let mut connection = stream.expect("Server did not accept connections within 3s");

    connection
        .write_all(b"GET /nope HTTP/1.1\r\n\r\n")
        .expect("Request should be accepted");

    let mut response = String::new();
    connection
        .read_to_string(&mut response)
        .expect("Read should be successful");

    assert!(response.starts_with("HTTP/1.1 404 NOT FOUND"), "{response}");
    assert!(
        response.contains("<p>Sorry, I don't know what you're asking for.</p>"),
        "{response}"
    );
}
