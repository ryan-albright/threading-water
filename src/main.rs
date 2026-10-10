use std::{
    error::Error,
    fs,
    io::{BufReader, prelude::*},
    net::{TcpListener, TcpStream},
    process, thread,
    time::Duration,
};

use server::ThreadPool;

fn main() {
    let port = "127.0.0.1:7878";
    let listener = match TcpListener::bind(port) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Unable to bind at {port} {e}");
            process::exit(1)
        }
    };

    let pool = match ThreadPool::build(4) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            let mut next_error = e.source();
            while let Some(new_error) = next_error {
                eprintln!("{new_error}");
                next_error = new_error.source()
            }
            process::exit(1)
        }
    };

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if let Err(e) = pool.execute(|| handle_connection(s)) {
                    eprintln!("Failed to execute {e}");
                    break;
                }
            }
            Err(e) => {
                eprintln!("Encountered error {e}");
            }
        }
    }

    println!("Shutting down.");
}

fn handle_connection(mut stream: TcpStream) {
    let buf_reader = BufReader::new(&stream);

    let request_line = match buf_reader.lines().next() {
        Some(Ok(r)) => r,
        Some(Err(r)) => {
            eprintln!("unreadable request {r}");
            return;
        }
        None => String::new(),
    };

    let (status_line, filename) = match &request_line[..] {
        "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "hello.html"),
        "GET /sleep HTTP/1.1" => {
            thread::sleep(Duration::from_secs(5));
            ("HTTP/1.1 200 OK", "hello.html")
        }
        _ => ("HTTP/1.1 404 NOT FOUND", "404.html"),
    };

    let (status_line, contents) = match fs::read_to_string(filename) {
        Ok(f) => (status_line, f),
        Err(e) => {
            eprintln!("Internal server error {e}");
            (
                "HTTP/1.1 500 Internal Server Error",
                String::from("500 Internal Server Error"),
            )
        }
    };

    let length = contents.len();

    let response = format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{contents}");

    if let Err(e) = stream.write_all(response.as_bytes()) {
        eprintln!("Failed to write a response {e}");
    }
}
