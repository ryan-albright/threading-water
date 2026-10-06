use std::{
    fs,
    io::{BufReader, prelude::*},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
    process,
};

use server::ThreadPool;

fn main() {
    let port = "127.0.0.1:7878";
    let listener = match TcpListener::bind(&port) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Unable to bind at {port} {e}");
            process::exit(1)
        }
    };

    let pool = ThreadPool::new(4);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                pool.execute(|| handle_connection(s));
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
