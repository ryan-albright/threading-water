use std::{
    error::Error,
    fs,
    io::{self, BufReader, prelude::*},
    net::{TcpListener, TcpStream},
    process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, sleep},
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

    let mut pool = match ThreadPool::build(4) {
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

    let shutdown_requested: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    // optional for linux systems
    if let Err(e) = listener.set_nonblocking(true) {
        eprintln!("Failed to set non-blocking: {e}");
        process::exit(1)
    }

    let for_thread = shutdown_requested.clone();
    thread::spawn(move || {
        let lines = io::stdin().lines();
        for line in lines {
            match line {
                Ok(l) => {
                    if l.trim() == "q" {
                        for_thread.store(true, Ordering::Relaxed)
                    };
                }
                Err(_) => {
                    for_thread.store(true, Ordering::Relaxed);
                }
            }
        }
        for_thread.store(true, Ordering::Relaxed);
    });

    while !shutdown_requested.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((socket, _)) => {
                if let Err(e) = pool.execute(|| handle_connection(socket)) {
                    eprintln!("Failed to execute {e}");
                    break;
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    sleep(Duration::from_millis(50));
                } else {
                    eprintln!("Encountered error: {e}");
                }
            }
        }
    }

    println!("Shutting down...");
    let report = pool.shutdown_timeout(Duration::from_secs(2));
    for (id, msg) in &report.panicked_workers {
        eprintln!("worker {id} panicked during shutdown: {msg}");
    }

    for id in &report.timed_out_workers {
        eprintln!("worker {id} timed out; job abandoned");
    }
    println!("Shutdown complete.");
    if !report.timed_out_workers.is_empty() || !report.panicked_workers.is_empty() {
        process::exit(1)
    }
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
