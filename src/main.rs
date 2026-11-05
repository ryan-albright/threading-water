use std::{
    collections::HashMap,
    env::{self, var},
    error::Error,
    fmt, fs,
    io::{self, BufReader, prelude::*},
    net::{TcpListener, TcpStream},
    process,
    sync::atomic::{AtomicBool, Ordering},
    thread::{self, sleep},
    time::Duration,
};

use server::ThreadPool;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn handler(_signum: i32) {
    SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
}

unsafe extern "C" {
    fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
}

const MAX_LINE: u64 = 8 * 1024;
const MAX_BODY: u64 = 1024 * 1024;

fn main() {
    let port_number: u16 = match var("SERVER_PORT") {
        Ok(p) => match str::parse(&p) {
            Ok(s) => s,
            Err(_) => {
                eprintln!("SERVER_PORT is not valid: {p}");
                process::exit(1)
            }
        },
        Err(env::VarError::NotPresent) => 7878,
        Err(env::VarError::NotUnicode(bad_port)) => {
            eprintln!("SERVER_PORT is not valid Unicode: {:?}", bad_port);
            process::exit(1)
        }
    };

    let port = format!("127.0.0.1:{port_number}");

    let listener = match TcpListener::bind(&port) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Unable to bind at {port} {e}");
            process::exit(1)
        }
    };

    match listener.local_addr() {
        Ok(address) => println!("Listening on {}", address),
        Err(e) => {
            eprintln!("Unable to retrive address: {e}");
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

    // optional for linux systems
    if let Err(e) = listener.set_nonblocking(true) {
        eprintln!("Failed to set non-blocking: {e}");
        process::exit(1)
    }

    // linux values
    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    const SIG_ERR: usize = usize::MAX;
    for sig in [SIGINT, SIGTERM] {
        let previous = unsafe { signal(sig, handler) };
        if previous == SIG_ERR {
            let recent_err = io::Error::last_os_error();
            eprintln!("Failed to install handler for signal {sig}: {recent_err}");
            process::exit(1)
        }
    }

    thread::spawn(|| {
        let lines = io::stdin().lines();
        for line in lines {
            match line {
                Ok(l) => {
                    if l.trim() == "q" {
                        SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed)
                    };
                }
                Err(_) => {
                    SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
                }
            }
        }
        SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
    });

    while !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
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

#[derive(Debug)]
enum ParseError {
    MissingMethod,
    MissingPath,
    MalformedHeader,
    Io(io::Error),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::MissingMethod => write!(f, "Method was not provided"),
            ParseError::MissingPath => write!(f, "Path was not provided"),
            ParseError::MalformedHeader => write!(f, "Header was missing ':'"),
            ParseError::Io(_) => write!(f, "Failed to read request"),
        }
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParseError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ParseError {
    fn from(value: io::Error) -> Self {
        ParseError::Io(value)
    }
}

struct Request {
    method: String,
    path: String,
    headers: HashMap<String, String>,
}

fn handle_connection(mut stream: TcpStream) {
    let mut buf_reader = BufReader::new(&stream);
    let request_iterator = buf_reader.by_ref().take(MAX_LINE).lines();

    let request = match parse_request(request_iterator) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to parse request: {e}");
            match e {
                ParseError::Io(_) => {}
                _ => {
                    write_response(&mut stream, "HTTP/1.1 400 BAD REQUEST", "400.html");
                }
            }
            return;
        }
    };

    // let content_length = match headers
    //     .get("content-length")
    //     .unwrap_or(&"0".to_string())
    //     .parse::<u64>() {
    //         Ok(h) => h,
    //         Err(h) => {
    //             let status_line
    //         }
    //     };

    // if content_length < MAX_BODY {
    //     let mut body = Vec::with_capacity(content_length as usize);
    //     // below read does not handle client sending less than promised
    //     buf_reader
    //         .by_ref()
    //         .take(content_length)
    //         .read_to_end(&mut body);
    // }

    let (status_line, filename) = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => ("HTTP/1.1 200 OK", "hello.html"),
        ("GET", "/sleep") => {
            thread::sleep(Duration::from_secs(5));
            ("HTTP/1.1 200 OK", "hello.html")
        }
        ("GET", _) => ("HTTP/1.1 404 NOT FOUND", "404.html"),
        ("POST", "/") => ("HTTP/1.1 200 OK", "hello.html"),
        ("POST", "/sleep") => ("HTTP/1.1 405 METHOD NOT ALLOWED", "405.html"),
        ("POST", "/echo") => ("HTTP/1.1 200 OK", "hello.html"),
        ("POST", _) => ("HTTP/1.1 404 NOT FOUND", "404.html"),
        (_, _) => ("HTTP/1.1 405 METHOD NOT ALLOWED", "405.html"),
    };

    write_response(&mut stream, status_line, filename);
}

fn parse_request(
    mut request_iterator: impl Iterator<Item = io::Result<String>>,
) -> Result<Request, ParseError> {
    let request_line = request_iterator.next().ok_or(ParseError::MissingMethod)??;

    let mut request_line_iter = request_line.split_whitespace();
    let method = request_line_iter
        .next()
        .ok_or(ParseError::MissingMethod)?
        .to_string();
    let path = request_line_iter
        .next()
        .ok_or(ParseError::MissingPath)?
        .to_string();

    let mut headers: HashMap<String, String> = HashMap::new();
    for line in request_iterator {
        let line = line?;
        if line.is_empty() {
            break;
        }

        let (name, val) = line.split_once(":").ok_or(ParseError::MalformedHeader)?;
        let name_clean = name.trim().to_ascii_lowercase();
        let val_clean = val.trim().to_string();
        headers.insert(name_clean, val_clean);
    }

    Ok(Request {
        method,
        path,
        headers,
    })
}

fn write_response(stream: &mut TcpStream, status_line: &str, filename: &str) {
    let (status_line, contents) = match fs::read_to_string(filename) {
        Ok(f) => (status_line, f),
        Err(e) => {
            eprintln!("Internal server error {e}");
            (
                "HTTP/1.1 500 Internal Server Error",
                "500 Internal Server Error".to_string(),
            )
        }
    };

    let length = contents.len();

    let response = format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{contents}");

    if let Err(e) = stream.write_all(response.as_bytes()) {
        eprintln!("Failed to write a response {e}");
    }
}
