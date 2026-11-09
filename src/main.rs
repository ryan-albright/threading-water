use std::{
    collections::HashMap,
    env::{self, var},
    error::Error,
    fmt, fs,
    io::{self, BufReader, prelude::*},
    net::{TcpListener, TcpStream},
    process,
    str::FromStr,
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
    UnknownMethod,
    MissingPath,
    MalformedHeader,
    // a followup is to split this error into an unparseable content length
    // versus a content length that is too long
    BadContentLength,
    Io(io::Error),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::MissingMethod => write!(f, "Method was not provided"),
            ParseError::UnknownMethod => write!(f, "Supplied method is not supported"),
            ParseError::MissingPath => write!(f, "Path was not provided"),
            ParseError::MalformedHeader => write!(f, "Header was missing ':'"),
            ParseError::BadContentLength => {
                write!(f, "Content length was unparseable or too long")
            }
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
    method: Method,
    path: String,
    headers: HashMap<String, String>,
}

#[derive(Debug)]
enum Method {
    Get,
    Post,
}

impl FromStr for Method {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GET" => Ok(Method::Get),
            "POST" => Ok(Method::Post),
            _ => Err(ParseError::UnknownMethod),
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    if let Err(e) = stream.set_read_timeout(Some(Duration::from_secs(1))) {
        eprintln!("Failed to set read timeout: {e}")
    }
    let mut buf_reader = BufReader::new(&stream);
    let request_iterator = buf_reader.by_ref().take(MAX_LINE).lines();

    let request = match parse_request(request_iterator) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to parse request: {e}");
            match e {
                ParseError::UnknownMethod => {
                    let (status_line, body) =
                        page_or_500("HTTP/1.1 405 METHOD NOT ALLOWED", "405.html");
                    write_response(&mut stream, status_line, body);
                }
                ParseError::Io(_) => {}
                _ => {
                    let (status_line, body) = page_or_500("HTTP/1.1 400 BAD REQUEST", "400.html");
                    write_response(&mut stream, status_line, body);
                }
            }
            return;
        }
    };

    let content_length = match get_content_length(&request.headers, MAX_BODY) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Invalid body length: {e}");
            let (status_line, body) = page_or_500("HTTP/1.1 400 BAD REQUEST", "400.html");
            write_response(&mut stream, status_line, body);
            return;
        }
    };

    let mut body = Vec::with_capacity(content_length as usize);

    let read_result = buf_reader
        .by_ref()
        .take(content_length)
        .read_to_end(&mut body);

    // A WouldBlock/TimedOut error here means the client sent less than it
    // promised, which is the client's fault: a 408 Request Timeout fits better
    // than a 500. Needs a 408.html first.
    if read_result.is_err() {
        let (status_line, body) = page_or_500("HTTP/1.1 500 Internal Server Error", "500.html");
        write_response(&mut stream, status_line, body);
        return;
    }

    let (status_line, body) = match (request.method, request.path.as_str()) {
        (Method::Get, "/") => page_or_500("HTTP/1.1 200 OK", "hello.html"),
        (Method::Get, "/sleep") => {
            thread::sleep(Duration::from_secs(5));
            page_or_500("HTTP/1.1 200 OK", "hello.html")
        }
        (Method::Get, _) => page_or_500("HTTP/1.1 404 NOT FOUND", "404.html"),
        (Method::Post, "/") => page_or_500("HTTP/1.1 200 OK", "hello.html"),
        (Method::Post, "/sleep") => page_or_500("HTTP/1.1 405 METHOD NOT ALLOWED", "405.html"),
        (Method::Post, "/echo") => ("HTTP/1.1 200 OK", body),
        (Method::Post, _) => page_or_500("HTTP/1.1 404 NOT FOUND", "404.html"),
    };

    write_response(&mut stream, status_line, body);
}

fn parse_request(
    mut request_iterator: impl Iterator<Item = io::Result<String>>,
) -> Result<Request, ParseError> {
    let request_line = request_iterator.next().ok_or(ParseError::MissingMethod)??;

    let mut request_line_iter = request_line.split_whitespace();
    let method = request_line_iter
        .next()
        .ok_or(ParseError::MissingMethod)?
        .parse::<Method>()?;
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

fn get_content_length(
    headers: &HashMap<String, String>,
    max_length: u64,
) -> Result<u64, ParseError> {
    let content_length = match headers
        .get("content-length")
        .map(|s| s.parse::<u64>())
        .transpose()
    {
        Ok(l) => l.unwrap_or(0),
        Err(_) => return Err(ParseError::BadContentLength),
    };

    if content_length <= max_length {
        Ok(content_length)
    } else {
        Err(ParseError::BadContentLength)
    }
}

fn page_or_500<'a>(status_line: &'a str, filename: &str) -> (&'a str, Vec<u8>) {
    match fs::read(filename) {
        Ok(v) => (status_line, v),
        Err(e) => {
            eprintln!("Unable to read {filename}: {e}");
            (
                "HTTP/1.1 500 Internal Server Error",
                "500 Internal Server Error".as_bytes().to_vec(),
            )
        }
    }
}

// Add Content-Type in future version to use request's `content-type` header (already in
// `request.headers`) or fall back to `application/octet-stream`. Either add a parameter or create a `Response` struct
fn write_response(stream: &mut TcpStream, status_line: &str, body: Vec<u8>) {
    let length = body.len();
    let header = format!("{status_line}\r\nContent-Length: {length}\r\n\r\n");
    if let Err(e) = stream.write_all(header.as_bytes()) {
        eprintln!("Failed to write headers {e}");
    }
    if let Err(e) = stream.write_all(&body) {
        eprintln!("Failed to write body {e}");
    }
}
