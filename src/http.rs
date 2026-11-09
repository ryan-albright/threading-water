use std::{
    collections::HashMap,
    fmt, fs,
    io::{self, BufReader, prelude::*},
    net::TcpStream,
    str::FromStr,
    thread,
    time::Duration,
};

const MAX_LINE: u64 = 8 * 1024;
const MAX_BODY: u64 = 1024 * 1024;

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

pub fn handle_connection(mut stream: TcpStream) {
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
                        page_or_500("HTTP/1.1 405 METHOD NOT ALLOWED", "pages/405.html");
                    write_response(&mut stream, status_line, body);
                }
                ParseError::Io(_) => {}
                _ => {
                    let (status_line, body) =
                        page_or_500("HTTP/1.1 400 BAD REQUEST", "pages/400.html");
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
            let (status_line, body) = page_or_500("HTTP/1.1 400 BAD REQUEST", "pages/400.html");
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
        let (status_line, body) =
            page_or_500("HTTP/1.1 500 Internal Server Error", "pages/500.html");
        write_response(&mut stream, status_line, body);
        return;
    }

    let (status_line, body) = match (request.method, request.path.as_str()) {
        (Method::Get, "/") => page_or_500("HTTP/1.1 200 OK", "pages/hello.html"),
        (Method::Get, "/sleep") => {
            thread::sleep(Duration::from_secs(5));
            page_or_500("HTTP/1.1 200 OK", "pages/hello.html")
        }
        (Method::Get, _) => page_or_500("HTTP/1.1 404 NOT FOUND", "pages/404.html"),
        (Method::Post, "/") => page_or_500("HTTP/1.1 200 OK", "pages/hello.html"),
        (Method::Post, "/sleep") => {
            page_or_500("HTTP/1.1 405 METHOD NOT ALLOWED", "pages/405.html")
        }
        (Method::Post, "/echo") => ("HTTP/1.1 200 OK", body),
        (Method::Post, _) => page_or_500("HTTP/1.1 404 NOT FOUND", "pages/404.html"),
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
