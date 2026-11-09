# web_server

A multithreaded HTTP server written in Rust using only the standard library.
It started as the final project of the Brown edition of
[*The Rust Programming Language*](https://rust-book.cs.brown.edu/) and has been
extended beyond the book: fallible construction, panic resilience, graceful
shutdown with a deadline, and a unit-test suite.

It is also a learning project, my first in Rust past the book's small
exercises, so the code favours clarity over cleverness and uses no external
crates.

## Layout

The crate is named `server` and is both a library and a binary.

| Path | Role |
| --- | --- |
| `src/main.rs` | Binary. Accepts connections and handles shutdown. |
| `src/lib.rs` | Library root: `mod` declarations and re-exports only. |
| `src/pool.rs` | `ThreadPool`, `ShutdownReport`, shutdown. |
| `src/worker.rs` | `Worker`, `DoneGuard`, `err_to_string`. |
| `src/error.rs` | `PoolFailure`. |
| `src/http.rs` | Request parsing, routing and responses (`handle_connection`). |
| `tests/pool.rs` | Pool contract tests. |
| `tests/server.rs` | Integration tests that run the binary. |
| `pages/*.html` | Response pages (`hello`, `400`, `404`, `405`, `500`). |

The HTML files are read by relative path, so **run the server from the crate
root**.

## Running it

```
cargo run
```

The server listens on `127.0.0.1:7878`.

| Request | Result |
| --- | --- |
| `GET /` | `pages/hello.html` |
| `GET /sleep` | Sleeps 5 seconds, then `pages/hello.html` (simulates a slow request) |
| anything else | `pages/404.html` |
| page file missing on disk | `500` response |

To stop it, type `q` and press Enter in the terminal where it is running.
End-of-file on stdin also stops it, so running with stdin closed
(`< /dev/null`, `nohup`, some background jobs) shuts it down immediately.

Run the tests with `cargo test`.

## How it works

- **Thread pool.** `ThreadPool` owns `Worker` threads that pull boxed closures
  from a shared `mpsc` channel (`Arc<Mutex<mpsc::Receiver<Job>>>`). Each job
  runs inside `catch_unwind`, so a panicking job doesn't shrink the pool.
- **Errors, not panics.** `ThreadPool::build` and `execute` return
  `Result<_, PoolFailure>`, and the binary has no `.unwrap()` on I/O.
- **Graceful shutdown.** The accept loop is non-blocking and polls a flag set by
  a stdin-reading thread. `shutdown_timeout(Duration)` then drains the pool under
  one overall deadline and reports panicked and abandoned workers.

## More

- [`CLAUDE.md`](CLAUDE.md): design notes, known loose ends and working
  conventions (written for Claude Code, but readable as a design record).
