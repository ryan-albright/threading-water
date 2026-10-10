# web_server — Rust book final project

## What this is

The multithreaded web server from the final chapter of the Brown edition of
*The Rust Programming Language* (https://rust-book.cs.brown.edu/). Code here is
worked through from the book, then iterated on by the author beyond what the
book covers (error handling, panic resilience, tests).

This directory is the cargo crate itself. The package is named `server` and is
both a binary and a library (the binary imports the pool as `server::ThreadPool`):

- `src/main.rs` — binary. Binds `127.0.0.1:7878`, accepts connections,
  hands each one to the pool. `handle_connection` routes `/` and `/sleep` to
  `hello.html` and anything else to `404.html`.
- `src/lib.rs` — library. `ThreadPool`, the part being built out.
- `hello.html`, `404.html` — served responses. Read by relative path, so the
  binary must be run from this directory (the crate root).

## Who is working on it

The author is learning Rust and has **no prior Rust experience**. This is their
first project past the book's small exercises.

Implications for how to help here:

- Teach, don't just deliver. Explain *why* a construct is the Rust way to do
  something (ownership, trait bounds, `Send`/`'static`, interior mutability),
  not only what to type.
- Prefer the book's own progression. Don't skip ahead to the finished
  `ThreadPool` with `Arc<Mutex<mpsc::Receiver<Job>>>` if the current chapter
  section hasn't reached it yet.
- **The author writes the code.** Do not write implementation or test code
  for them, even when asked to "fix" or "add" something, unless they
  explicitly ask you to write it. Instead: review their code, explain the
  concept and point to book chapters or std docs, describe the shape of a
  solution in prose, and help diagnose compiler errors. Small illustrative
  fragments to explain a concept are fine; a ready-to-paste solution is not.
  If you add a marker such as a `TODO(human)` comment, that is the only edit
  to their logic you make. Doc edits (like this file) and running
  `cargo test`/`fmt`/`clippy` are fine.
- Standard library only. No external crates unless the author asks — the book
  deliberately avoids them here.

## Current state

`ThreadPool` is fully implemented: `Arc<Mutex<mpsc::Receiver<Job>>>` shared by
`Worker` threads, graceful shutdown in `Drop` (drop the sender, then join every
worker), and `execute` returns `Result<(), PoolFailure>`. Workers wrap each job
in `catch_unwind(AssertUnwindSafe(job))`, so a panicking job is logged and the
worker keeps running instead of dying.

`main.rs` no longer uses `.unwrap()` on I/O: bind failure exits with code 1, bad
requests and file/write errors are logged to stderr, and a missing page yields a
500 response.

Known loose ends (deliberate, not bugs to fix unprompted):

- `PoolFailure::NoWorkers` and `ShuttingDown`: `NoWorkers` is effectively
  unreachable through the public API now that workers survive panics, and
  `ShuttingDown` is unused. Their meaning needs a design decision.
- `Worker::new` uses `thread::spawn`, which panics if the OS can't create a
  thread. `ThreadPool::build` (returning `Result`) is a commented-out stub.
- A `FUTURE` comment above `impl Drop for ThreadPool` sketches a
  `shutdown(&mut self) -> Vec<usize>` method that returns the ids of workers
  that panicked, with `Drop` as a thin wrapper. Its "pick up alongside worker
  respawning" wording predates the `catch_unwind` choice; respawning is not
  planned.
- The server never exits its accept loop on its own, so `Drop` for the pool is
  not reached when running the binary (no Ctrl-C/shutdown handling yet).

## Testing

Unit tests live in `src/lib.rs` under `#[cfg(test)] mod tests`, which is
the conventional Rust location for them and can see private items. Run with:

```
cargo test
```

Tests are written against the real contract and drove the implementation:
write the test, watch it fail for the right reason, then fix the code. When a
fix changes behaviour, check whether an older test now passes trivially or
describes the old behaviour (e.g. `drop_survives_a_panicked_worker` no longer
exercises the `join()` error path now that jobs can't kill a worker).

Prefer deterministic synchronisation over sleeps. For "all workers have reached
a known point", have each job send on a channel *before* it panics, then
`recv_timeout` that many signals. A passing assertion on a timeout
(`recv_timeout(..).is_err()`) can never catch a missing event.

Closures given to `execute` are bound by `FnOnce() + Send + 'static`, so a test
cannot observe them by mutating a local variable. Use a channel or an
`Arc<AtomicUsize>` and a bounded wait (`recv_timeout`) so a broken pool fails
the test rather than hanging it.

Tests that drop a pool join its workers, so a worker loop that never exits
(e.g. a lost `break` on `recv()` returning `Err`) makes them hang rather than
fail.
