# web_server — Rust book final project

## What this is

The multithreaded web server from the final chapter of the Brown edition of
*The Rust Programming Language* (https://rust-book.cs.brown.edu/). Part of the
`crabs` repo, which holds one directory per chapter of that book. Code here is
worked through from the book, not original design.

This directory is the cargo crate itself. The package is named `hello` (the
book's name, kept so the book's code pastes in unchanged) and is both a binary
and a library:

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
- Leave the interesting decisions to the author. Scaffold structure and
  boilerplate; let them write the logic.
- Standard library only. No external crates unless the author asks — the book
  deliberately avoids them here.

## Current state

`ThreadPool` is still the book's compile-only stub: a unit struct whose `new`
ignores `size` and whose `execute` drops the closure without running it.
`cargo build` therefore warns about unused `size` and `f` — expected, not a bug
to fix.

`main.rs` carries the author's own TODO notes about adding error handling in
place of `.unwrap()` (see comments at the `request_line`, `read_to_string`, and
`write_all` calls).

## Testing

Unit tests live in `src/lib.rs` under `#[cfg(test)] mod tests`, which is
the conventional Rust location for them and can see private items. Run with:

```
cargo test
```

The `ThreadPool` stub is the chosen starting point for unit testing. Expect
tests written against the real contract (e.g. "`execute` actually runs the
closure") to **fail** until the implementation catches up — that is intentional,
and those failures are what drives the next chunk of implementation.

Closures given to `execute` are bound by `FnOnce() + Send + 'static`, so a test
cannot observe them by mutating a local variable. Use a channel or an
`Arc<AtomicUsize>` and a bounded wait (`recv_timeout`) so a broken pool fails
the test rather than hanging it.
