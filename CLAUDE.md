# web_server — Rust book final project

The multithreaded web server from the final chapter of the Brown edition of
*The Rust Programming Language* (https://rust-book.cs.brown.edu/), iterated on
by the author beyond the book. See `README.md` for what it does, how to run it
and the file layout. Planned work is tracked outside the repo; ask the author.
This file holds what Claude needs that README does not: how to help, design decisions, and conventions.

Package `server` is both a binary and a library (`src/main.rs` imports
`server::ThreadPool`). The HTML pages are read by relative path, so the binary
must run from the crate root.

## Who is working on it

The author is learning Rust and has **no prior Rust experience**. This is their
first project past the book's small exercises.

- Teach, don't just deliver. Explain *why* a construct is the Rust way to do
  something (ownership, trait bounds, `Send`/`'static`, interior mutability),
  not only what to type.
- Prefer the book's own progression. Don't skip ahead to constructs the current
  chapter section hasn't reached.
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

## Design decisions worth knowing

These are not obvious from reading the code once; the code is the source of
truth for everything else.

- **Shutdown.** `shutdown()` and `shutdown_timeout(Duration)` both take `&mut
  self` and are idempotent: they drain `self.workers`, so a second call or the
  later `Drop` finds nothing to do. `Drop` uses the unbounded `shutdown`, so call
  `shutdown_timeout` explicitly before the pool drops.
- **How the deadline works.** Each `Worker` holds a `Sender<usize>` clone. A
  private `DoneGuard`, created first in the thread closure, sends the worker id
  from its `Drop`, so a normal exit and an unwinding panic (the `.expect` on a
  poisoned receiver mutex, outside `catch_unwind`) both report. `join` still
  waits out the unwind, so the panic payload reaches `panicked_workers`. The
  guard ignores a failed send, which only happens when the pool has dropped
  `done_receiver` (an abandoned worker finishing late). `shutdown_timeout` calls
  `recv_timeout` with the time left and stops on a timeout or a disconnect; the
  workers it heard from are safe to `join`, the rest are *abandoned* (handle
  dropped, thread detached, since std cannot cancel a thread). Queued jobs still
  run first, so the deadline also bounds a backlog.
- **Partial `build` failure.** `build` constructs the `ThreadPool` first (empty
  `workers`) and pushes each `Worker` into it, so if worker 3 of 4 cannot spawn,
  the `?` drops the pool and `Drop` joins the workers that did start. This is
  untested because spawn failure is hard to trigger on demand.
- **Error convention.** `PoolFailure::Display` describes only its own layer; the
  cause is exposed through `source()` and whoever reports the error walks the
  chain. `From<io::Error>` maps to `Creation`.
- **`execute` and a failed `send`.** `execute` returns `ShuttingDown` once a
  shutdown has taken the job sender. A failed `send` is not an error variant but
  an `.expect`: it needs every worker to have dropped its receiver, and workers
  only exit when the sender is dropped (which `execute` reports first), since
  jobs run inside `catch_unwind` and the mutex guard is released before the job
  runs, so nothing poisons it. A panic here means that invariant broke. The old
  `NoWorkers` variant was removed for being unreachable.
- **Accept loop.** The listener is non-blocking and polls a `static
  SHUTDOWN_REQUESTED: AtomicBool`, sleeping 50 ms on `WouldBlock`, because std's
  `accept()` has no timeout and cannot be woken from another thread. The stdin
  thread sets the flag on `q`, a read error, and EOF.
- **Signals.** `main` registers `handler` for SIGINT and SIGTERM through a
  hand-declared `unsafe extern "C" fn signal`, before spawning any thread. The
  flag is a `static` because a handler cannot capture; the handler does one
  atomic store, the only async-signal-safe work it may do. The signal numbers
  (2 and 15) are the Linux values. `signal` returns `SIG_ERR` (`usize::MAX`)
  on failure; `main` checks for it, reports `io::Error::last_os_error()` and
  exits with code 1.
- **No shutdown route, on purpose.** Anyone who can reach the port could stop the
  server, so the trigger is out of band (stdin).
- **Shutdown limit vs `/sleep`.** `/sleep` takes 5 s, so a shutdown limit shorter
  than that abandons an in-flight `/sleep` request and `main` exits with code 1.
  The limit is set in `main.rs`.
- **Port and the ready line.** `SERVER_PORT` is an optional `u16` (default 7878;
  `0` means any free port). A bad or non-Unicode value exits with code 1. After
  binding, `main` prints `Listening on <addr>` to stdout, using the address the
  listener reports from `local_addr`. `tests/server.rs` parses that line to learn
  the port, so keep the wording stable.

## Known loose ends

Deliberate, not bugs to fix unprompted:

- Abandoned workers are not cancelled. They keep running until the process
  exits, which kills them mid-request.
- `signal` is the old BSD/glibc interface. `sigaction` would be the more
  portable choice but needs a `struct` layout declared by hand.
- `println!` panics if stdout is a closed pipe (`main` and the workers print).
  `cargo run | head -1`, or a test that drops the child's stdout reader, kills
  the server this way.
- `tests/server.rs` covers `q` on stdin but not SIGINT or SIGTERM. Check those by
  hand: `cargo run`, then `curl localhost:7878/sleep &`, then press Ctrl-C.

## Testing

Unit tests live in `src/lib.rs` under `#[cfg(test)] mod tests`, which is
the conventional Rust location for them and can see private items. Run with
`cargo test`.

Tests are written against the real contract and drove the implementation:
write the test, watch it fail for the right reason, then fix the code. When a
fix changes behaviour, check whether an older test now passes trivially or
describes the old behaviour (e.g. `drop_survives_a_panicked_worker` no longer
exercises the `join()` error path now that jobs can't kill a worker).

Because a wrong thread name or a `source()` that returns `None` still compiles,
verify such tests by temporarily breaking the code and confirming the test goes
red. Use `matches!` for `PoolFailure` assertions, since it is not `PartialEq`
(`io::Error` isn't).

Prefer deterministic synchronisation over sleeps. For "all workers have reached
a known point", have each job send on a channel *before* it panics, then
`recv_timeout` that many signals. A passing assertion on a timeout
(`recv_timeout(..).is_err()`) can never catch a missing event.

Closures given to `execute` are bound by `FnOnce() + Send + 'static`, so a test
cannot observe them by mutating a local variable. Use a channel or an
`Arc<AtomicUsize>` and a bounded wait (`recv_timeout`) so a broken pool fails
the test rather than hanging it. Tests that drop a pool join its workers, so a
worker loop that never exits (e.g. a lost `break` on `recv()` returning `Err`)
makes them hang rather than fail.

The `shutdown_timeout` tests use a *gate*: a job calls `gate_rx.recv()` so it is
stuck until the test sends on `gate_tx`. Keep `gate_tx` alive until after the
assertions (dropping it makes `recv()` return `Err` and the job finishes), and
release it last. Do not use `sleep` to hold a job. To learn which worker is
stuck (jobs cannot be sent to a specific worker), the job sends
`thread::current().name()` before it blocks. Use two durations: a generous
`recv_timeout` for "the job started" and a short limit for the shutdown, since
the stuck worker always waits out the whole limit.

These tests were checked by mutation: dropping the push to `timed_out_workers`
fails the stuck and separates tests; treating every worker as unfinished fails
the happy-path and separates tests. If `shutdown_timeout` regresses to a loop
whose error arms do not `break`, they hang instead of failing; running the call
on a helper thread and waiting with `recv_timeout` would fix that (not done yet).

### Integration tests

`tests/server.rs` runs the real binary (`CARGO_BIN_EXE_server`) from the crate
root with `SERVER_PORT=0`, so parallel tests cannot share a port. (Picking a
free port, closing it and handing it to the child is racy: another test can
take it in between, and a connect probe then succeeds against the wrong server.)

- `ServerProcess` owns the `Child` and its three pipes, and kills and waits in
  `Drop`. It is built *before* waiting for the ready line, so a panic during
  startup still cleans up the child.
- `start_server` reads the `Listening on` line on a helper thread and waits with
  `recv_timeout`. A silent server fails with a message instead of hanging, and a
  dead reader thread reports `Disconnected`. Checked by mutation: removing the
  `println!` in `main.rs` fails after the limit and leaves no process behind.
- Never drop the child's stdout reader while the server runs (see the `println!`
  loose end). The `BufReader` sits in an `Option` so the helper thread can take it
  and send it back; read it with `as_mut()`, not `take()`.
- Keep the child's stdin open for the whole test: EOF on it triggers shutdown.
  Send `"q\n"` with the newline, because the server reads whole lines.
- `wait_for_exit` polls `try_wait` against a deadline. Read stdout and stderr only
  after the process has exited, so `read_to_string` reaches EOF instead of
  blocking. Diagnostics such as "timed out; job abandoned" go to stderr.
- The in-flight `/sleep` test sends `/sleep` and keeps the stream open, then does
  a `GET /` and waits for its reply. The accept loop takes connections in order,
  so that reply proves `/sleep` was accepted first. Then it sends `q` and expects
  exit code 1.
