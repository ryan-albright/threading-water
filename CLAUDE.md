# web_server — Rust book final project

## What this is

The multithreaded web server from the final chapter of the Brown edition of
*The Rust Programming Language* (https://rust-book.cs.brown.edu/). Code here is
worked through from the book, then iterated on by the author beyond what the
book covers (error handling, panic resilience, tests).

This directory is the cargo crate itself. The package is named `server` and is
both a binary and a library (the binary imports the pool as `server::ThreadPool`):

- `src/main.rs` — binary. Binds `127.0.0.1:7878`, accepts connections,
  hands each one to the pool, and shuts down when `q` is typed on stdin.
  `handle_connection` routes `/` and `/sleep` (sleeps 5 s) to `hello.html` and
  anything else to `404.html`.
- `src/lib.rs` — library. `ThreadPool`, `ShutdownReport`, the part being
  built out.
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
`Worker` threads, and `execute` returns `Result<(), PoolFailure>`. Workers wrap
each job in `catch_unwind(AssertUnwindSafe(job))`, so a panicking job is logged
and the worker keeps running instead of dying.

Shutdown has two forms, both taking `&mut self` and both idempotent (they drain
`self.workers`, so a second call or the later `Drop` finds nothing to do):

- `shutdown() -> Vec<(usize, String)>` drops the job sender and joins every
  worker with no time limit. It returns the id and message of each worker that
  panicked. `Drop` is a thin wrapper around it that logs those.
- `shutdown_timeout(Duration) -> ShutdownReport` is the same drain bounded by
  one overall deadline. `ShutdownReport` has public fields `panicked_workers:
  Vec<(usize, String)>` and `timed_out_workers: Vec<usize>`.

How the deadline works: each `Worker` gets a clone of a `Sender<usize>` and
sends its id on a "done" channel when its thread closure ends, however it ends.
A private `DoneGuard` (id plus `Sender<usize>`) is created first in the closure
and sends the id from its `Drop`, so a normal exit (`recv()` on the job channel
returns `Err`) and an unwinding panic (the `.expect` on a poisoned receiver
mutex, outside `catch_unwind`) both report. The id is sent during unwinding,
but `join` waits out the last instructions, so the panic payload reaches
`panicked_workers`. The guard ignores a failed send: it only fails when the
pool has dropped `done_receiver`, e.g. an abandoned worker finishing late.
The pool keeps the `Receiver` as `done_receiver`.
`shutdown_timeout` calls `recv_timeout` with the time remaining until the
deadline and stops on either a timeout or a disconnect, so the workers it heard
from are safe to `join` without blocking. The rest are *abandoned*: their
`JoinHandle` is dropped, which detaches the thread (std cannot cancel a
thread), and their ids go in `timed_out_workers`. Jobs still queued when the
sender is dropped are still run first, so the deadline also bounds a backlog.
Call `shutdown_timeout` explicitly before the pool drops; `Drop` itself uses the
unbounded `shutdown`.

Construction is fallible: `ThreadPool::build(size) -> Result<ThreadPool,
PoolFailure>` replaced `new` (which panicked on size 0). `Worker::new` returns
`io::Result<Worker>` and spawns via `thread::Builder` with the name
`worker {id}`, so an OS spawn failure propagates with `?` instead of panicking.

`PoolFailure` variants are `InvalidSize` (size 0), `Creation(io::Error)` (a
worker thread could not be spawned), `NoWorkers` and `ShuttingDown`. It
implements `Display`, `std::error::Error` and `From<io::Error>` (which maps to
`Creation`). Convention used: `Display` describes only that layer, and the
underlying cause is exposed through `source()`, not repeated in the message.
Whoever reports the error walks the `source()` chain.

`main.rs` no longer uses `.unwrap()` on I/O: bind failure and pool-build failure
exit with code 1 (the build failure prints the error and then its `source()`
chain), bad requests and file/write errors are logged to stderr, and a missing
page yields a 500 response.

The binary shuts down gracefully. The listener is non-blocking and the accept
loop polls an `Arc<AtomicBool>` (`shutdown_requested`), sleeping 50 ms on
`WouldBlock`, because std's `accept()` has no timeout and cannot be woken from
another thread. A thread reading `io::stdin().lines()` sets the flag on a line
equal to `q`, on a read error, and on EOF. Note the EOF case: running with stdin
closed (`< /dev/null`, `nohup`, some background jobs) shuts the server down
immediately. After the loop, `main` calls `pool.shutdown_timeout(..)` with a
limit set in `main.rs`, prints each panicked worker and each abandoned worker to
stderr, and exits with code 1 if any worker timed out. `/sleep` takes 5 s, so a
limit shorter than that abandons an in-flight `/sleep` request; a longer one
lets it finish. A shutdown *route* was rejected on purpose: anyone who can
reach the port could stop the server, so the trigger is out of band (stdin).

Known loose ends (deliberate, not bugs to fix unprompted):

- `PoolFailure::NoWorkers` is effectively unreachable through the public API
  now that workers survive panics and `build` rejects size 0. Its meaning needs a
  design decision. `ShuttingDown` is live: `execute` returns it after a
  shutdown has taken the job sender.
- If `build` fails partway (e.g. worker 3 of 4 cannot spawn), the already
  spawned workers are not joined. The sender is dropped on the early return,
  so they exit on their own, but they are detached. Constructing the
  `ThreadPool` first and pushing workers into it would let `Drop` join them.
- Abandoned workers are not cancelled. They keep running until the process
  exits, which kills them mid-request.
- Only `q` on stdin triggers shutdown. SIGINT/SIGTERM (Ctrl-C) are not handled;
  that needs `unsafe` FFI (`signal`, a `static AtomicBool`) or a crate such as
  `ctrlc`, and is deferred until the author asks for it.
- `main.rs` has no automated tests. Check shutdown by hand: `cargo run`, then
  `curl localhost:7878/sleep &`, then type `q`.

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

Tests also cover the error plumbing: `build(0)` yields `InvalidSize`,
`io::Error` converts into `Creation` and `source()` returns it, and spawned
workers carry the expected thread name. Because a wrong thread name or a
`source()` that returns `None` still compiles, verify each such test by
temporarily breaking the code and confirming the test goes red. Use `matches!`
for `PoolFailure` assertions, since it is not `PartialEq` (`io::Error` isn't).

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

The `shutdown_timeout` tests use a *gate*: a job calls `gate_rx.recv()` so it is
stuck until the test sends on `gate_tx`. Keep `gate_tx` alive until after the
assertions (dropping it makes `recv()` return `Err` and the job finishes), and
release it last. Do not use `sleep` to hold a job: it makes the test depend on
timing. The tests are:

- `shutdown_timeout_records_stuck_worker`: one worker, one gated job; expects
  `timed_out_workers == [0]`.
- `shutdown_timeout_reports_nothing_when_jobs_finish`: quick jobs; both vectors
  are empty.
- `shutdown_timeout_separates_workers`: two workers, one gated job. The job
  sends `thread::current().name()` (an owned `String`, `"worker N"`) before it
  blocks, so the test learns which worker is stuck even though jobs cannot be
  sent to a specific worker, then expects `timed_out_workers == [that id]`. This
  one proves a healthy worker is not reported.

Use two different durations in such a test: a generous `recv_timeout` for "the
job started" and a short limit for the shutdown, since the stuck worker always
waits out the whole shutdown limit. Verified by mutation: dropping the push to
`timed_out_workers` fails the stuck and separates tests; treating every worker
as unfinished fails the happy-path and separates tests. If `shutdown_timeout`
regresses to a loop whose error arms do not `break`, these tests hang instead
of failing; running the call on a helper thread and waiting with `recv_timeout`
would turn that into a failure (not done yet).
