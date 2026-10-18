# web_server

A multithreaded HTTP server written in Rust using only the standard library.
It started as the final project of the Brown edition of
[*The Rust Programming Language*](https://rust-book.cs.brown.edu/) and has been
extended well beyond the book: fallible construction, panic resilience,
graceful shutdown with a deadline, and a unit-test suite.

It is also a learning project. It is my first Rust project past the book's
small exercises, so the code favours clarity over cleverness, and there are
no external crates.

## Layout

The crate is named `server` and is both a library and a binary.

| Path | Role |
| --- | --- |
| `src/main.rs` | Binary. Accepts connections, routes requests, handles shutdown. |
| `src/lib.rs` | Library. `ThreadPool`, `Worker`, `PoolFailure`, `ShutdownReport`, plus unit tests. |
| `hello.html` | Response for `/` and `/sleep`. |
| `404.html` | Response for any other path. |

The HTML files are read by relative path, so **run the server from the crate
root**.

## Running it

```
cargo run
```

The server listens on `127.0.0.1:7878`.

| Request | Result |
| --- | --- |
| `GET /` | `hello.html` |
| `GET /sleep` | Sleeps 5 seconds, then `hello.html` (simulates a slow request) |
| anything else | `404.html` |
| page file missing on disk | `500` response |

To stop it, type `q` and press Enter in the terminal where it is running.

> **Note:** end-of-file on stdin also triggers shutdown, so running with stdin
> closed (`< /dev/null`, `nohup`, some background jobs) stops the server
> immediately.

Run the tests with:

```
cargo test
```

## How it works

### The thread pool

`ThreadPool` owns a set of `Worker` threads. Jobs are boxed closures sent down
an `mpsc` channel. The receiving end is shared between workers as
`Arc<Mutex<mpsc::Receiver<Job>>>`, so each job is picked up by exactly one
worker.

- **Fallible construction.** `ThreadPool::build(size)` returns
  `Result<ThreadPool, PoolFailure>`. Size 0 is `InvalidSize`, and a worker
  thread that the OS can't spawn is `Creation`. Workers are spawned through
  `thread::Builder` and named `worker {id}`.
- **Fallible `execute`.** It returns `Result<(), PoolFailure>`, for example
  `ShuttingDown` once the pool has stopped accepting work.
- **Panic resilience.** Each job runs inside
  `catch_unwind(AssertUnwindSafe(job))`. A panicking job is logged and the
  worker carries on, so one bad request can't shrink the pool.
- **Error type.** `PoolFailure` implements `Display`, `std::error::Error` and
  `From<io::Error>`. `Display` describes only its own layer. The underlying
  cause is exposed through `source()`, and whoever reports the error walks that
  chain.

### Shutdown

There are two forms. Both take `&mut self` and are idempotent.

- `shutdown()` drops the job sender and joins every worker with no time limit.
  It returns the id and message of each worker that panicked. `Drop` is a thin
  wrapper around it.
- `shutdown_timeout(Duration)` does the same drain under one overall deadline
  and returns a `ShutdownReport { panicked_workers, timed_out_workers }`.

The deadline works through a "done" channel. Each worker sends its id as its
loop ends, and `shutdown_timeout` calls `recv_timeout` with the time remaining.
Workers it heard from are safe to `join`. The rest are *abandoned*: their
`JoinHandle` is dropped, which detaches the thread, because std can't cancel a
thread. Jobs already queued still run first, so the deadline also bounds a
backlog.

### The binary

`main` binds the listener, builds the pool, and hands each connection to it.

- Bind and pool-build failures print the error and its `source()` chain, then
  exit with code 1. There are no `.unwrap()` calls on I/O.
- The listener is **non-blocking**. The accept loop polls an
  `Arc<AtomicBool>` and sleeps 50 ms on `WouldBlock`, because std's `accept()`
  has no timeout and can't be woken from another thread.
- A separate thread reads `io::stdin().lines()` and sets the flag when it sees
  `q`, a read error, or EOF.
- After the loop, `main` calls `pool.shutdown_timeout(..)` (currently 2 s),
  prints each panicked and abandoned worker to stderr, and exits with code 1 if
  any worker timed out. Since `/sleep` takes 5 s, the current limit abandons an
  in-flight `/sleep` request. A longer limit would let it finish.
- A shutdown *route* was rejected on purpose. Anyone who can reach the port
  could stop the server, so the trigger stays out of band.

## Testing approach

Unit tests live in `src/lib.rs` under `#[cfg(test)] mod tests`. They were
written test-first against the intended behaviour. Principles in use:

- **Deterministic synchronisation over sleeps.** Jobs signal over channels, and
  tests wait with `recv_timeout`, so a broken pool fails a test instead of
  hanging it.
- **Gates for stuck workers.** The `shutdown_timeout` tests block a job on a
  channel the test controls, which avoids timing assumptions.
- **Mutation checks.** Tests for things that still compile when wrong (thread
  names, `source()`) were verified by temporarily breaking the code and
  watching them go red.

Coverage includes `build(0)`, `io::Error` to `Creation` conversion, worker
names, panic survival, and three `shutdown_timeout` scenarios (stuck worker,
all jobs finish, a stuck worker kept separate from a healthy one).

`main.rs` has no automated tests. To check shutdown by hand:

```
cargo run
curl localhost:7878/sleep &
# then type q in the server terminal
```

## Known loose ends

These are deliberate and documented rather than overlooked:

- `PoolFailure::NoWorkers` is effectively unreachable now that workers survive
  panics and `build` rejects size 0. It needs a design decision.
- If `build` fails partway (say worker 3 of 4 can't spawn), the workers that did
  start are detached rather than joined. Constructing the pool first and
  pushing workers into it would let `Drop` join them.
- A worker reports its id only on the normal exit path. If it dies another way
  (the `.expect` on a poisoned receiver mutex), `shutdown_timeout` reports it as
  timed out and its panic payload is lost. A guard struct whose `Drop` sends the
  id would cover both exits.
- Abandoned workers keep running until the process exits, which kills them
  mid-request.
- Only `q` on stdin triggers shutdown. Ctrl-C (SIGINT/SIGTERM) is not handled.

## Future directions

Roughly in the order I'm interested in them:

1. **Close the loose ends above.** The worker guard struct and building the pool
   before spawning workers are the most instructive, and both are good
   exercises in `Drop` and RAII.
2. **Signal handling.** Graceful shutdown on Ctrl-C. Plain std needs `unsafe`
   FFI and a `static AtomicBool`. The `ctrlc` crate is the usual shortcut, but
   I'd only reach for it deliberately, since the project avoids dependencies.
3. **Settle `NoWorkers`.** Decide whether it should be removed or given a real
   meaning, such as a pool that has lost every worker.
4. **Tests for the binary.** Integration tests under `tests/` that start the
   server and make real requests, and a helper-thread wrapper so a regressed
   `shutdown_timeout` fails a test instead of hanging it.
5. **Parse headers and handle `Content-Length`.** This turns the request-line
   router into a real HTTP server: read the headers, then read exactly the
   declared body length.
6. **Read and write timeouts on the `TcpStream`.** A slow or stalled client
   currently ties up a worker indefinitely. Timeouts fix that for almost no
   code.
7. **Restructure the repo into modules** inside the single `server` package, with
   no new crates (there is no second consumer of `ThreadPool` yet; book
   ch. 14.3 covers workspaces if that changes). Target layout:
   - `src/lib.rs`: only `mod` declarations and `pub use` re-exports
   - `src/error.rs`: `PoolFailure` and its `Display`/`Error`/`From` impls
   - `src/pool.rs`: `ThreadPool`, `Job`, `Drop`, shutdown
   - `src/worker.rs`: `Worker`, `err_to_string`
   - `src/main.rs`: wiring only
   - a separate module for the HTTP handler

   Lowest risk first, with `cargo test` green after every step:
   1. Extract `error.rs`, re-exporting `PoolFailure` so `server::PoolFailure`
      and `main.rs` are untouched.
   2. Extract `worker.rs`. `Worker` becomes `pub(crate)`: visible to `pool.rs`
      but not part of the public API.
   3. Extract `pool.rs`, leaving `lib.rs` as declarations and re-exports.
   4. Move `handle_connection` into its own module. Stretch: make it generic
      over `Read + Write` instead of `TcpStream`, so routing is testable
      without a socket.
   5. Move contract tests (`build(0)` is `InvalidSize`, jobs run, drop joins,
      shutdown reports panicked ids) to `tests/pool.rs`. Keep tests that need
      private access (thread names, `err_to_string`) in `#[cfg(test)] mod tests`
      beside their code.
   6. Move `hello.html` and `404.html` into `pages/` and update the paths in
      `main.rs`.

   Expect E0603 "is private" errors after each move. Fix them with the
   narrowest visibility, usually `pub(crate)`. Housekeeping: update the
   "Current state" notes in `CLAUDE.md` and delete or gitignore `a.txt`. This
   comes before item 8, because the property tests live in `tests/`.
8. **Property-based testing with only the standard library.** The aim is to
   practise PBT on a concurrent system, and maybe port to `proptest` afterwards
   to compare. Hand-written pieces: a seeded xorshift64 `Rng` (non-zero seed),
   generators (pool size `0..=8`, a `Vec` of ops: Add / Panic / Slow), and a
   runner that loops N cases with a per-case seed and prints the seed on
   failure. Properties, easy to hard:
   1. `build(size)` is `Ok` iff `size > 0`, and worker names are `worker {id}`.
   2. Exactly-once: every non-panicking job runs once (compare against a
      sequential model).
   3. Panics don't cost throughput: all Add jobs finish for any Add/Panic mix.
   4. Drop drains: after `drop(pool)`, every job submitted before it has
      finished.
   5. Concurrency bound (safety): an `AtomicUsize` high-water mark never
      exceeds `size`.
   6. Concurrency reached (liveness): `size` jobs blocked on a `Barrier` all
      arrive. Pair this with 5, or 5 passes trivially.
   7. With `size == 1`, execution order equals submission order, which is the
      only valid ordering assertion.

   Rules: assert invariants, not ordering; use `recv_timeout` everywhere so a
   broken pool fails instead of hanging; run many cases per test; mutation-check
   each property (remove a `break` or the `catch_unwind` and confirm it goes
   red). Shrinking covers inputs, not thread schedules, so failures can be
   flaky. Systematic schedule exploration (`loom`, `shuttle`) needs crates and
   is for later.

Configuration (pool size, port, shutdown limit from arguments or environment)
is a possible follow-up but isn't planned yet.
