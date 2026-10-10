use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, mpsc},
    thread,
};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug)]
pub enum PoolFailure {
    Creation,
    NoWorkers,
    ShuttingDown,
}

impl fmt::Display for PoolFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolFailure::Creation => write!(f, "Pool instantiation failed"),
            PoolFailure::NoWorkers => write!(f, "No workers in pool"),
            PoolFailure::ShuttingDown => write!(f, "Pool shutting down"),
        }
    }
}

impl ThreadPool {
    /// Create a new ThreadPool
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    ///The 'new' function will panic if the size is zero.
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    // pub fn build(size: usize) -> Result<ThreadPool, PoolCreationError> {
    // }

    pub fn execute<F>(&self, f: F) -> Result<(), PoolFailure>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        match self
            .sender
            .as_ref()
            .expect("Sender should be accessible")
            .send(job)
        {
            Ok(_s) => Ok(()),
            Err(_e) => Err(PoolFailure::NoWorkers),
        }
    }
}

// FUTURE (pick up alongside worker respawning):
//
// Today the outcome of each worker's `join()` is only printed with `eprintln!`,
// so nothing (a test, a caller, a supervisor) can observe *which* workers
// panicked. `Drop` can't return a value, so the fix is to move the shutdown
// logic out of `Drop` into a method whose result is observable:
//
//   pub fn shutdown(&mut self) -> Vec<usize>   // ids of workers that panicked
//
// - `shutdown` takes the sender (closing the channel), drains and joins every
//   worker, and collects the ids whose `join()` returned `Err`. It must be
//   idempotent: a second call finds no sender and no workers, returns `vec![]`.
// - `Drop` becomes a thin wrapper that calls `self.shutdown()` and logs the
//   result, so existing behaviour is unchanged.
// - Tests can then call `shutdown()` directly and assert on the returned ids
//   (e.g. `[0]`) instead of using an unwind-guard plus a sleep to prove the
//   dead worker was joined.
// - Consider a richer return type (`Vec<(usize, Box<dyn Any + Send>)>`, or a
//   small struct) if the panic payload is wanted. Decide once respawning shows
//   what a supervisor actually needs to know.
impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());

        for worker in self.workers.drain(..) {
            let id = worker.id;
            println!("Shutting down worker {id}");

            if let Err(e) = worker.thread.join() {
                eprintln!("Worker {id} panicked {e:?}");
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

impl Worker {
    // below method will panic if there are not enough system resources
    // and will cause the whole server to panic. For prod, use
    // std::thread::Builder and its spawn method returning Result instead
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || {
            loop {
                let message = receiver
                    .lock()
                    .expect("Receiver mutex should not be poisoned")
                    .recv();

                match message {
                    Ok(job) => {
                        println!("Worker {id} got a job; executing.");

                        if catch_unwind(AssertUnwindSafe(job)).is_err() {
                            eprintln!("Worker {id} survived panic.");
                        };
                    }
                    Err(_) => {
                        println!("Worker {id} disconnected; shutting down.");
                        break;
                    }
                }
            }
        });

        Worker { id, thread }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    #[test]
    #[should_panic(expected = "assertion failed: size > 0")]
    fn panics_on_zero() {
        let _ = ThreadPool::new(0);
    }

    #[test]
    fn execute_runs_the_closure() {
        let pool = ThreadPool::new(1);
        let (tx, rx) = mpsc::channel();

        pool.execute(move || tx.send(()).unwrap()).unwrap();
        let status = rx.recv_timeout(Duration::from_secs(1));
        assert!(status.is_ok());
    }

    #[test]
    fn drop_survives_a_panicked_worker() {
        let pool = ThreadPool::new(1);
        pool.execute(|| panic!("test: worker job panicking on purpose"))
            .unwrap();

        let status = catch_unwind(AssertUnwindSafe(|| drop(pool)));
        assert!(status.is_ok())
    }

    #[test]
    fn pool_runs_jobs_after_one_worker_panics() {
        let pool = ThreadPool::new(2);
        pool.execute(|| panic!("test: worker job panicking on purpose"))
            .unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        const EXPECTED_VAL: usize = 5;
        for _ in 0..EXPECTED_VAL {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }

        let status = catch_unwind(AssertUnwindSafe(|| drop(pool)));
        assert!(status.is_ok());

        let val = counter.load(Ordering::Relaxed);
        assert_eq!(val, EXPECTED_VAL);
    }

    #[test]
    fn pool_runs_jobs_after_all_workers_panic() {
        const WORKER_COUNT: usize = 3;
        let pool = ThreadPool::new(WORKER_COUNT);
        let (tx, rx) = mpsc::channel();

        for _ in 0..WORKER_COUNT {
            let tx = tx.clone();
            pool.execute(move || {
                tx.send("test job").expect("Sender should send job");
                panic!("test: worker job panicking on purpose");
            })
            .unwrap();
            let status = rx.recv_timeout(Duration::from_secs(1));
            assert!(status.is_ok());
        }

        let (tx, rx) = mpsc::channel();
        pool.execute(move || tx.send(()).unwrap())
            .expect("Pool should accept jobs");
        let status = rx.recv_timeout(Duration::from_secs(1));
        assert!(status.is_ok());
    }
}
