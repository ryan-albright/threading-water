use std::{
    any::Any,
    fmt, io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::{Duration, Instant},
};

pub struct ThreadPool {
    workers: Vec<Worker>,
    job_sender: Option<mpsc::Sender<Job>>,
    done_receiver: mpsc::Receiver<usize>,
}

pub struct ShutdownReport {
    pub panicked_workers: Vec<(usize, String)>,
    pub timed_out_workers: Vec<usize>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug)]
pub enum PoolFailure {
    InvalidSize,
    Creation(io::Error),
    NoWorkers,
    ShuttingDown,
}

impl fmt::Display for PoolFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolFailure::InvalidSize => write!(f, "Pool size cannot be zero"),
            PoolFailure::Creation(_) => write!(f, "Pool instantiation failed"),
            PoolFailure::NoWorkers => write!(f, "No workers in pool"),
            PoolFailure::ShuttingDown => write!(f, "Pool shutting down"),
        }
    }
}

impl std::error::Error for PoolFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PoolFailure::Creation(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for PoolFailure {
    fn from(value: io::Error) -> Self {
        PoolFailure::Creation(value)
    }
}

impl ThreadPool {
    /// Create a new ThreadPool
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Errors
    ///
    /// The 'build' function will return InvalidSize error if the size is zero.
    pub fn build(size: usize) -> Result<ThreadPool, PoolFailure> {
        if size == 0 {
            Err(PoolFailure::InvalidSize)
        } else {
            let (tx, rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let rx = Arc::new(Mutex::new(rx));
            let workers = Vec::with_capacity(size);
            let mut pool = ThreadPool {
                workers,
                job_sender: Some(tx),
                done_receiver: done_rx,
            };

            for id in 0..size {
                pool.workers
                    .push(Worker::new(id, Arc::clone(&rx), done_tx.clone())?);
            }

            Ok(pool)
        }
    }

    pub fn execute<F>(&self, f: F) -> Result<(), PoolFailure>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        match self.job_sender.as_ref() {
            Some(s) => s.send(job).map_err(|_| PoolFailure::NoWorkers),
            None => Err(PoolFailure::ShuttingDown),
        }
    }

    pub fn shutdown(&mut self) -> Vec<(usize, String)> {
        drop(self.job_sender.take());
        let mut panicked_workers: Vec<(usize, String)> = Vec::new();

        for worker in self.workers.drain(..) {
            let id = worker.id;

            if let Err(e) = worker.thread.join() {
                let msg = err_to_string(e.as_ref());
                panicked_workers.push((id, msg));
            }
        }
        panicked_workers
    }

    pub fn shutdown_timeout(&mut self, time_limit: Duration) -> ShutdownReport {
        drop(self.job_sender.take());

        let mut finished_workers = Vec::new();
        let deadline = Instant::now() + time_limit;
        while finished_workers.len() < self.workers.len() {
            let remaining_time = deadline.saturating_duration_since(Instant::now());
            match self.done_receiver.recv_timeout(remaining_time) {
                Ok(v) => finished_workers.push(v),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        let mut panicked_workers: Vec<(usize, String)> = Vec::new();
        let mut timed_out_workers = Vec::new();

        for worker in self.workers.drain(..) {
            let id = worker.id;

            if finished_workers.contains(&id) {
                if let Err(e) = worker.thread.join() {
                    let msg = err_to_string(e.as_ref());
                    panicked_workers.push((id, msg));
                }
            } else {
                timed_out_workers.push(id);
            }
        }
        ShutdownReport {
            panicked_workers,
            timed_out_workers,
        }
    }
}

fn err_to_string(err: &dyn Any) -> String {
    // rewrite this with map/unwrap_or
    if let Some(s) = err.downcast_ref::<&str>() {
        String::from(*s)
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string payload error".to_string()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        let err_worker_ids = self.shutdown();

        for (id, msg) in err_worker_ids {
            eprintln!("Worker {id:?} panicked: {msg}");
        }
    }
}

struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

struct DoneGuard {
    id: usize,
    sender: mpsc::Sender<usize>,
}

impl Drop for DoneGuard {
    fn drop(&mut self) {
        if self.sender.send(self.id).is_err() {
            let id = self.id;
            eprintln!("worker {id}: pool stopped listening")
        }
    }
}

impl Worker {
    fn new(
        id: usize,
        receiver: Arc<Mutex<mpsc::Receiver<Job>>>,
        sender: mpsc::Sender<usize>,
    ) -> io::Result<Worker> {
        let builder = thread::Builder::new().name(format!("worker {id}"));
        let thread = builder.spawn(move || {
            let _guard = DoneGuard { id, sender };
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
        })?;
        Ok(Worker { id, thread })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        error::Error,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn worker_name_valid() {
        let (tx, rx) = mpsc::channel();
        let (done_tx, _) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let test_worker = Worker::new(1, rx, done_tx).unwrap();
        let test_worker_name = test_worker.thread.thread().name();
        drop(tx);
        assert!(test_worker_name == Some("worker 1"))
    }

    #[test]
    fn poisoned_mutex_returns_id() {
        let (_, rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let thread_mutex = Arc::clone(&rx);
        let handle = thread::spawn(move || {
            let _guard = thread_mutex.lock().unwrap();
            panic!("test: Poisoning the mutex");
        });
        _ = handle.join();

        let test_worker = Worker::new(0, rx, done_tx).unwrap();
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 0);
        assert!(test_worker.thread.join().is_err());
    }

    #[test]
    fn build_rejects_zero_size() {
        assert!(matches!(
            ThreadPool::build(0),
            Err(PoolFailure::InvalidSize)
        ));
    }

    #[test]
    fn io_error_converts() {
        let io_error = io::Error::other("test io error");
        let converted_error = io_error.into();
        assert!(matches!(converted_error, PoolFailure::Creation(_)));
        assert!(converted_error.source().is_some())
    }

    #[test]
    fn execute_runs_the_closure() {
        let pool = ThreadPool::build(1).unwrap();
        let (tx, rx) = mpsc::channel();

        pool.execute(move || tx.send(()).unwrap()).unwrap();
        let status = rx.recv_timeout(Duration::from_secs(1));
        assert!(status.is_ok());
    }

    #[test]
    fn drop_survives_a_panicked_worker() {
        let pool = ThreadPool::build(1).unwrap();
        pool.execute(|| panic!("test: worker job panicking on purpose"))
            .unwrap();

        let status = catch_unwind(AssertUnwindSafe(|| drop(pool)));
        assert!(status.is_ok())
    }

    #[test]
    fn pool_runs_jobs_after_one_worker_panics() {
        let pool = ThreadPool::build(2).unwrap();
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
        let pool = ThreadPool::build(WORKER_COUNT).unwrap();
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

    #[test]
    fn execute_on_shutdown_pool_returns_shutting_down() {
        let mut pool = ThreadPool::build(3).unwrap();
        let (tx, _) = mpsc::channel();

        pool.shutdown();
        let status = pool.execute(move || tx.send(()).unwrap()).err();
        assert!(matches!(status, Some(PoolFailure::ShuttingDown)))
    }

    #[test]
    fn pool_shutdown_idempotent() {
        let mut pool = ThreadPool::build(3).unwrap();

        let result_1 = pool.shutdown();
        assert!(result_1.is_empty());

        let result_2 = pool.shutdown();
        assert!(result_2.is_empty());
    }

    #[test]
    fn shutdown_timeout_records_stuck_worker() {
        let mut pool = ThreadPool::build(1).unwrap();
        let (gate_tx, gate_rx) = mpsc::channel::<()>();

        let _ = pool.execute(move || {
            let _ = gate_rx.recv();
        });

        let report = pool.shutdown_timeout(Duration::from_millis(100));
        assert_eq!(report.timed_out_workers, vec![0]);
        gate_tx.send(()).unwrap()
    }

    #[test]
    fn shutdown_timeout_reports_nothing_when_jobs_finish() {
        const WORKER_COUNT: usize = 3;
        let mut pool = ThreadPool::build(WORKER_COUNT).unwrap();
        let (tx, rx) = mpsc::channel();

        for _ in 0..WORKER_COUNT {
            let tx = tx.clone();
            pool.execute(move || tx.send(()).unwrap()).unwrap();
        }

        for _ in 0..WORKER_COUNT {
            assert!(rx.recv_timeout(Duration::from_secs(1)).is_ok());
        }

        let report = pool.shutdown_timeout(Duration::from_secs(5));
        assert!(report.timed_out_workers.is_empty());
        assert!(report.panicked_workers.is_empty());
    }

    #[test]
    fn shutdown_timeout_separates_workers() {
        let mut pool = ThreadPool::build(2).unwrap();
        let (name_tx, name_rx) = mpsc::channel();
        let (gate_tx, gate_rx) = mpsc::channel::<()>();

        let _ = pool.execute(move || {
            let current_thread = thread::current();
            let _ = name_tx.send(
                current_thread
                    .name()
                    .expect("job should report its worker name")
                    .to_string(),
            );
            let _ = gate_rx.recv();
        });

        let stuck_worker_name = name_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let stuck_worker_id = stuck_worker_name
            .strip_prefix("worker ")
            .unwrap()
            .parse::<usize>()
            .unwrap();

        // todo: call the shutdown in a seperate timeout to avoid test hanging in case loop is changed

        let report = pool.shutdown_timeout(Duration::from_millis(100));
        assert_eq!(report.timed_out_workers, vec![stuck_worker_id]);
        assert!(report.panicked_workers.is_empty());

        gate_tx.send(()).unwrap()
    }

    #[test]
    fn err_to_string_works() {
        let box_str = Box::new("boom");
        assert_eq!(err_to_string(box_str.as_ref()), "boom");

        let box_string = Box::new("boom".to_string());
        assert_eq!(err_to_string(box_string.as_ref()), "boom");

        let box_non_string = Box::new(37);
        assert_eq!(err_to_string(&box_non_string), "non-string payload error");
    }
}
