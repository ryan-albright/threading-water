use std::{
    any::Any,
    fmt, io,
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
            let rx = Arc::new(Mutex::new(rx));
            let mut workers = Vec::with_capacity(size);

            for id in 0..size {
                workers.push(Worker::new(id, Arc::clone(&rx))?);
            }

            Ok(ThreadPool {
                workers,
                sender: Some(tx),
            })
        }
    }

    pub fn execute<F>(&self, f: F) -> Result<(), PoolFailure>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        match self.sender.as_ref() {
            Some(s) => s.send(job).map_err(|_| PoolFailure::NoWorkers),
            None => Err(PoolFailure::ShuttingDown),
        }
    }

    pub fn shutdown(&mut self) -> Vec<(usize, String)> {
        drop(self.sender.take());
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

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> io::Result<Worker> {
        let builder = thread::Builder::new().name(format!("worker {id}"));
        let thread = builder.spawn(move || {
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
        time::Duration,
    };

    #[test]
    fn worker_name_valid() {
        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let test_worker = Worker::new(1, rx).unwrap();
        let test_worker_name = test_worker.thread.thread().name();
        drop(tx);
        assert!(test_worker_name == Some("worker 1"))
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
}
