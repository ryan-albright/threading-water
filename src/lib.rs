use std::{
    fmt,
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

        match self.sender.as_ref().expect("Access sender").send(job) {
            Ok(_s) => Ok(()),
            Err(_e) => Err(PoolFailure::NoWorkers),
        }
    }
}

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
                let message = receiver.lock().expect("Message received").recv();

                match message {
                    Ok(job) => {
                        println!("Worker {id} got a job; executing.");

                        job();
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
        panic::{AssertUnwindSafe, catch_unwind},
        time::{Duration, Instant},
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
    fn execute_reports_no_workers_when_all_workers_died() {
        let pool = ThreadPool::new(1);
        pool.execute(|| panic!("test: worker job panicking on purpose"))
            .unwrap();
        let few_ms = Duration::from_millis(3);
        let now = Instant::now();

        loop {
            let result = pool.execute(|| ());
            if matches!(result, Err(PoolFailure::NoWorkers)) {
                break;
            }
            if now.elapsed() > Duration::from_secs(1) {
                panic!("execute never reported NoWorkers after a worker panic");
            }
            thread::sleep(few_ms);
        }
    }

    // todo: add a test that confirms pool with n > 1 cleans up later
    // workers if one worker panics
}
