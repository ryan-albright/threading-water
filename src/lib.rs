use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

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

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender.as_ref().unwrap().send(job).unwrap();
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
                // change unwrap to an expect if a lock fails
                let message = receiver.lock().unwrap().recv();

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

        pool.execute(move || tx.send(()).unwrap());
        let status = rx.recv_timeout(Duration::from_secs(1));
        assert!(status.is_ok());
    }

    #[test]
    fn drop_survives_a_panicked_worker() {
        let pool = ThreadPool::new(1);
        pool.execute(|| panic!("test: worker job panicking on purpose"));

        let status = catch_unwind(AssertUnwindSafe(|| drop(pool)));
        assert!(status.is_ok())
    }
}
