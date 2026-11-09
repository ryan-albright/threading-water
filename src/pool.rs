use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError},
    },
    time::{Duration, Instant},
};

use crate::{
    error::PoolFailure,
    worker::{Worker, err_to_string},
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

pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

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
            Some(s) => {
                s.send(job)
                    .expect("workers hold the receiver until the sender is dropped");
                Ok(())
            }
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

impl Drop for ThreadPool {
    fn drop(&mut self) {
        let err_worker_ids = self.shutdown();

        for (id, msg) in err_worker_ids {
            eprintln!("Worker {id:?} panicked: {msg}");
        }
    }
}
