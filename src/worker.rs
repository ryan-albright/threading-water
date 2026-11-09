use std::{
    any::Any,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::pool::Job;

pub(crate) struct Worker {
    pub(crate) id: usize,
    pub(crate) thread: thread::JoinHandle<()>,
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
    pub(crate) fn new(
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

pub(crate) fn err_to_string(err: &dyn Any) -> String {
    // rewrite this with map/unwrap_or
    if let Some(s) = err.downcast_ref::<&str>() {
        String::from(*s)
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string payload error".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const EVENT_WAIT: Duration = Duration::from_secs(1);

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
        assert_eq!(done_rx.recv_timeout(EVENT_WAIT).unwrap(), 0);
        assert!(test_worker.thread.join().is_err());
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
