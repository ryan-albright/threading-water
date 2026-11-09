use std::{
    error::Error,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::Duration,
};

use server::{PoolFailure, ThreadPool};

const EVENT_WAIT: Duration = Duration::from_secs(1);
const SHORT_SHUTDOWN: Duration = Duration::from_millis(100);
const GENEROUS_SHUTDOWN: Duration = Duration::from_secs(5);
const SHUTDOWN_BUFFER: Duration = Duration::from_millis(200);

const SHUTDOWN_GUARD: Duration = SHORT_SHUTDOWN.saturating_add(SHUTDOWN_BUFFER);

fn within<F, T>(time_limit: Duration, f: F) -> Result<T, RecvTimeoutError>
where
    F: Send + 'static,
    T: Send + 'static,
    F: FnOnce() -> T,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = f();
        let _ = tx.send(result);
    });

    rx.recv_timeout(time_limit)
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
    let status = rx.recv_timeout(EVENT_WAIT);
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
        let status = rx.recv_timeout(EVENT_WAIT);
        assert!(status.is_ok());
    }

    let (tx, rx) = mpsc::channel();
    pool.execute(move || tx.send(()).unwrap())
        .expect("Pool should accept jobs");
    let status = rx.recv_timeout(EVENT_WAIT);
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

    let report = within(SHUTDOWN_GUARD, move || {
        pool.shutdown_timeout(SHORT_SHUTDOWN)
    })
    .unwrap_or_else(|_| panic!("Server shutdown should not exceed {SHORT_SHUTDOWN:?}"));
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
        assert!(rx.recv_timeout(EVENT_WAIT).is_ok());
    }

    let report = within(SHUTDOWN_GUARD, move || {
        pool.shutdown_timeout(GENEROUS_SHUTDOWN)
    })
    .unwrap_or_else(|_| panic!("Server shutdown should not exceed {GENEROUS_SHUTDOWN:?}"));
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

    let stuck_worker_name = name_rx.recv_timeout(EVENT_WAIT).unwrap();
    let stuck_worker_id = stuck_worker_name
        .strip_prefix("worker ")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    let report = within(SHUTDOWN_GUARD, move || {
        pool.shutdown_timeout(SHORT_SHUTDOWN)
    })
    .unwrap_or_else(|_| panic!("Server shutdown should not exceed {SHORT_SHUTDOWN:?}"));
    assert_eq!(report.timed_out_workers, vec![stuck_worker_id]);
    assert!(report.panicked_workers.is_empty());

    gate_tx.send(()).unwrap()
}
