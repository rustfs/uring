//! Deterministic admission tests; no kernel ring is needed.
use super::*;
use std::task::Waker;

fn poll_once(future: &mut AcquireFut) -> Poll<Result<ReadPermits, tokio::sync::AcquireError>> {
    future.as_mut().poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn byte_saturation_returns_partial_count_reservation_on_drop() {
    let count = Arc::new(Semaphore::new(2));
    let bytes = Arc::new(Semaphore::new(8));
    let first = try_read_permits(&count, Some(&bytes), 8).unwrap();
    let mut waiting = acquire_read_permits(count.clone(), Some(bytes.clone()), 8);
    assert!(poll_once(&mut waiting).is_pending());
    assert_eq!(count.available_permits(), 0);
    drop(waiting);
    assert_eq!(count.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 0);
    drop(first);
    assert_eq!(bytes.available_permits(), 8);
}

#[test]
fn dropping_count_waiter_does_not_reserve_bytes() {
    let count = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(8));
    let first = try_read_permits(&count, Some(&bytes), 4).unwrap();
    let mut waiting = acquire_read_permits(count.clone(), Some(bytes.clone()), 4);
    assert!(poll_once(&mut waiting).is_pending());
    drop(waiting);
    assert_eq!(bytes.available_permits(), 4);
    drop(first);
    assert_eq!(count.available_permits(), 1);
}

#[test]
fn shard_exit_closes_bytes_and_releases_waiting_count_permit() {
    let count = Arc::new(Semaphore::new(2));
    let admission = Arc::new(ByteAdmission::new(8));
    admission.register(&count);
    let bytes = admission.bytes.clone();
    let first = try_read_permits(&count, Some(&bytes), 8).unwrap();
    let mut waiting = acquire_read_permits(count.clone(), Some(bytes.clone()), 8);
    assert!(poll_once(&mut waiting).is_pending());
    drop(CloseByteAdmission(Some(admission)));
    assert!(matches!(poll_once(&mut waiting), Poll::Ready(Err(_))));
    assert_eq!(count.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 0);
    drop(first);
}

#[test]
fn global_close_wakes_count_stage_waiter_without_releasing_hung_read() {
    struct LockCheckingWake {
        admission: Arc<ByteAdmission>,
        wakes: AtomicUsize,
    }
    impl std::task::Wake for LockCheckingWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            assert!(self.admission.registry.try_lock().is_ok(), "registry lock held across task wake");
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    let count = Arc::new(Semaphore::new(1));
    let admission = Arc::new(ByteAdmission::new(8));
    admission.register(&count);
    let hung = try_read_permits(&count, Some(&admission.bytes), 8).unwrap();
    let mut waiting = acquire_read_permits(count.clone(), Some(admission.bytes.clone()), 8);
    let wake = Arc::new(LockCheckingWake {
        admission: admission.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    // Model a different shard exiting while this shard's read stays hung.
    drop(CloseByteAdmission(Some(admission.clone())));
    assert!(wake.wakes.load(Ordering::SeqCst) > 0, "count waiter was not woken");
    assert!(matches!(poll_once(&mut waiting), Poll::Ready(Err(_))));
    assert!(matches!(
        try_read_permits(&count, Some(&admission.bytes), 8),
        Err(TryAcquireError::Closed)
    ));
    let mut new_waiter = acquire_read_permits(count.clone(), Some(admission.bytes.clone()), 8);
    assert!(matches!(poll_once(&mut new_waiter), Poll::Ready(Err(_))));
    assert_eq!(admission.bytes.available_permits(), 0, "hung read must remain charged");
    drop(hung);
}

#[test]
fn late_registration_is_closed_after_terminal_admission_close() {
    let admission = ByteAdmission::new(8);
    admission.close();
    let count = Arc::new(Semaphore::new(1));
    admission.register(&count);
    assert!(count.is_closed());
    assert!(admission.registry.lock().unwrap().counts.is_empty());
}

#[test]
fn concurrent_registration_cannot_escape_terminal_close() {
    for _ in 0..16 {
        let admission = ByteAdmission::new(8);
        let count = Arc::new(Semaphore::new(1));
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                admission.register(&count);
            });
            barrier.wait();
            admission.close();
        });
        assert!(count.is_closed());
        assert!(admission.bytes.is_closed());
    }
}

#[test]
fn canceling_partial_fifo_byte_reservation_unblocks_next_waiter() {
    let count = Arc::new(Semaphore::new(4));
    let bytes = Arc::new(Semaphore::new(8));
    let held = try_read_permits(&count, Some(&bytes), 4).unwrap();
    let mut large = acquire_read_permits(count.clone(), Some(bytes.clone()), 6);
    assert!(poll_once(&mut large).is_pending());
    assert_eq!(bytes.available_permits(), 0, "large FIFO waiter should reserve the available four bytes");
    let mut small = acquire_read_permits(count.clone(), Some(bytes.clone()), 2);
    assert!(poll_once(&mut small).is_pending());
    drop(large);
    let Poll::Ready(Ok(small)) = poll_once(&mut small) else {
        panic!("canceling partial reservation did not advance FIFO")
    };
    assert_eq!(bytes.available_permits(), 2);
    drop(held);
    drop(small);
    assert_eq!(bytes.available_permits(), 8);
    assert_eq!(count.available_permits(), 4);
}

#[test]
fn permits_retained_by_leaked_operation_remain_charged_after_close() {
    let count = Arc::new(Semaphore::new(1));
    let admission = Arc::new(ByteAdmission::new(8));
    admission.register(&count);
    let bytes = admission.bytes.clone();
    let leaked = try_read_permits(&count, Some(&bytes), 8).unwrap();
    std::mem::forget(leaked);
    drop(CloseByteAdmission(Some(admission)));
    assert_eq!(bytes.available_permits(), 0);
    assert_eq!(count.available_permits(), 0);
}

#[test]
fn shared_byte_budget_serializes_reads_from_distinct_shards() {
    let counts = [Arc::new(Semaphore::new(1)), Arc::new(Semaphore::new(1))];
    let bytes = Arc::new(Semaphore::new(8));
    let first = try_read_permits(&counts[0], Some(&bytes), 8).unwrap();
    assert!(matches!(try_read_permits(&counts[1], Some(&bytes), 1), Err(TryAcquireError::NoPermits)));
    assert_eq!(counts[1].available_permits(), 1);
    let mut waiting = acquire_read_permits(counts[1].clone(), Some(bytes.clone()), 8);
    assert!(poll_once(&mut waiting).is_pending());
    drop(first);
    let Poll::Ready(Ok(second)) = poll_once(&mut waiting) else { panic!("budget was not released") };
    drop(second);
    assert_eq!(bytes.available_permits(), 8);
}

fn fake_driver(limits: ReadLimits) -> (UringDriver, mpsc::Receiver<Msg>) {
    let (tx, rx) = mpsc::channel();
    let count = Arc::new(Semaphore::new(8));
    let byte_admission = limits.max_in_flight_bytes.map(|bytes| Arc::new(ByteAdmission::new(bytes)));
    if let Some(admission) = &byte_admission {
        admission.register(&count);
    }
    (
        UringDriver {
            limits,
            shard_policy: ShardPolicy::default(),
            byte_admission,
            shards: vec![Shard {
                tx,
                handle: None,
                stats: Arc::new(DriverStats::default()),
                sem: count,
                wake_efd: Arc::new(EventFd::new().unwrap()),
            }],
            next_id: AtomicU64::new(1),
            rr: AtomicUsize::new(0),
        },
        rx,
    )
}

#[tokio::test]
async fn logical_limit_rejects_before_message_or_allocation() {
    let (driver, rx) = fake_driver(ReadLimits {
        max_read_len: Some(4),
        max_in_flight_bytes: Some(8192),
    });
    let err = driver
        .read_at(Arc::new(File::open("/dev/zero").unwrap()), 0, 5)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn direct_budget_includes_superset_padding_and_zero_length_geometry() {
    for (offset, len, charge) in [(1, 4096, 12287), (0, 0, 4095), (1, 0, 8191)] {
        let (driver, rx) = fake_driver(ReadLimits {
            max_read_len: None,
            max_in_flight_bytes: Some(charge - 1),
        });
        let file = Arc::new(File::open("/dev/zero").unwrap());
        let err = driver.read_at_direct(file.clone(), offset, len, 4096).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        let (driver, rx) = fake_driver(ReadLimits {
            max_read_len: None,
            max_in_flight_bytes: Some(charge),
        });
        let handle = driver.read_at_direct(file, offset, len, 4096).without_cancel_on_drop();
        let msg = rx.try_recv().unwrap();
        assert_eq!(driver.byte_admission.as_ref().unwrap().bytes.available_permits(), 0);
        drop(handle);
        assert_eq!(
            driver.byte_admission.as_ref().unwrap().bytes.available_permits(),
            0,
            "caller drop released bytes"
        );
        drop(msg);
        assert_eq!(driver.byte_admission.as_ref().unwrap().bytes.available_permits(), charge);
    }
}

#[test]
fn invalid_budget_is_rejected_before_kernel_probe() {
    for budget in [0, Semaphore::MAX_PERMITS + 1] {
        let result = UringDriver::probe_and_start_with_limits(
            8,
            1,
            ReadLimits {
                max_read_len: None,
                max_in_flight_bytes: Some(budget),
            },
        );
        assert!(matches!(result, Err(ProbeFailure::Setup(err)) if err.kind() == io::ErrorKind::InvalidInput));
    }
}
