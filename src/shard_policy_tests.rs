// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

// No ring or driver thread: queued messages retain real admission permits, and
// tests observe the exact destination of reads and cancels without I/O races.
fn driver(limits: ReadLimits) -> (UringDriver, Vec<mpsc::Receiver<Msg>>) {
    let byte_admission = limits.max_in_flight_bytes.map(|bytes| Arc::new(ByteAdmission::new(bytes)));
    let mut receivers = Vec::new();
    let shards = (0..2)
        .map(|_| {
            let (tx, rx) = mpsc::channel();
            receivers.push(rx);
            let sem = Arc::new(Semaphore::new(1));
            if let Some(admission) = &byte_admission {
                admission.register(&sem);
            }
            Shard {
                tx,
                handle: None,
                stats: Arc::new(DriverStats::default()),
                sem,
                wake_efd: Arc::new(EventFd::new().unwrap()),
            }
        })
        .collect();
    (
        UringDriver {
            limits,
            shard_policy: ShardPolicy::default(),
            byte_admission,
            shards,
            next_id: AtomicU64::new(1),
            rr: AtomicUsize::new(0),
        },
        receivers,
    )
}

fn file() -> Arc<File> {
    Arc::new(File::open("/dev/zero").unwrap())
}

fn poll(handle: &mut ReadHandle) -> Poll<io::Result<Vec<u8>>> {
    Pin::new(handle).poll(&mut Context::from_waker(std::task::Waker::noop()))
}

#[test]
fn capacity_aware_read_and_cancel_use_the_acquired_shard() {
    let (driver, receivers) = driver(ReadLimits::default());
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    let held = Arc::clone(&driver.shards[0].sem).try_acquire_owned().unwrap();
    let handle = driver.read_at(file(), 0, 8);
    let id = handle.id;
    let HandleState::Submitted { wake } = &handle.state else { panic!("free shard was not selected") };
    assert!(Arc::ptr_eq(wake, &driver.shards[1].wake_efd));
    let message = receivers[1].try_recv().unwrap();
    assert!(matches!(&message, Msg::Read { id: actual, .. } if *actual == id));
    drop(handle);
    assert!(matches!(receivers[1].try_recv(), Ok(Msg::Cancel { id: actual }) if actual == id));
    assert!(matches!(receivers[0].try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(driver.shards[1].sem.available_permits(), 0, "cancel must not release accepted permits");
    drop(message);
    drop(held);
    assert!(driver.shards.iter().all(|shard| shard.sem.available_permits() == 1));
}

#[test]
fn default_and_stream_routing_still_wait_on_the_round_robin_shard() {
    for stream in [false, true] {
        let (driver, receivers) = driver(ReadLimits::default());
        let driver = if stream {
            driver.with_shard_policy(ShardPolicy::CapacityAware)
        } else {
            driver
        };
        let _held = Arc::clone(&driver.shards[0].sem).try_acquire_owned().unwrap();
        let handle = if stream {
            driver.read_current(file(), 8)
        } else {
            driver.read_at(file(), 0, 8)
        };
        let HandleState::WaitingPermit { wake, .. } = &handle.state else { panic!("legacy route changed") };
        assert!(Arc::ptr_eq(wake, &driver.shards[0].wake_efd));
        assert!(receivers.iter().all(|rx| matches!(rx.try_recv(), Err(TryRecvError::Empty))));
        assert_eq!(driver.shards[1].sem.available_permits(), 1);
        drop(handle);
    }
}

#[test]
fn capacity_aware_skips_closed_shards_but_default_does_not() {
    for policy in [ShardPolicy::RoundRobin, ShardPolicy::CapacityAware] {
        let (driver, receivers) = driver(ReadLimits::default());
        let driver = driver.with_shard_policy(policy);
        driver.shards[0].sem.close();
        let mut handle = driver.read_at(file(), 0, 8);
        if policy == ShardPolicy::CapacityAware {
            assert!(matches!(receivers[1].try_recv(), Ok(Msg::Read { .. })));
        } else {
            assert!(matches!(poll(&mut handle), Poll::Ready(Err(_))));
            assert!(matches!(receivers[1].try_recv(), Err(TryRecvError::Empty)));
        }
        assert!(matches!(receivers[0].try_recv(), Err(TryRecvError::Empty)));
    }
}

#[test]
fn capacity_aware_rejects_when_every_shard_is_closed() {
    let (driver, receivers) = driver(ReadLimits::default());
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    for shard in &driver.shards {
        shard.sem.close();
    }
    let mut handle = driver.read_at(file(), 0, 8);
    assert!(matches!(poll(&mut handle), Poll::Ready(Err(_))));
    assert!(receivers.iter().all(|rx| matches!(rx.try_recv(), Err(TryRecvError::Empty))));
}

#[test]
fn capacity_aware_deferred_owner_remains_fixed_when_another_shard_frees() {
    let (driver, receivers) = driver(ReadLimits::default());
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    let first = Arc::clone(&driver.shards[0].sem).try_acquire_owned().unwrap();
    let second = Arc::clone(&driver.shards[1].sem).try_acquire_owned().unwrap();
    let mut handle = driver.read_at(file(), 0, 8);
    assert!(poll(&mut handle).is_pending());
    drop(second);
    assert!(poll(&mut handle).is_pending());
    assert!(matches!(receivers[1].try_recv(), Err(TryRecvError::Empty)));
    drop(first);
    assert!(poll(&mut handle).is_pending()); // submitted, awaiting its CQE
    assert!(matches!(receivers[0].try_recv(), Ok(Msg::Read { .. })));
    drop(handle);
    assert!(matches!(receivers[0].try_recv(), Ok(Msg::Cancel { .. })));
    assert!(matches!(receivers[1].try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn capacity_aware_deferred_waiters_keep_semaphore_queue_order() {
    let (driver, receivers) = driver(ReadLimits::default());
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    let held = Arc::clone(&driver.shards[0].sem).try_acquire_owned().unwrap();
    let _other = Arc::clone(&driver.shards[1].sem).try_acquire_owned().unwrap();
    let mut first = driver.read_at(file(), 0, 8);
    driver.rr.store(0, Ordering::Relaxed);
    let mut second = driver.read_at(file(), 8, 8);
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    drop(held);
    assert!(poll(&mut second).is_pending(), "later waiter must not overtake the first");
    assert!(matches!(receivers[0].try_recv(), Err(TryRecvError::Empty)));
    assert!(poll(&mut first).is_pending());
    let message = receivers[0].try_recv().unwrap();
    assert!(matches!(&message, Msg::Read { id, .. } if *id == first.id));
    assert!(poll(&mut second).is_pending());
    drop(message);
    assert!(poll(&mut second).is_pending());
    assert!(matches!(receivers[0].try_recv(), Ok(Msg::Read { id, .. }) if id == second.id));
}

#[test]
fn byte_shortage_releases_failed_count_reservation_and_waiter_drop_releases_partial_admission() {
    let (driver, receivers) = driver(ReadLimits {
        max_read_len: None,
        max_in_flight_bytes: Some(8),
    });
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    let admission = driver.byte_admission.as_ref().unwrap();
    let bytes = Arc::clone(&admission.bytes).try_acquire_many_owned(8).unwrap();
    let mut handle = driver.read_at(file(), 0, 8);
    assert!(driver.shards.iter().all(|shard| shard.sem.available_permits() == 1));
    assert!(poll(&mut handle).is_pending());
    assert_eq!(driver.shards[0].sem.available_permits(), 0);
    assert_eq!(driver.shards[1].sem.available_permits(), 1);
    drop(handle);
    drop(bytes);
    assert!(driver.shards.iter().all(|shard| shard.sem.available_permits() == 1));
    assert_eq!(admission.bytes.available_permits(), 8);
    assert!(receivers.iter().all(|rx| matches!(rx.try_recv(), Err(TryRecvError::Empty))));
}

#[test]
fn closed_byte_admission_is_terminal_even_with_available_count_permits() {
    let (driver, receivers) = driver(ReadLimits {
        max_read_len: None,
        max_in_flight_bytes: Some(8),
    });
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    // Close just bytes to exercise the race before registry count closure.
    driver.byte_admission.as_ref().unwrap().bytes.close();
    let mut handle = driver.read_at(file(), 0, 8);
    assert!(matches!(poll(&mut handle), Poll::Ready(Err(_))));
    assert!(driver.shards.iter().all(|shard| shard.sem.available_permits() == 1));
    assert!(receivers.iter().all(|rx| matches!(rx.try_recv(), Err(TryRecvError::Empty))));
}

#[cfg(feature = "diagnostics")]
#[test]
fn capacity_aware_diagnostics_sample_only_the_final_owner() {
    let (driver, receivers) = driver(ReadLimits::default());
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    let held = Arc::clone(&driver.shards[0].sem).try_acquire_owned().unwrap();
    let handle = driver.read_at(file(), 0, 8); // rr 0, accepted on 1
    assert_eq!(driver.shard_diagnostics()[0].admission.count, 0);
    assert_eq!(driver.shard_diagnostics()[1].admission.count, 1);
    drop(receivers[1].try_recv().unwrap());
    drop(handle);
    drop(held);
    // The failed candidate did not consume shard 0's first sample position.
    driver.rr.store(0, Ordering::Relaxed);
    let _handle = driver.read_at(file(), 0, 8);
    assert_eq!(driver.shard_diagnostics()[0].admission.count, 1);
}
