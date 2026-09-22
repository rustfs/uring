// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Threadless driver: message queues own real permits, and eventfd counters let
// tests count notifications without races with a running consumer.
fn driver(shard_count: usize, capacity: usize, bytes: Option<usize>) -> (UringDriver, Vec<mpsc::Receiver<Msg>>) {
    let byte_admission = bytes.map(|bytes| Arc::new(ByteAdmission::new(bytes)));
    let mut receivers = Vec::new();
    let shards = (0..shard_count)
        .map(|_| {
            let (tx, rx) = mpsc::channel();
            receivers.push(rx);
            let sem = Arc::new(Semaphore::new(capacity));
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
            limits: ReadLimits {
                max_read_len: None,
                max_in_flight_bytes: bytes,
            },
            shard_policy: ShardPolicy::default(),
            byte_admission,
            shards,
            next_id: AtomicU64::new(1),
            rr: AtomicUsize::new(0),
        },
        receivers,
    )
}

fn requests(count: usize) -> Vec<ReadRequest> {
    let file = Arc::new(File::open("/dev/zero").unwrap());
    (0..count)
        .map(|index| ReadRequest {
            file: Arc::clone(&file),
            offset: index as u64,
            len: 8,
        })
        .collect()
}

fn notifications(event: &EventFd) -> u64 {
    let mut count = 0u64;
    // SAFETY: read writes one u64 into valid memory; eventfd is nonblocking.
    let result = unsafe { libc::read(event.as_raw(), (&mut count as *mut u64).cast(), 8) };
    if result == -1 {
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EAGAIN));
        return 0;
    }
    assert_eq!(result, 8);
    count
}

fn poll(handle: &mut ReadHandle) -> Poll<io::Result<Vec<u8>>> {
    Pin::new(handle).poll(&mut Context::from_waker(std::task::Waker::noop()))
}

#[test]
fn batch_signals_once_per_owner_while_single_reads_keep_individual_signals() {
    let (driver, receivers) = driver(2, 64, None);
    let handles = driver.read_at_batch(requests(MAX_BATCH_READS)).unwrap();
    assert_eq!(handles.len(), MAX_BATCH_READS);
    for (shard, receiver) in driver.shards.iter().zip(&receivers) {
        assert_eq!(notifications(&shard.wake_efd), 1);
        assert_eq!(receiver.try_iter().count(), MAX_BATCH_READS / 2);
    }
    let single: Vec<_> = requests(4)
        .into_iter()
        .map(|r| driver.read_at(r.file, r.offset, r.len))
        .collect();
    for shard in &driver.shards {
        assert_eq!(notifications(&shard.wake_efd), 2);
    }
    drop(single);
    drop(handles);
}

#[test]
fn oversized_batch_rejects_every_request_before_submission() {
    let (driver, receivers) = driver(1, 64, None);
    let error = driver.read_at_batch(requests(MAX_BATCH_READS + 1)).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(driver.next_id.load(Ordering::Relaxed), 1);
    assert_eq!(driver.rr.load(Ordering::Relaxed), 0);
    assert!(matches!(receivers[0].try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(notifications(&driver.shards[0].wake_efd), 0);
}

#[test]
fn empty_and_invalid_requests_do_not_notify_or_reject_valid_members() {
    let (driver, receivers) = driver(1, 4, None);
    assert!(driver.read_at_batch(Vec::new()).unwrap().is_empty());
    assert_eq!(notifications(&driver.shards[0].wake_efd), 0);
    let mut invalid = requests(1);
    invalid[0].offset = u64::MAX;
    let mut invalid_handles = driver.read_at_batch(invalid).unwrap();
    assert!(matches!(poll(&mut invalid_handles[0]), Poll::Ready(Err(_))));
    assert_eq!(notifications(&driver.shards[0].wake_efd), 0);
    assert!(matches!(receivers[0].try_recv(), Err(TryRecvError::Empty)));
    let mut reads = requests(2);
    reads[0].offset = u64::MAX;
    let mut handles = driver.read_at_batch(reads).unwrap();
    assert!(matches!(poll(&mut handles[0]), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::InvalidInput));
    assert!(matches!(&handles[1].state, HandleState::Submitted { .. }));
    assert_eq!(receivers[0].try_iter().count(), 1);
    assert_eq!(notifications(&driver.shards[0].wake_efd), 1);
}

#[test]
fn capacity_aware_batch_notifies_final_owner_only() {
    let (driver, receivers) = driver(2, 4, None);
    let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
    let _held = Arc::clone(&driver.shards[0].sem).try_acquire_many_owned(4).unwrap();
    let handles = driver.read_at_batch(requests(4)).unwrap();
    assert_eq!(notifications(&driver.shards[0].wake_efd), 0);
    assert_eq!(notifications(&driver.shards[1].wake_efd), 1);
    assert!(matches!(receivers[0].try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(receivers[1].try_iter().count(), 4);
    for handle in &handles {
        let HandleState::Submitted { wake } = &handle.state else { panic!("expected eager admission") };
        assert!(Arc::ptr_eq(wake, &driver.shards[1].wake_efd));
    }
}

#[test]
fn deferred_batch_member_notifies_when_polled_after_byte_permit_returns() {
    let (driver, receivers) = driver(1, 2, Some(8));
    let mut handles = driver.read_at_batch(requests(2)).unwrap();
    assert!(matches!(&handles[1].state, HandleState::WaitingPermit { .. }));
    assert_eq!(notifications(&driver.shards[0].wake_efd), 1);
    assert!(poll(&mut handles[1]).is_pending());
    assert_eq!(notifications(&driver.shards[0].wake_efd), 0);
    let accepted = receivers[0].try_recv().unwrap();
    assert!(matches!(&accepted, Msg::Read { .. }));
    drop(accepted); // Simulate completed ownership release; no kernel used.
    assert!(poll(&mut handles[1]).is_pending());
    assert_eq!(notifications(&driver.shards[0].wake_efd), 1);
    assert!(matches!(receivers[0].try_recv(), Ok(Msg::Read { .. })));
}

#[test]
fn dropping_batch_routes_cancels_and_keeps_accepted_permits_owned() {
    let (driver, receivers) = driver(1, 2, Some(16));
    let handles = driver.read_at_batch(requests(2)).unwrap();
    assert_eq!(notifications(&driver.shards[0].wake_efd), 1);
    let accepted: Vec<_> = receivers[0].try_iter().collect();
    let ids: Vec<_> = handles.iter().map(|handle| handle.id).collect();
    drop(handles);
    assert_eq!(notifications(&driver.shards[0].wake_efd), 2);
    let cancelled: Vec<_> = receivers[0]
        .try_iter()
        .map(|msg| match msg {
            Msg::Cancel { id } => id,
            _ => panic!("expected cancel"),
        })
        .collect();
    assert_eq!(cancelled, ids);
    assert_eq!(driver.shards[0].sem.available_permits(), 0);
    assert_eq!(driver.byte_admission.as_ref().unwrap().bytes.available_permits(), 0);
    drop(accepted);
    assert_eq!(driver.shards[0].sem.available_permits(), 2);
    assert_eq!(driver.byte_admission.as_ref().unwrap().bytes.available_permits(), 16);
}

#[test]
fn interrupted_construction_drops_handles_and_wakes_previously_queued_reads() {
    let (driver, receivers) = driver(1, 2, None);
    // Force the existing id-overflow assertion on the second batch member.
    driver.next_id.store(CANCEL_BIT - 1, Ordering::Relaxed);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| driver.read_at_batch(requests(2))));
    assert!(result.is_err());
    assert_eq!(notifications(&driver.shards[0].wake_efd), 1, "unwind cancellation wakes the owner");
    let accepted = receivers[0].try_recv().unwrap();
    assert!(matches!(&accepted, Msg::Read { id, .. } if *id == CANCEL_BIT - 1));
    assert!(matches!(receivers[0].try_recv(), Ok(Msg::Cancel { id }) if id == CANCEL_BIT - 1));
    assert_eq!(driver.shards[0].sem.available_permits(), 1);
    drop(accepted);
    assert_eq!(driver.shards[0].sem.available_permits(), 2);
}
