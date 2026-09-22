// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Whole-driver read-quota reservations. Kernel tests print SKIP only for
//! expected restrictions; configuration tests do not require io_uring setup.
#![cfg(target_os = "linux")]

use std::fs::File;
use std::io::{self, Write};
use std::os::fd::FromRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Waker};
use std::time::Duration;

use rustfs_uring::{ProbeFailure, ReadHandle, ReadLimits, ReadRequest, ShardPolicy, SharedReadBudget, UringDriver};

fn limits(bytes: usize) -> ReadLimits {
    ReadLimits {
        max_read_len: None,
        max_in_flight_bytes: Some(bytes),
    }
}

fn assert_setup_kind(result: Result<UringDriver, ProbeFailure>, kind: io::ErrorKind) {
    match result {
        Err(ProbeFailure::Setup(error)) => assert_eq!(error.kind(), kind),
        Err(other) => panic!("wrong failure boundary: {other}"),
        Ok(driver) => {
            driver.shutdown();
            panic!("invalid or unavailable quota was admitted");
        }
    }
}

#[test]
fn shared_budget_accepts_large_usize_capacity_without_allocating_read_buffers() {
    assert!(matches!(SharedReadBudget::new(0), Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    for total in [1, 16, usize::MAX] {
        let budget = SharedReadBudget::new(total).expect("positive accounting capacity");
        let clone = budget.clone();
        assert_eq!(budget.capacity(), total);
        assert_eq!(clone.capacity(), total);
        assert_eq!(budget.available(), total);
        drop(budget);
        assert_eq!(clone.available(), total);
    }
}

#[test]
fn shared_constructor_rejects_missing_zero_or_oversized_local_quota_before_probe() {
    let budget = SharedReadBudget::new(8).expect("test shared budget");
    for local in [ReadLimits::default(), limits(0), limits(9)] {
        assert_setup_kind(
            UringDriver::probe_and_start_with_shared_budget(1, 1, local, &budget),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(budget.available(), 8, "invalid configuration must not retain quota");
    }
    let largest = SharedReadBudget::new(usize::MAX).expect("global capacity has no u32 limit");
    assert_setup_kind(
        UringDriver::probe_and_start_with_shared_budget(1, 1, limits(tokio::sync::Semaphore::MAX_PERMITS + 1), &largest),
        io::ErrorKind::InvalidInput,
    );
    assert_eq!(largest.available(), usize::MAX);
}

fn driver_or_skip(name: &str, entries: u32, shards: usize, bytes: usize, budget: &SharedReadBudget) -> Option<UringDriver> {
    let available_before = budget.available();
    match UringDriver::probe_and_start_with_shared_budget(entries, shards, limits(bytes), budget) {
        Ok(driver) => Some(driver),
        Err(error) => {
            // Each test owns this pool; no concurrent constructor changes it.
            // A restricted-kernel failure must refund the tentative whole quota.
            assert_eq!(budget.available(), available_before, "failed probe retained a whole-driver quota");
            assert!(error.is_expected_restriction(), "unexpected shared-budget probe failure: {error}");
            eprintln!("SKIP {name}: restricted environment ({error})");
            None
        }
    }
}

fn assert_would_block(result: Result<UringDriver, ProbeFailure>) {
    match result {
        Err(error) => {
            assert!(
                !error.is_expected_restriction(),
                "quota exhaustion must not be negatively cached as a kernel restriction"
            );
            assert_setup_kind(Err(error), io::ErrorKind::WouldBlock);
        }
        Ok(driver) => {
            driver.shutdown();
            panic!("exhausted shared quota must refuse construction");
        }
    }
}

fn pipe() -> (Arc<File>, File) {
    let mut fds = [0; 2];
    // SAFETY: valid two-fd output array; success creates two owned descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: each fresh descriptor transfers into exactly one File owner.
    unsafe { (Arc::new(File::from_raw_fd(fds[0])), File::from_raw_fd(fds[1])) }
}

async fn wait_submitted(driver: &UringDriver, count: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while driver.stats().submitted != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pipe read enters driver pending table");
}

async fn wait_finished(driver: &UringDriver) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !driver.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("ordinary test driver threads should finish");
}

async fn read_result(handle: ReadHandle) -> io::Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("test read should resolve")
}

#[test]
fn shared_quota_is_reserved_once_per_driver_and_clean_shutdown_recycles_it() {
    let budget = SharedReadBudget::new(24).expect("three whole-driver quotas");
    let clone = budget.clone();
    let Some(first) = driver_or_skip(
        "shared_quota_is_reserved_once_per_driver_and_clean_shutdown_recycles_it",
        1,
        2,
        8,
        &budget,
    ) else {
        return;
    };
    assert_eq!(clone.available(), 16, "two shards share one driver quota, not two reservations");
    let second = driver_or_skip("second shared driver", 1, 1, 8, &clone).expect("kernel precondition already proved");
    assert_eq!(budget.available(), 8);
    assert_would_block(UringDriver::probe_and_start_with_shared_budget(1, 1, limits(9), &budget));
    assert_eq!(budget.available(), 8, "refused reservation must not consume partial capacity");
    let third = driver_or_skip("exact remaining shared quota", 1, 1, 8, &budget).expect("remaining quota fits");
    assert_eq!(budget.available(), 0);
    assert_eq!(first.shutdown().in_flight, 0);
    assert_eq!(budget.available(), 8);
    assert_eq!(second.shutdown().in_flight, 0);
    assert_eq!(budget.available(), 16);
    assert_eq!(third.shutdown().in_flight, 0);
    assert_eq!(budget.available(), 24);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn driver_quota_larger_than_u32_is_not_truncated_to_per_read_permit_width() {
    let quota = u32::MAX as usize + 1;
    let budget = SharedReadBudget::new(quota * 2).expect("large accounting capacity");
    let Some(driver) = driver_or_skip(
        "driver_quota_larger_than_u32_is_not_truncated_to_per_read_permit_width",
        1,
        1,
        quota,
        &budget,
    ) else {
        return;
    };
    // Startup only: no application read or multi-gigabyte buffer is allocated.
    assert_eq!(driver.stats().submitted, 0);
    assert_eq!(budget.available(), quota);
    driver.shutdown();
    assert_eq!(budget.available(), quota * 2);
}

#[tokio::test(flavor = "current_thread")]
async fn shutting_down_one_driver_does_not_close_another_drivers_admission() {
    let budget = SharedReadBudget::new(16).expect("two driver quotas");
    let Some(first) = driver_or_skip("shutting_down_one_driver_does_not_close_another_drivers_admission", 1, 1, 8, &budget)
    else {
        return;
    };
    let second = driver_or_skip("independent peer driver", 1, 2, 8, &budget)
        .expect("second driver must start")
        .with_shard_policy(ShardPolicy::CapacityAware);
    let (read, mut write) = pipe();
    let active = second.read_current(read, 8);
    wait_submitted(&second, 1).await;
    first.request_shutdown();
    let rejected = first.read_at(Arc::new(File::open("/dev/zero").expect("open fixture")), 0, 8);
    assert!(read_result(rejected).await.is_err());
    wait_finished(&first).await;
    assert_eq!(first.shutdown().in_flight, 0);
    assert_eq!(budget.available(), 8);
    write.write_all(b"12345678").expect("complete peer pipe read");
    let retained = read_result(active)
        .await
        .expect("peer read must survive another driver stopping");
    assert_eq!(retained, b"12345678");
    let zero = Arc::new(File::open("/dev/zero").expect("open fixture"));
    let batch = second
        .read_at_batch(vec![
            ReadRequest {
                file: Arc::clone(&zero),
                offset: 0,
                len: 4,
            },
            ReadRequest {
                file: zero,
                offset: 4,
                len: 4,
            },
        ])
        .expect("capacity-aware peer accepts new batch reads");
    for next in batch {
        assert_eq!(read_result(next).await.expect("peer remains open to new reads"), [0; 4]);
    }
    let stats = second.shutdown();
    assert_eq!(stats.submitted, 3);
    assert_eq!(stats.submitted, stats.delivered + stats.orphan_reclaimed);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(budget.available(), 16, "retained caller Vec is outside driver in-flight quota");
    assert_eq!(retained, b"12345678");
}

#[tokio::test(flavor = "current_thread")]
async fn unpolled_deferred_count_handle_retains_whole_quota_after_clean_driver_shutdown() {
    let budget = SharedReadBudget::new(8).expect("one driver quota");
    let Some(driver) = driver_or_skip(
        "unpolled_deferred_count_handle_retains_whole_quota_after_clean_driver_shutdown",
        1,
        1,
        8,
        &budget,
    ) else {
        return;
    };
    let (read, _write) = pipe();
    let active = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    let waiting = driver.read_at(Arc::new(File::open("/dev/zero").expect("open fixture")), 0, 8);
    // No poll: this handle has not joined the local count queue or submitted I/O.
    driver.request_shutdown();
    assert!(read_result(active).await.is_err());
    wait_finished(&driver).await;
    assert_eq!(driver.shutdown().in_flight, 0);
    assert_eq!(budget.available(), 0, "deferred handle still owns the retired driver's receipt");
    assert_would_block(UringDriver::probe_and_start_with_shared_budget(1, 1, limits(8), &budget));
    drop(waiting);
    assert_eq!(budget.available(), 8);
    let replacement = driver_or_skip("quota recycled after final deferred owner", 1, 1, 8, &budget).expect("quota must recycle");
    replacement.shutdown();
    assert_eq!(budget.available(), 8);
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_polled_byte_waiter_releases_local_reservation_but_live_driver_keeps_quota() {
    let budget = SharedReadBudget::new(8).expect("one driver quota");
    let Some(driver) = driver_or_skip(
        "canceled_polled_byte_waiter_releases_local_reservation_but_live_driver_keeps_quota",
        2,
        1,
        8,
        &budget,
    ) else {
        return;
    };
    let (read, mut write) = pipe();
    let active = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    let zero = Arc::new(File::open("/dev/zero").expect("open fixture"));
    let mut waiting = driver.read_at(Arc::clone(&zero), 0, 8);
    assert!(
        Pin::new(&mut waiting)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(waiting);
    assert_eq!(budget.available(), 0, "a live idle-or-busy driver retains its entire quota");
    write.write_all(b"12345678").expect("complete original read");
    assert_eq!(read_result(active).await.expect("original read completes"), b"12345678");
    assert_eq!(
        read_result(driver.read_at(zero, 0, 8))
            .await
            .expect("canceled waiter must not strand local permits"),
        [0; 8]
    );
    let stats = driver.shutdown();
    assert_eq!(stats.submitted, 2, "canceled deferred request never submitted");
    assert_eq!(stats.submitted, stats.delivered + stats.orphan_reclaimed);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(budget.available(), 8);
}

#[tokio::test(flavor = "current_thread")]
async fn retired_drivers_polled_byte_waiter_returns_final_receipt_when_dropped() {
    let budget = SharedReadBudget::new(8).expect("one driver quota");
    let Some(driver) = driver_or_skip("retired_drivers_polled_byte_waiter_returns_final_receipt_when_dropped", 2, 1, 8, &budget)
    else {
        return;
    };
    let (read, _write) = pipe();
    let active = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    let mut waiting = driver.read_at(Arc::new(File::open("/dev/zero").expect("open fixture")), 0, 8);
    assert!(
        Pin::new(&mut waiting)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    driver.request_shutdown();
    assert!(read_result(active).await.is_err());
    wait_finished(&driver).await;
    assert_eq!(driver.shutdown().in_flight, 0);
    assert_eq!(budget.available(), 0, "unconsumed closed waiter retains its receipt");
    assert!(read_result(waiting).await.is_err()); // Consumes and drops the handle.
    assert_eq!(budget.available(), 8);
}

#[cfg(feature = "fault-injection")]
#[test]
fn leaked_read_retains_entire_driver_quota_without_closing_shared_budget() {
    const CHILD: &str = "RUSTFS_URING_SHARED_QUOTA_LEAK_CHILD";
    const MARKER: &str = "SHARED_QUOTA_LEAK_RETENTION_OK";
    let name = "leaked_read_retains_entire_driver_quota_without_closing_shared_budget";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("child runtime");
        runtime.block_on(async {
            let budget = SharedReadBudget::new(12).expect("shared test capacity");
            let Some(driver) = driver_or_skip(name, 2, 1, 8, &budget) else { return };
            let driver = driver.with_shard_policy(ShardPolicy::CapacityAware);
            // The capacity-aware eager path and batch path must carry the same
            // receipt as ordinary submit. Leak one byte, retain the whole eight.
            let active = driver
                .read_at_batch(vec![ReadRequest {
                    file: Arc::new(File::open("/dev/zero").expect("open leak fixture")),
                    offset: 0,
                    len: 1,
                }])
                .expect("single-read batch")
                .pop()
                .expect("batch contains its one read");
            wait_submitted(&driver, 1).await;
            driver.request_shutdown();
            assert!(read_result(active).await.is_err());
            wait_finished(&driver).await;
            assert_eq!(driver.shutdown().in_flight, 1, "injected drain must really leak a pending entry");
            assert_eq!(budget.available(), 4, "leak must not refund any of the driver's whole quota");
            assert_would_block(UringDriver::probe_and_start_with_shared_budget(1, 1, limits(5), &budget));
            // Startup probe is independent of the drive-loop stuck-CQE seam.
            // No application read is submitted to this second driver.
            let peer = driver_or_skip("healthy peer uses unreserved capacity", 1, 1, 4, &budget)
                .expect("remaining capacity must stay usable");
            assert_eq!(budget.available(), 0);
            assert_eq!(peer.shutdown().in_flight, 0);
            assert_eq!(budget.available(), 4);
            assert_would_block(UringDriver::probe_and_start_with_shared_budget(1, 1, limits(5), &budget));
            eprintln!("{MARKER}");
        });
        return;
    }
    let budget = SharedReadBudget::new(12).expect("parent precondition capacity");
    let Some(driver) = driver_or_skip(name, 1, 1, 8, &budget) else { return };
    driver.shutdown();
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .env("RUSTFS_URING_FAULT_STUCK_DRAIN", "1")
        .env("RUSTFS_URING_FAULT_DRAIN_TIMEOUT_MS", "400")
        .output()
        .expect("run isolated leaked-quota case");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "child failed: {stderr}");
    assert!(stderr.contains(MARKER), "child did not prove the leaked-quota path: {stderr}");
}
