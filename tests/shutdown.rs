// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Native shutdown API tests. Expected io_uring restrictions print SKIP; only
//! an unrestricted Linux run proves the driver paths. Timeouts bound test
//! failures for ordinary pipes, not hung-kernel shutdown guarantees.
#![cfg(target_os = "linux")]

use std::fs::File;
use std::io;
use std::os::fd::FromRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rustfs_uring::{ReadHandle, ReadLimits, StatsSnapshot, UringDriver};

fn driver_or_skip(name: &str, entries: u32, limits: ReadLimits) -> Option<UringDriver> {
    match UringDriver::probe_and_start_with_limits(entries, 2, limits) {
        Ok(driver) => Some(driver),
        Err(error) => {
            assert!(error.is_expected_restriction(), "unexpected probe failure: {error}");
            eprintln!("SKIP {name}: restricted environment ({error})");
            None
        }
    }
}

fn pipe() -> (Arc<File>, File) {
    let mut fds = [0; 2];
    // SAFETY: valid output array; successful pipe2 creates two owned fds.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0, "create pending-read pipe");
    // SAFETY: transfer each fresh descriptor into exactly one File owner.
    unsafe { (Arc::new(File::from_raw_fd(fds[0])), File::from_raw_fd(fds[1])) }
}

async fn wait_submitted(driver: &UringDriver, count: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while driver.stats().submitted != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pipe reads should reach the driver");
}

async fn wait_complete(driver: &UringDriver) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !driver.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("ordinary test driver threads should exit");
}

async fn assert_read_error(handle: ReadHandle) {
    let result = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("shutdown must resolve a pending read handle");
    assert!(result.is_err(), "closed admission or canceled pipe must not return data");
}

fn assert_clean_conservation(stats: StatsSnapshot, submitted: u64) {
    assert_eq!(stats.submitted, submitted);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(stats.submitted, stats.delivered + stats.orphan_reclaimed);
}

async fn request_and_drain(name: &str, limits: ReadLimits) {
    let Some(driver) = driver_or_skip(name, 2, limits) else { return };
    assert!(!driver.is_finished(), "idle but live driver threads are not finished");
    let (read, _write) = pipe();
    let first = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    driver.request_shutdown();
    driver.request_shutdown(); // Idempotent even before a driver processes it.
    for _ in 0..2 {
        assert_read_error(driver.read_at(Arc::new(File::open("/dev/zero").expect("open zero fixture")), 0, 8)).await;
    }
    assert_read_error(first).await;
    wait_complete(&driver).await;
    driver.request_shutdown(); // Repeated request after thread exit is harmless.
    assert_clean_conservation(driver.shutdown(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn request_shutdown_drains_default_driver_and_rejects_new_reads() {
    request_and_drain("request_shutdown_drains_default_driver_and_rejects_new_reads", ReadLimits::default()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn request_shutdown_drains_byte_limited_driver_and_rejects_new_reads() {
    request_and_drain(
        "request_shutdown_drains_byte_limited_driver_and_rejects_new_reads",
        ReadLimits {
            max_read_len: Some(8),
            max_in_flight_bytes: Some(8),
        },
    )
    .await;
}

fn start_waiter(mut handle: ReadHandle) -> (tokio::task::JoinHandle<io::Result<Vec<u8>>>, tokio::sync::oneshot::Receiver<()>) {
    let (entered, ready) = tokio::sync::oneshot::channel();
    let mut entered = Some(entered);
    let waiter = tokio::spawn(async move {
        std::future::poll_fn(|cx| {
            let result = Pin::new(&mut handle).poll(cx);
            if result.is_pending()
                && let Some(entered) = entered.take()
            {
                let _ = entered.send(());
            }
            result
        })
        .await
    });
    (waiter, ready)
}

async fn waiting_admission_is_woken(name: &str, limited: bool) {
    let limits = if limited {
        ReadLimits {
            max_read_len: Some(8),
            max_in_flight_bytes: Some(8),
        }
    } else {
        ReadLimits::default()
    };
    let Some(driver) = driver_or_skip(name, 1, limits) else { return };
    let (read, _write) = pipe();
    let first = driver.read_current(read, 8); // Owner shard 0.
    wait_submitted(&driver, 1).await;
    let zero = Arc::new(File::open("/dev/zero").expect("open zero fixture"));
    let waiting = if limited {
        // Owner shard 1 has count capacity, but shared byte capacity is held.
        driver.read_at(Arc::clone(&zero), 0, 8)
    } else {
        // Advance the cursor over shard 1 with an invalid inert read; the real
        // waiter binds to saturated shard 0 without introducing another op.
        assert_read_error(driver.read_at(Arc::clone(&zero), u64::MAX, 8)).await;
        driver.read_at(Arc::clone(&zero), 0, 8)
    };
    let (waiter, ready) = start_waiter(waiting);
    tokio::time::timeout(Duration::from_secs(5), ready)
        .await
        .expect("waiter should be polled")
        .expect("waiter must actually register Pending before shutdown");
    driver.request_shutdown();
    let result = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("shutdown must wake the registered admission task")
        .expect("admission waiter task must not panic");
    assert!(result.is_err());
    assert_read_error(first).await;
    wait_complete(&driver).await;
    assert_clean_conservation(driver.shutdown(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn request_shutdown_wakes_registered_count_waiter() {
    waiting_admission_is_woken("request_shutdown_wakes_registered_count_waiter", false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn request_shutdown_wakes_registered_cross_shard_byte_waiter() {
    waiting_admission_is_woken("request_shutdown_wakes_registered_cross_shard_byte_waiter", true).await;
}

#[cfg(feature = "tokio-runtime")]
#[tokio::test(flavor = "current_thread")]
async fn shutdown_async_is_send_and_returns_drained_stats_on_current_thread() {
    fn require_send<T: Send>(value: T) -> T {
        value
    }
    let Some(driver) = driver_or_skip(
        "shutdown_async_is_send_and_returns_drained_stats_on_current_thread",
        2,
        ReadLimits {
            max_read_len: Some(8),
            max_in_flight_bytes: Some(8),
        },
    ) else {
        return;
    };
    let (read, _write) = pipe();
    let first = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    let stats = tokio::time::timeout(Duration::from_secs(10), require_send(driver.shutdown_async()))
        .await
        .expect("ordinary async shutdown should finish")
        .expect("blocking join task should succeed");
    assert_read_error(first).await;
    assert_clean_conservation(stats, 1);
}

#[cfg(feature = "tokio-runtime")]
#[tokio::test(flavor = "current_thread")]
async fn dropping_unpolled_shutdown_async_future_does_not_abandon_cleanup() {
    let Some(driver) = driver_or_skip(
        "dropping_unpolled_shutdown_async_future_does_not_abandon_cleanup",
        2,
        ReadLimits::default(),
    ) else {
        return;
    };
    let (read, _write) = pipe();
    let weak = Arc::downgrade(&read);
    let first = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    let shutdown = driver.shutdown_async();
    drop(shutdown); // Never poll the returned future.
    assert_read_error(first).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached shutdown still reclaims completed pending resources");
}

#[cfg(feature = "fault-injection")]
#[test]
fn is_finished_reports_thread_exit_not_clean_drain() {
    const CHILD: &str = "RUSTFS_URING_SHUTDOWN_NONCLEAN_CHILD";
    const MARKER: &str = "SHUTDOWN_THREAD_EXIT_NONCLEAN_OK";
    let name = "is_finished_reports_thread_exit_not_clean_drain";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("build child runtime");
        runtime.block_on(async {
            let Some(driver) = driver_or_skip(name, 2, ReadLimits::default()) else { return };
            let (read, _write) = pipe();
            let first = driver.read_current(read, 8);
            wait_submitted(&driver, 1).await;
            driver.request_shutdown();
            assert_read_error(first).await;
            wait_complete(&driver).await;
            assert_eq!(driver.stats().in_flight, 1, "fault seam must leave a leaked pending entry");
            assert_eq!(driver.shutdown().in_flight, 1);
            eprintln!("{MARKER}");
        });
        return;
    }
    let Some(driver) = driver_or_skip(name, 2, ReadLimits::default()) else { return };
    driver.shutdown();
    // Only the child inherits injected drain settings; no process-wide env
    // mutation can leak into this test binary's concurrently running tests.
    let output = std::process::Command::new(std::env::current_exe().expect("locate test executable"))
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .env("RUSTFS_URING_FAULT_STUCK_DRAIN", "1")
        .env("RUSTFS_URING_FAULT_DRAIN_TIMEOUT_MS", "400")
        .output()
        .expect("run isolated nonclean-shutdown test");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "child failed: {stderr}");
    assert!(stderr.contains(MARKER), "child did not execute the injected nonclean path: {stderr}");
}
