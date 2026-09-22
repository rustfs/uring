//! Native Linux resource-admission tests (rustfs/backlog#2647).
#![cfg(target_os = "linux")]

use std::fs::File;
use std::io::Write;
use std::os::fd::FromRawFd;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use rustfs_uring::{ReadLimits, UringDriver};

fn driver_or_skip() -> Option<UringDriver> {
    match UringDriver::probe_and_start_with_limits(
        8,
        2,
        ReadLimits {
            max_read_len: Some(8),
            max_in_flight_bytes: Some(8),
        },
    ) {
        Ok(driver) => Some(driver),
        Err(err) => {
            assert!(err.is_expected_restriction(), "unexpected probe failure: {err}");
            eprintln!("SKIP admission: restricted environment ({err})");
            None
        }
    }
}

fn pipe() -> (Arc<File>, File) {
    let mut fds = [0; 2];
    // SAFETY: valid output array; successful pipe creates two owned descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: each fresh descriptor transfers ownership exactly once.
    unsafe { (Arc::new(File::from_raw_fd(fds[0])), File::from_raw_fd(fds[1])) }
}

async fn wait_submitted(driver: &UringDriver, count: u64) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while driver.stats().submitted != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("read did not enter the driver");
}

async fn assert_pending(future: &mut rustfs_uring::ReadHandle) {
    std::future::poll_fn(|cx| {
        assert!(std::pin::Pin::new(&mut *future).poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn orphan_holds_shared_bytes_until_its_real_read_completes() {
    let Some(driver) = driver_or_skip() else { return };
    let (read, mut write) = pipe();
    let first = driver.read_current(read, 8).without_cancel_on_drop();
    wait_submitted(&driver, 1).await;
    let mut next = driver.read_at(Arc::new(File::open("/dev/zero").unwrap()), 0, 8);
    assert_pending(&mut next).await;
    drop(first);
    assert_pending(&mut next).await;
    assert_eq!(driver.stats().submitted, 1);
    write.write_all(b"12345678").unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), next).await.unwrap().unwrap(), vec![0; 8]);
    let stats = driver.shutdown();
    assert_eq!(stats.orphan_reclaimed, 1);
    assert_eq!(stats.submitted, 2);
    assert_eq!(stats.in_flight, 0);
}

#[tokio::test]
async fn dropping_a_byte_waiter_does_not_submit_or_strand_capacity() {
    let Some(driver) = driver_or_skip() else { return };
    let (read, mut write) = pipe();
    let first = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    let file = Arc::new(File::open("/dev/zero").unwrap());
    let mut canceled = driver.read_at(file.clone(), 0, 8);
    assert_pending(&mut canceled).await;
    drop(canceled);
    write.write_all(b"12345678").unwrap();
    assert_eq!(first.await.unwrap(), b"12345678");
    let result = tokio::time::timeout(Duration::from_secs(2), driver.read_at(file, 0, 8))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, vec![0; 8]);
    assert_eq!(driver.shutdown().submitted, 2);
}

#[tokio::test]
async fn shutdown_wakes_byte_waiters_while_canceling_pending_read() {
    let Some(driver) = driver_or_skip() else { return };
    let (read, _write) = pipe();
    let first = driver.read_current(read, 8);
    wait_submitted(&driver, 1).await;
    let mut waiting = driver.read_at(Arc::new(File::open("/dev/zero").unwrap()), 0, 8);
    assert_pending(&mut waiting).await;
    driver.shutdown();
    assert!(tokio::time::timeout(Duration::from_secs(2), waiting).await.unwrap().is_err());
    assert!(tokio::time::timeout(Duration::from_secs(2), first).await.unwrap().is_err());
}

#[tokio::test]
async fn completed_results_do_not_hold_driver_byte_admission() {
    let Some(driver) = driver_or_skip() else { return };
    let file = Arc::new(File::open("/dev/zero").unwrap());
    let retained = driver.read_at(file.clone(), 0, 8).await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(2), driver.read_at(file, 0, 8))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained, next);
    assert_eq!(driver.shutdown().submitted, 2);
}
