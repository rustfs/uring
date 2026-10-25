// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Native completion ownership must outlive the caller's future.
#![cfg(target_os = "linux")]

use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::os::fd::FromRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use rustfs_uring::OwnedUringDriver;

struct Owner(Arc<AtomicUsize>);

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn driver() -> Option<OwnedUringDriver<Owner>> {
    match OwnedUringDriver::probe_and_start(2) {
        Ok(driver) => Some(driver),
        Err(error) => {
            assert!(error.is_expected_restriction(), "unexpected probe failure: {error}");
            eprintln!("SKIP completion owner: restricted io_uring ({error})");
            None
        }
    }
}

fn owner() -> (Owner, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (Owner(Arc::clone(&drops)), drops)
}

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}

fn pipe() -> (Arc<File>, File) {
    let mut fds = [-1; 2];
    // SAFETY: pipe2 initializes both descriptors on success; each is owned once.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: each successful pipe descriptor is transferred to one File.
    unsafe { (Arc::new(File::from_raw_fd(fds[0])), File::from_raw_fd(fds[1])) }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "native completion deadline");
        std::thread::yield_now();
    }
}

#[test]
fn submitted_owner_survives_caller_drop() {
    let Some(driver) = driver() else { return };
    let (reader, mut writer) = pipe();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Owner(Arc::clone(&drops));
    let future = driver.read_current_with_owner(reader, 4, owner).without_cancel_on_drop();
    wait_until(|| driver.stats().in_flight == 1);
    drop(future);
    let premature_release = drops.load(Ordering::SeqCst);
    assert_eq!(driver.stats().in_flight, 1, "pipe has no data or EOF");
    writer.write_all(b"done").expect("complete native pipe read");
    wait_until(|| driver.stats().in_flight == 0);
    assert_eq!(driver.shutdown().orphan_reclaimed, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1, "release after completion");
    assert_eq!(premature_release, 0, "caller drop must not release a pending read owner");
}

#[test]
fn native_owner_survives_request_runtime_shutdown() {
    let Some(driver) = driver() else { return };
    let (reader, mut writer) = pipe();
    let (owner, drops) = owner();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("request runtime");
    let future = driver.read_current_with_owner(reader, 4, owner).without_cancel_on_drop();
    let task = runtime.spawn(future);
    runtime.block_on(tokio::task::yield_now());
    wait_until(|| driver.stats().in_flight == 1);
    drop(runtime);
    assert!(task.is_finished(), "runtime shutdown drops the requesting task");
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    writer
        .write_all(b"done")
        .expect("complete native read after runtime shutdown");
    wait_until(|| driver.stats().in_flight == 0);
    assert_eq!(driver.shutdown().orphan_reclaimed, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn default_drop_cancellation_reclaims_owner_with_native_read() {
    let Some(driver) = driver() else { return };
    let (reader, _writer) = pipe();
    let (owner, drops) = owner();
    let handle = driver.read_current_with_owner(reader, 4, owner);
    wait_until(|| driver.stats().in_flight == 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(handle);
    wait_until(|| driver.stats().in_flight == 0);
    assert_eq!(driver.shutdown().orphan_reclaimed, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn unsubmitted_owner_releases_when_waiter_is_dropped() {
    let Some(driver) = driver() else { return };
    let (reader, _writer) = pipe();
    let busy = [
        driver.read_current(Arc::clone(&reader), 4),
        driver.read_current(Arc::clone(&reader), 4),
    ];
    wait_until(|| driver.stats().in_flight == 2);
    let (owner, drops) = owner();
    let waiting = driver.read_current_with_owner(reader, 4, owner);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(waiting);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(busy);
    let stats = driver.shutdown();
    assert_eq!(stats.submitted, 2, "unpolled waiter never submitted");
    assert_eq!(stats.in_flight, 0);
}

#[test]
fn deferred_owner_moves_to_native_completion_after_admission() {
    let Some(driver) = driver() else { return };
    let (busy_reader, _busy_writer) = pipe();
    let first = driver.read_current(Arc::clone(&busy_reader), 4);
    let second = driver.read_current(busy_reader, 4);
    wait_until(|| driver.stats().in_flight == 2);
    let (reader, mut writer) = pipe();
    let (owner, drops) = owner();
    let mut waiting = driver.read_current_with_owner(reader, 4, owner).without_cancel_on_drop();
    assert!(poll_once(&mut waiting).is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(first);
    wait_until(|| {
        assert!(poll_once(&mut waiting).is_pending());
        driver.stats().submitted == 3
    });
    drop(waiting);
    assert_eq!(drops.load(Ordering::SeqCst), 0, "admitted owner moved off caller");
    writer.write_all(b"done").expect("complete admitted read");
    wait_until(|| drops.load(Ordering::SeqCst) == 1);
    drop(second);
    assert_eq!(driver.shutdown().in_flight, 0);
}

#[test]
fn closed_admission_releases_owner_even_if_finished_handle_is_retained() {
    let Some(driver) = driver() else { return };
    let (reader, _writer) = pipe();
    let busy = [
        driver.read_current(Arc::clone(&reader), 4),
        driver.read_current(Arc::clone(&reader), 4),
    ];
    wait_until(|| driver.stats().in_flight == 2);
    let (owner, drops) = owner();
    let mut waiting = driver.read_current_with_owner(reader, 4, owner);
    assert!(poll_once(&mut waiting).is_pending());
    driver.request_shutdown();
    assert!(matches!(poll_once(&mut waiting), Poll::Ready(Err(_))));
    assert_eq!(drops.load(Ordering::SeqCst), 1, "finished waiter must release its owner");
    drop(busy);
    assert_eq!(driver.shutdown().in_flight, 0);
    drop(waiting);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rejected_and_empty_reads_release_owners_at_their_completion_boundary() {
    let Some(driver) = driver() else { return };
    let file = Arc::new(File::open("/dev/null").expect("fixture"));
    for (offset, len, align) in [(u64::MAX, 4, 1), (0, 4, 3)] {
        let (owner, drops) = owner();
        let mut handle = driver.read_at_direct_with_owner(Arc::clone(&file), offset, len, align, owner);
        let Poll::Ready(result) = poll_once(&mut handle) else { panic!("unsubmitted read must be ready") };
        assert_eq!(result.expect_err("invalid parameters").kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(drops.load(Ordering::SeqCst), 1, "rejected owner releases immediately");
    }
    assert_eq!(driver.stats().submitted, 0);
    // Zero-length reads follow the normal submission path in the existing API.
    let (owner, drops) = owner();
    let empty = driver.read_at_with_owner(file, 0, 0, owner);
    let result = tokio::time::timeout(Duration::from_secs(5), empty)
        .await
        .expect("empty read deadline")
        .expect("empty read result");
    assert!(result.is_empty());
    wait_until(|| drops.load(Ordering::SeqCst) == 1);
    assert_eq!(driver.shutdown().submitted, 1);
}

#[tokio::test]
async fn buffered_and_direct_results_do_not_retain_completion_owners() {
    let Some(driver) = driver() else { return };
    let path = std::env::temp_dir().join(format!("uring-completion-owner-{}", std::process::id()));
    let content: Vec<_> = (0..8192).map(|index| (index % 251) as u8).collect();
    std::fs::write(&path, &content).expect("write fixture");
    for (direct, length) in [(false, 4097), (true, 4097), (false, 16384), (true, 16384)] {
        let file = Arc::new(
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(if direct { libc::O_DIRECT } else { 0 })
                .open(&path)
                .expect("open actual read mode"),
        );
        let (owner, drops) = owner();
        let handle = if direct {
            driver.read_at_direct_with_owner(file, 17, length, 4096, owner)
        } else {
            driver.read_at_with_owner(file, 17, length, owner)
        };
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("read deadline")
            .expect("native read");
        assert_eq!(result, content[17..(17 + length).min(content.len())]);
        wait_until(|| drops.load(Ordering::SeqCst) == 1);
        assert_eq!(
            result.len(),
            length.min(content.len() - 17),
            "retained bytes do not retain the native owner"
        );
    }
    assert_eq!(driver.shutdown().delivered, 4);
    std::fs::remove_file(path).expect("remove fixture");
}

#[cfg(feature = "fault-injection")]
#[test]
fn unresolved_native_completion_keeps_owner_on_bounded_shutdown() {
    const CHILD: &str = "RUSTFS_URING_OWNER_DRAIN_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let Some(probe) = driver() else { return };
        assert_eq!(probe.shutdown().in_flight, 0);
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "unresolved_native_completion_keeps_owner_on_bounded_shutdown",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("RUSTFS_URING_FAULT_STUCK_DRAIN", "1")
            .env("RUSTFS_URING_FAULT_DRAIN_TIMEOUT_MS", "50")
            .output()
            .expect("bounded-drain subprocess");
        assert!(output.status.success(), "child failed: {}", String::from_utf8_lossy(&output.stderr));
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("SKIP "),
            "child must execute native path"
        );
        return;
    }
    let Some(driver) = driver() else { panic!("child must run real io_uring") };
    let (reader, _writer) = pipe();
    let (owner, drops) = owner();
    let handle = driver.read_current_with_owner(reader, 4, owner);
    wait_until(|| driver.stats().in_flight == 1);
    drop(handle);
    let stats = driver.shutdown();
    assert_eq!(stats.in_flight, 1, "fault seam withholds the native completion");
    assert_eq!(drops.load(Ordering::SeqCst), 0, "unresolved I/O must retain its owner");
}
