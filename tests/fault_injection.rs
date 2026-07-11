// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Deterministic fault-injection tests for the cancel-safety escape hatches
//! (rustfs/backlog#1103). These exercise failure modes the normal API cannot
//! reach — a driver-thread panic (#1054), a completion that never arrives
//! (#1055), and a probe that cannot confirm its read op (#1053) — so each needs
//! a production seam that exists only under the `fault-injection` feature. The
//! whole file is gated on it.
//!
//! Skip contract is identical to `cancel.rs`: in a restricted environment
//! (Docker default seccomp, gVisor) the probe fails with an expected-restriction
//! errno and every test degrades to a `SKIP`. Run under
//! `--security-opt seccomp=unconfined` (run-docker.sh leg 2) to exercise the
//! real io_uring paths.
#![cfg(all(target_os = "linux", feature = "fault-injection"))]

use std::fs::File;
use std::os::fd::FromRawFd;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustfs_uring::UringDriver;

/// A pipe whose read side blocks until the write side is written or closed — the
/// only portable way to hold an op provably in flight.
fn os_pipe() -> (Arc<File>, File) {
    let mut fds = [0i32; 2];
    // SAFETY: `fds` is a valid two-int array; pipe(2) fills it with owned fds.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "pipe(2) failed");
    // SAFETY: both fds are freshly owned by this process.
    let read = unsafe { File::from_raw_fd(fds[0]) };
    let write = unsafe { File::from_raw_fd(fds[1]) };
    (Arc::new(read), write)
}

fn wait_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    cond()
}

const ABORT_CHILD_ENV: &str = "RUSTFS_URING_FAULT_ABORT_CHILD";

/// #1054 (C2): a driver-thread panic must ABORT the process — never unwind and
/// free the in-flight buffers the kernel can still write into. The abort itself
/// takes down the process, so it runs in a child re-exec of this test binary and
/// the parent asserts the child died from `SIGABRT`.
#[test]
fn driver_panic_aborts_instead_of_freeing_in_flight_buffers() {
    if std::env::var_os(ABORT_CHILD_ENV).is_some() {
        run_abort_child();
        // Only reachable if the abort barrier failed to fire.
        eprintln!("child: abort barrier did NOT fire within the wait window — safety regression");
        std::process::exit(0);
    }

    // Parent. Honor the leg-1 skip contract before spawning anything.
    match UringDriver::probe_and_start(64) {
        Ok(d) => {
            let _ = d.shutdown();
        }
        Err(e) => {
            assert!(
                e.is_expected_restriction(),
                "probe failed OUTSIDE the expected restriction errno class: {e:?}"
            );
            eprintln!("SKIP driver_panic_aborts_instead_of_freeing_in_flight_buffers: restricted environment ({e:?})");
            return;
        }
    }

    let exe = std::env::current_exe().expect("locate test binary");
    let status = std::process::Command::new(exe)
        .args([
            "--exact",
            "driver_panic_aborts_instead_of_freeing_in_flight_buffers",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ABORT_CHILD_ENV, "1")
        .status()
        .expect("spawn abort child");

    assert_eq!(
        status.signal(),
        Some(libc::SIGABRT),
        "a driver-thread panic must abort the process (leak over UAF), got {status:?}"
    );
}

fn run_abort_child() {
    let driver = match UringDriver::probe_and_start(64) {
        Ok(d) => d,
        Err(e) => {
            // leg-1 child: no io_uring. The parent skipped too and never inspects
            // this exit; exit cleanly.
            eprintln!("SKIP abort child: restricted environment ({e:?})");
            std::process::exit(0);
        }
    };
    let (pipe_read, pipe_write) = os_pipe();
    // Never awaited, never cancelled: the read stays in flight so `pending` is
    // non-empty when the driver thread panics.
    let _handle = driver.read_current(Arc::clone(&pipe_read), 4096).without_cancel_on_drop();
    assert!(
        wait_until(Duration::from_secs(2), || driver.stats().in_flight == 1),
        "read never reached in-flight state"
    );
    driver.test_inject_panic();
    // The abort should fire on the driver thread within milliseconds; give it a
    // generous window before declaring a regression.
    std::thread::sleep(Duration::from_secs(3));
    // Kept alive until here so the read never hit EOF before the panic.
    drop(pipe_write);
}

/// #1055 (C4): when an in-flight op's completion never comes, `shutdown`'s
/// bounded drain must bail out at `DRAIN_TIMEOUT` and LEAK the ring + buffers
/// (memory-safe) rather than unmap under the kernel or block forever. The seam
/// drops the op's (cancel-induced) completion so it stays pending; the drain
/// timeout is shortened via env so the test is fast.
#[test]
fn bounded_drain_bails_out_and_leaks_on_a_stuck_op() {
    // Set before the driver thread starts — it reads these once at spawn.
    // SAFETY: `--test-threads=1` serializes tests, so no other thread reads the
    // environment concurrently.
    unsafe {
        std::env::set_var("RUSTFS_URING_FAULT_STUCK_DRAIN", "1");
        std::env::set_var("RUSTFS_URING_FAULT_DRAIN_TIMEOUT_MS", "400");
    }

    let driver = match UringDriver::probe_and_start(64) {
        Ok(d) => d,
        Err(e) => {
            clear_stuck_env();
            assert!(
                e.is_expected_restriction(),
                "probe failed OUTSIDE the expected restriction errno class: {e:?}"
            );
            eprintln!("SKIP bounded_drain_bails_out_and_leaks_on_a_stuck_op: restricted environment ({e:?})");
            return;
        }
    };

    let (pipe_read, pipe_write) = os_pipe();
    let _handle = driver.read_current(Arc::clone(&pipe_read), 4096).without_cancel_on_drop();
    assert!(
        wait_until(Duration::from_secs(2), || driver.stats().in_flight == 1),
        "read never reached in-flight state"
    );

    let start = Instant::now();
    let snap = driver.shutdown();
    let elapsed = start.elapsed();
    clear_stuck_env();

    assert!(
        elapsed >= Duration::from_millis(300),
        "shutdown returned before the bounded-drain deadline ({elapsed:?}) — the timeout path was not taken"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "shutdown took {elapsed:?} — the shortened fault drain-timeout was not honored (real 5 s path?)"
    );
    assert!(
        snap.in_flight >= 1,
        "the stuck op should still count as in flight after the leak-over-UAF bailout: {snap:?}"
    );
    // Reaching here without a crash proves the ring + buffers were leaked (kept
    // mapped/allocated), not unmapped under an in-flight op.
    drop(pipe_write);
}

/// #1161: the bounded-drain bailout must FAIL every stranded caller before it
/// leaks the pending table, so a `ReadHandle` still awaited across the timeout
/// resolves with an error instead of hanging forever. Same stuck-op seam as
/// above, but this time the handle is kept and awaited after shutdown.
#[test]
fn stranded_handle_errors_after_bounded_drain_bailout() {
    // SAFETY: `--test-threads=1` serializes tests; no concurrent env readers.
    unsafe {
        std::env::set_var("RUSTFS_URING_FAULT_STUCK_DRAIN", "1");
        std::env::set_var("RUSTFS_URING_FAULT_DRAIN_TIMEOUT_MS", "400");
    }

    let driver = match UringDriver::probe_and_start(64) {
        Ok(d) => d,
        Err(e) => {
            clear_stuck_env();
            assert!(
                e.is_expected_restriction(),
                "probe failed OUTSIDE the expected restriction errno class: {e:?}"
            );
            eprintln!("SKIP stranded_handle_errors_after_bounded_drain_bailout: restricted environment ({e:?})");
            return;
        }
    };

    let (pipe_read, pipe_write) = os_pipe();
    // Keep the handle (do not drop it): its oneshot receiver must still be live
    // when the bailout runs so we can prove the bailout delivered an error.
    let handle = driver.read_current(Arc::clone(&pipe_read), 4096).without_cancel_on_drop();
    assert!(
        wait_until(Duration::from_secs(2), || driver.stats().in_flight == 1),
        "read never reached in-flight state"
    );

    // Drive the stuck op to the bounded-drain bailout.
    let snap = driver.shutdown();
    clear_stuck_env();
    assert!(
        snap.in_flight >= 1,
        "the stuck op should still count as in flight after the leak-over-UAF bailout: {snap:?}"
    );

    // The still-awaited handle MUST resolve with an error within a bounded
    // window — never hang (rustfs/backlog#1161). Before the fix the forgotten
    // oneshot sender left this future pending forever and the timeout would fire.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("build current-thread runtime");
    match rt.block_on(async { tokio::time::timeout(Duration::from_secs(2), handle).await }) {
        Ok(Err(_)) => {} // driver-gone error delivered — correct.
        Ok(Ok(_)) => panic!("stranded handle unexpectedly succeeded on a stuck op"),
        Err(_) => panic!("stranded handle HUNG after bounded-drain bailout (rustfs/backlog#1161 regression)"),
    }
    drop(pipe_write);
}

fn clear_stuck_env() {
    // SAFETY: `--test-threads=1` teardown; no concurrent environment readers.
    unsafe {
        std::env::remove_var("RUSTFS_URING_FAULT_STUCK_DRAIN");
        std::env::remove_var("RUSTFS_URING_FAULT_DRAIN_TIMEOUT_MS");
    }
}

/// #1053 (C1): if the startup probe cannot confirm its read op terminated it
/// must LEAK the probe buffer/file (the kernel may still write into them) and
/// return a `ReadOp` failure — never drop them under an in-flight SQE and never
/// crash. The seam forces the failure only AFTER the real CQE has arrived, so no
/// live write races the leak; the point is to cover the fallback branch and
/// prove a forced probe failure degrades cleanly without corrupting global state.
#[test]
fn probe_drain_failure_leaks_and_degrades() {
    // SAFETY: `--test-threads=1` setup; no concurrent environment readers.
    unsafe {
        std::env::set_var("RUSTFS_URING_FAULT_PROBE_DRAIN", "1");
    }
    let result = UringDriver::probe_and_start(64);
    // SAFETY: `--test-threads=1` teardown.
    unsafe {
        std::env::remove_var("RUSTFS_URING_FAULT_PROBE_DRAIN");
    }

    match result {
        Ok(_) => panic!("probe must fail when the drain is forced to error"),
        Err(e) if e.is_expected_restriction() => {
            // leg 1: io_uring was blocked before the probe read op ran, so the
            // forced-drain fault was never reached. Honor the skip contract.
            eprintln!("SKIP probe_drain_failure_leaks_and_degrades: restricted environment ({e:?})");
        }
        Err(e) => {
            // leg 2: the forced ReadOp failure ran the leak-over-UAF fallback and
            // the process is still healthy. It must NOT be misclassified as an
            // environment restriction (that would wrongly latch a healthy disk).
            assert!(
                !e.is_expected_restriction(),
                "forced probe-drain failure must not look like an environment restriction: {e:?}"
            );
            // A subsequent NORMAL probe must still succeed — the forced fault did
            // not corrupt any global state.
            let driver = UringDriver::probe_and_start(64).expect("a normal probe after a forced-fault probe must still work");
            let _ = driver.shutdown();
        }
    }
}

/// #1164: driver-thread spawn failure (EAGAIN under a cgroup pids-limit /
/// RLIMIT_NPROC) must degrade to a `ProbeFailure` so the caller selects the std
/// backend — never panic out of async disk init. The seam forces the spawn step
/// to fail; the probe must return an error, not unwind, and leave global state
/// intact for a later normal probe.
#[test]
fn spawn_failure_degrades_instead_of_panicking() {
    // SAFETY: `--test-threads=1` serializes tests; no concurrent env readers.
    unsafe {
        std::env::set_var("RUSTFS_URING_FAULT_SPAWN", "1");
    }
    let result = UringDriver::probe_and_start_sharded(64, 3);
    // SAFETY: `--test-threads=1` teardown.
    unsafe {
        std::env::remove_var("RUSTFS_URING_FAULT_SPAWN");
    }

    match result {
        Ok(_) => panic!("driver started despite the forced spawn failure"),
        Err(e) if e.is_expected_restriction() => {
            // leg 1: io_uring was blocked before the probe reached the spawn seam.
            eprintln!("SKIP spawn_failure_degrades_instead_of_panicking: restricted environment ({e:?})");
        }
        Err(e) => {
            // leg 2: the forced EAGAIN propagated as a ProbeFailure (degrade, not
            // panic). EAGAIN is not a restriction errno, so the caller logs an
            // unexpected-probe warning and still falls back to the std backend.
            assert!(
                !e.is_expected_restriction(),
                "forced spawn EAGAIN must not look like an environment restriction: {e:?}"
            );
            // A subsequent NORMAL probe must still succeed — the aborted start
            // cleaned up any partially-started shard without corrupting state.
            let driver =
                UringDriver::probe_and_start_sharded(64, 2).expect("a normal probe after a forced spawn failure must still work");
            let _ = driver.shutdown();
        }
    }
}
