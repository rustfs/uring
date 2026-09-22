// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0
#![cfg(target_os = "linux")]

use std::fs::File;
use std::os::fd::FromRawFd;
use std::sync::Arc;
use std::time::Duration;

use rustfs_uring::{ShardPolicy, UringDriver};

async fn exercise_busy_shard(policy: ShardPolicy) {
    let driver = match UringDriver::probe_and_start_sharded(1, 2) {
        Ok(driver) => driver.with_shard_policy(policy),
        Err(error) => {
            assert!(error.is_expected_restriction(), "unexpected setup error: {error}");
            eprintln!("SKIP shard_policy {policy:?}: {error}");
            return;
        }
    };
    let mut fds = [0; 2];
    // SAFETY: the array holds two fds; each successful fd is owned by one File.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    let pipe_read = Arc::new(unsafe { File::from_raw_fd(fds[0]) });
    let pipe_write = unsafe { File::from_raw_fd(fds[1]) };
    let held = driver.read_current(pipe_read, 8); // shard 0 stays blocked
    let path = std::env::temp_dir().join(format!("uring-shard-policy-{policy:?}-{}.bin", std::process::id()));
    std::fs::write(&path, [42; 8]).unwrap();
    let file = Arc::new(File::open(&path).unwrap());
    // Advance the cursor over shard 1 without occupying its permit. Waiting for
    // stats.in_flight after a real read would race that read's permit destructor.
    assert_eq!(
        driver.read_at(Arc::clone(&file), u64::MAX, 8).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while driver.stats().in_flight != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("only the blocked pipe should remain");

    let mut read = driver.read_at(file, 0, 8); // round-robin starts at busy shard 0
    match policy {
        ShardPolicy::CapacityAware => {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), &mut read)
                    .await
                    .expect("must use the idle shard")
                    .unwrap(),
                [42; 8]
            );
            // The pipe writer remains open and empty throughout this read.
            drop(held);
        }
        ShardPolicy::RoundRobin => {
            assert!(tokio::time::timeout(Duration::from_millis(100), &mut read).await.is_err());
            drop(held);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), read)
                    .await
                    .expect("cancellation should release the original shard")
                    .unwrap(),
                [42; 8]
            );
        }
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while driver.stats().in_flight != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reads should drain after cancellation");
    let snapshot = driver.shutdown();
    assert_eq!(snapshot.submitted, 2);
    assert_eq!(snapshot.delivered, 1);
    assert_eq!(snapshot.orphan_reclaimed, 1);
    drop(pipe_write);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_aware_bypasses_a_shard_saturated_by_a_blocked_pipe() {
    exercise_busy_shard(ShardPolicy::CapacityAware).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_robin_still_waits_for_its_original_busy_shard() {
    exercise_busy_shard(ShardPolicy::RoundRobin).await;
}
