// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Native Linux io-wq startup reporting. Registration failure remains a valid
//! best-effort outcome; this test does not force that failure or prove a worker
//! limit was enforced by a particular I/O workload.
#![cfg(target_os = "linux")]

use std::fs::File;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use rustfs_uring::{IoWqRegistration, IoWqSetup, UringDriver};

fn assert_requested_limits(setup: &IoWqSetup) {
    assert_eq!(setup.requested, [16, 0], "requested limits must not be replaced by kernel output");
    match &setup.result {
        IoWqRegistration::Registered { .. } => {
            // The kernel returns PREVIOUS limits. Neither equality with [16, 0]
            // nor inequality is portable, and they are not a current-limit query.
        }
        IoWqRegistration::Failed {
            error_kind,
            raw_os_error,
        } => {
            if let Some(errno) = raw_os_error {
                assert_eq!(*error_kind, io::Error::from_raw_os_error(*errno).kind());
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn each_shard_reports_startup_iowq_result_without_changing_read_correctness() {
    let driver = match UringDriver::probe_and_start_sharded(8, 2) {
        Ok(driver) => driver,
        Err(error) => {
            assert!(error.is_expected_restriction(), "unexpected io_uring probe failure: {error}");
            eprintln!("SKIP each_shard_reports_startup_iowq_result_without_changing_read_correctness: {error}");
            return;
        }
    };
    let before = driver.shard_iowq_setup();
    assert_eq!(before.len(), 2, "one setup result is required for every started shard");
    for setup in &before {
        assert_requested_limits(setup);
    }

    // Default round-robin routes the two reads to distinct owning shards. This
    // uses real kernel reads but does not assert that /dev/zero used io-wq.
    let file = Arc::new(File::open("/dev/zero").expect("open deterministic read fixture"));
    for offset in [0, 17] {
        let bytes = tokio::time::timeout(Duration::from_secs(5), driver.read_at(Arc::clone(&file), offset, 32))
            .await
            .expect("ordinary fixture read should complete")
            .expect("best-effort io-wq registration must not disable working reads");
        assert_eq!(bytes, [0; 32]);
    }

    let after = driver.shard_iowq_setup();
    assert_eq!(after.len(), 2);
    for (before, after) in before.iter().zip(&after) {
        assert_requested_limits(after);
        match (&before.result, &after.result) {
            (
                IoWqRegistration::Registered { previous_limits: before },
                IoWqRegistration::Registered { previous_limits: after },
            ) => assert_eq!(before, after, "previous-limit startup snapshot must remain historical"),
            (
                IoWqRegistration::Failed {
                    error_kind: before_kind,
                    raw_os_error: before_errno,
                },
                IoWqRegistration::Failed {
                    error_kind: after_kind,
                    raw_os_error: after_errno,
                },
            ) => {
                assert_eq!(before_kind, after_kind);
                assert_eq!(before_errno, after_errno);
            }
            _ => panic!("startup registration outcome changed after reads"),
        }
    }
    let stats = driver.shutdown();
    assert_eq!(stats.submitted, 2);
    assert_eq!(stats.delivered, 2);
    assert_eq!(stats.orphan_reclaimed, 0);
    assert_eq!(stats.in_flight, 0);
}
