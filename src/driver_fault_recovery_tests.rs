// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn direct_prefix() -> Pending {
    Pending {
        #[cfg(feature = "diagnostics")]
        timing: None,
        // Simulated completed CQE: no kernel ever references this allocation.
        buf: vec![99, 10, 11, 12, 13, 14, 88, 88, 88],
        file: Arc::new(File::open("/dev/null").unwrap()),
        done: None,
        offset: 4096,
        nread: 5,
        _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        pad: 1,
        head: 2,
        want: 6,
        region_len: 8,
        align: 4,
        transient_retries: 0,
    }
}

#[test]
fn direct_metadata_failure_preserves_errno_without_delivering_prefix() {
    let mut pending = direct_prefix();
    let before = pending.buf.clone();
    let error = finish_direct_short_read(&mut pending, Err(io::Error::from_raw_os_error(libc::EIO))).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EIO));
    assert_eq!(pending.buf, before, "metadata failure must not turn into prefix delivery");
}

#[test]
fn direct_mid_file_short_read_errors_without_delivering_prefix() {
    let mut pending = direct_prefix();
    let error = finish_direct_short_read(&mut pending, Ok(8192)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(!pending.buf.is_empty());
}

#[test]
fn direct_confirmed_tail_returns_only_initialized_logical_bytes() {
    let mut pending = direct_prefix();
    assert_eq!(finish_direct_short_read(&mut pending, Ok(4101)).unwrap(), [12, 13, 14]);
}

#[test]
fn direct_concurrent_truncation_preserves_completed_prefix() {
    let mut pending = direct_prefix();
    // A truncate after the CQE does not erase bytes the read already completed.
    assert_eq!(finish_direct_short_read(&mut pending, Ok(4096)).unwrap(), [12, 13, 14]);
}

#[test]
fn direct_eof_before_logical_start_returns_empty() {
    let mut pending = direct_prefix();
    pending.nread = 1;
    assert!(finish_direct_short_read(&mut pending, Ok(4097)).unwrap().is_empty());
}

fn small_ring() -> Option<IoUring> {
    match IoUring::builder().setup_cqsize(2).build(2) {
        Ok(ring) => {
            assert!(ring.params().is_feature_nodrop(), "overflow recovery requires NODROP");
            Some(ring)
        }
        Err(error) => {
            let failure = ProbeFailure::Setup(error);
            assert!(failure.is_expected_restriction(), "unexpected io_uring setup failure: {failure}");
            eprintln!("SKIP fault recovery kernel test: io_uring restricted ({failure})");
            None
        }
    }
}

fn push_nop(ring: &mut IoUring, id: u64) {
    // SAFETY: NOP has no pointers or external resources, including on unwind.
    unsafe { ring.submission().push(&opcode::Nop::new().build().user_data(id)) }.unwrap();
}

#[test]
fn idle_ring_skips_enter() {
    let Some(mut ring) = small_ring() else { return };
    assert!(submit_if_needed(&mut ring).is_none());
}

#[test]
fn empty_sq_flushes_real_kernel_overflow_after_reap() {
    let Some(mut ring) = small_ring() else { return };
    assert_eq!(ring.completion().capacity(), 2);
    push_nop(&mut ring, 1);
    push_nop(&mut ring, 2);
    assert_eq!(ring.submit_and_wait(2).unwrap(), 2);
    // Leave the CQ full, then complete a third NOP into the NODROP overflow list.
    push_nop(&mut ring, 3);
    assert_eq!(ring.submit().unwrap(), 1);
    assert!(ring.submission().is_empty());
    assert!(ring.submission().cq_overflow(), "test must exercise real kernel overflow");
    let first: Vec<_> = ring.completion().map(|cqe| (cqe.user_data(), cqe.result())).collect();
    assert_eq!(first, [(1, 0), (2, 0)]);

    // Regression: the old empty-SQ early return skipped exactly this enter.
    assert_eq!(submit_if_needed(&mut ring).expect("overflow must enter").unwrap(), 0);
    let recovered: Vec<_> = ring.completion().map(|cqe| (cqe.user_data(), cqe.result())).collect();
    assert_eq!(recovered, [(3, 0)]);
    assert!(submit_if_needed(&mut ring).is_none());
}

#[test]
fn sq_capacity_leaves_backlog_owned_until_later_submission() {
    let Some(mut ring) = small_ring() else { return };
    let mut backlog: VecDeque<_> = (1..=5).map(|id| opcode::Nop::new().build().user_data(id)).collect();
    let mut completed = Vec::new();
    for remaining in [3, 1, 0] {
        flush_backlog(&mut ring, &mut backlog);
        assert_eq!(backlog.len(), remaining);
        submit_if_needed(&mut ring).expect("queued NOPs must submit").unwrap();
        completed.extend(ring.completion().map(|cqe| {
            assert_eq!(cqe.result(), 0);
            cqe.user_data()
        }));
    }
    assert_eq!(completed, [1, 2, 3, 4, 5]);
    assert!(submit_if_needed(&mut ring).is_none());
}
