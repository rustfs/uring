// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Default)]
struct ResultHarness {
    stats: DriverStats,
    consecutive_errors: u32,
    logged: bool,
    shutting_down: bool,
    shutdown_calls: usize,
    pending: HashMap<u64, Pending>,
    backlog: VecDeque<io_uring::squeue::Entry>,
    queued_cancels: HashSet<u64>,
}

impl ResultHarness {
    fn apply(&mut self, result: io::Result<usize>) -> usize {
        handle_submit_result(
            result,
            &self.stats,
            &mut self.consecutive_errors,
            &mut self.logged,
            &mut self.shutting_down,
            || {
                self.shutdown_calls += 1;
                for id in self.pending.keys() {
                    queue_cancel(&mut self.backlog, &mut self.queued_cancels, *id);
                }
            },
        )
    }

    fn fail(&mut self, errno: i32) -> usize {
        self.apply(Err(io::Error::from_raw_os_error(errno)))
    }
}

fn pending_read(file: Arc<File>, count: &Arc<Semaphore>, bytes: &Arc<Semaphore>) -> Pending {
    Pending {
        #[cfg(feature = "diagnostics")]
        timing: None,
        buf: vec![42; 16],
        file,
        done: None,
        offset: 0,
        nread: 0,
        _permit: ReadPermits {
            _count: Arc::clone(count).try_acquire_owned().unwrap(),
            _bytes: Some(Arc::clone(bytes).try_acquire_many_owned(16).unwrap()),
        },
        pad: 0,
        head: 0,
        want: 16,
        region_len: 16,
        align: 1,
        transient_retries: 0,
        cancel_requested: false,
    }
}

#[test]
fn partial_positive_acceptance_resets_errors_and_allows_bounded_retry() {
    let mut harness = ResultHarness::default();
    harness.fail(libc::EIO);
    let submitted = harness.apply(Ok(2)); // Model 2 accepted out of a larger SQ.
    assert_eq!(submitted, 2);
    assert_eq!(harness.consecutive_errors, 0);
    assert_eq!(harness.stats.submit_errors.load(Ordering::SeqCst), 1);
    assert!(TurnBudget::default().continue_without_wait(false, true, submitted));
    assert_eq!(harness.shutdown_calls, 0);
}

#[test]
fn zero_acceptance_resets_error_streak_but_waits_with_remaining_sqes() {
    let mut harness = ResultHarness::default();
    harness.fail(libc::EIO);
    let submitted = harness.apply(Ok(0));
    assert_eq!(harness.consecutive_errors, 0);
    assert!(!TurnBudget::default().continue_without_wait(false, true, submitted));
    assert_eq!(harness.shutdown_calls, 0);
}

#[test]
fn interrupted_or_busy_submission_resets_streak_without_counting_or_spinning() {
    for errno in [libc::EINTR, libc::EBUSY] {
        let mut harness = ResultHarness::default();
        harness.fail(libc::EIO);
        let submitted = harness.fail(errno);
        assert_eq!(harness.consecutive_errors, 0);
        assert_eq!(harness.stats.submit_errors.load(Ordering::SeqCst), 1);
        assert!(!TurnBudget::default().continue_without_wait(false, true, submitted));
        assert!(!harness.shutting_down);
    }
}

#[test]
fn persistent_failures_shutdown_once_and_keep_every_pending_resource() {
    let mut harness = ResultHarness::default();
    let file = Arc::new(File::open("/dev/null").unwrap());
    let count = Arc::new(Semaphore::new(2));
    let bytes = Arc::new(Semaphore::new(32));
    harness.pending.insert(1, pending_read(Arc::clone(&file), &count, &bytes));
    harness.pending.insert(2, pending_read(Arc::clone(&file), &count, &bytes));
    let ptrs: Vec<_> = (1..=2).map(|id| harness.pending[&id].buf.as_ptr()).collect();
    // Model an already queued drop-cancel; shutdown must not duplicate it.
    queue_cancel(&mut harness.backlog, &mut harness.queued_cancels, 1);

    for attempt in 1..=MAX_CONSECUTIVE_SUBMIT_ERRORS + 2 {
        assert_eq!(harness.fail(libc::EPERM), 0);
        assert_eq!(harness.consecutive_errors, attempt);
        assert_eq!(harness.shutting_down, attempt >= MAX_CONSECUTIVE_SUBMIT_ERRORS);
        assert_eq!(harness.shutdown_calls, usize::from(attempt >= MAX_CONSECUTIVE_SUBMIT_ERRORS));
        assert_eq!(harness.pending.len(), 2);
        for id in 1..=2 {
            assert_eq!(harness.pending[&id].buf.as_ptr(), ptrs[(id - 1) as usize]);
            assert_eq!(harness.pending[&id].buf, [42; 16]);
        }
        assert_eq!(count.available_permits(), 0);
        assert_eq!(bytes.available_permits(), 0);
        assert_eq!(Arc::strong_count(&file), 3);
    }
    assert!(harness.logged);
    assert_eq!(harness.backlog.len(), 2);
    assert_eq!(harness.queued_cancels, HashSet::from([1, 2]));
    assert_eq!(
        harness.stats.submit_errors.load(Ordering::SeqCst),
        u64::from(MAX_CONSECUTIVE_SUBMIT_ERRORS + 2)
    );
    // No SQEs were submitted to a kernel in this model, so dropping is safe.
    drop(harness);
    assert_eq!(count.available_permits(), 2);
    assert_eq!(bytes.available_permits(), 32);
    assert_eq!(Arc::strong_count(&file), 1);
}

#[test]
fn success_and_transient_errors_break_the_consecutive_shutdown_threshold() {
    for reset in [Ok(1), Ok(0), Err(libc::EINTR), Err(libc::EBUSY)] {
        let mut harness = ResultHarness::default();
        for _ in 1..MAX_CONSECUTIVE_SUBMIT_ERRORS {
            harness.fail(libc::EIO);
        }
        harness.apply(reset.map_err(io::Error::from_raw_os_error));
        harness.fail(libc::EIO);
        assert_eq!(harness.consecutive_errors, 1);
        assert_eq!(harness.shutdown_calls, 0);
    }
}

#[test]
fn eagain_is_counted_as_a_bounded_submit_failure() {
    let mut harness = ResultHarness::default();
    assert_eq!(harness.fail(libc::EAGAIN), 0);
    assert_eq!(harness.consecutive_errors, 1);
    assert_eq!(harness.stats.submit_errors.load(Ordering::SeqCst), 1);
}
