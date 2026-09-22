// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn readable(event: &EventFd) -> bool {
    let mut fd = libc::pollfd {
        fd: event.as_raw(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: initialized pollfd, zero timeout, no pointer retained by kernel.
    unsafe { libc::poll(&mut fd, 1, 0) > 0 }
}

#[test]
fn drained_eventfd_does_not_strand_messages_beyond_intake_budget() {
    let wake = EventFd::new().unwrap();
    let (tx, rx) = mpsc::channel();
    for id in 0..TURN_MESSAGES + 1 {
        tx.send(id).unwrap();
        wake.signal();
    }
    wake.drain();
    assert!(!readable(&wake));

    let mut first = TurnBudget::default();
    while first.can_take_message() {
        rx.try_recv().unwrap();
        first.messages += 1;
    }
    assert!(first.continue_without_wait(false, false, 0));
    // No additional producer or wake signal: the next turn consumes the tail.
    assert_eq!(rx.try_recv().unwrap(), TURN_MESSAGES);
    let second = TurnBudget {
        messages: 1,
        ..TurnBudget::default()
    };
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    assert!(!second.continue_without_wait(false, false, 0));
}

#[test]
fn exact_message_boundary_costs_one_empty_turn_then_sleeps() {
    let full = TurnBudget {
        messages: TURN_MESSAGES,
        ..TurnBudget::default()
    };
    assert!(full.continue_without_wait(false, false, 0));
    assert!(!TurnBudget::default().continue_without_wait(false, false, 0));
}

#[test]
fn oversized_allocation_makes_progress_then_yields_to_reap() {
    let mut budget = TurnBudget::default();
    assert!(budget.can_take_message());
    budget.messages += 1;
    // Record one already-accepted read, larger than the fairness threshold.
    // It is not rejected or repeatedly deferred by the turn budget.
    budget.allocation_bytes = TURN_ALLOCATION_BYTES * 2;
    assert!(!budget.can_take_message());
    assert!(budget.can_reap());
    assert!(budget.continue_without_wait(false, false, 0));
    assert!(TurnBudget::default().can_take_message());
}

#[test]
fn allocation_volume_yields_before_message_limit() {
    let mut budget = TurnBudget::default();
    while budget.can_take_message() {
        budget.messages += 1;
        budget.allocation_bytes += TURN_ALLOCATION_BYTES / 4;
    }
    assert_eq!(budget.messages, 4);
    assert!(budget.continue_without_wait(false, false, 0));
}

#[test]
fn completion_burst_yields_to_intake_and_continues_without_a_new_edge() {
    let mut cq: VecDeque<_> = (0..TURN_COMPLETIONS + 1).collect();
    let mut budget = TurnBudget::default();
    while budget.can_reap() {
        cq.pop_front().unwrap();
        budget.completions += 1;
    }
    assert_eq!(cq.len(), 1);
    assert!(budget.continue_without_wait(true, false, 0));
    // A new turn starts with intake, so shutdown/cancel can be observed before
    // the next chunk of completions rather than waiting for CQ to empty.
    assert!(TurnBudget::default().can_take_message());
}

#[test]
fn failed_or_zero_progress_submissions_cannot_spin_from_queued_sqes_alone() {
    let budget = TurnBudget::default();
    // submit_ring maps EBUSY/EINTR/other errors and Ok(0) to zero; each must
    // return to the eventfd/heartbeat wait when there is no other ready work.
    assert!(!budget.continue_without_wait(false, true, 0));
    // A partial positive submission allows a bounded immediate follow-up; if
    // that next turn makes no progress, it waits again.
    assert!(budget.continue_without_wait(false, true, 1));
    assert!(!budget.continue_without_wait(false, true, 0));
}

#[test]
fn completion_readiness_wins_even_when_last_submit_failed() {
    assert!(TurnBudget::default().continue_without_wait(true, true, 0));
}

#[test]
fn submission_progress_without_remaining_work_does_not_busy_poll() {
    assert!(!TurnBudget::default().continue_without_wait(false, false, 2));
}
