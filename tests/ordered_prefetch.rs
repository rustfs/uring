// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

#[path = "../examples/ordered_prefetch/reader.rs"]
mod reader;

use reader::{Config, OrderedReader};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Control {
    submitted: Vec<(u64, usize)>,
    completed: BTreeMap<u64, io::Result<Vec<u8>>>,
    polls: BTreeMap<u64, usize>,
    dropped: Vec<u64>,
}

struct ManualRead {
    offset: u64,
    control: Rc<RefCell<Control>>,
}

impl Future for ManualRead {
    type Output = io::Result<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let mut control = self.control.borrow_mut();
        *control.polls.entry(self.offset).or_default() += 1;
        match control.completed.remove(&self.offset) {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

impl Drop for ManualRead {
    fn drop(&mut self) {
        self.control.borrow_mut().dropped.push(self.offset);
    }
}

fn config() -> Config {
    Config {
        offset: 0,
        length: 32,
        chunk: 4,
        window: 3,
        max_bytes: 12,
    }
}

fn source(control: &Rc<RefCell<Control>>) -> impl FnMut(u64, usize) -> ManualRead + use<> {
    let control = Rc::clone(control);
    move |offset, len| {
        control.borrow_mut().submitted.push((offset, len));
        ManualRead {
            offset,
            control: Rc::clone(&control),
        }
    }
}

fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn out_of_order_results_remain_ordered_and_no_refill_occurs_after_yield() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut reader = OrderedReader::new(config(), source(&control)).unwrap();
    assert!(control.borrow().submitted.is_empty(), "constructor must not start work");
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(8, Ok(vec![8; 4]));
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(4, Ok(vec![4; 4]));
    control.borrow_mut().completed.insert(0, Ok(vec![0; 4]));
    let Poll::Ready(Ok(Some(chunk))) = poll(&mut Box::pin(reader.next())) else {
        panic!("head must be ready")
    };
    assert_eq!((chunk.offset, chunk.bytes), (0, vec![0; 4]));
    assert_eq!(control.borrow().submitted.len(), 3, "slow consumer must not trigger refill");
    let Poll::Ready(Ok(Some(chunk))) = poll(&mut Box::pin(reader.next())) else {
        panic!("cached second chunk")
    };
    assert_eq!((chunk.offset, chunk.bytes), (4, vec![4; 4]));
    assert_eq!(control.borrow().polls[&8], 2, "a completed tail must never be repolled");
}

#[test]
fn dropping_pending_next_keeps_head_handle_and_offset() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut reader = OrderedReader::new(config(), source(&control)).unwrap();
    let mut next = Box::pin(reader.next());
    assert!(poll(&mut next).is_pending());
    drop(next);
    assert!(control.borrow().dropped.is_empty());
    control.borrow_mut().completed.insert(0, Ok(vec![1; 4]));
    let Poll::Ready(Ok(Some(chunk))) = poll(&mut Box::pin(reader.next())) else {
        panic!("same head must resume")
    };
    assert_eq!(chunk.offset, 0);
    assert_eq!(control.borrow().submitted.len(), 3);
}

#[test]
fn ready_tail_remains_charged_to_the_same_logical_byte_window() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut cfg = config();
    cfg.max_bytes = 8;
    let mut reader = OrderedReader::new(cfg, source(&control)).unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(4, Ok(vec![4; 4]));
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    assert_eq!(control.borrow().submitted, [(0, 4), (4, 4)]);
}

#[test]
fn early_tail_eof_cancels_later_slots_but_preserves_earlier_output() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut reader = OrderedReader::new(config(), source(&control)).unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(4, Ok(vec![4; 2]));
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    assert!(control.borrow().dropped.contains(&8));
    control.borrow_mut().completed.insert(0, Ok(vec![0; 4]));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(Some(_)))));
    let Poll::Ready(Ok(Some(chunk))) = poll(&mut Box::pin(reader.next())) else {
        panic!("short final chunk")
    };
    assert_eq!((chunk.offset, chunk.bytes), (4, vec![4; 2]));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(None))));
    assert_eq!(control.borrow().submitted.len(), 3);
}

#[test]
fn error_is_delivered_at_its_ordered_position_then_reader_is_fused() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut reader = OrderedReader::new(config(), source(&control)).unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(4, Err(io::Error::other("fault")));
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(0, Ok(vec![0; 4]));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(Some(_)))));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Err(_))));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(None))));
    assert!(control.borrow().dropped.contains(&8));
}

#[test]
fn empty_range_submits_nothing_and_zero_byte_eof_returns_no_chunk() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut cfg = config();
    cfg.length = 0;
    let mut empty = OrderedReader::new(cfg, source(&control)).unwrap();
    assert!(matches!(poll(&mut Box::pin(empty.next())), Poll::Ready(Ok(None))));
    assert!(control.borrow().submitted.is_empty());
    let mut reader = OrderedReader::new(config(), source(&control)).unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(0, Ok(Vec::new()));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(None))));
    assert!(control.borrow().dropped.contains(&4));
    assert!(control.borrow().dropped.contains(&8));
}

#[test]
fn dropping_reader_drops_every_active_handle() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut reader = OrderedReader::new(config(), source(&control)).unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    drop(reader);
    assert_eq!(control.borrow().dropped, [0, 4, 8]);
}

#[test]
fn oversized_source_result_is_invalid_data_and_cancels_tail() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut reader = OrderedReader::new(config(), source(&control)).unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    control.borrow_mut().completed.insert(0, Ok(vec![0; 5]));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::InvalidData));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(None))));
    assert!(control.borrow().dropped.contains(&4));
    assert!(control.borrow().dropped.contains(&8));
}

#[test]
fn final_requested_slot_is_shortened_to_range_end_without_extra_reads() {
    let control = Rc::new(RefCell::new(Control::default()));
    let mut reader = OrderedReader::new(
        Config {
            offset: 7,
            length: 6,
            ..config()
        },
        source(&control),
    )
    .unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    assert_eq!(control.borrow().submitted, [(7, 4), (11, 2)]);
    control.borrow_mut().completed.insert(7, Ok(vec![7; 4]));
    control.borrow_mut().completed.insert(11, Ok(vec![11; 2]));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(Some(_)))));
    let Poll::Ready(Ok(Some(chunk))) = poll(&mut Box::pin(reader.next())) else {
        panic!("final range slot")
    };
    assert_eq!((chunk.offset, chunk.bytes), (11, vec![11; 2]));
    assert!(matches!(poll(&mut Box::pin(reader.next())), Poll::Ready(Ok(None))));
    assert_eq!(control.borrow().submitted.len(), 2);
}

#[test]
fn invalid_limits_and_overflow_fail_before_any_source_call() {
    let control = Rc::new(RefCell::new(Control::default()));
    let base = config();
    for cfg in [
        Config { chunk: 0, ..base },
        Config { window: 0, ..base },
        Config { window: 65, ..base },
        Config { max_bytes: 3, ..base },
        Config {
            offset: u64::MAX,
            ..base
        },
        Config {
            offset: i64::MAX as u64,
            length: 1,
            ..base
        },
        Config {
            chunk: usize::MAX,
            ..base
        },
    ] {
        assert!(OrderedReader::new(cfg, source(&control)).is_err());
    }
    assert!(control.borrow().submitted.is_empty());
}

#[test]
fn deferred_tail_assigned_bytes_is_repolled_even_while_head_waits_on_admission() {
    let head_count = Arc::new(tokio::sync::Semaphore::new(1));
    let tail_count = Arc::new(tokio::sync::Semaphore::new(1));
    let bytes = Arc::new(tokio::sync::Semaphore::new(4));
    let held_head = Arc::clone(&head_count).try_acquire_owned().unwrap();
    let held_bytes = Arc::clone(&bytes).try_acquire_many_owned(4).unwrap();
    let submitted = Rc::new(RefCell::new(Vec::new()));
    let accepted = Rc::clone(&submitted);
    let mut reader = OrderedReader::new(
        Config {
            window: 2,
            max_bytes: 8,
            ..config()
        },
        move |offset, len| {
            let count = Arc::clone(if offset == 0 { &head_count } else { &tail_count });
            let bytes = Arc::clone(&bytes);
            let accepted = Rc::clone(&accepted);
            Box::pin(async move {
                let _count = count.acquire_owned().await.unwrap();
                let _bytes = bytes.acquire_many_owned(len as u32).await.unwrap();
                accepted.borrow_mut().push(offset);
                Ok(vec![offset as u8; len])
            })
        },
    )
    .unwrap();
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    // Tokio assigns bytes to the already queued tail waiter before head can
    // acquire its count permit. Polling only head now would strand those bytes.
    drop(held_bytes);
    drop(held_head);
    assert!(poll(&mut Box::pin(reader.next())).is_pending());
    assert_eq!(*submitted.borrow(), [4], "tail must progress without head completion");
    let Poll::Ready(Ok(Some(chunk))) = poll(&mut Box::pin(reader.next())) else {
        panic!("head should now acquire released bytes")
    };
    assert_eq!(chunk.offset, 0);
    assert_eq!(*submitted.borrow(), [4, 0]);
}
