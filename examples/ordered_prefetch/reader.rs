// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Example-only ordered consumer state machine. No production API or runtime.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

const MAX_WINDOW: usize = 64;
const MAX_CHUNK: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy)]
pub struct Config {
    pub offset: u64,
    pub length: u64,
    pub chunk: usize,
    pub window: usize,
    pub max_bytes: usize,
}

impl Config {
    pub fn validate(self) -> io::Result<u64> {
        if self.chunk == 0 || self.chunk > MAX_CHUNK || self.window == 0 || self.window > MAX_WINDOW {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "chunk must be 1..=8 MiB and window 1..=64"));
        }
        if self.max_bytes < self.chunk || self.max_bytes > MAX_WINDOW * MAX_CHUNK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_bytes must be at least chunk and at most 512 MiB",
            ));
        }
        self.offset
            .checked_add(self.length)
            .filter(|end| *end <= i64::MAX as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range end must fit signed file offsets"))
    }
}

pub struct Chunk {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

enum State<F> {
    Pending(F),
    Ready(io::Result<Vec<u8>>),
}

struct Slot<F> {
    offset: u64,
    len: usize,
    state: State<F>,
}

pub struct OrderedReader<F, S> {
    config: Config,
    end: u64,
    next_offset: u64,
    reserved: usize,
    slots: VecDeque<Slot<F>>,
    source: S,
    stop_scheduling: bool,
    finished: bool,
}

impl<F, S> OrderedReader<F, S>
where
    F: Future<Output = io::Result<Vec<u8>>> + Unpin,
    S: FnMut(u64, usize) -> F,
{
    pub fn new(config: Config, source: S) -> io::Result<Self> {
        let end = config.validate()?;
        Ok(Self {
            config,
            end,
            next_offset: config.offset,
            reserved: 0,
            slots: VecDeque::with_capacity(config.window),
            source,
            stop_scheduling: false,
            finished: false,
        })
    }

    /// Dropping a pending next future leaves every handle and cursor in self.
    /// No slot is removed before Ready, and Ready results are never repolled.
    pub async fn next(&mut self) -> io::Result<Option<Chunk>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    fn fill_window(&mut self) {
        while !self.stop_scheduling && self.next_offset < self.end && self.slots.len() < self.config.window {
            let len = (self.end - self.next_offset).min(self.config.chunk as u64) as usize;
            if len > self.config.max_bytes - self.reserved {
                break;
            }
            let handle = (self.source)(self.next_offset, len);
            self.slots.push_back(Slot {
                offset: self.next_offset,
                len,
                state: State::Pending(handle),
            });
            self.reserved += len;
            self.next_offset += len as u64;
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Chunk>>> {
        if self.finished {
            return Poll::Ready(Ok(None));
        }
        // Refill only in a consumer poll, never after delivering a chunk.
        self.fill_window();
        let mut terminal_index = None;
        for (index, slot) in self.slots.iter_mut().enumerate() {
            if let State::Pending(handle) = &mut slot.state
                && let Poll::Ready(mut result) = Pin::new(handle).poll(cx)
            {
                if result.as_ref().is_ok_and(|bytes| bytes.len() > slot.len) {
                    result = Err(io::Error::new(io::ErrorKind::InvalidData, "source returned more bytes than requested"));
                }
                slot.state = State::Ready(result);
            }
            if let State::Ready(result) = &slot.state
                && !result.as_ref().is_ok_and(|bytes| bytes.len() == slot.len)
            {
                // Preserve earlier chunks and the first ordered terminal result;
                // later reads can no longer contribute and are dropped/cancelled.
                terminal_index = Some(index);
                break;
            }
        }
        if let Some(index) = terminal_index {
            self.stop_scheduling = true;
            while self.slots.len() > index + 1 {
                if let Some(slot) = self.slots.pop_back() {
                    self.reserved -= slot.len;
                }
            }
        }

        match self.slots.front() {
            Some(Slot {
                state: State::Pending(_),
                ..
            }) => return Poll::Pending,
            None => {
                self.finished = true;
                return Poll::Ready(Ok(None));
            }
            Some(_) => {}
        }
        let slot = self.slots.pop_front().expect("ready front checked above");
        self.reserved -= slot.len;
        let State::Ready(result) = slot.state else { unreachable!("ready front checked above") };
        match result {
            Ok(bytes) => {
                if bytes.len() < slot.len {
                    self.finished = true;
                }
                Poll::Ready(Ok((!bytes.is_empty()).then_some(Chunk {
                    offset: slot.offset,
                    bytes,
                })))
            }
            Err(error) => {
                self.finished = true;
                Poll::Ready(Err(error))
            }
        }
    }
}
