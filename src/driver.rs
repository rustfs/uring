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

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io;
use std::io::Write as _;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

/// Upper bound on how long shutdown waits for in-flight ops to drain before
/// leaking the ring+buffers and exiting (C4, rustfs/backlog#1055). ASYNC_CANCEL
/// cannot interrupt an in-execution regular-file read on a D-state/NFS-hung
/// disk, so drain-to-zero can be non-terminating; this bounds it.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, oneshot};

/// user_data bit marking the CQE of an `AsyncCancel` SQE itself (as opposed
/// to the CQE of the read op it targets).
const CANCEL_BIT: u64 = 1 << 63;

/// `offset` value meaning "use the file's current position" (read(2)
/// semantics); required for pipes/sockets where pread returns ESPIPE.
const CURRENT_POSITION: u64 = u64::MAX;

/// Kernel single-read cap: `MAX_RW_COUNT = INT_MAX & PAGE_MASK` (2 GiB − 4 KiB
/// on 4 KiB pages). io_uring's READ length field is a u32, and any request
/// above this short-reads. We reject beyond it in `submit` so a `len as u32`
/// truncation can never silently turn a huge read into a 0-byte "EOF" (C6,
/// rustfs/backlog#1057); P2 must chunk reads larger than this.
const MAX_READ_LEN: usize = 0x7fff_f000;

/// Block-aligned superset geometry for a read (rustfs/backlog#1102).
///
/// Returns `(kernel_offset, head, region_len)`: the offset handed to the kernel,
/// how many bytes of the read region precede the caller's logical range, and how
/// many bytes the kernel is asked to read. `align == 1` is the buffered case and
/// passes `offset` (which may be `CURRENT_POSITION`) straight through.
///
/// `None` when `align` is not a power of two or the aligned range would overflow.
fn aligned_geometry(offset: u64, len: usize, align: usize) -> Option<(u64, usize, usize)> {
    // A real device block is tiny (512..=4096). Capping alignment at the read
    // cap keeps `align_offset(align)` always satisfiable (so it never returns
    // `usize::MAX`, which would make the driver's later `ptr::add(pad)` UB) and
    // keeps the `region_len + align - 1` allocation from overflowing `usize`.
    if align == 0 || !align.is_power_of_two() || align > MAX_READ_LEN {
        return None;
    }
    if align == 1 {
        return Some((offset, 0, len));
    }
    let mask = align as u64 - 1;
    let kernel_offset = offset & !mask;
    let head = usize::try_from(offset - kernel_offset).ok()?;
    let region_len = head.checked_add(len)?.checked_next_multiple_of(align)?;
    Some((kernel_offset, head, region_len))
}

/// Heartbeat bound on the driver loop's blocking wait (backlog#1102). The loop
/// normally wakes on a CQE (the ring's registered eventfd) or a new message
/// (the wakeup eventfd); this timeout only bounds the wait so the bounded-drain
/// deadline is still checked and any queued cancel is picked up promptly.
const LOOP_HEARTBEAT: Duration = Duration::from_millis(50);

/// Consecutive non-transient `ring.submit()` failures the driver tolerates
/// before it stops retrying silently and shuts the shard down, so callers get a
/// driver-gone error and fall back to the std backend instead of stalling
/// forever on ops the kernel will never accept (rustfs/backlog#1162). With the
/// 50 ms heartbeat this bounds the silent-retry window to a few seconds.
const MAX_CONSECUTIVE_SUBMIT_ERRORS: u32 = 128;

/// How many times a single logical read retries a transient CQE errno
/// (EINTR/EAGAIN) without making progress before it surfaces the error, so a
/// pathological storm cannot spin the driver thread (rustfs/backlog#1166).
const MAX_TRANSIENT_RETRIES: u32 = 16;

/// Owned `eventfd(2)` used to wake the driver loop (backlog#1102): one is
/// registered with the ring so the kernel signals it on every CQE, the other is
/// signaled by `submit`/shutdown so a new message wakes the loop immediately —
/// together they replace the spike's 200 µs busy-poll.
struct EventFd {
    fd: std::os::fd::RawFd,
}

impl EventFd {
    fn new() -> io::Result<Self> {
        // SAFETY: eventfd returns a fresh owned fd or -1; the flags are valid.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    fn as_raw(&self) -> std::os::fd::RawFd {
        self.fd
    }

    /// Make the fd readable. A saturated counter (EAGAIN) is fine — it is
    /// already readable, which is all a wakeup needs.
    fn signal(&self) {
        let v: u64 = 1;
        // SAFETY: writing 8 bytes from a valid u64 to an eventfd we own.
        unsafe {
            libc::write(self.fd, (&v as *const u64).cast(), 8);
        }
    }

    /// Reset the counter. EFD_NONBLOCK guarantees this never blocks; a single
    /// successful read drains the whole counter, the next returns EAGAIN.
    fn drain(&self) {
        let mut v: u64 = 0;
        // SAFETY: reading 8 bytes into a valid u64 from an eventfd we own.
        while unsafe { libc::read(self.fd, (&mut v as *mut u64).cast(), 8) } == 8 {}
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        // SAFETY: we own this fd and drop it exactly once.
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// Block until a CQE is ready (`cq`), a new message arrives (`wake`), or the
/// heartbeat elapses. The return value is ignored: a spurious wakeup, timeout,
/// or EINTR just runs one loop turn (intake + reap), which is always safe.
fn wait_for_events(cq: &EventFd, wake: &EventFd, timeout: Duration) {
    let mut fds = [
        libc::pollfd {
            fd: cq.as_raw(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: wake.as_raw(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let ms = timeout.as_millis().min(i32::MAX as u128) as libc::c_int;
    // SAFETY: `fds` is a valid, initialized array of two pollfds.
    unsafe {
        libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, ms);
    }
}

/// Why the probe refused to start the io_uring driver.
///
/// Mirrors the P2 degradation contract (backlog#894): a restricted
/// environment must be recognized and answered with a silent fallback to the
/// std backend, never surfaced to callers.
#[derive(Debug)]
pub enum ProbeFailure {
    /// `io_uring_setup` itself failed (seccomp/gVisor/old kernel).
    Setup(io::Error),
    /// The ring was created but a real `IORING_OP_READ` did not complete
    /// correctly (gVisor accepts setup but fails ops; also covers silent
    /// data corruption, which we treat as "unusable").
    ReadOp(io::Error),
}

impl ProbeFailure {
    /// True when the **probe-time** errno belongs to the "expected
    /// restriction" class that P2 maps to permanent per-disk fallback:
    /// EACCES/EPERM/ENOSYS/EINVAL/EOPNOTSUPP. Anything else is a genuine bug
    /// worth surfacing.
    ///
    /// IMPORTANT (C7, rustfs/backlog#1059): this classification is valid ONLY
    /// for a one-shot startup probe, where these errnos unambiguously mean
    /// "io_uring is unusable here" (gVisor/seccomp/old kernel). Runtime
    /// per-op errnos have different semantics and MUST NOT reuse this class.
    /// In particular EINVAL is triple-meaning at runtime — offset > i64::MAX
    /// (signed loff_t), O_DIRECT buffer/offset/len misalignment (P2 will use
    /// O_DIRECT), and setup `entries` over the cap — none of which imply the
    /// disk should be permanently degraded off io_uring. P2's degradation
    /// contract must split errnos into three classes:
    ///
    ///   * probe-time restriction  -> degrade this disk to the std backend;
    ///   * runtime parameter error -> return the error to the caller (and,
    ///     for a suspected bug, re-verify once via std pread) — never latch;
    ///   * transient (EINTR/EAGAIN) -> retry, never surface.
    ///
    /// See `submit` for the offset guard that keeps a caller arithmetic bug
    /// from ever reaching the kernel as a runtime EINVAL.
    pub fn is_expected_restriction(&self) -> bool {
        let err = match self {
            ProbeFailure::Setup(e) | ProbeFailure::ReadOp(e) => e,
        };
        matches!(
            err.raw_os_error(),
            Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
        )
    }
}

// Submission-side backpressure (C10, rustfs/backlog#1060; async in #1102).
//
// A `tokio::sync::Semaphore` with `entries` permits bounds in-flight ops below
// CQ capacity. The load-bearing rule is the RELEASE POINT: a permit is released
// at the CQE (when the pending-table entry is removed), NOT at future drop.
// Tying a permit to the future (the natural RAII shape) would let a quorum
// dropping many futures return permits while their orphan buffers still sit in
// the pending table awaiting slow-disk CQEs, decoupling the permit count from
// resident memory and reopening the memory-DoS surface.
//
// That rule is now enforced by the type system rather than by a manual
// `release()` call: the `OwnedSemaphorePermit` travels with `Msg::Read` into the
// `Pending` entry and is dropped exactly when the entry is removed at the final
// CQE. A short-read resubmit keeps the entry — and thus the permit.
//
// Acquisition never blocks the caller's thread: `submit` takes the permit with
// `try_acquire_owned()` on the common unsaturated path (no allocation, no await,
// submission stays eager), and when saturated it hands the acquire future to the
// returned `ReadHandle`, which awaits it on its first poll and submits then.

/// Boxed `Semaphore::acquire_owned` future held by a saturated `ReadHandle`.
type AcquireFut = Pin<Box<dyn Future<Output = Result<OwnedSemaphorePermit, tokio::sync::AcquireError>> + Send>>;

#[derive(Default)]
struct DriverStats {
    submitted: AtomicU64,
    delivered: AtomicU64,
    orphan_reclaimed: AtomicU64,
    in_flight: AtomicU64,
    cancel_succeeded: AtomicU64,
    cancel_not_found: AtomicU64,
    cancel_already: AtomicU64,
    cq_overflow: AtomicU64,
    submit_errors: AtomicU64,
}

/// Point-in-time copy of the driver counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatsSnapshot {
    /// Read ops handed to the kernel.
    pub submitted: u64,
    /// CQEs whose result was received by a live caller.
    pub delivered: u64,
    /// CQEs whose caller had dropped the future: the buffer stayed in the
    /// pending table the whole time and was reclaimed here, at the CQE.
    pub orphan_reclaimed: u64,
    /// Ops submitted but not yet completed. The kernel may still write into
    /// their buffers.
    pub in_flight: u64,
    /// ASYNC_CANCEL CQEs that reported the target op was canceled (res == 0).
    pub cancel_succeeded: u64,
    /// ASYNC_CANCEL CQEs that reported the target was not found (-ENOENT):
    /// the op had already completed.
    pub cancel_not_found: u64,
    /// ASYNC_CANCEL CQEs that reported the target was already executing and
    /// could not be interrupted (-EALREADY). A rising count is the hung-disk
    /// signal that makes drain-to-zero non-terminating (C4,
    /// rustfs/backlog#1055).
    pub cancel_already: u64,
    /// Kernel CQ-ring overflow counter. MUST stay 0: a non-zero value means
    /// CQEs were lost, so their pending entries are never reclaimed and drain
    /// never completes. Treated as fatal (C5, rustfs/backlog#1056).
    pub cq_overflow: u64,
    /// `ring.submit()` calls that returned a non-transient error. A rising count
    /// means `io_uring_enter` is persistently failing (e.g. a seccomp/LSM policy
    /// applied after startup); the driver shuts the shard down after a bounded
    /// run of consecutive failures so callers fall back instead of stalling
    /// (rustfs/backlog#1162).
    pub submit_errors: u64,
}

enum Msg {
    Read {
        id: u64,
        file: Arc<File>,
        offset: u64,
        len: usize,
        done: oneshot::Sender<io::Result<Vec<u8>>>,
        /// Backpressure permit, acquired before the op reaches the driver and
        /// released only when the pending entry is dropped at the final CQE
        /// (rustfs/backlog#1060/#1102). If the driver rejects the op (shutting
        /// down) the permit is dropped with the message — released immediately.
        permit: OwnedSemaphorePermit,
        /// Block size the read must be aligned to. `1` means a normal buffered
        /// read; `> 1` means the file was opened `O_DIRECT` and the driver must
        /// read the block-aligned superset range into a block-aligned buffer
        /// (rustfs/backlog#1102).
        align: usize,
    },
    Cancel {
        id: u64,
    },
    Shutdown,
    /// Test-only fault injection (rustfs/backlog#1103): unwind the driver thread
    /// with ops in flight so the `DriverState::Drop` abort barrier (C2/#1054) is
    /// exercised. Never present in a default build.
    #[cfg(feature = "fault-injection")]
    TestPanic,
}

/// One in-flight LOGICAL read. This struct — not the caller — owns everything
/// the kernel touches:
///
/// - `buf`: the destination buffer. Its heap allocation must stay put until
///   the final CQE; the `Vec` itself may move (HashMap rehash) since that
///   never relocates the heap block. It is never resized or dropped before
///   the CQE handler removes this entry.
/// - `file`: keeps the fd open even if every caller-side clone is dropped, and
///   supplies the fd for short-read resubmission. Without it, dropping the
///   future could close the fd while an SQE built from that fd still sits in
///   the backlog (SQE construction → io_uring_enter window), and a recycled
///   fd number would make the kernel read the WRONG file (spike finding, with
///   the corrected mechanism per rustfs/backlog#1063).
/// - `offset`/`nread`: track a short-read resubmit loop (C9,
///   rustfs/backlog#1058). io_uring may legally short-read a regular file;
///   the driver resubmits the remainder into `buf[nread..]` until the request
///   is fully satisfied or a real EOF (res == 0) is seen, so reclamation
///   happens only at the FINAL CQE of the logical read.
/// - `_permit`: the backpressure permit. Holding it here makes the
///   "release at the CQE, never at future drop" rule (rustfs/backlog#1060) a
///   property of the type: the permit is dropped exactly when this entry is
///   removed at the final CQE. A short-read resubmit keeps the entry, and thus
///   the permit, so in-flight memory stays bounded.
/// - Alignment geometry (rustfs/backlog#1102). For a buffered read these are
///   `pad = head = 0`, `align = 1`, `region_len = want`, so every rule below
///   collapses to the plain case. For an `O_DIRECT` read the driver reads the
///   block-aligned superset `[offset, offset + region_len)` into
///   `buf[pad .. pad + region_len]` (both block-aligned) and hands the caller
///   only `buf[pad + head .. pad + head + want]` — alignment padding never
///   escapes.
struct Pending {
    buf: Vec<u8>,
    file: Arc<File>,
    done: Option<oneshot::Sender<io::Result<Vec<u8>>>>,
    /// Kernel read offset: the block-aligned offset for a direct read, the
    /// logical offset for a buffered one, `CURRENT_POSITION` for a stream.
    offset: u64,
    /// Bytes already read into the read region (`buf[pad..]`).
    nread: usize,
    _permit: OwnedSemaphorePermit,
    /// Offset inside `buf` where the block-aligned read region starts.
    pad: usize,
    /// Bytes of the read region that precede the caller's logical range.
    head: usize,
    /// Logical length the caller asked for.
    want: usize,
    /// Bytes the kernel is asked to read (block-aligned for a direct read).
    region_len: usize,
    /// `1` for buffered, the block size for `O_DIRECT`.
    align: usize,
    /// Consecutive transient-errno (EINTR/EAGAIN) retries since the last byte of
    /// progress, bounded by `MAX_TRANSIENT_RETRIES` so a storm cannot spin the
    /// driver thread (rustfs/backlog#1166). Reset whenever a read makes progress.
    transient_retries: u32,
}

/// Where a [`ReadHandle`] is in its lifecycle (rustfs/backlog#1102).
enum HandleState {
    /// Nothing was ever handed to the driver (a rejected parameter, or the
    /// driver was already gone). The result is already sitting in `rx`, and
    /// there is no buffer, permit, or SQE to reclaim.
    Inert,
    /// Backpressure was saturated at `submit` time, so the permit — and with it
    /// the submission — is deferred to the first poll. The caller's thread is
    /// never blocked. Dropping the handle in this state submitted nothing.
    WaitingPermit {
        acquire: AcquireFut,
        file: Arc<File>,
        offset: u64,
        len: usize,
        align: usize,
        done: oneshot::Sender<io::Result<Vec<u8>>>,
        wake: Arc<EventFd>,
    },
    /// The op is with the driver: its buffer lives in the pending table and is
    /// reclaimed only at the CQE.
    Submitted {
        /// The accepting shard's wakeup eventfd, so a cancel sent on drop wakes
        /// the driver loop now instead of after the heartbeat
        /// (rustfs/backlog#1163).
        wake: Arc<EventFd>,
    },
}

/// Handle to a read. Await it for the result.
///
/// Dropping it before completion abandons the result only; if the op was
/// already submitted it also sends `IORING_OP_ASYNC_CANCEL` (best effort) so the
/// CQE — and with it the buffer reclamation — arrives sooner.
/// `without_cancel_on_drop` disables that to model the bare "quorum drops the
/// future" case.
///
/// Submission is eager whenever a backpressure permit is immediately available
/// (the common case, unchanged from the blocking implementation). Only when the
/// semaphore is saturated does the handle acquire the permit and submit on its
/// first poll, so `submit` never blocks a runtime worker.
pub struct ReadHandle {
    id: u64,
    rx: oneshot::Receiver<io::Result<Vec<u8>>>,
    tx: mpsc::Sender<Msg>,
    finished: bool,
    cancel_on_drop: bool,
    state: HandleState,
}

impl ReadHandle {
    pub fn without_cancel_on_drop(mut self) -> Self {
        self.cancel_on_drop = false;
        self
    }
}

impl Future for ReadHandle {
    type Output = io::Result<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        // Saturated at submit time: take the permit, then hand the op to the
        // driver. The permit rides along in the message and is released only
        // when the pending entry is dropped at the CQE.
        let acquired = match &mut this.state {
            HandleState::WaitingPermit { acquire, .. } => match acquire.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(res) => Some(res),
            },
            _ => None,
        };
        if let Some(res) = acquired {
            let Ok(permit) = res else {
                // The semaphore was closed: the driver is gone.
                this.finished = true;
                return Poll::Ready(Err(io::Error::other("uring driver shut down")));
            };
            // Clone the wake before moving the WaitingPermit out, so the new
            // Submitted state carries it for the drop-cancel path
            // (rustfs/backlog#1163).
            let submitted_wake = match &this.state {
                HandleState::WaitingPermit { wake, .. } => Arc::clone(wake),
                _ => unreachable!("state was WaitingPermit"),
            };
            let HandleState::WaitingPermit {
                file,
                offset,
                len,
                align,
                done,
                wake,
                ..
            } = std::mem::replace(&mut this.state, HandleState::Submitted { wake: submitted_wake })
            else {
                unreachable!("state was WaitingPermit")
            };
            if this
                .tx
                .send(Msg::Read {
                    id: this.id,
                    file,
                    offset,
                    len,
                    done,
                    permit,
                    align,
                })
                .is_err()
            {
                // Driver gone between the acquire and the send; the message
                // (with its permit) is dropped, releasing it.
                this.finished = true;
                return Poll::Ready(Err(io::Error::other("uring driver shut down")));
            }
            wake.signal();
        }

        match Pin::new(&mut this.rx).poll(cx) {
            Poll::Ready(res) => {
                this.finished = true;
                Poll::Ready(match res {
                    Ok(inner) => inner,
                    Err(_) => Err(io::Error::other("uring driver shut down before completion")),
                })
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ReadHandle {
    fn drop(&mut self) {
        // The buffer is deliberately NOT touched here: the driver owns it
        // until the CQE. All we may do is ask the kernel to hurry up. A handle
        // dropped before it was submitted (Inert / WaitingPermit) has no buffer,
        // no permit and no SQE, so there is nothing to cancel.
        if let HandleState::Submitted { wake } = &self.state {
            if !self.finished && self.cancel_on_drop {
                // Close the receiver BEFORE waking the driver. The wake below
                // makes the driver process the cancel immediately, possibly while
                // this drop is still running — before the `rx` field is
                // destroyed. Closing it first guarantees the cancel-induced
                // completion the driver reaps is counted as an orphan reclaim, not
                // delivered to a receiver that is about to drop anyway
                // (rustfs/backlog#1163).
                self.rx.close();
                let _ = self.tx.send(Msg::Cancel { id: self.id });
                // Wake the loop so the cancel is queued now, not after the
                // heartbeat. On an idle ring (the hung-disk case cancel-on-drop
                // exists for) this keeps orphan reclamation prompt.
                wake.signal();
            }
        }
    }
}

/// Process-level io_uring driver: one ring, one driver thread.
/// One io_uring ring plus the thread that drives it.
///
/// Every cancel-safety invariant holds *per shard*, exactly as it did when a
/// driver owned a single ring: this shard's pending table owns its buffers and
/// fds until their CQEs, its permits are released only when a pending entry is
/// dropped, and its bounded drain is what shutdown joins on. A `ReadHandle`
/// carries the `tx` and `wake` of the shard that accepted it, so a cancel or a
/// deferred submission always routes back to that same shard.
struct Shard {
    tx: mpsc::Sender<Msg>,
    handle: Option<JoinHandle<()>>,
    stats: Arc<DriverStats>,
    /// Backpressure permits (one per allowed in-flight op on this ring). Closed
    /// when the driver thread exits so any waiting `ReadHandle` resolves with a
    /// driver-gone error instead of hanging (rustfs/backlog#1102).
    sem: Arc<Semaphore>,
    /// Signaled after every message send so this shard's loop wakes immediately
    /// instead of waiting out the heartbeat (backlog#1102).
    wake_efd: Arc<EventFd>,
}

impl Shard {
    /// Ask the shard's thread to drain and exit, then join it. Idempotent: the
    /// `JoinHandle` is taken, so a later `Drop` is a no-op.
    fn join(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = self.tx.send(Msg::Shutdown);
            self.wake_efd.signal();
            let _ = h.join();
        }
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        self.join();
    }
}

pub struct UringDriver {
    /// One or more independent rings. A cache-hit buffered read completes inline
    /// inside `io_uring_enter`, so the thread driving a ring performs that
    /// read's memcpy — which caps a single-ring driver at one core's memory
    /// bandwidth (~5 GB/s measured, rustfs/backlog#1145). Sharding lifts that
    /// ceiling roughly linearly while keeping the ring set per-disk, so a stalled
    /// disk still cannot starve another disk's rings (rustfs/backlog#1055).
    shards: Vec<Shard>,
    next_id: AtomicU64,
    /// Round-robin cursor for shard selection. Relaxed: it only has to spread
    /// ops, never to order them.
    rr: AtomicUsize,
}

impl UringDriver {
    /// Create the ring AND verify a real `IORING_OP_READ` round-trip on a
    /// temp file before accepting work. `io_uring_setup` succeeding is not
    /// enough: gVisor/seccomp environments can create a ring whose ops then
    /// fail with ENOSYS/EINVAL (backlog#894 probe design).
    /// Start a single-ring driver. Identical to `probe_and_start_sharded(entries, 1)`.
    pub fn probe_and_start(entries: u32) -> Result<Self, ProbeFailure> {
        Self::probe_and_start_sharded(entries, 1)
    }

    /// Start a driver backed by `shards` independent rings, each with `entries`
    /// SQ slots and its own driver thread.
    ///
    /// Use more than one shard when the workload hits the page cache: such reads
    /// complete inline in `io_uring_enter`, so a single driver thread performs
    /// every one of their memcpys and caps the driver at one core's memory
    /// bandwidth. Measured on a 16-core host (rustfs/backlog#1145): 1 ring →
    /// 4890 MB/s, 2 → 8969 MB/s, 4 → 15806 MB/s, with per-ring throughput flat.
    /// Reads that miss the cache are device-bound and do not need sharding.
    ///
    /// In-flight ops are capped at `entries` *per shard* (the invariant that
    /// makes CQ overflow structurally unreachable holds per ring), so the whole
    /// driver admits up to `shards * entries` concurrent reads.
    ///
    /// `shards` is clamped to at least 1. Probing happens on the first shard, so
    /// a restricted environment fails exactly as it does for a single ring; if a
    /// later shard fails to start, the ones already running are shut down and
    /// joined before the error is returned.
    pub fn probe_and_start_sharded(entries: u32, shards: usize) -> Result<Self, ProbeFailure> {
        let mut started = Vec::with_capacity(shards.max(1));
        for i in 0..shards.max(1) {
            // Probe only the first shard (rustfs/backlog#1165): the probe read
            // exercises io_uring against the environment-global temp_dir, so one
            // confirmation is representative. Shards 2..n only create a ring and
            // verify NODROP — this avoids `shards - 1` extra O_TMPFILE
            // create+write+read round-trips per disk on every start and renew.
            // `?` drops `started`, whose `Shard::drop` joins each running thread.
            started.push(Self::start_shard(entries, i == 0)?);
        }
        Ok(Self {
            shards: started,
            next_id: AtomicU64::new(1),
            rr: AtomicUsize::new(0),
        })
    }

    /// Pick the shard for the next op. Round-robin spreads the inline-completion
    /// memcpy across driver threads; correctness does not depend on the choice,
    /// because the handle remembers which shard took the op.
    fn shard(&self) -> &Shard {
        let n = self.shards.len();
        &self.shards[self.rr.fetch_add(1, Ordering::Relaxed) % n]
    }

    fn start_shard(entries: u32, probe: bool) -> Result<Shard, ProbeFailure> {
        let mut ring = IoUring::new(entries).map_err(ProbeFailure::Setup)?;
        // Require the NODROP feature (kernel >= 5.5). Without it, CQ overflow
        // silently drops CQEs, stranding pending entries forever and hanging
        // shutdown (C5, rustfs/backlog#1056). ENOSYS is in the expected-
        // restriction class, so this degrades to the std backend cleanly.
        if !ring.params().is_feature_nodrop() {
            return Err(ProbeFailure::Setup(io::Error::from_raw_os_error(libc::ENOSYS)));
        }
        // Only the first shard runs the real-read probe (rustfs/backlog#1165); the
        // rest still create a ring and check NODROP above, which is what makes
        // io_uring usable, but skip the redundant temp_dir round-trip.
        if probe {
            probe_real_read(&mut ring).map_err(ProbeFailure::ReadOp)?;
        }

        // Wake the driver loop on CQEs (kernel-signaled via a registered
        // eventfd) and on new messages (submit-signaled), replacing the 200 µs
        // busy-poll (backlog#1102). Registration needs the ring, which the
        // driver thread then owns; `cq_efd` is moved in alongside so it outlives
        // the ring (dropped after it, unregistering cleanly).
        let cq_efd = EventFd::new().map_err(ProbeFailure::Setup)?;
        ring.submitter()
            .register_eventfd(cq_efd.as_raw())
            .map_err(ProbeFailure::Setup)?;
        let wake_efd = Arc::new(EventFd::new().map_err(ProbeFailure::Setup)?);
        let thread_wake = Arc::clone(&wake_efd);

        let (tx, rx) = mpsc::channel();
        let stats = Arc::new(DriverStats::default());
        let thread_stats = Arc::clone(&stats);
        // Cap in-flight at the SQ depth (entries), which is < CQ capacity
        // (2*entries), so CQ overflow is structurally unreachable (C5/C10).
        let sem = Arc::new(Semaphore::new(entries as usize));
        let thread_sem = Arc::clone(&sem);
        // Deterministic spawn-failure seam (rustfs/backlog#1164): exercise the
        // degrade-not-panic path without a real cgroup pids-limit. Never present
        // in a default build.
        #[cfg(feature = "fault-injection")]
        if std::env::var_os("RUSTFS_URING_FAULT_SPAWN").is_some() {
            return Err(ProbeFailure::Setup(io::Error::from_raw_os_error(libc::EAGAIN)));
        }

        // Thread creation fails with EAGAIN under a cgroup pids-limit or
        // RLIMIT_NPROC — exactly the constrained environments the probe/degrade
        // design exists for. Degrade to the std backend instead of panicking out
        // of async disk init/reconnect (rustfs/backlog#1164). The spawn happens
        // after the probe read already drained, so on failure `ring`/`cq_efd`
        // (moved into the closure) drop cleanly with no SQE in flight.
        let handle = std::thread::Builder::new()
            .name("uring-spike-driver".into())
            .spawn(move || drive(ring, rx, thread_stats, thread_sem, cq_efd, thread_wake))
            .map_err(ProbeFailure::Setup)?;

        Ok(Shard {
            tx,
            handle: Some(handle),
            stats,
            sem,
            wake_efd,
        })
    }

    /// Positioned read (pread semantics) — regular files, buffered.
    pub fn read_at(&self, file: Arc<File>, offset: u64, len: usize) -> ReadHandle {
        assert_ne!(offset, CURRENT_POSITION, "offset u64::MAX is reserved");
        self.submit(file, offset, len, 1)
    }

    /// Read at the file's current position (read(2) semantics) — pipes.
    pub fn read_current(&self, file: Arc<File>, len: usize) -> ReadHandle {
        self.submit(file, CURRENT_POSITION, len, 1)
    }

    /// Positioned read from a file opened with `O_DIRECT` (rustfs/backlog#1102).
    ///
    /// `align` is the device's logical block size — a power of two, typically
    /// 512 or 4096. `offset` and `len` are the caller's *logical* range and need
    /// no alignment: the driver reads the block-aligned superset range into a
    /// block-aligned buffer and returns exactly `[offset, offset + len)`.
    /// Alignment padding never reaches the caller, so a `BitrotReader` expecting
    /// an exact shard length never sees padded output.
    ///
    /// The caller must have opened `file` with `O_DIRECT`; otherwise this is
    /// just a (correct but pointless) buffered read of the superset range.
    pub fn read_at_direct(&self, file: Arc<File>, offset: u64, len: usize, align: usize) -> ReadHandle {
        assert_ne!(offset, CURRENT_POSITION, "offset u64::MAX is reserved");
        self.submit(file, offset, len, align)
    }

    fn submit(&self, file: Arc<File>, offset: u64, len: usize, align: usize) -> ReadHandle {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        assert_eq!(id & CANCEL_BIT, 0, "op id overflowed into the cancel bit");
        let (done, rx) = oneshot::channel();

        // Bind the op to one shard for its whole life: the permit, the message,
        // the wake, and any later cancel all go to this ring. The handle holds
        // clones of that shard's `tx`/`wake_efd`, so nothing can route a cancel
        // to a ring whose pending table does not hold the op. The rejection paths
        // below return an `Inert` handle that never sends, but still need a `tx`.
        let shard = self.shard();

        // Reject an offset the kernel would answer with a runtime EINVAL that
        // must NOT be mistaken for an environment restriction (C7,
        // rustfs/backlog#1059). The kernel reads `off` as a signed loff_t, so
        // offset > i64::MAX becomes a negative ki_pos → EINVAL. A caller
        // offset-arithmetic bug has to surface as an error here, never as a
        // permanent per-disk fallback. CURRENT_POSITION is the reserved
        // read(2) sentinel and bypasses this check.
        if offset != CURRENT_POSITION && offset > i64::MAX as u64 {
            let _ = done.send(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX (kernel loff_t is signed)",
            )));
            return ReadHandle {
                id,
                rx,
                tx: shard.tx.clone(),
                finished: false,
                cancel_on_drop: false,
                state: HandleState::Inert,
            };
        }

        // Reject a length the kernel would short-read past MAX_RW_COUNT and
        // that the SQE's u32 `len` field would silently truncate: len == 2^32
        // becomes a 0-byte read the caller decodes as a false EOF (C6,
        // rustfs/backlog#1057). Failing fast here also removes the caller-
        // controlled `vec![0u8; len]` capacity-overflow panic that made the
        // unwind-UAF (rustfs/backlog#1054) reachable. P2 must chunk instead.
        if len > MAX_READ_LEN {
            let _ = done.send(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read length exceeds MAX_RW_COUNT (2 GiB - 4 KiB); caller must chunk",
            )));
            return ReadHandle {
                id,
                rx,
                tx: shard.tx.clone(),
                finished: false,
                cancel_on_drop: false,
                state: HandleState::Inert,
            };
        }

        // Reject a bad O_DIRECT alignment, a request whose block-aligned superset
        // range would exceed the kernel's single-read cap, and one whose aligned
        // END crosses i64::MAX — the kernel reads pos as a signed loff_t, so
        // `kernel_offset + region_len > i64::MAX` fails at runtime with
        // EINVAL/EOVERFLOW, exactly the errno class the C7 guard must pre-empt at
        // submit (rustfs/backlog#1102, #1166). Pre-empting it here also makes
        // every resubmit's `next_off < kernel_offset + region_len` provably
        // <= i64::MAX. `align == 1` (buffered) always passes the alignment part.
        match aligned_geometry(offset, len, align) {
            // CURRENT_POSITION (stream) reads use no positional offset — the
            // kernel reads from the current file position — so the i64::MAX end
            // check does not apply to them (their sentinel offset would overflow
            // it). Exempt them exactly as the offset guard above does.
            Some((kernel_offset, _, region_len))
                if region_len <= MAX_READ_LEN
                    && (offset == CURRENT_POSITION
                        || kernel_offset
                            .checked_add(region_len as u64)
                            .is_some_and(|end| end <= i64::MAX as u64)) => {}
            _ => {
                let _ = done.send(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "alignment must be a power of two, and the block-aligned range must fit MAX_RW_COUNT and end within i64::MAX",
                )));
                return ReadHandle {
                    id,
                    rx,
                    tx: shard.tx.clone(),
                    finished: false,
                    cancel_on_drop: false,
                    state: HandleState::Inert,
                };
            }
        }

        // Take a backpressure permit BEFORE the op reaches the driver; it is
        // released only when the pending entry is dropped at the CQE (C10,
        // rustfs/backlog#1060). Acquisition never blocks the caller's thread
        // (rustfs/backlog#1102).
        match Arc::clone(&shard.sem).try_acquire_owned() {
            // Fast path: a permit was free, so submit eagerly — no allocation,
            // no await, and the op is in flight the moment `submit` returns,
            // exactly as with the previous blocking implementation.
            Ok(permit) => {
                if let Err(mpsc::SendError(msg)) = shard.tx.send(Msg::Read {
                    id,
                    file,
                    offset,
                    len,
                    done,
                    permit,
                    align,
                }) {
                    // Driver gone: the op never reached it. Surface an explicit
                    // driver-gone error through `done` instead of letting the
                    // caller infer one from the dropped oneshot, matching the
                    // `Closed` arm below. The permit rides back in `msg` and is
                    // released when it drops here.
                    if let Msg::Read { done, .. } = msg {
                        let _ = done.send(Err(io::Error::other("uring driver shut down")));
                    }
                    return ReadHandle {
                        id,
                        rx,
                        tx: shard.tx.clone(),
                        finished: false,
                        cancel_on_drop: false,
                        state: HandleState::Inert,
                    };
                }
                // Wake the driver loop so the read starts immediately.
                shard.wake_efd.signal();
                ReadHandle {
                    id,
                    rx,
                    tx: shard.tx.clone(),
                    finished: false,
                    cancel_on_drop: true,
                    state: HandleState::Submitted {
                        wake: Arc::clone(&shard.wake_efd),
                    },
                }
            }
            // Saturated: `entries` ops are already in flight. Do NOT block the
            // calling (runtime worker) thread — hand the acquire future to the
            // handle, which awaits it on its first poll and submits then.
            Err(TryAcquireError::NoPermits) => ReadHandle {
                id,
                rx,
                tx: shard.tx.clone(),
                finished: false,
                cancel_on_drop: true,
                state: HandleState::WaitingPermit {
                    acquire: Box::pin(Arc::clone(&shard.sem).acquire_owned()),
                    file,
                    offset,
                    len,
                    align,
                    done,
                    wake: Arc::clone(&shard.wake_efd),
                },
            },
            // The driver has exited and closed the semaphore.
            Err(TryAcquireError::Closed) => {
                let _ = done.send(Err(io::Error::other("uring driver shut down")));
                ReadHandle {
                    id,
                    rx,
                    tx: shard.tx.clone(),
                    finished: false,
                    cancel_on_drop: false,
                    state: HandleState::Inert,
                }
            }
        }
    }

    /// Counters summed across every shard. The conservation identities the
    /// cancel-safety tests assert (`submitted == delivered + orphan_reclaimed`,
    /// `in_flight == 0` after a clean drain) hold per shard, so they hold for
    /// the sum.
    pub fn stats(&self) -> StatsSnapshot {
        let mut snap = StatsSnapshot::default();
        for shard in &self.shards {
            let s = &shard.stats;
            snap.submitted += s.submitted.load(Ordering::SeqCst);
            snap.delivered += s.delivered.load(Ordering::SeqCst);
            snap.orphan_reclaimed += s.orphan_reclaimed.load(Ordering::SeqCst);
            snap.in_flight += s.in_flight.load(Ordering::SeqCst);
            snap.cancel_succeeded += s.cancel_succeeded.load(Ordering::SeqCst);
            snap.cancel_not_found += s.cancel_not_found.load(Ordering::SeqCst);
            snap.cancel_already += s.cancel_already.load(Ordering::SeqCst);
            snap.cq_overflow += s.cq_overflow.load(Ordering::SeqCst);
            snap.submit_errors += s.submit_errors.load(Ordering::SeqCst);
        }
        snap
    }

    /// Test-only fault injection (rustfs/backlog#1103): poison one driver thread
    /// so it panics with ops in flight, exercising the `DriverState::Drop` abort
    /// barrier (C2/#1054). Compiled out entirely unless the `fault-injection`
    /// feature is on — never in a default/production build.
    #[cfg(feature = "fault-injection")]
    pub fn test_inject_panic(&self) {
        let shard = self.shard();
        let _ = shard.tx.send(Msg::TestPanic);
        shard.wake_efd.signal();
    }

    /// Stop accepting work, cancel all in-flight ops, drain every ring to
    /// `in_flight == 0`, then join each driver thread. Only after that is a ring
    /// dropped/unmapped — the shutdown ordering P2 requires, per shard.
    ///
    /// Shards are asked to stop first and joined afterwards, so their bounded
    /// drains overlap instead of serializing `shards * DRAIN_TIMEOUT`.
    pub fn shutdown(mut self) -> StatsSnapshot {
        for shard in &self.shards {
            let _ = shard.tx.send(Msg::Shutdown);
            shard.wake_efd.signal();
        }
        for shard in &mut self.shards {
            shard.join();
        }
        let snap = self.stats();
        // A clean drain leaves in_flight == 0. A non-zero count here means some
        // shard's bounded drain bailed out on a hung device and leaked its
        // ring+buffers to stay memory-safe (C4, rustfs/backlog#1055) — a degraded
        // but safe outcome, not a panic. Callers/tests that require a clean drain
        // assert on the returned snapshot themselves.
        if snap.in_flight != 0 {
            eprintln!(
                "uring-spike shutdown: {} ops still in flight (bounded-drain bailout on a hung device)",
                snap.in_flight
            );
        }
        snap
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {
        // Ask every shard to stop before joining any of them, so their bounded
        // drains overlap. Dropping the `Vec<Shard>` would instead run each
        // `Shard::drop` in turn, serializing up to `shards * DRAIN_TIMEOUT` on a
        // hung device. `Shard::join` is idempotent, so the later drops are no-ops.
        for shard in &self.shards {
            let _ = shard.tx.send(Msg::Shutdown);
            shard.wake_efd.signal();
        }
        for shard in &mut self.shards {
            shard.join();
        }
    }
}

fn probe_real_read(ring: &mut IoUring) -> io::Result<()> {
    let pattern: Vec<u8> = (0..512u32).map(|i| (i * 7 + 13) as u8).collect();

    // Open an anonymous probe file seeded with the pattern. File setup runs
    // BEFORE any SQE, so its errors early-return safely — nothing is in flight.
    let file = open_probe_file(&pattern)?;

    let mut buf = vec![0u8; pattern.len()];
    let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), buf.len() as u32)
        .offset(0)
        .build()
        .user_data(0xB0BE);

    // SAFETY: a push failure means the kernel never accepted the SQE, so
    // `buf`/`file` may be dropped safely on this early return.
    if unsafe { ring.submission().push(&sqe) }.is_err() {
        return Err(io::Error::other("probe: submission queue full"));
    }

    // C1 (rustfs/backlog#1053): once the SQE is handed to the kernel, the read
    // may be punted to io-wq and write into `buf` at ANY later point. Until its
    // CQE arrives, `buf`/`file` must NOT be dropped and the ring must NOT be
    // unmapped — otherwise the kernel writes into freed memory (UAF). The probe
    // path has no pending-table backstop, so we must drain to the CQE here, and
    // any early exit first leaks the buffer ("leak over UAF").
    let res = match drain_probe_cqe(ring) {
        Ok(res) => res,
        Err(e) => {
            // Could not confirm the op terminated: leak `buf` (the real UAF
            // hazard — the kernel may still write 512 bytes into it) and,
            // defensively, `file`. Leaking one 512-byte startup-probe buffer is
            // trivially cheaper than a silent heap corruption.
            std::mem::forget(buf);
            std::mem::forget(file);
            return Err(e);
        }
    };

    // The CQE has arrived: the kernel is done with `buf`, so dropping it and
    // `file` below is now safe.
    if res < 0 {
        Err(io::Error::from_raw_os_error(-res))
    } else if res as usize != pattern.len() || buf != pattern {
        Err(io::Error::other("probe: read completed but data mismatched"))
    } else {
        Ok(())
    }
}

/// Open a probe file seeded with `pattern`, avoiding the symlink/TOCTOU/
/// leftover hazards of a predictable temp path (C3, rustfs/backlog#1061).
///
/// Primary: `O_TMPFILE` — an anonymous inode with no name at all, so there is
/// nothing for an attacker to pre-plant a symlink at, no TOCTOU window, and no
/// leftover file. Fallback (filesystems without O_TMPFILE): create in the temp
/// dir with `O_CREAT|O_EXCL|O_NOFOLLOW` + 0600 + a per-process nonce, then
/// unlink immediately so no attacker-planted symlink is followed and no named
/// file survives.
fn open_probe_file(pattern: &[u8]) -> io::Result<File> {
    let dir = std::env::temp_dir();
    let c_dir = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(|_| io::Error::other("probe dir path has NUL"))?;
    // SAFETY: `c_dir` is a valid NUL-terminated path; O_TMPFILE requires a
    // directory and O_RDWR/O_WRONLY. On success we own the returned fd.
    let fd = unsafe { libc::open(c_dir.as_ptr(), libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC, 0o600) };
    if fd >= 0 {
        let mut file = unsafe { File::from_raw_fd(fd) };
        file.write_all(pattern)?;
        return Ok(file);
    }
    open_probe_file_exclusive(&dir, pattern)
}

fn open_probe_file_exclusive(dir: &std::path::Path, pattern: &[u8]) -> io::Result<File> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nonce = SEQ.fetch_add(1, Ordering::Relaxed);
    let path = dir.join(format!("uring-spike-probe-{}-{}", std::process::id(), nonce));
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| io::Error::other("probe path has NUL"))?;
    // O_EXCL refuses a pre-existing file; O_NOFOLLOW refuses a symlink; 0600 is
    // owner-only. SAFETY: `c_path` is a valid NUL-terminated path; on success
    // we own the fd.
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_RDWR | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut file = unsafe { File::from_raw_fd(fd) };
    file.write_all(pattern)?;
    // Unlink now: the fd stays valid, no named leftover remains.
    // SAFETY: `c_path` is still a valid NUL-terminated path.
    unsafe {
        libc::unlink(c_path.as_ptr());
    }
    Ok(file)
}

/// Wait for the probe SQE's CQE and return its raw result.
///
/// The SQE has already been pushed; this only drains it. `submit_and_wait`
/// interrupted by a signal returns EINTR — since the kernel consumed the SQE
/// atomically before the wait phase, we retry the WAIT only and never re-push
/// (C8, backlog#1059). A bounded attempt count keeps a probe that hit a hung
/// device from blocking forever; exhausting it returns an error that drives
/// the caller's leak-over-UAF fallback.
fn drain_probe_cqe(ring: &mut IoUring) -> io::Result<i32> {
    // Bound the wait by WALL-CLOCK, not by an attempt count. `submit_and_wait(1)`
    // parks in the kernel's io_cqring_wait until a CQE or a signal, so a single
    // call can block forever when the probe read never completes — e.g. a
    // temp_dir backed by a hung/D-state or NFS device. Since this runs on the
    // caller's (async disk-init) thread, an unbounded block hangs startup. On
    // kernels with EXT_ARG (>= 5.11) pass a timeout to the enter; on older
    // kernels fall back to the blocking wait, whose only real risk is a hung
    // temp_dir (rare) and which the deadline still re-checks between returns
    // (rustfs/backlog#1165). On expiry, error out so the caller's leak-over-UAF
    // fallback degrades the disk to the std backend instead of hanging.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let ext_arg = ring.params().is_feature_ext_arg();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::other("probe: no CQE within the bounded wait"));
        }
        let waited = if ext_arg {
            let ts = types::Timespec::new().sec(remaining.as_secs()).nsec(remaining.subsec_nanos());
            let args = types::SubmitArgs::new().timespec(&ts);
            ring.submitter().submit_with_args(1, &args)
        } else {
            ring.submit_and_wait(1)
        };
        match waited {
            Ok(_) => {}
            // Signal interrupted the wait; the SQE is already in flight, so wait
            // again (do NOT re-push). The deadline still bounds the total time.
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => {}
            // EXT_ARG timeout elapsed with no CQE: loop to re-check the deadline.
            Err(e) if e.raw_os_error() == Some(libc::ETIME) => {}
            Err(e) => return Err(e),
        }
        if let Some(cqe) = ring.completion().next() {
            // fault-injection (backlog#1103 → C1/#1053): the real CQE has arrived,
            // so the kernel is finished with the probe buffer. Forcing the error
            // path here exercises probe_real_read's leak-over-UAF fallback with no
            // live in-flight write to race.
            #[cfg(feature = "fault-injection")]
            if std::env::var_os("RUSTFS_URING_FAULT_PROBE_DRAIN").is_some() {
                return Err(io::Error::other("fault-injection: forced probe drain failure"));
            }
            return Ok(cqe.result());
        }
    }
}

/// Owns everything the kernel can still be writing into: the ring, the
/// pending (orphan) table of in-flight buffers, and the SQE backlog.
///
/// C2 (rustfs/backlog#1054): the "CQE is the only reclamation point"
/// invariant holds only while the driver thread does NOT unwind. On a panic,
/// Rust would drop the pending table (freeing every in-flight buffer) while
/// the kernel may still write into them → mass UAF; reversing drop order does
/// not help because io_uring teardown on ring drop is asynchronous and does
/// not wait for in-flight ops. So this type's `Drop` refuses to run field
/// destructors during an unwind: it aborts the process first, leaving the
/// ring mapped and the buffers allocated (leak over UAF). A storage read path
/// silently corrupting memory is worse than a crash.
struct DriverState {
    ring: IoUring,
    pending: HashMap<u64, Pending>,
    backlog: VecDeque<io_uring::squeue::Entry>,
}

impl Drop for DriverState {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // Abort BEFORE any field destructor runs: the ring stays mapped
            // and the in-flight buffers stay allocated, so the kernel can
            // never write into freed memory.
            eprintln!(
                "uring-spike driver thread panicked with {} ops in flight; \
                 aborting to avoid UAF of in-flight buffers",
                self.pending.len()
            );
            std::process::abort();
        }
        // Normal drop: the shutdown invariant guarantees pending/backlog are
        // empty and in_flight == 0, so unmapping the ring here is safe.
    }
}

/// Hand the caller exactly the logical range `[head, head + want)` of the read
/// region, truncated to what was actually read (rustfs/backlog#1102).
///
/// Alignment padding (`buf[..pad]`), the bytes before the logical range
/// (`head`), and the block-aligned tail after it never reach the caller — a
/// `BitrotReader` expecting an exact shard length would flag padded output as
/// corruption. Only bytes the kernel actually wrote are exposed: `avail` is
/// clamped to `nread`, so the zero-filled remainder of the buffer stays hidden
/// (content hygiene, C12 / rustfs/backlog#1062).
fn deliver(p: &mut Pending) -> Vec<u8> {
    let avail = p.nread.saturating_sub(p.head).min(p.want);
    let start = p.pad + p.head;
    // The buffered path (`align == 1`) has `pad == 0` and `head == 0`, so the
    // logical range already starts at byte 0 — skip the full-buffer memmove and
    // just truncate. Only the O_DIRECT path (nonzero start) needs the shift.
    if start != 0 && avail != 0 {
        p.buf.copy_within(start..start + avail, 0);
    }
    p.buf.truncate(avail);
    std::mem::take(&mut p.buf)
}

/// What to do with a pending entry after its CQE (C9, rustfs/backlog#1058).
enum ReapStep {
    /// The logical read is done: remove the entry and deliver this result.
    Finish(io::Result<Vec<u8>>),
    /// Short read, not EOF: re-queue this SQE for the remainder; keep the entry.
    Resubmit(io_uring::squeue::Entry),
}

fn drive(
    ring: IoUring,
    rx: mpsc::Receiver<Msg>,
    stats: Arc<DriverStats>,
    sem: Arc<Semaphore>,
    cq_efd: EventFd,
    wake_efd: Arc<EventFd>,
) {
    let mut state = DriverState {
        ring,
        pending: HashMap::new(),
        backlog: VecDeque::new(),
    };
    let mut shutting_down = false;
    let mut drain_deadline: Option<Instant> = None;
    // Consecutive non-transient submit failures, and a once-only log latch, for
    // the persistent-submit-failure escape hatch (rustfs/backlog#1162).
    let mut consecutive_submit_errors: u32 = 0;
    let mut submit_error_logged = false;

    // Bounded-drain deadline (C4, rustfs/backlog#1055). Production always uses the
    // fixed DRAIN_TIMEOUT; a fault-injection build may shorten it via env so the
    // leak-over-UAF escape hatch is testable without a 5 s wait (backlog#1103).
    // Read once here (not per turn) so a `--test-threads=1` env toggle in one
    // test never leaks into another's already-running driver thread.
    #[cfg(not(feature = "fault-injection"))]
    let drain_timeout = DRAIN_TIMEOUT;
    #[cfg(feature = "fault-injection")]
    let drain_timeout = std::env::var("RUSTFS_URING_FAULT_DRAIN_TIMEOUT_MS")
        .ok()
        .and_then(|ms| ms.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(DRAIN_TIMEOUT);
    // When set, drop an op's real completion on the floor so it stays pending and
    // the bounded drain is forced onto its timeout path (backlog#1103 → C4/#1055).
    #[cfg(feature = "fault-injection")]
    let fault_stuck_drain = std::env::var_os("RUSTFS_URING_FAULT_STUCK_DRAIN").is_some();

    loop {
        // Block until a CQE is ready (the ring's registered eventfd), a new
        // message arrives (the wakeup eventfd), or the heartbeat elapses —
        // this replaces the spike's 200 µs busy-poll (backlog#1102). Draining
        // both eventfds after waking keeps them from staying spuriously
        // readable; a missed edge is harmless because the CQ/mpsc are re-checked
        // unconditionally below.
        wait_for_events(&cq_efd, &wake_efd, LOOP_HEARTBEAT);
        cq_efd.drain();
        wake_efd.drain();

        // 1. Intake: drain all queued messages (the wait above did the blocking,
        //    so this is purely non-blocking).
        loop {
            let msg = match rx.try_recv() {
                Ok(m) => m,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    shutting_down = true;
                    break;
                }
            };
            match msg {
                Msg::Read {
                    id,
                    file,
                    offset,
                    len,
                    done,
                    permit,
                    align,
                } => {
                    if shutting_down {
                        let _ = done.send(Err(io::Error::other("uring driver shutting down")));
                        // The op never became in-flight; dropping `permit` here
                        // returns it immediately.
                        drop(permit);
                        continue;
                    }
                    // `submit` already validated this geometry.
                    let (kernel_offset, head, region_len) =
                        aligned_geometry(offset, len, align).expect("submit validated the geometry");
                    // For an O_DIRECT read the kernel needs a block-aligned
                    // buffer, so over-allocate by `align - 1` and start the read
                    // region at the first aligned byte inside the allocation.
                    // For a buffered read this degenerates to `vec![0u8; len]`.
                    // `submit` already capped `align <= MAX_READ_LEN` and
                    // `region_len <= MAX_READ_LEN`, so this add cannot overflow;
                    // the checked form keeps the invariant explicit rather than
                    // relying on it silently.
                    let cap = match region_len.checked_add(align - 1) {
                        Some(cap) => cap,
                        None => {
                            let _ = done.send(Err(io::Error::other("aligned O_DIRECT allocation size overflow")));
                            drop(permit);
                            continue;
                        }
                    };
                    let mut buf = vec![0u8; cap];
                    let pad = buf.as_ptr().align_offset(align);
                    // Runtime guard (not a debug-only assert): if the allocator
                    // ever returned a block `align_offset` cannot satisfy, refuse
                    // the read instead of doing UB pointer arithmetic below.
                    if pad == usize::MAX || pad.checked_add(region_len).is_none_or(|end| end > buf.len()) {
                        let _ = done.send(Err(io::Error::other("could not align O_DIRECT read buffer")));
                        drop(permit);
                        continue;
                    }

                    // The raw pointer is captured before `buf` moves into the
                    // table; moving the Vec never relocates its heap block, and
                    // the entry is only removed at the CQE. `region_len as u32`
                    // is lossless: `submit` rejected anything > MAX_READ_LEN.
                    //
                    // SAFETY: `pad <= align - 1` and `pad + region_len <=
                    // buf.len()`, so the pointer stays inside the allocation.
                    let region_ptr = unsafe { buf.as_mut_ptr().add(pad) };
                    let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), region_ptr, region_len as u32)
                        .offset(kernel_offset)
                        .build()
                        .user_data(id);
                    state.pending.insert(
                        id,
                        Pending {
                            buf,
                            file,
                            done: Some(done),
                            offset: kernel_offset,
                            nread: 0,
                            // Released exactly when this entry is removed at the
                            // final CQE — never at future drop (backlog#1060).
                            _permit: permit,
                            pad,
                            head,
                            want: len,
                            region_len,
                            align,
                            transient_retries: 0,
                        },
                    );
                    stats.submitted.fetch_add(1, Ordering::SeqCst);
                    stats.in_flight.fetch_add(1, Ordering::SeqCst);
                    state.backlog.push_back(sqe);
                }
                Msg::Cancel { id } => {
                    if state.pending.contains_key(&id) {
                        state
                            .backlog
                            .push_back(opcode::AsyncCancel::new(id).build().user_data(id | CANCEL_BIT));
                    }
                }
                Msg::Shutdown => {
                    shutting_down = true;
                    for id in state.pending.keys() {
                        state
                            .backlog
                            .push_back(opcode::AsyncCancel::new(*id).build().user_data(*id | CANCEL_BIT));
                    }
                }
                #[cfg(feature = "fault-injection")]
                Msg::TestPanic => {
                    // Panic WITH buffers still in flight: the abort barrier in
                    // `DriverState::Drop` must fire rather than let the unwind
                    // free them under the kernel (rustfs/backlog#1103 → C2/#1054).
                    panic!(
                        "fault-injection: driver thread panic requested with {} ops in flight",
                        state.pending.len()
                    );
                }
            }
        }

        // 2. Push backlog into the SQ (stop when full; retry next turn).
        {
            let mut sq = state.ring.submission();
            while let Some(sqe) = state.backlog.pop_front() {
                // SAFETY: read SQEs point into `pending`-owned buffers that
                // live until their CQE; cancel SQEs carry no pointers.
                if unsafe { sq.push(&sqe) }.is_err() {
                    state.backlog.push_front(sqe);
                    break;
                }
            }
        }
        match state.ring.submit() {
            Ok(_) => consecutive_submit_errors = 0,
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                // CQ-overflow backpressure on pre-5.19 NODROP kernels: the
                // kernel refuses new submissions until we reap. Keep the
                // backlog and reap this turn instead of spinning (C5,
                // rustfs/backlog#1056). Transient — not a submit failure.
                consecutive_submit_errors = 0;
            }
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => {
                // A signal interrupted the enter and the SQEs were not consumed;
                // retry on the next loop turn (transient).
                consecutive_submit_errors = 0;
            }
            Err(e) => {
                // Any other errno: the queued SQEs were not accepted, so their
                // CQEs never arrive and their callers would hang. A brief run may
                // be transient (EAGAIN under memory pressure); a persistent one
                // (e.g. EPERM from a seccomp/LSM policy applied after startup)
                // must NOT be retried forever in silence. Count it, log once, and
                // after a bounded run of consecutive failures shut the shard down
                // so the drain + bounded-drain bailout fail every pending caller
                // with an error — they fall back to the std backend
                // (rustfs/backlog#1162).
                stats.submit_errors.fetch_add(1, Ordering::SeqCst);
                consecutive_submit_errors += 1;
                if !submit_error_logged {
                    submit_error_logged = true;
                    eprintln!("uring-spike driver: ring.submit() failed ({e}); retrying, will shut down if persistent");
                }
                if !shutting_down && consecutive_submit_errors >= MAX_CONSECUTIVE_SUBMIT_ERRORS {
                    eprintln!(
                        "uring-spike driver: {consecutive_submit_errors} consecutive submit failures; \
                         shutting down so callers fall back to the std backend"
                    );
                    shutting_down = true;
                    for id in state.pending.keys() {
                        state
                            .backlog
                            .push_back(opcode::AsyncCancel::new(*id).build().user_data(*id | CANCEL_BIT));
                    }
                }
            }
        }

        // 3. Reap. A Pending entry (and thus its buffer) is dropped ONLY when
        //    the logical read finishes; a short read is resubmitted for the
        //    remainder and the entry stays put (C9, rustfs/backlog#1058).
        while let Some(cqe) = state.ring.completion().next() {
            let ud = cqe.user_data();
            if ud & CANCEL_BIT != 0 {
                // Result of the AsyncCancel op itself; the read's own CQE
                // (ECANCELED or success) still arrives separately. Record the
                // three-state outcome for diagnosability (C4,
                // rustfs/backlog#1055): EALREADY means the read is executing
                // and cannot be interrupted, i.e. its CQE may never come on a
                // hung device — the signal the bounded drain below relies on.
                match cqe.result() {
                    0 => stats.cancel_succeeded.fetch_add(1, Ordering::SeqCst),
                    r if r == -libc::ENOENT => stats.cancel_not_found.fetch_add(1, Ordering::SeqCst),
                    r if r == -libc::EALREADY => stats.cancel_already.fetch_add(1, Ordering::SeqCst),
                    _ => 0,
                };
                continue;
            }
            // fault-injection (backlog#1103 → C4/#1055): drop this real completion
            // so the op stays pending and the bounded drain must take its
            // DRAIN_TIMEOUT leak path. The CQE has already arrived, so the kernel
            // is done with the buffer — the eventual `forget` leaks a completed
            // allocation, never live memory.
            #[cfg(feature = "fault-injection")]
            if fault_stuck_drain && state.pending.contains_key(&ud) {
                continue;
            }
            let res = cqe.result();
            if !state.pending.contains_key(&ud) {
                continue;
            }

            // Decide the next step while borrowing the entry, then act after
            // the borrow ends (finish removes it; resubmit re-queues an SQE).
            let step = {
                let p = state.pending.get_mut(&ud).expect("checked above");
                if res < 0 {
                    let err = -res;
                    // C7 three-class contract (rustfs/backlog#1166): a transient
                    // errno (EINTR/EAGAIN) must be retried, not surfaced as the
                    // read's final result — surfacing it would also discard the
                    // already-read prefix of a resubmit. Bounded per logical read
                    // so a storm cannot spin the driver thread. Streams
                    // (CURRENT_POSITION) cannot resubmit positionally; ECANCELED
                    // and every other errno terminate the logical read.
                    let transient = err == libc::EINTR || err == libc::EAGAIN;
                    if transient
                        && p.offset != CURRENT_POSITION
                        && p.nread < p.region_len
                        && p.transient_retries < MAX_TRANSIENT_RETRIES
                    {
                        p.transient_retries += 1;
                        let remaining = p.region_len - p.nread;
                        // SAFETY: `pad + nread < pad + region_len <= buf.len()`,
                        // and the buffer lives in the pending table until the CQE.
                        let ptr = unsafe { p.buf.as_mut_ptr().add(p.pad + p.nread) };
                        let next_off = p.offset + p.nread as u64;
                        let sqe = opcode::Read::new(types::Fd(p.file.as_raw_fd()), ptr, remaining as u32)
                            .offset(next_off)
                            .build()
                            .user_data(ud);
                        ReapStep::Resubmit(sqe)
                    } else {
                        // Error (incl. ECANCELED, or a transient errno past its
                        // retry budget) terminates the logical read.
                        ReapStep::Finish(Err(io::Error::from_raw_os_error(err)))
                    }
                } else if res == 0 {
                    // Real EOF: deliver whatever of the logical range was read.
                    ReapStep::Finish(Ok(deliver(p)))
                } else {
                    p.nread += res as usize;
                    // Progress resets the transient-retry budget (rustfs/backlog#1166).
                    p.transient_retries = 0;
                    // Only POSITIONED reads (read_at / read_at_direct, whole-range
                    // pread contract) resubmit a short read. CURRENT_POSITION
                    // reads (read_current on pipes/streams) follow read(2)
                    // semantics: a short read is a valid final result and must be
                    // delivered as-is — resubmitting would block forever waiting
                    // for stream data that may never come.
                    let is_stream = p.offset == CURRENT_POSITION;
                    let covered = p.nread >= p.head + p.want;
                    // An O_DIRECT resubmit must stay block-aligned. The kernel
                    // returns block multiples except at the file tail, so a
                    // non-multiple means we reached EOF: stop and deliver.
                    let unaligned_tail = p.align > 1 && !p.nread.is_multiple_of(p.align);
                    if is_stream || covered || unaligned_tail || p.nread >= p.region_len {
                        ReapStep::Finish(Ok(deliver(p)))
                    } else {
                        // Positioned short read, not EOF: resubmit the remainder
                        // into the read region. The buffer stays owned by the
                        // driver and in_flight is unchanged — one logical op.
                        // For a direct read, `pad + nread` and `offset + nread`
                        // are both block-aligned, as is `remaining`.
                        let remaining = p.region_len - p.nread;
                        // SAFETY: `pad + nread < pad + region_len <= buf.len()`,
                        // and the buffer lives in the pending table until the CQE.
                        let ptr = unsafe { p.buf.as_mut_ptr().add(p.pad + p.nread) };
                        let next_off = p.offset + p.nread as u64;
                        let sqe = opcode::Read::new(types::Fd(p.file.as_raw_fd()), ptr, remaining as u32)
                            .offset(next_off)
                            .build()
                            .user_data(ud);
                        ReapStep::Resubmit(sqe)
                    }
                }
            };

            match step {
                ReapStep::Finish(outcome) => {
                    // Content hygiene (C12, rustfs/backlog#1062): the delivered
                    // bytes are ⊆ [0, res) — buf was freshly zeroed per op and
                    // truncated to res. When P3 reuses a driver-owned slab
                    // across requests, this ⊆ [0, res) property MUST be
                    // preserved or a previous tenant's object bytes leak.
                    let mut p = state.pending.remove(&ud).expect("checked above");
                    match p.done.take().expect("done sender set at submit").send(outcome) {
                        Ok(()) => stats.delivered.fetch_add(1, Ordering::SeqCst),
                        // Caller dropped the future: the buffer survived in
                        // the table until this final CQE and is reclaimed here.
                        Err(_) => stats.orphan_reclaimed.fetch_add(1, Ordering::SeqCst),
                    };
                    stats.in_flight.fetch_sub(1, Ordering::SeqCst);
                    // `p` (and with it `_permit`) is dropped here, at the CQE
                    // and pending-table removal — never at future drop (C10,
                    // rustfs/backlog#1060). No manual release to forget.
                }
                ReapStep::Resubmit(sqe) => state.backlog.push_back(sqe),
            }
        }

        // A short-read resubmit queued during reap must reach the kernel in THIS
        // turn, not wait out the next heartbeat: reap runs after the earlier
        // push+submit, so without this flush the remainder sits idle for up to
        // LOOP_HEARTBEAT (rustfs/backlog#1163). Only the resubmit/backlog-residue
        // case makes this non-empty, so a fully idle turn skips it.
        if !state.backlog.is_empty() {
            {
                let mut sq = state.ring.submission();
                while let Some(sqe) = state.backlog.pop_front() {
                    // SAFETY: read SQEs point into `pending`-owned buffers that
                    // live until their CQE; cancel SQEs carry no pointers.
                    if unsafe { sq.push(&sqe) }.is_err() {
                        state.backlog.push_front(sqe);
                        break;
                    }
                }
            }
            // A persistent submit failure is already counted and acted on by the
            // main submit above (rustfs/backlog#1162); this same-turn flush just
            // retries opportunistically, so a transient error here is fine.
            let _ = state.ring.submit();
        }

        // Monitor CQ overflow. With NODROP (asserted at probe) the crate's
        // submit() auto-flushes the kernel overflow list, so this should stay
        // 0; any non-zero value means CQEs were lost — pending entries would
        // never be reclaimed. Record it as a fatal signal (C5,
        // rustfs/backlog#1056).
        let overflow = state.ring.completion().overflow();
        if overflow != 0 {
            stats.cq_overflow.store(overflow as u64, Ordering::SeqCst);
            eprintln!("uring-spike driver: CQ overflow = {overflow}; CQEs lost — treat as fatal in P2");
        }

        // 4. Exit when drained: the kernel no longer references any buffer, so
        //    dropping the ring (unmap) is safe. If a hung device keeps a CQE
        //    from ever arriving, bail out under a bounded deadline instead of
        //    blocking forever (C4, rustfs/backlog#1055).
        if shutting_down {
            if state.pending.is_empty() && state.backlog.is_empty() {
                // Close the semaphore so any handle still awaiting a permit
                // resolves with a driver-gone error instead of hanging.
                sem.close();
                return; // clean drain: DriverState drops normally, ring unmaps.
            }
            let deadline = *drain_deadline.get_or_insert_with(|| Instant::now() + drain_timeout);
            if Instant::now() >= deadline {
                // A CQE may never arrive (ASYNC_CANCEL cannot interrupt an
                // in-execution regular-file read on a hung disk). We must NOT
                // unmap the ring or free the still-in-flight buffers — leak the
                // whole state (leak over UAF) and exit so shutdown() returns.
                eprintln!(
                    "uring-spike driver: bounded drain timed out with {} ops still in flight; \
                     leaking ring + buffers to stay memory-safe",
                    state.pending.len()
                );
                // Fail every stranded caller BEFORE leaking the pending table.
                // `oneshot::Sender::send` consumes the sender and never touches
                // `p.buf`, so the kernel-owned buffer stays allocated (leak over
                // UAF preserved) while an awaited `ReadHandle` resolves with an
                // error instead of pending forever — every other driver-gone path
                // already delivers an error, and this one must too
                // (rustfs/backlog#1161).
                for p in state.pending.values_mut() {
                    if let Some(tx) = p.done.take() {
                        let _ = tx.send(Err(io::Error::other("uring driver leaked op on bounded-drain timeout")));
                    }
                }
                // Close the semaphore so any handle still awaiting a permit
                // resolves with a driver-gone error too. The leaked pending
                // entries keep their permits, which is fine: nothing waits on
                // them any more.
                sem.close();
                std::mem::forget(state);
                return;
            }
        }
        // No pacing sleep: `wait_for_events` at the top of the loop blocks until
        // the next CQE, message, or heartbeat (backlog#1102).
    }
}
