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

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io;
use std::io::Write as _;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;

#[cfg(test)]
#[path = "shard_policy_tests.rs"]
mod shard_policy_tests;

#[cfg(test)]
#[path = "batch_read_tests.rs"]
mod batch_read_tests;

#[cfg(feature = "diagnostics")]
use crate::diagnostics::{Diagnostics, DiagnosticsSnapshot, Trace};

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

/// Heartbeat used when the shard is fully idle — no in-flight ops and not
/// shutting down (rustfs/backlog#1169). New work still wakes the loop instantly
/// via `wake_efd` and completions via the registered `cq_efd`; this only bounds
/// the fallback wait, so a much longer value cuts idle timer/​syscall churn
/// across many per-disk shards without affecting latency.
const IDLE_HEARTBEAT: Duration = Duration::from_secs(1);

/// Per-ring cap on io-wq BOUNDED workers (rustfs/backlog#1169). Cold buffered
/// and O_DIRECT reads punted to io-wq each spawn a bounded worker, and the
/// kernel default is min(sq_entries, 4*nCPU) PER ring — one ring per shard per
/// disk can otherwise materialize thousands of PF_IO_WORKER threads under a
/// cold-read burst. Best-effort (needs kernel >= 5.15); older kernels keep the
/// default.
const IOWQ_MAX_BOUNDED_WORKERS: u32 = 16;

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

// Bound each phase so intake/allocation cannot indefinitely defer reap, and a
// CQ burst cannot indefinitely defer cancel/shutdown intake. These are fairness
// limits, not tuned throughput settings (rustfs/backlog#2647).
const TURN_MESSAGES: usize = 64;
const TURN_COMPLETIONS: usize = 64;
const TURN_ALLOCATION_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct TurnBudget {
    messages: usize,
    completions: usize,
    allocation_bytes: usize,
}

impl TurnBudget {
    fn can_take_message(&self) -> bool {
        self.messages < TURN_MESSAGES && self.allocation_bytes < TURN_ALLOCATION_BYTES
    }

    fn can_reap(&self) -> bool {
        self.completions < TURN_COMPLETIONS
    }

    fn continue_without_wait(&self, ready_cqe: bool, queued_submission: bool, submitted: usize) -> bool {
        // A hit budget can leave work behind after its eventfd edge was drained.
        // One extra empty turn at an exact boundary is harmless. Merely having
        // unaccepted SQEs must not spin after EBUSY, EINTR, errors or Ok(0).
        !self.can_take_message() || !self.can_reap() || ready_cqe || (queued_submission && submitted > 0)
    }
}

#[cfg(test)]
#[path = "driver_loop_budget_tests.rs"]
mod loop_budget_tests;

/// Owned `eventfd(2)` used to wake the driver loop (backlog#1102): one is
/// registered with the ring so the kernel signals it on every CQE, the other is
/// signaled by `submit`/shutdown so a new message wakes the loop immediately —
/// together they replace the spike's 200 µs busy-poll.
struct EventFd {
    fd: std::os::fd::RawFd,
    error_logged: AtomicBool,
}

impl EventFd {
    fn new() -> io::Result<Self> {
        // SAFETY: eventfd returns a fresh owned fd or -1; the flags are valid.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            error_logged: AtomicBool::new(false),
        })
    }

    fn as_raw(&self) -> std::os::fd::RawFd {
        self.fd
    }

    /// Make the fd readable. A saturated counter (EAGAIN) is fine — it is
    /// already readable, which is all a wakeup needs.
    fn signal(&self) {
        let v: u64 = 1;
        self.transfer("signal", || {
            // SAFETY: writing 8 bytes from a valid u64 to an eventfd we own.
            unsafe { libc::write(self.fd, (&v as *const u64).cast(), 8) }
        });
    }

    /// Reset the counter. EFD_NONBLOCK guarantees this never blocks; a single
    /// successful read drains the whole counter. A later concurrent signal is
    /// left readable for the next turn; intake and CQ are checked after drain.
    fn drain(&self) {
        let mut v: u64 = 0;
        self.transfer("drain", || {
            // SAFETY: reading 8 bytes into a valid u64 from an eventfd we own.
            unsafe { libc::read(self.fd, (&mut v as *mut u64).cast(), 8) }
        });
    }

    fn transfer(&self, operation: &'static str, mut syscall: impl FnMut() -> isize) {
        let result = eventfd_transfer(|| {
            let count = syscall();
            if count < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count as usize)
            }
        });
        if let Err(error) = result
            && !self.error_logged.swap(true, Ordering::Relaxed)
        {
            // At most one warning per fd, including shared producer wake fds.
            // The heartbeat still checks queued work if a wake syscall fails.
            tracing::warn!(operation, %error, "uring driver: eventfd operation failed; heartbeat remains active");
        }
    }
}

/// An eventfd transfer is all-or-nothing. Retry interrupted syscalls; EAGAIN
/// means a signal is already pending (write) or no signal remains (read).
fn eventfd_transfer(mut syscall: impl FnMut() -> io::Result<usize>) -> io::Result<()> {
    loop {
        match syscall() {
            Ok(8) => return Ok(()),
            Ok(_) => return Err(io::Error::other("eventfd transferred an unexpected byte count")),
            Err(error) if error.raw_os_error() == Some(libc::EINTR) => continue,
            Err(error) if error.raw_os_error() == Some(libc::EAGAIN) => return Ok(()),
            Err(error) => return Err(error),
        }
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

impl std::fmt::Display for ProbeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Setup(err) => write!(f, "io_uring setup failed: {err}"),
            Self::ReadOp(err) => write!(f, "io_uring probe read failed: {err}"),
        }
    }
}

impl std::error::Error for ProbeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Setup(err) | Self::ReadOp(err) => err,
        })
    }
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
// `release()` call: the count and optional byte permits travel with `Msg::Read` into the
// `Pending` entry and is dropped exactly when the entry is removed at the final
// CQE. A short-read resubmit keeps the entry — and thus the permit.
//
// Acquisition never blocks the caller's thread: `submit` takes the permit with
// `try_acquire_owned()` on the common unsaturated path (no allocation, no await,
// submission stays eager), and when saturated it hands the acquire future to the
// returned `ReadHandle`, which awaits it on its first poll and submits then.

/// Boxed `Semaphore::acquire_owned` future held by a saturated `ReadHandle`.
type AcquireFut = Pin<Box<dyn Future<Output = Result<ReadPermits, tokio::sync::AcquireError>> + Send>>;

/// Optional resource limits shared by all shards of one driver.
///
/// Defaults preserve the existing count-only admission policy. Limits cover
/// driver-owned read allocations, not queued handle metadata, allocator overhead,
/// completion copies, or returned results retained by the caller. They are not
/// a whole-process RSS bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadLimits {
    /// Maximum logical length of one read. `None` retains the kernel read cap.
    /// A request exceeding this limit returns `InvalidInput` without read-buffer allocation.
    pub max_read_len: Option<usize>,
    /// Maximum sum of reserved read-buffer bytes across all shards.
    ///
    /// Direct reads charge their aligned superset plus `align - 1` allocation
    /// padding. Reservations start before allocation and end at terminal read
    /// completion, including canceled operations; leaked operations stay charged.
    /// Requests larger than this budget return `InvalidInput`, never wait.
    /// Zero and values above Tokio's `Semaphore::MAX_PERMITS` are invalid.
    /// Shutdown or any shard exit closes count and byte admission for the entire
    /// driver when enabled, waking all waiters even if buffers must be leaked.
    pub max_in_flight_bytes: Option<usize>,
}

/// Shared whole-driver reservations for in-flight read-buffer budgets.
///
/// Clone this handle to give multiple drivers the same pool. Each driver reserves
/// its entire configured [`ReadLimits::max_in_flight_bytes`] before probing, even
/// while idle; local per-read admission is unchanged. Reservations are returned
/// only after their driver, deferred admission and accepted reads release them.
/// A leaked pending read retains the driver's whole reservation permanently.
///
/// This bounds the sum of participating drivers' reserved read-buffer limits,
/// not completed results, allocator overhead, probe/ring allocations or RSS.
/// Shutting down one driver never closes the pool or another driver's admission.
/// Creating a separate pool creates a separate accounting domain.
///
/// # Example
///
/// ```no_run
/// use std::{fs::File, sync::Arc};
/// use rustfs_uring::{ReadLimits, SharedReadBudget, UringDriver};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = SharedReadBudget::new(24 << 20)?;
/// let first = UringDriver::probe_and_start_with_shared_budget(
///     64, 1,
///     ReadLimits { max_read_len: None, max_in_flight_bytes: Some(8 << 20) },
///     &pool,
/// )?;
/// let second = UringDriver::probe_and_start_with_shared_budget(
///     64, 2,
///     ReadLimits { max_read_len: None, max_in_flight_bytes: Some(16 << 20) },
///     &pool,
/// )?;
/// assert_eq!(pool.available(), 0); // whole reservations, even while idle
/// let file = Arc::new(File::open("object.bin")?);
/// let reads = [first.read_at(file.clone(), 0, 4096), second.read_at(file, 4096, 4096)];
/// drop(reads); // also release any deferred admission owners
/// let first_stats = first.shutdown();
/// let second_stats = second.shutdown();
/// if first_stats.in_flight == 0 && second_stats.in_flight == 0 {
///     assert_eq!(pool.available(), pool.capacity());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct SharedReadBudget {
    inner: Arc<SharedReadBudgetInner>,
}

#[derive(Debug)]
struct SharedReadBudgetInner {
    capacity: usize,
    available: AtomicUsize,
}

impl SharedReadBudget {
    /// Create a pool with `total` bytes of reservation capacity.
    ///
    /// Zero returns `InvalidInput`. This does not allocate `total` bytes or
    /// require a runtime. Individual driver limits must still fit Tokio's
    /// `Semaphore::MAX_PERMITS`, but pool capacity is not narrowed to `u32`.
    pub fn new(total: usize) -> io::Result<Self> {
        if total == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "shared read budget must be nonzero"));
        }
        Ok(Self {
            inner: Arc::new(SharedReadBudgetInner {
                capacity: total,
                available: AtomicUsize::new(total),
            }),
        })
    }

    /// The pool's fixed whole-driver reservation capacity, in bytes.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Advisory snapshot of unreserved capacity, in bytes.
    ///
    /// This is not a measurement of live buffers: an idle participating driver
    /// still holds its whole reservation. Concurrent construction or cleanup
    /// can change this value immediately after it is read.
    pub fn available(&self) -> usize {
        self.inner.available.load(Ordering::Acquire)
    }

    fn reserve(&self, bytes: usize) -> io::Result<Arc<SharedReadReservation>> {
        if bytes == 0 || bytes > self.inner.capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "driver read limit must fit the shared read budget",
            ));
        }
        self.inner
            .available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| available.checked_sub(bytes))
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "shared read budget has insufficient unreserved capacity"))?;
        Ok(Arc::new(SharedReadReservation {
            pool: Arc::clone(&self.inner),
            bytes,
        }))
    }
}

struct SharedReadReservation {
    pool: Arc<SharedReadBudgetInner>,
    bytes: usize,
}

impl Drop for SharedReadReservation {
    fn drop(&mut self) {
        // One receipt refunds its successful checked subtraction exactly once.
        // Read-path clones only touch Arc counts, never this global counter.
        self.pool.available.fetch_add(self.bytes, Ordering::Release);
    }
}

/// Shard selection for positioned reads. Stream reads retain round-robin routing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShardPolicy {
    /// Bind to the next shard even when its admission is full or closed.
    #[default]
    RoundRobin,
    /// Starting at the round-robin cursor, try each shard's count permit once,
    /// skipping closed shards. If every healthy shard is full, wait fairly on
    /// the first healthy shard. A shared byte-budget shortage waits on the first
    /// shard with count capacity; a closed byte budget terminates admission.
    /// Accepted and waiting reads never migrate to another shard.
    CapacityAware,
}

struct ReadPermits {
    _count: OwnedSemaphorePermit,
    _bytes: Option<OwnedSemaphorePermit>,
    _shared_reservation: Option<Arc<SharedReadReservation>>,
}

fn try_read_permits(count: &Arc<Semaphore>, bytes: Option<&Arc<Semaphore>>, charge: u32) -> Result<ReadPermits, TryAcquireError> {
    let count = Arc::clone(count).try_acquire_owned()?;
    let bytes = bytes.map(|sem| Arc::clone(sem).try_acquire_many_owned(charge)).transpose()?;
    Ok(ReadPermits {
        _count: count,
        _bytes: bytes,
        _shared_reservation: None,
    })
}

fn acquire_read_permits(
    count: Arc<Semaphore>,
    bytes: Option<Arc<Semaphore>>,
    charge: u32,
    shared_reservation: Option<Arc<SharedReadReservation>>,
) -> AcquireFut {
    Box::pin(async move {
        // Every admission takes count before bytes. Pending reads need neither
        // resource to complete, so there is no inverse acquisition cycle.
        let count = count.acquire_owned().await?;
        let bytes = match bytes {
            Some(sem) => Some(sem.acquire_many_owned(charge).await?),
            None => None,
        };
        Ok(ReadPermits {
            _count: count,
            _bytes: bytes,
            _shared_reservation: shared_reservation,
        })
    })
}

/// Registration and terminal closure happen only at startup/shutdown, never on
/// read admission. The shared lock orders registration against shard failure.
struct ByteAdmission {
    bytes: Arc<Semaphore>,
    registry: Mutex<AdmissionRegistry>,
    shared_reservation: Option<Arc<SharedReadReservation>>,
}

#[derive(Default)]
struct AdmissionRegistry {
    closed: bool,
    counts: Vec<Arc<Semaphore>>,
}

impl ByteAdmission {
    fn new(bytes: usize) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(bytes)),
            registry: Mutex::new(AdmissionRegistry::default()),
            shared_reservation: None,
        }
    }

    fn register(&self, count: &Arc<Semaphore>) {
        let close = {
            let mut registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if registry.closed {
                true
            } else {
                registry.counts.push(Arc::clone(count));
                false
            }
        };
        // Semaphore::close may run arbitrary task wakers. Never hold the
        // registry lock across it, including registration after terminal close.
        if close {
            count.close();
        }
    }

    fn close(&self) {
        let counts = {
            let mut registry = self.registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            registry.closed = true;
            std::mem::take(&mut registry.counts)
        };
        self.bytes.close();
        for count in counts {
            count.close();
        }
    }
}

struct CloseByteAdmission(Option<Arc<ByteAdmission>>);

impl Drop for CloseByteAdmission {
    fn drop(&mut self) {
        if let Some(admission) = &self.0 {
            admission.close();
        }
    }
}

#[derive(Default)]
struct DriverStats {
    #[cfg(feature = "diagnostics")]
    diagnostics: Arc<Diagnostics>,
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
    /// Logical reads accepted into the pending table, before kernel submission.
    /// Short-read resubmissions do not increment this count.
    pub submitted: u64,
    /// Logical results successfully sent to the caller's channel. This does not
    /// prove the caller has polled or consumed the result.
    pub delivered: u64,
    /// CQEs whose caller had dropped the future: the buffer stayed in the
    /// pending table the whole time and was reclaimed here, at the CQE.
    pub orphan_reclaimed: u64,
    /// Logical reads in the pending table, including queued reads not yet
    /// submitted. The kernel may still write into submitted reads' buffers.
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
    /// Kernel CQ-ring overflow counter. With NODROP (asserted at probe) overflow
    /// CQEs are buffered in the kernel overflow list and flushed on the next
    /// enter, NOT lost — so a non-zero value is a backpressure warning, not fatal
    /// loss (C5, rustfs/backlog#1056, #1167). In-flight is capped at `entries`
    /// and cancels are deduped, keeping completions <= 2*entries, so it should
    /// stay 0 in practice.
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
        #[cfg(feature = "diagnostics")]
        timing: Option<Arc<Trace>>,
        file: Arc<File>,
        offset: u64,
        len: usize,
        done: oneshot::Sender<io::Result<Vec<u8>>>,
        /// Backpressure permit, acquired before the op reaches the driver and
        /// released only when the pending entry is dropped at the final CQE
        /// (rustfs/backlog#1060/#1102). If the driver rejects the op (shutting
        /// down) the permit is dropped with the message — released immediately.
        permit: ReadPermits,
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
    #[cfg(feature = "diagnostics")]
    timing: Option<Arc<Trace>>,
    buf: Vec<u8>,
    file: Arc<File>,
    done: Option<oneshot::Sender<io::Result<Vec<u8>>>>,
    /// Kernel read offset: the block-aligned offset for a direct read, the
    /// logical offset for a buffered one, `CURRENT_POSITION` for a stream.
    offset: u64,
    /// Bytes already read into the read region (`buf[pad..]`).
    nread: usize,
    _permit: ReadPermits,
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
    /// Explicit cancel intent, independent of receiver closure: opting out of
    /// drop-cancel must still finish positioned reads after abandoning results.
    cancel_requested: bool,
}

impl Pending {
    /// Build the read SQE for the not-yet-read remainder
    /// `[pad + nread, pad + region_len)` at file offset `offset + nread`. This is
    /// the single place a read SQE is constructed: the initial submit calls it
    /// with `nread == 0` (the whole region), and a short-read or transient-errno
    /// resubmit calls it after `nread` has advanced (rustfs/backlog#1058/#1166).
    /// For an `O_DIRECT` read `pad + nread`, `offset + nread`, and the remaining
    /// length are all block-aligned.
    fn read_sqe(&self, ud: u64) -> io_uring::squeue::Entry {
        let remaining = self.region_len - self.nread;
        // SAFETY: `pad + nread < pad + region_len <= buf.len()`, and the buffer
        // lives in the pending table until the CQE, so the kernel may write here.
        // The read region is exclusively owned by this entry (no live aliases),
        // so deriving a `*mut` from the shared `as_ptr` is sound.
        let ptr = unsafe { self.buf.as_ptr().add(self.pad + self.nread).cast_mut() };
        let next_off = self.offset + self.nread as u64;
        opcode::Read::new(types::Fd(self.file.as_raw_fd()), ptr, remaining as u32)
            .offset(next_off)
            .build()
            .user_data(ud)
    }
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
#[must_use = "a read handle must be awaited or explicitly dropped"]
pub struct ReadHandle {
    id: u64,
    #[cfg(feature = "diagnostics")]
    timing: Option<Arc<Trace>>,
    rx: oneshot::Receiver<io::Result<Vec<u8>>>,
    tx: mpsc::Sender<Msg>,
    finished: bool,
    cancel_on_drop: bool,
    state: HandleState,
}

impl ReadHandle {
    /// Keep the driver's buffer until the normal CQE even when this handle is
    /// dropped.
    ///
    /// By default, dropping an in-flight handle sends a best-effort
    /// `IORING_OP_ASYNC_CANCEL` request to accelerate reclamation. This method
    /// disables that request while preserving the same memory-safety guarantee:
    /// the driver still owns the buffer and file descriptor until the read's
    /// completion arrives. It is useful when cancellation traffic would add
    /// more work than the abandoned read itself.
    #[must_use = "the returned handle carries the changed cancellation policy"]
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
            #[cfg(feature = "diagnostics")]
            if let Some(timing) = &this.timing {
                timing.enqueue();
            }
            if this
                .tx
                .send(Msg::Read {
                    id: this.id,
                    #[cfg(feature = "diagnostics")]
                    timing: this.timing.clone(),
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
                #[cfg(feature = "diagnostics")]
                if !this.finished
                    && let Some(timing) = &this.timing
                {
                    timing.received();
                }
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
        // dropped before it was submitted (Inert / WaitingPermit) has no buffer
        // and no SQE. A waiting handle releases any partial reservation by drop.
        if let HandleState::Submitted { wake } = &self.state
            && !self.finished
            && self.cancel_on_drop
        {
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
    /// Signaled after message sends (once per shard for an explicit eager batch)
    /// so the loop wakes without waiting out the heartbeat (backlog#1102).
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

/// Process-level io_uring read driver.
///
/// A driver owns one or more independent Linux io_uring shards. It is safe to
/// share by reference across async tasks; each read returns a [`ReadHandle`]
/// that can be awaited or dropped without freeing memory still visible to the
/// kernel. Construct it through [`UringDriver::probe_and_start`] so restricted
/// environments can fall back to a blocking backend before serving traffic.
pub struct UringDriver {
    limits: ReadLimits,
    shard_policy: ShardPolicy,
    byte_admission: Option<Arc<ByteAdmission>>,
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

/// Maximum requests accepted by one [`UringDriver::read_at_batch`] call.
pub const MAX_BATCH_READS: usize = 64;

/// One buffered positioned read in an explicit notification batch.
#[derive(Debug)]
pub struct ReadRequest {
    /// File whose ownership is retained through completion of an accepted read.
    pub file: Arc<File>,
    /// Positioned byte offset, with the same validation as [`UringDriver::read_at`].
    pub offset: u64,
    /// Logical byte count, subject to the driver's configured read limits.
    pub len: usize,
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
        Self::probe_and_start_with_limits(entries, shards, ReadLimits::default())
    }

    /// Start one or more shards with opt-in logical-size and read-allocation limits.
    ///
    /// Ring setup and probing follow [`Self::probe_and_start_sharded`]. Invalid
    /// limits return [`ProbeFailure::Setup`] with `InvalidInput` before probing.
    /// Saturated admission waits asynchronously and fairly on Tokio semaphores;
    /// dropping a waiting handle returns any partial reservation.
    pub fn probe_and_start_with_limits(entries: u32, shards: usize, limits: ReadLimits) -> Result<Self, ProbeFailure> {
        Self::validate_read_limits(limits)?;
        Self::start_with_limits(entries, shards, limits, None)
    }

    /// Reserve a whole driver budget from `budget`, then probe and start it.
    ///
    /// `limits.max_in_flight_bytes` must be explicitly set and nonzero. It
    /// remains the driver's independent local byte limit, shared by its shards.
    /// A limit larger than the pool capacity returns `ProbeFailure::Setup` with
    /// `InvalidInput`; temporary reservation shortage returns `WouldBlock`
    /// immediately, without probing or waiting for another driver to retire.
    /// These errors have no restriction errno and do not imply io_uring is
    /// unsupported. No fairness or automatic retry is promised.
    ///
    /// Startup failure refunds its reservation after partial shards are cleaned
    /// up. Successful drivers keep it while idle, through shutdown and deferred
    /// or accepted reads; a leaked pending read keeps the whole reservation.
    /// Completed result buffers are not covered by this accounting.
    pub fn probe_and_start_with_shared_budget(
        entries: u32,
        shards: usize,
        limits: ReadLimits,
        budget: &SharedReadBudget,
    ) -> Result<Self, ProbeFailure> {
        Self::validate_read_limits(limits)?;
        let bytes = limits.max_in_flight_bytes.ok_or_else(|| {
            ProbeFailure::Setup(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared read budget requires an explicit local byte limit",
            ))
        })?;
        let reservation = budget.reserve(bytes).map_err(ProbeFailure::Setup)?;
        Self::start_with_limits(entries, shards, limits, Some(reservation))
    }

    fn validate_read_limits(limits: ReadLimits) -> Result<(), ProbeFailure> {
        if limits
            .max_in_flight_bytes
            .is_some_and(|bytes| bytes == 0 || bytes > Semaphore::MAX_PERMITS)
        {
            return Err(ProbeFailure::Setup(io::Error::new(
                io::ErrorKind::InvalidInput,
                "byte budget must be between 1 and Semaphore::MAX_PERMITS",
            )));
        }
        Ok(())
    }

    fn start_with_limits(
        entries: u32,
        shards: usize,
        limits: ReadLimits,
        shared_reservation: Option<Arc<SharedReadReservation>>,
    ) -> Result<Self, ProbeFailure> {
        let byte_admission = limits.max_in_flight_bytes.map(|bytes| {
            let mut admission = ByteAdmission::new(bytes);
            admission.shared_reservation = shared_reservation;
            Arc::new(admission)
        });
        let mut started = Vec::with_capacity(shards.max(1));
        for i in 0..shards.max(1) {
            // Probe only the first shard (rustfs/backlog#1165): the probe read
            // exercises io_uring against the environment-global temp_dir, so one
            // confirmation is representative. Shards 2..n only create a ring and
            // verify NODROP — this avoids `shards - 1` extra O_TMPFILE
            // create+write+read round-trips per disk on every start and renew.
            // `?` drops `started`, whose `Shard::drop` joins each running thread.
            started.push(Self::start_shard(entries, i == 0, byte_admission.clone())?);
        }
        Ok(Self {
            limits,
            shard_policy: ShardPolicy::default(),
            byte_admission,
            shards: started,
            next_id: AtomicU64::new(1),
            rr: AtomicUsize::new(0),
        })
    }

    /// Set the policy for future positioned reads, independently of resource limits.
    ///
    /// Existing handles retain their owning shard. [`Self::read_current`] keeps
    /// round-robin behavior under either policy; callers must still serialize
    /// stream reads themselves when ordering matters. Capacity-aware selection
    /// is opt-in and has not established a throughput or latency improvement.
    #[must_use]
    pub fn with_shard_policy(mut self, policy: ShardPolicy) -> Self {
        self.shard_policy = policy;
        self
    }

    fn select_read_shard(
        &self,
        start: usize,
        charge: u32,
        capacity_aware: bool,
    ) -> (&Shard, Result<ReadPermits, TryAcquireError>) {
        let first = &self.shards[start];
        let bytes = self.byte_admission.as_ref().map(|admission| &admission.bytes);
        if !capacity_aware {
            return (first, try_read_permits(&first.sem, bytes, charge));
        }
        if bytes.is_some_and(|sem| sem.is_closed()) {
            return (first, Err(TryAcquireError::Closed));
        }
        let mut waiting = None;
        for index in (start..self.shards.len()).chain(0..start) {
            let shard = &self.shards[index];
            match Arc::clone(&shard.sem).try_acquire_owned() {
                Ok(count) => {
                    // A shared byte shortage cannot be solved on another shard.
                    // map drops count on either byte error, before async waiting.
                    let permits = bytes
                        .map(|sem| Arc::clone(sem).try_acquire_many_owned(charge))
                        .transpose()
                        .map(|bytes| ReadPermits {
                            _count: count,
                            _bytes: bytes,
                            _shared_reservation: None,
                        });
                    return (shard, permits);
                }
                Err(TryAcquireError::NoPermits) => {
                    waiting.get_or_insert(shard);
                }
                Err(TryAcquireError::Closed) => {}
            }
        }
        // Closure racing with the scan is terminal, even if an earlier count
        // attempt saw NoPermits. Later closure wakes the deferred acquire future.
        if bytes.is_some_and(|sem| sem.is_closed()) {
            return (first, Err(TryAcquireError::Closed));
        }
        match waiting {
            Some(shard) => (shard, Err(TryAcquireError::NoPermits)),
            None => (first, Err(TryAcquireError::Closed)),
        }
    }

    /// Pick the shard for the next op. Round-robin spreads the inline-completion
    /// memcpy across driver threads; correctness does not depend on the choice,
    /// because the handle remembers which shard took the op.
    #[cfg(feature = "fault-injection")]
    fn shard(&self) -> &Shard {
        let n = self.shards.len();
        &self.shards[self.rr.fetch_add(1, Ordering::Relaxed) % n]
    }

    fn start_shard(entries: u32, probe: bool, byte_admission: Option<Arc<ByteAdmission>>) -> Result<Shard, ProbeFailure> {
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

        // Cap the ring's io-wq bounded worker pool so a cold-read burst cannot
        // materialize thousands of PF_IO_WORKER threads against the process's
        // TasksMax/RLIMIT_NPROC (rustfs/backlog#1169). Best-effort: 0 leaves the
        // unbounded pool unchanged, and a kernel without this op (< 5.15) keeps
        // the default — neither is fatal to a working ring.
        let mut iowq_max = [IOWQ_MAX_BOUNDED_WORKERS, 0u32];
        let _ = ring.submitter().register_iowq_max_workers(&mut iowq_max);

        let wake_efd = Arc::new(EventFd::new().map_err(ProbeFailure::Setup)?);
        let thread_wake = Arc::clone(&wake_efd);

        let (tx, rx) = mpsc::channel();
        let stats = Arc::new(DriverStats::default());
        let thread_stats = Arc::clone(&stats);
        // Cap in-flight at the SQ depth (entries), which is < CQ capacity
        // (2*entries), so CQ overflow is structurally unreachable (C5/C10).
        let sem = Arc::new(Semaphore::new(entries as usize));
        if let Some(admission) = &byte_admission {
            admission.register(&sem);
        }
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
            .spawn(move || {
                let _close_bytes = CloseByteAdmission(byte_admission);
                drive(ring, rx, thread_stats, thread_sem, cq_efd, thread_wake);
            })
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
    ///
    /// The offset must be at most `i64::MAX`; `u64::MAX` is reserved for
    /// [`Self::read_current`] and is returned as an asynchronous
    /// `io::ErrorKind::InvalidInput` result rather than panicking.
    pub fn read_at(&self, file: Arc<File>, offset: u64, len: usize) -> ReadHandle {
        self.submit(file, offset, len, 1, false, true)
    }

    /// Create handles in input order for up to [`MAX_BATCH_READS`] buffered positioned reads.
    ///
    /// Eagerly accepted requests share one notification per owning shard after
    /// constructing the handles. Saturated requests retain normal asynchronous
    /// admission and notify their own shard when polled and admitted. The batch
    /// is not atomic and does not guarantee completion order or snapshot reads.
    /// Empty batches send no notification. Dropping any handle retains normal
    /// cancel safety, including during unwinding of interrupted construction.
    ///
    /// # Errors
    ///
    /// More than [`MAX_BATCH_READS`] requests returns `InvalidInput` before any
    /// submission. Individual invalid requests return errors from their handles,
    /// exactly like [`Self::read_at`]; they do not reject other batch members.
    pub fn read_at_batch(&self, requests: Vec<ReadRequest>) -> io::Result<Vec<ReadHandle>> {
        if requests.len() > MAX_BATCH_READS {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "read batch exceeds MAX_BATCH_READS"));
        }
        let mut handles = Vec::with_capacity(requests.len());
        let mut wakes: Vec<Arc<EventFd>> = Vec::with_capacity(requests.len());
        for ReadRequest { file, offset, len } in requests {
            let handle = self.submit(file, offset, len, 1, false, false);
            if let HandleState::Submitted { wake } = &handle.state
                && !wakes.iter().any(|queued| Arc::ptr_eq(queued, wake))
            {
                wakes.push(Arc::clone(wake));
            }
            // A partially built vector owns every submitted handle. If a later
            // construction unwinds, their Drop sends cancels and signals the
            // final owner, so delayed batch notification cannot strand reads.
            handles.push(handle);
        }
        for wake in wakes {
            wake.signal();
        }
        Ok(handles)
    }

    /// Read at the file's current position (read(2) semantics) — pipes.
    pub fn read_current(&self, file: Arc<File>, len: usize) -> ReadHandle {
        self.submit(file, CURRENT_POSITION, len, 1, true, true)
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
    /// Invalid alignment, range, or reserved-offset inputs are returned through
    /// the awaited result as `io::ErrorKind::InvalidInput`.
    /// A non-aligned short read that does not cover the requested range succeeds
    /// only when metadata confirms EOF; a failed metadata lookup returns an error.
    pub fn read_at_direct(&self, file: Arc<File>, offset: u64, len: usize, align: usize) -> ReadHandle {
        self.submit(file, offset, len, align, false, true)
    }

    fn submit(
        &self,
        file: Arc<File>,
        offset: u64,
        len: usize,
        align: usize,
        allow_current_position: bool,
        notify_now: bool,
    ) -> ReadHandle {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        assert_eq!(id & CANCEL_BIT, 0, "op id overflowed into the cancel bit");
        let (done, rx) = oneshot::channel();

        // Rejected requests retain the legacy round-robin cursor behavior.
        // Valid capacity-aware reads choose their final owner after validation.
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % self.shards.len();
        let shard = &self.shards[start];
        let capacity_aware = self.shard_policy == ShardPolicy::CapacityAware && !allow_current_position;
        #[cfg(feature = "diagnostics")]
        let timing = (!capacity_aware).then(|| Trace::sample(&shard.stats.diagnostics)).flatten();

        // `CURRENT_POSITION` is an internal sentinel used only by
        // `read_current`; accepting it through a positioned API would silently
        // change pread semantics into read(2) semantics. Return a normal
        // `InvalidInput` result instead of panicking on caller-controlled data.
        if !allow_current_position && offset == CURRENT_POSITION {
            let _ = done.send(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset u64::MAX is reserved for read_current",
            )));
            return ReadHandle {
                id,
                #[cfg(feature = "diagnostics")]
                timing,
                rx,
                tx: shard.tx.clone(),
                finished: false,
                cancel_on_drop: false,
                state: HandleState::Inert,
            };
        }

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
                #[cfg(feature = "diagnostics")]
                timing,
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
        if len > MAX_READ_LEN || self.limits.max_read_len.is_some_and(|max| len > max) {
            let _ = done.send(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read length exceeds MAX_RW_COUNT or configured logical read limit; caller must chunk",
            )));
            return ReadHandle {
                id,
                #[cfg(feature = "diagnostics")]
                timing,
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
        let allocation_bytes = match aligned_geometry(offset, len, align) {
            // CURRENT_POSITION (stream) reads use no positional offset — the
            // kernel reads from the current file position — so the i64::MAX end
            // check does not apply to them (their sentinel offset would overflow
            // it). Exempt them exactly as the offset guard above does.
            Some((kernel_offset, _, region_len))
                if region_len <= MAX_READ_LEN
                    && (allow_current_position && offset == CURRENT_POSITION
                        || kernel_offset
                            .checked_add(region_len as u64)
                            .is_some_and(|end| end <= i64::MAX as u64)) =>
            {
                region_len + align - 1
            }
            _ => {
                let _ = done.send(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "alignment must be a power of two, and the block-aligned range must fit MAX_RW_COUNT and end within i64::MAX",
                )));
                return ReadHandle {
                    id,
                    #[cfg(feature = "diagnostics")]
                    timing,
                    rx,
                    tx: shard.tx.clone(),
                    finished: false,
                    cancel_on_drop: false,
                    state: HandleState::Inert,
                };
            }
        };

        if self.limits.max_in_flight_bytes.is_some_and(|max| allocation_bytes > max) {
            let _ = done.send(Err(io::Error::new(io::ErrorKind::InvalidInput, "aligned allocation exceeds byte budget")));
            return ReadHandle {
                id,
                #[cfg(feature = "diagnostics")]
                timing,
                rx,
                tx: shard.tx.clone(),
                finished: false,
                cancel_on_drop: false,
                state: HandleState::Inert,
            };
        }
        // Both region and alignment are capped at MAX_READ_LEN, so their sum
        // fits u32, even on 32-bit Linux. Charge the exact Vec allocation length.
        let charge = allocation_bytes as u32;

        #[cfg(feature = "diagnostics")]
        let selection_started = capacity_aware.then(Instant::now);
        let (shard, permits) = self.select_read_shard(start, charge, capacity_aware);
        // Sample only the final owner, never the unsuccessful candidates. Default
        // routing keeps its original sample sequence, including invalid requests.
        #[cfg(feature = "diagnostics")]
        let timing = match selection_started {
            Some(started) if !matches!(&permits, Err(TryAcquireError::Closed)) => {
                Trace::sample_since(&shard.stats.diagnostics, started)
            }
            Some(_) => None,
            None => timing,
        };

        // Take a backpressure permit BEFORE the op reaches the driver; it is
        // released only when the pending entry is dropped at the CQE (C10,
        // rustfs/backlog#1060). Acquisition never blocks the caller's thread
        // (rustfs/backlog#1102).
        // This owner is fixed for admission, message send, wake, and cancellation.
        match permits {
            // Fast path: a permit was free, so submit eagerly — no allocation,
            // no await, and the op is in flight the moment `submit` returns,
            // exactly as with the previous blocking implementation.
            Ok(mut permit) => {
                permit._shared_reservation = self
                    .byte_admission
                    .as_ref()
                    .and_then(|admission| admission.shared_reservation.clone());
                #[cfg(feature = "diagnostics")]
                if let Some(timing) = &timing {
                    timing.enqueue();
                }
                if let Err(mpsc::SendError(msg)) = shard.tx.send(Msg::Read {
                    id,
                    #[cfg(feature = "diagnostics")]
                    timing: timing.clone(),
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
                        #[cfg(feature = "diagnostics")]
                        timing,
                        rx,
                        tx: shard.tx.clone(),
                        finished: false,
                        cancel_on_drop: false,
                        state: HandleState::Inert,
                    };
                }
                // Wake the driver loop so the read starts immediately.
                if notify_now {
                    shard.wake_efd.signal();
                }
                ReadHandle {
                    id,
                    #[cfg(feature = "diagnostics")]
                    timing,
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
                #[cfg(feature = "diagnostics")]
                timing,
                rx,
                tx: shard.tx.clone(),
                finished: false,
                cancel_on_drop: true,
                state: HandleState::WaitingPermit {
                    acquire: acquire_read_permits(
                        Arc::clone(&shard.sem),
                        self.byte_admission.as_ref().map(|admission| Arc::clone(&admission.bytes)),
                        charge,
                        self.byte_admission
                            .as_ref()
                            .and_then(|admission| admission.shared_reservation.clone()),
                    ),
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
                    #[cfg(feature = "diagnostics")]
                    timing,
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

    /// Sampled stage histograms aggregated across shards. Available only with
    /// the opt-in `diagnostics` feature; the default driver has no timing fields.
    /// See [`DiagnosticsSnapshot`] for stage boundaries and snapshot consistency.
    #[cfg(feature = "diagnostics")]
    pub fn diagnostics(&self) -> DiagnosticsSnapshot {
        let mut snapshot = DiagnosticsSnapshot::default();
        for shard in &self.shards {
            snapshot.merge(&shard.stats.diagnostics.snapshot());
        }
        snapshot
    }

    /// Per-shard sampled histograms in stable shard-index order. Snapshot reads
    /// allocate only this output vector and do not acquire driver-thread locks.
    #[cfg(feature = "diagnostics")]
    pub fn shard_diagnostics(&self) -> Vec<DiagnosticsSnapshot> {
        self.shards.iter().map(|shard| shard.stats.diagnostics.snapshot()).collect()
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

    /// Close admission and ask every shard to cancel/drain, without joining
    /// driver threads or waiting for pending reads to complete.
    ///
    /// Count and byte waiters are woken with an admission error. An already
    /// kernel-submitted operation still owns its buffer until its read CQE, or retains
    /// it through the leak-over-UAF escape path. Concurrent and repeated calls
    /// are safe; every call closes all admission before returning.
    pub fn request_shutdown(&self) {
        if let Some(admission) = &self.byte_admission {
            admission.close();
        }
        // Also close count-only admission when byte limits are disabled. Do not
        // use a once flag: a concurrent caller must not return before this work
        // is complete merely because another caller started the request.
        for shard in &self.shards {
            shard.sem.close();
        }
        for shard in &self.shards {
            let _ = shard.tx.send(Msg::Shutdown);
            shard.wake_efd.signal();
        }
    }

    /// Advisory query: whether every driver thread's join handle reports
    /// finished (or has already been joined). This does not join threads or
    /// imply a clean drain; use [`Self::shutdown`] to join and inspect its stats.
    ///
    /// Inspect [`Self::stats`]: nonzero `in_flight` can remain after the bounded
    /// drain leaks kernel-owned resources and the driver threads finish.
    /// Like [`std::thread::JoinHandle::is_finished`], this can become true just
    /// before final thread teardown completes; it is not proof of a completed
    /// synchronous join and does not impose a deadline on a blocked syscall.
    pub fn is_finished(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| shard.handle.as_ref().is_none_or(JoinHandle::is_finished))
    }

    /// Request shutdown immediately and move the consuming shutdown/join onto
    /// the current Tokio runtime's blocking pool. Await the returned future for
    /// the final snapshot, or an error if the blocking task fails to join.
    ///
    /// Available with `tokio-runtime`. This is deliberately not an `async fn`:
    /// the driver is handed off when this method is called, before the returned
    /// future is polled. Dropping that future, even unpolled, only detaches the
    /// join handle; it does not cancel started blocking work or drop the driver
    /// on the caller during normal runtime operation.
    ///
    /// Runtime shutdown may reject or discard blocking work and synchronously
    /// drop its captured driver. Scheduling is not a hard cleanup deadline, and
    /// neither this adapter nor a timeout can kill a hung kernel syscall. A
    /// successful snapshot may still report leaked reads via nonzero `in_flight`.
    ///
    /// # Panics
    ///
    /// Panics if called without an entered Tokio runtime, matching
    /// [`tokio::runtime::Handle::current`]. Unwinding then drops the driver using
    /// its synchronous cleanup path.
    #[cfg(feature = "tokio-runtime")]
    pub fn shutdown_async(self) -> impl Future<Output = io::Result<StatsSnapshot>> + Send {
        let runtime = tokio::runtime::Handle::current();
        self.request_shutdown();
        let shutdown = runtime.spawn_blocking(move || self.shutdown());
        async move { shutdown.await.map_err(io::Error::other) }
    }

    /// Stop accepting work, cancel/drain every ring, and join the driver threads
    /// synchronously. A clean drain returns `in_flight == 0`; a bounded-drain
    /// escape can return a nonzero count with the ring and buffers still leaked.
    ///
    /// Shards are asked to stop first and joined afterwards, so their bounded
    /// drains overlap instead of serializing `shards * DRAIN_TIMEOUT`.
    pub fn shutdown(mut self) -> StatsSnapshot {
        self.request_shutdown();
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
            tracing::warn!(
                in_flight = snap.in_flight,
                "uring shutdown: ops still in flight (bounded-drain bailout on a hung device)"
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
        self.request_shutdown();
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

/// Finish a non-aligned O_DIRECT short read only after confirming the file tail.
/// Keeping the metadata result explicit lets tests inject errors without closing
/// a live fd or changing process-wide environment state (rustfs/backlog#2647).
fn finish_direct_short_read(p: &mut Pending, file_len: io::Result<u64>) -> io::Result<Vec<u8>> {
    let file_len = file_len?;
    if p.offset + (p.nread as u64) < file_len {
        return Err(io::Error::other("io_uring O_DIRECT: non-block-aligned short read before EOF"));
    }
    Ok(deliver(p))
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
    if p.done.as_ref().is_none_or(oneshot::Sender::is_closed) {
        // Only called after the terminal read CQE. Keep the allocation in the
        // entry for normal reclamation, avoiding an orphan's O_DIRECT memmove.
        return Vec::new();
    }
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

/// Decide what follows a READ completion, never an AsyncCancel completion.
/// Cancellation only suppresses continuation: an already-complete successful
/// read still wins the race, and streams keep their read(2) short-read result.
fn reap_read(p: &mut Pending, id: u64, res: i32, shutting_down: bool) -> ReapStep {
    let stop_continuation = p.cancel_requested || shutting_down;
    if res < 0 {
        let err = -res;
        let transient = err == libc::EINTR || err == libc::EAGAIN;
        if transient && p.offset != CURRENT_POSITION && p.nread < p.region_len {
            if stop_continuation {
                return ReapStep::Finish(Err(io::Error::from_raw_os_error(libc::ECANCELED)));
            }
            if p.transient_retries < MAX_TRANSIENT_RETRIES {
                p.transient_retries += 1;
                return ReapStep::Resubmit(p.read_sqe(id));
            }
        }
        return ReapStep::Finish(Err(io::Error::from_raw_os_error(err)));
    }
    if res == 0 {
        return ReapStep::Finish(Ok(deliver(p)));
    }
    p.nread += res as usize;
    // Progress resets the transient retry budget (rustfs/backlog#1166).
    p.transient_retries = 0;
    let is_stream = p.offset == CURRENT_POSITION;
    let covered = p.nread >= p.head + p.want;
    if is_stream || covered || p.nread >= p.region_len {
        return ReapStep::Finish(Ok(deliver(p)));
    }
    if stop_continuation {
        // This read SQE has completed, so reclamation is safe now. Do not start
        // another positioned read for an explicitly cancelled logical request.
        // Never report its incomplete prefix as successful whole-range output.
        return ReapStep::Finish(Err(io::Error::from_raw_os_error(libc::ECANCELED)));
    }
    if p.align > 1 && !p.nread.is_multiple_of(p.align) {
        // Disambiguate a genuine direct-I/O tail from a non-aligned short read
        // before EOF; that offset cannot be resubmitted (rustfs/backlog#1168).
        let file_len = p.file.metadata().map(|metadata| metadata.len());
        ReapStep::Finish(finish_direct_short_read(p, file_len))
    } else {
        ReapStep::Resubmit(p.read_sqe(id))
    }
}

/// Queue at most one `AsyncCancel` per op (rustfs/backlog#1167): a drop-cancel
/// followed by a shutdown, or the submit-error shutdown, must not enqueue a
/// second cancel for the same id. The set is bounded by the pending table
/// because ids are monotonic and an entry is removed when its op is reaped.
fn queue_cancel(backlog: &mut VecDeque<io_uring::squeue::Entry>, queued_cancels: &mut HashSet<u64>, id: u64) {
    if queued_cancels.insert(id) {
        backlog.push_back(opcode::AsyncCancel::new(id).build().user_data(id | CANCEL_BIT));
    }
}

/// Push as much of the backlog into the SQ as fits, stopping when the ring is
/// full (the remainder retries next turn).
fn flush_backlog(ring: &mut IoUring, backlog: &mut VecDeque<io_uring::squeue::Entry>) {
    let mut sq = ring.submission();
    while let Some(sqe) = backlog.pop_front() {
        // SAFETY: read SQEs point into `pending`-owned buffers that live until
        // their CQE; cancel SQEs carry no pointers.
        if unsafe { sq.push(&sqe) }.is_err() {
            backlog.push_front(sqe);
            break;
        }
    }
}

/// An empty SQ is idle only when no kernel completion work needs an enter.
/// io-uring 0.7.15 adds GETEVENTS in submit() for overflow/taskrun, even with no
/// new SQEs. Bypassing that enter strands the kernel's NODROP overflow list.
fn submit_if_needed(ring: &mut IoUring) -> Option<io::Result<usize>> {
    let needs_enter = {
        let sq = ring.submission();
        !sq.is_empty() || sq.cq_overflow() || sq.taskrun()
    };
    needs_enter.then(|| ring.submit())
}

#[cfg(test)]
#[path = "driver_fault_recovery_tests.rs"]
mod fault_recovery_tests;

/// Flush the backlog into the SQ and submit it, with submit-error classification
/// (rustfs/backlog#1162). The single submit path for the whole loop: called once
/// after intake and once more after reap. Skips the `io_uring_enter` syscall only
/// when both submissions and kernel completion work are absent. EINTR/EBUSY are
/// transient; any other errno is counted and, after a bounded run, transitions
/// the shard to shutdown so callers fall back to the std backend.
/// Returns the accepted SQE count, or zero when idle or submission failed.
fn submit_ring(
    state: &mut DriverState,
    stats: &DriverStats,
    consecutive_submit_errors: &mut u32,
    submit_error_logged: &mut bool,
    shutting_down: &mut bool,
    queued_cancels: &mut HashSet<u64>,
) -> usize {
    flush_backlog(&mut state.ring, &mut state.backlog);
    let Some(result) = submit_if_needed(&mut state.ring) else {
        return 0;
    };
    handle_submit_result(result, stats, consecutive_submit_errors, submit_error_logged, shutting_down, || {
        let ids: Vec<u64> = state.pending.keys().copied().collect();
        for id in ids {
            queue_cancel(&mut state.backlog, queued_cancels, id);
        }
    })
}

/// Classify a completed submit syscall without inferring which buffers the
/// kernel owns. The callback only queues cancellation on the first shutdown
/// transition; it never reclaims pending reads. Tests inject syscall results at
/// this boundary, not into a real kernel ring.
fn handle_submit_result(
    result: io::Result<usize>,
    stats: &DriverStats,
    consecutive_submit_errors: &mut u32,
    submit_error_logged: &mut bool,
    shutting_down: &mut bool,
    on_shutdown: impl FnOnce(),
) -> usize {
    match result {
        Ok(submitted) => {
            *consecutive_submit_errors = 0;
            return submitted;
        }
        // CQ-overflow backpressure (EBUSY) and signal interruption (EINTR) are
        // transient — retry next turn without counting them (C5, backlog#1056).
        Err(e) if matches!(e.raw_os_error(), Some(libc::EBUSY) | Some(libc::EINTR)) => *consecutive_submit_errors = 0,
        Err(e) => {
            // Submission or kernel completion progress failed. Do not infer
            // per-op acceptance or reclaim buffers from this syscall error.
            // A brief run may be transient (EAGAIN); a persistent one (e.g.
            // EPERM from a later seccomp policy) must not retry forever silently.
            stats.submit_errors.fetch_add(1, Ordering::SeqCst);
            *consecutive_submit_errors += 1;
            if !*submit_error_logged {
                *submit_error_logged = true;
                tracing::warn!(error = %e, "uring driver: ring.submit() failed; retrying, will shut down if persistent");
            }
            if !*shutting_down && *consecutive_submit_errors >= MAX_CONSECUTIVE_SUBMIT_ERRORS {
                tracing::warn!(
                    consecutive_errors = *consecutive_submit_errors,
                    "uring driver: consecutive submit failures; shutting down so callers fall back to the std backend"
                );
                *shutting_down = true;
                on_shutdown();
            }
        }
    }
    0
}

#[cfg(test)]
#[path = "driver_submit_result_tests.rs"]
mod submit_result_tests;

/// Publish changes to the kernel's cumulative u32 counter. Warn only for a new
/// nonzero observation; a wrap to zero updates the snapshot and rearms logging
/// for the next increment without emitting a misleading zero-overflow warning.
fn update_cq_overflow(stats: &DriverStats, previous: &mut u32, observed: u32) -> bool {
    if observed == *previous {
        return false;
    }
    *previous = observed;
    stats.cq_overflow.store(u64::from(observed), Ordering::SeqCst);
    observed != 0
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
    let mut previous_cq_overflow = 0;
    // Ids with an AsyncCancel already queued, so a drop-cancel followed by a
    // shutdown (or vice versa) does not enqueue a second cancel for the same op —
    // keeping total completions <= 2*entries and CQ overflow unreachable
    // (rustfs/backlog#1167). Ids are monotonic, so an entry is removed only when
    // its pending op is reaped; the set stays bounded by the pending table.
    let mut queued_cancels: HashSet<u64> = HashSet::new();

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

    let mut continue_without_wait = false;
    loop {
        // Block until a CQE is ready (the ring's registered eventfd), a new
        // message arrives (the wakeup eventfd), or the heartbeat elapses —
        // this replaces the spike's 200 µs busy-poll (backlog#1102). Draining
        // both eventfds after waking keeps them from staying spuriously
        // readable; a missed edge is harmless because the CQ/mpsc are re-checked
        // unconditionally below.
        // Adaptive heartbeat (rustfs/backlog#1169): poll at 50 ms only while
        // there is in-flight work to reap or a drain deadline to honor; when the
        // shard is fully idle, wait up to IDLE_HEARTBEAT. New work still wakes us
        // immediately via wake_efd and completions via cq_efd, so the longer idle
        // wait only cuts timer/syscall churn.
        let heartbeat = if shutting_down || !state.pending.is_empty() {
            LOOP_HEARTBEAT
        } else {
            IDLE_HEARTBEAT
        };
        if !continue_without_wait {
            wait_for_events(&cq_efd, &wake_efd, heartbeat);
        }
        cq_efd.drain();
        wake_efd.drain();

        let mut budget = TurnBudget::default();
        // 1. Bounded intake. An individual large accepted read still makes
        // progress; its allocation ends this phase rather than deferring forever.
        while budget.can_take_message() {
            let msg = match rx.try_recv() {
                Ok(m) => m,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    shutting_down = true;
                    break;
                }
            };
            budget.messages += 1;
            match msg {
                Msg::Read {
                    id,
                    #[cfg(feature = "diagnostics")]
                    timing,
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
                    #[cfg(feature = "diagnostics")]
                    if let Some(timing) = &timing {
                        timing.enter();
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
                    let buf = vec![0u8; cap];
                    budget.allocation_bytes = budget.allocation_bytes.saturating_add(cap);
                    let pad = buf.as_ptr().align_offset(align);
                    // Runtime guard (not a debug-only assert): if the allocator
                    // ever returned a block `align_offset` cannot satisfy, refuse
                    // the read instead of doing UB pointer arithmetic below.
                    if pad == usize::MAX || pad.checked_add(region_len).is_none_or(|end| end > buf.len()) {
                        let _ = done.send(Err(io::Error::other("could not align O_DIRECT read buffer")));
                        drop(permit);
                        continue;
                    }

                    // Move the buffer into the pending table (which owns it until
                    // the CQE), THEN build the SQE from the entry: the initial read
                    // is `read_sqe` with `nread == 0`, so the read-region pointer
                    // math and the `Read` builder live in exactly one place.
                    // Moving the Vec never relocates its heap block, so the pointer
                    // the SQE captures stays valid.
                    state.pending.insert(
                        id,
                        Pending {
                            #[cfg(feature = "diagnostics")]
                            timing,
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
                            cancel_requested: false,
                        },
                    );
                    let sqe = state.pending.get(&id).expect("just inserted").read_sqe(id);
                    stats.submitted.fetch_add(1, Ordering::SeqCst);
                    stats.in_flight.fetch_add(1, Ordering::SeqCst);
                    state.backlog.push_back(sqe);
                    #[cfg(feature = "diagnostics")]
                    if let Some(timing) = state.pending.get(&id).and_then(|p| p.timing.as_ref()) {
                        timing.prepared();
                    }
                }
                Msg::Cancel { id } => {
                    if let Some(pending) = state.pending.get_mut(&id) {
                        pending.cancel_requested = true;
                        queue_cancel(&mut state.backlog, &mut queued_cancels, id);
                    }
                }
                Msg::Shutdown => {
                    shutting_down = true;
                    let ids: Vec<u64> = state.pending.keys().copied().collect();
                    for id in ids {
                        queue_cancel(&mut state.backlog, &mut queued_cancels, id);
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

        // 2. Flush the backlog into the SQ and submit it (the single submit path;
        //    see `submit_ring`).
        let submitted_before_reap = submit_ring(
            &mut state,
            &stats,
            &mut consecutive_submit_errors,
            &mut submit_error_logged,
            &mut shutting_down,
            &mut queued_cancels,
        );

        // 3. Reap. A Pending entry (and thus its buffer) is dropped ONLY when
        //    the logical read finishes; a short read is resubmitted for the
        //    remainder and the entry stays put (C9, rustfs/backlog#1058).
        while budget.can_reap() {
            let Some(cqe) = state.ring.completion().next() else {
                break;
            };
            // Count every consumed CQE, including cancels and test-only drops.
            budget.completions += 1;
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
            #[cfg(feature = "diagnostics")]
            let timing = state
                .pending
                .get(&ud)
                .and_then(|p| p.timing.as_ref())
                .map(|trace| (Arc::clone(trace), trace.now()));

            // Decide the next step while borrowing the entry, then act after
            // the borrow ends (finish removes it; resubmit re-queues an SQE).
            let step = {
                let p = state.pending.get_mut(&ud).expect("checked above");
                reap_read(p, ud, res, shutting_down)
            };

            #[cfg(feature = "diagnostics")]
            if let Some((timing, started)) = &timing {
                timing.reaped(*started, matches!(&step, ReapStep::Finish(_)));
            }
            match step {
                ReapStep::Finish(outcome) => {
                    // Content hygiene (C12, rustfs/backlog#1062): the delivered
                    // bytes are ⊆ [0, res) — buf was freshly zeroed per op and
                    // truncated to res. When P3 reuses a driver-owned slab
                    // across requests, this ⊆ [0, res) property MUST be
                    // preserved or a previous tenant's object bytes leak.
                    let mut p = state.pending.remove(&ud).expect("checked above");
                    #[cfg(feature = "diagnostics")]
                    if let Some(timing) = &p.timing {
                        timing.sending();
                    }
                    match p.done.take().expect("done sender set at submit").send(outcome) {
                        Ok(()) => stats.delivered.fetch_add(1, Ordering::SeqCst),
                        // Caller dropped the future: the buffer survived in
                        // the table until this final CQE and is reclaimed here.
                        Err(_) => stats.orphan_reclaimed.fetch_add(1, Ordering::SeqCst),
                    };
                    stats.in_flight.fetch_sub(1, Ordering::SeqCst);
                    // Drop any queued-cancel bookkeeping for this now-gone op so
                    // the dedup set stays bounded by the pending table
                    // (rustfs/backlog#1167).
                    queued_cancels.remove(&ud);
                    // `p` (and with it `_permit`) is dropped here, at the CQE
                    // and pending-table removal — never at future drop (C10,
                    // rustfs/backlog#1060). No manual release to forget.
                }
                ReapStep::Resubmit(sqe) => state.backlog.push_back(sqe),
            }
        }

        // Reap freed CQ space: flush kernel overflow/task work even with an
        // empty SQ/backlog. Also retry SQEs left by partial submission and submit
        // short-read continuations this turn. Two bounded attempts per loop
        // preserve heartbeat pacing on EBUSY/EINTR/zero-progress submission;
        // fully idle calls skip the syscall (rustfs/backlog#2647).
        let submitted_after_reap = submit_ring(
            &mut state,
            &stats,
            &mut consecutive_submit_errors,
            &mut submit_error_logged,
            &mut shutting_down,
            &mut queued_cancels,
        );

        // Monitor CQ overflow. With NODROP (asserted at probe) overflowed CQEs
        // are BUFFERED in the kernel overflow list and flushed on the next enter,
        // never lost — so a non-zero value is a backpressure warning, not fatal
        // loss (rustfs/backlog#1056, #1167). In-flight reads are capped at
        // `entries` and cancels are deduped (at most one per op), keeping total
        // completions <= 2*entries, so this should stay 0 in practice.
        let overflow = state.ring.completion().overflow();
        if update_cq_overflow(&stats, &mut previous_cq_overflow, overflow) {
            tracing::warn!(
                overflow,
                "uring driver: CQ overflow; CQEs buffered (NODROP), not lost — backpressure warning"
            );
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
                tracing::warn!(
                    in_flight = state.pending.len(),
                    "uring driver: bounded drain timed out with ops still in flight; leaking ring + buffers to stay memory-safe"
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
                // The leaked ring still has `cq_efd` registered via
                // IORING_REGISTER_EVENTFD and in-flight ops that may post CQEs, so
                // the eventfd must outlive it. Leak it alongside the ring instead
                // of letting the returning `drive` drop (close) it out from under
                // the still-mapped ring, honoring start_shard's documented "cq_efd
                // outlives the ring" invariant on this exit too (rustfs/backlog#1167).
                std::mem::forget(cq_efd);
                std::mem::forget(state);
                return;
            }
        }
        let ready_cqe = !state.ring.completion().is_empty();
        continue_without_wait = budget.continue_without_wait(
            ready_cqe,
            !state.backlog.is_empty() || !state.ring.submission().is_empty(),
            submitted_before_reap.saturating_add(submitted_after_reap),
        );
    }
}

#[cfg(test)]
mod shared_budget_reservation_tests {
    use super::*;

    fn mock_driver(pool: &SharedReadBudget, bytes: usize, policy: ShardPolicy) -> (UringDriver, mpsc::Receiver<Msg>) {
        let mut admission = ByteAdmission::new(bytes);
        admission.shared_reservation = Some(pool.reserve(bytes).expect("reserve mock driver"));
        let admission = Arc::new(admission);
        let sem = Arc::new(Semaphore::new(1));
        admission.register(&sem);
        let (tx, rx) = mpsc::channel();
        (
            UringDriver {
                limits: ReadLimits {
                    max_read_len: None,
                    max_in_flight_bytes: Some(bytes),
                },
                shard_policy: policy,
                byte_admission: Some(admission),
                shards: vec![Shard {
                    tx,
                    handle: None,
                    stats: Arc::new(DriverStats::default()),
                    sem,
                    wake_efd: Arc::new(EventFd::new().expect("mock wake fd")),
                }],
                next_id: AtomicU64::new(1),
                rr: AtomicUsize::new(0),
            },
            rx,
        )
    }

    #[test]
    fn pool_reserves_whole_weights_and_refunds_only_the_last_receipt_owner() {
        assert_eq!(SharedReadBudget::new(0).expect_err("zero pool").kind(), io::ErrorKind::InvalidInput);
        let pool = SharedReadBudget::new(12).expect("pool");
        let alias = pool.clone();
        let first = pool.reserve(8).expect("first driver");
        let first_read = first.clone();
        let second = alias.reserve(4).expect("second driver");
        assert_eq!(pool.capacity(), 12);
        assert_eq!(alias.available(), 0);
        assert!(matches!(pool.reserve(1), Err(error) if error.kind() == io::ErrorKind::WouldBlock));
        drop(first);
        assert_eq!(pool.available(), 0, "an accepted read still owns the receipt");
        drop(first_read);
        assert_eq!(pool.available(), 8);
        drop(second);
        assert_eq!(pool.available(), 12);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn whole_driver_reservations_do_not_narrow_to_u32_or_overflow_refunds() {
        let pool = SharedReadBudget::new(usize::MAX).expect("large accounting-only pool");
        let bytes = usize::try_from(u32::MAX).expect("64-bit usize") + 1;
        let large = pool.reserve(bytes).expect("reservation above u32");
        assert_eq!(pool.available(), usize::MAX - bytes);
        let remaining = pool.reserve(usize::MAX - bytes).expect("reserve exact remainder");
        assert_eq!(pool.available(), 0);
        assert!(matches!(pool.reserve(1), Err(error) if error.kind() == io::ErrorKind::WouldBlock));
        drop(remaining);
        drop(large);
        assert_eq!(pool.available(), usize::MAX);
    }

    #[test]
    fn concurrent_reservations_never_exceed_pool_capacity() {
        let pool = SharedReadBudget::new(17).expect("pool");
        let gate = std::sync::Barrier::new(9);
        let successful = AtomicUsize::new(0);
        let unexpected_error = AtomicBool::new(false);
        let observed = std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    gate.wait();
                    let reservation = pool.reserve(3);
                    match &reservation {
                        Ok(_) => {
                            successful.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                        Err(_) => {
                            unexpected_error.store(true, Ordering::SeqCst);
                        }
                    }
                    gate.wait();
                    gate.wait();
                    drop(reservation);
                });
            }
            gate.wait();
            gate.wait();
            let observed = (pool.available(), successful.load(Ordering::SeqCst));
            gate.wait();
            observed
        });
        assert_eq!(observed, (2, 5));
        assert!(!unexpected_error.load(Ordering::SeqCst));
        assert_eq!(pool.available(), pool.capacity());
    }

    #[test]
    fn invalid_and_temporarily_unavailable_driver_limits_fail_before_probing() {
        let pool = SharedReadBudget::new(8).expect("pool");
        for limit in [None, Some(0), Some(9), Some(Semaphore::MAX_PERMITS + 1)] {
            let result = UringDriver::probe_and_start_with_shared_budget(
                0,
                1,
                ReadLimits {
                    max_read_len: None,
                    max_in_flight_bytes: limit,
                },
                &pool,
            );
            let Err(error) = result else { panic!("invalid constructor must fail") };
            assert!(!error.is_expected_restriction());
            assert!(matches!(error, ProbeFailure::Setup(ref error) if error.kind() == io::ErrorKind::InvalidInput));
            assert_eq!(pool.available(), 8);
        }
        let occupied = pool.reserve(8).expect("occupy pool");
        let result = UringDriver::probe_and_start_with_shared_budget(
            0,
            1,
            ReadLimits {
                max_read_len: None,
                max_in_flight_bytes: Some(1),
            },
            &pool,
        );
        let Err(error) = result else { panic!("insufficient shared budget must fail") };
        assert!(!error.is_expected_restriction());
        assert!(matches!(error, ProbeFailure::Setup(ref error) if error.kind() == io::ErrorKind::WouldBlock));
        assert_eq!(pool.available(), 0);
        drop(occupied);
        assert_eq!(pool.available(), 8);
    }

    #[test]
    fn eager_messages_keep_reservations_after_driver_and_caller_drop() {
        for policy in [ShardPolicy::RoundRobin, ShardPolicy::CapacityAware] {
            let pool = SharedReadBudget::new(8).expect("pool");
            let (driver, messages) = mock_driver(&pool, 8, policy);
            let handle = driver
                .read_at(Arc::new(File::open("/dev/zero").expect("fixture file")), 0, 4)
                .without_cancel_on_drop();
            let message = messages.try_recv().expect("eager read enqueued");
            assert!(matches!(message, Msg::Read { .. }));
            drop(handle);
            drop(driver);
            assert_eq!(pool.available(), 0, "accepted message must retain the whole driver reservation");
            drop(message);
            assert_eq!(pool.available(), 8);
        }
    }

    #[test]
    fn deferred_admission_keeps_reservation_until_closed_poll_or_drop() {
        for poll_closed in [false, true] {
            let pool = SharedReadBudget::new(8).expect("pool");
            let (driver, messages) = mock_driver(&pool, 8, ShardPolicy::RoundRobin);
            let file = Arc::new(File::open("/dev/zero").expect("fixture file"));
            let first = driver.read_at(file.clone(), 0, 8).without_cancel_on_drop();
            let message = messages.try_recv().expect("first read owns local permits");
            let mut waiting = driver.read_at(file, 0, 8);
            assert!(matches!(waiting.state, HandleState::WaitingPermit { .. }));
            drop(first);
            drop(message);
            drop(driver);
            assert_eq!(pool.available(), 0, "deferred acquire owns the reservation before its first poll");
            if poll_closed {
                let result = Pin::new(&mut waiting).poll(&mut Context::from_waker(std::task::Waker::noop()));
                assert!(matches!(result, Poll::Ready(Err(_))));
                assert_eq!(pool.available(), 8, "closed acquire releases its receipt");
            }
            drop(waiting);
            assert_eq!(pool.available(), 8);
        }
    }

    #[test]
    fn local_shutdown_does_not_close_another_participating_driver() {
        let pool = SharedReadBudget::new(8).expect("pool");
        let (first, _first_messages) = mock_driver(&pool, 4, ShardPolicy::RoundRobin);
        let (second, second_messages) = mock_driver(&pool, 4, ShardPolicy::RoundRobin);
        first.request_shutdown();
        assert!(first.shards[0].sem.is_closed());
        assert!(!second.shards[0].sem.is_closed());
        assert!(!second.byte_admission.as_ref().expect("local budget").bytes.is_closed());
        assert_eq!(pool.available(), 0, "shutdown request alone does not return an idle driver's reservation");
        let read = second
            .read_at(Arc::new(File::open("/dev/zero").expect("fixture file")), 0, 4)
            .without_cancel_on_drop();
        let message = second_messages.try_recv().expect("other driver still accepts reads");
        assert!(matches!(message, Msg::Read { .. }));
        drop(first);
        assert_eq!(pool.available(), 4);
        drop(second);
        drop(read);
        assert_eq!(pool.available(), 4, "second driver's queued operation still owns its reservation");
        drop(message);
        assert_eq!(pool.available(), 8);
    }
}

#[cfg(test)]
mod shutdown_request_tests {
    use super::*;

    fn mock_driver(shards: usize, bytes: Option<usize>) -> (UringDriver, Vec<mpsc::Receiver<Msg>>) {
        let byte_admission = bytes.map(|limit| Arc::new(ByteAdmission::new(limit)));
        let mut receivers = Vec::new();
        let shards = (0..shards)
            .map(|_| {
                let (tx, rx) = mpsc::channel();
                receivers.push(rx);
                let sem = Arc::new(Semaphore::new(1));
                if let Some(admission) = &byte_admission {
                    admission.register(&sem);
                }
                Shard {
                    tx,
                    handle: None,
                    stats: Arc::new(DriverStats::default()),
                    sem,
                    wake_efd: Arc::new(EventFd::new().expect("mock wake fd")),
                }
            })
            .collect();
        (
            UringDriver {
                limits: ReadLimits {
                    max_read_len: None,
                    max_in_flight_bytes: bytes,
                },
                shard_policy: ShardPolicy::RoundRobin,
                byte_admission,
                shards,
                next_id: AtomicU64::new(1),
                rr: AtomicUsize::new(0),
            },
            receivers,
        )
    }

    fn poll_acquire(future: &mut AcquireFut) -> Poll<Result<ReadPermits, tokio::sync::AcquireError>> {
        future.as_mut().poll(&mut Context::from_waker(std::task::Waker::noop()))
    }

    #[test]
    fn request_shutdown_closes_count_only_admission_before_returning() {
        let (driver, messages) = mock_driver(1, None);
        let count = Arc::clone(&driver.shards[0].sem);
        let held = try_read_permits(&count, None, 1).expect("occupy mock shard");
        let mut waiter = acquire_read_permits(count.clone(), None, 1, None);
        assert!(poll_acquire(&mut waiter).is_pending());
        driver.request_shutdown();
        assert!(count.is_closed());
        assert!(matches!(poll_acquire(&mut waiter), Poll::Ready(Err(_))));
        assert!(matches!(messages[0].try_recv(), Ok(Msg::Shutdown)));
        assert_eq!(count.available_permits(), 0, "request must not reclaim accepted work");
        drop(held);
    }

    #[test]
    fn request_shutdown_wakes_both_admission_stages_without_reclaiming_buffers() {
        let (driver, _messages) = mock_driver(2, Some(8));
        let bytes = Arc::clone(&driver.byte_admission.as_ref().expect("byte budget").bytes);
        let held = try_read_permits(&driver.shards[0].sem, Some(&bytes), 8).expect("occupy byte budget");
        let mut count_waiter = acquire_read_permits(driver.shards[0].sem.clone(), Some(bytes.clone()), 8, None);
        let mut byte_waiter = acquire_read_permits(driver.shards[1].sem.clone(), Some(bytes.clone()), 8, None);
        assert!(poll_acquire(&mut count_waiter).is_pending());
        assert!(poll_acquire(&mut byte_waiter).is_pending());
        driver.request_shutdown();
        assert!(matches!(poll_acquire(&mut count_waiter), Poll::Ready(Err(_))));
        assert!(matches!(poll_acquire(&mut byte_waiter), Poll::Ready(Err(_))));
        assert!(bytes.is_closed());
        assert_eq!(bytes.available_permits(), 0, "in-flight reservation remains owned");
        drop(held);
    }

    #[test]
    fn concurrent_shutdown_requests_each_return_with_all_admission_closed() {
        for bytes in [None, Some(8)] {
            let (driver, _messages) = mock_driver(2, bytes);
            let barrier = std::sync::Barrier::new(8);
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    scope.spawn(|| {
                        barrier.wait();
                        for _ in 0..2 {
                            driver.request_shutdown();
                            assert!(driver.shards.iter().all(|shard| shard.sem.is_closed()));
                            if let Some(admission) = &driver.byte_admission {
                                assert!(admission.bytes.is_closed());
                            }
                        }
                    });
                }
            });
        }
    }

    #[tokio::test]
    async fn finished_observes_thread_exit_not_request_or_clean_drain() {
        let (mut driver, _messages) = mock_driver(1, None);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (exit_tx, exit_rx) = mpsc::channel();
        driver.shards[0].handle = Some(std::thread::spawn(move || {
            entered_tx.send(()).expect("observe mock thread entry");
            exit_rx.recv_timeout(Duration::from_secs(10)).expect("allow mock thread exit");
        }));
        entered_rx.await.expect("mock thread started");
        assert!(!driver.is_finished());
        driver.request_shutdown();
        assert!(!driver.is_finished(), "request must return while a thread is still active");
        // A synthetic leak counter illustrates that thread completion alone is
        // not the clean-drain condition. No kernel ever touches this mock state.
        driver.shards[0].stats.in_flight.store(1, Ordering::SeqCst);
        exit_tx.send(()).expect("finish mock driver");
        tokio::time::timeout(Duration::from_secs(3), async {
            while !driver.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("finished thread must become observable");
        assert_eq!(driver.stats().in_flight, 1);
        driver.shards[0].join();
        assert!(driver.is_finished(), "joined handles also report finished");
    }

    #[cfg(feature = "tokio-runtime")]
    #[test]
    fn async_shutdown_transfers_driver_before_poll_and_unpolled_drop_does_not_join_caller() {
        let (mut driver, _messages) = mock_driver(1, None);
        let count = driver.shards[0].sem.clone();
        let retired = Arc::downgrade(&driver.shards[0].stats);
        let (driver_exit_tx, driver_exit_rx) = mpsc::channel();
        driver.shards[0].handle = Some(std::thread::spawn(move || {
            driver_exit_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("release mock driver thread");
        }));
        let (pool_release_tx, pool_release_rx) = mpsc::channel();
        let (returned_tx, returned_rx) = mpsc::channel();
        let (runtime_exit_tx, runtime_exit_rx) = mpsc::channel();
        let caller = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .build()
                .expect("shutdown adapter runtime");
            let (pool_entered_tx, pool_entered_rx) = mpsc::channel();
            let _occupied = runtime.spawn_blocking(move || {
                pool_entered_tx.send(()).expect("observe occupied blocking pool");
                pool_release_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("release blocking pool");
            });
            pool_entered_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("pool task started");
            {
                let _entered = runtime.enter();
                let future = driver.shutdown_async();
                // The shutdown closure cannot start yet. Admission must already
                // be closed at method return, not at the returned future's poll.
                let closed_before_poll = count.is_closed();
                drop(future);
                returned_tx.send(closed_before_poll).expect("observe unpolled drop return");
            }
            runtime_exit_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("keep runtime alive until cleanup");
        });

        let returned = returned_rx.recv_timeout(Duration::from_secs(3));
        // Release every gate before assertions, including on a broken adapter
        // that joined on the caller and caused the observation to time out.
        pool_release_tx.send(()).expect("allow shutdown closure to run");
        driver_exit_tx.send(()).expect("allow driver join to finish");
        let deadline = Instant::now() + Duration::from_secs(3);
        while retired.strong_count() != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let cleaned_up = retired.strong_count() == 0;
        runtime_exit_tx.send(()).expect("allow runtime teardown");
        caller.join().expect("shutdown caller thread");
        assert!(returned.expect("unpolled drop must return without waiting for the mock driver"));
        assert!(cleaned_up, "detached shutdown must still consume and drop the driver");
    }
}

#[cfg(test)]
mod cancellation_efficiency_tests {
    use super::*;

    // No real I/O is submitted: completion decisions get deterministic short
    // reads / errno results, so a naturally completed file cannot mask a retry.
    fn pending() -> (Pending, oneshot::Receiver<io::Result<Vec<u8>>>, Arc<Semaphore>) {
        let sem = Arc::new(Semaphore::new(1));
        let (done, rx) = oneshot::channel();
        let p = Pending {
            #[cfg(feature = "diagnostics")]
            timing: None,
            buf: (0..16).collect(),
            file: Arc::new(File::open("/dev/null").expect("open fixture fd")),
            done: Some(done),
            offset: 0,
            nread: 0,
            _permit: ReadPermits {
                _count: Arc::clone(&sem).try_acquire_owned().expect("fixture permit"),
                _bytes: None,
                _shared_reservation: None,
            },
            pad: 0,
            head: 0,
            want: 16,
            region_len: 16,
            align: 1,
            transient_retries: 0,
            cancel_requested: false,
        };
        (p, rx, sem)
    }

    fn assert_cancelled(step: ReapStep) {
        match step {
            ReapStep::Finish(Err(error)) => assert_eq!(error.raw_os_error(), Some(libc::ECANCELED)),
            _ => panic!("cancelled positioned continuation must finish with ECANCELED"),
        }
    }

    #[test]
    fn explicit_cancel_stops_short_read_without_releasing_resources_early() {
        let (mut p, _rx, sem) = pending();
        let file = Arc::downgrade(&p.file);
        let buffer = p.buf.as_ptr();
        p.cancel_requested = true;
        assert_eq!(sem.available_permits(), 0);
        assert_cancelled(reap_read(&mut p, 1, 4, false));
        assert_eq!(p.nread, 4);
        assert_eq!(p.buf.as_ptr(), buffer);
        assert_eq!(sem.available_permits(), 0);
        assert!(file.upgrade().is_some());
        drop(p);
        assert_eq!(sem.available_permits(), 1);
        assert!(file.upgrade().is_none());
    }

    #[test]
    fn explicit_cancel_stops_both_transient_errno_retries() {
        for errno in [libc::EINTR, libc::EAGAIN] {
            let (mut p, _rx, _sem) = pending();
            p.cancel_requested = true;
            assert_cancelled(reap_read(&mut p, 1, -errno, false));
            assert_eq!(p.transient_retries, 0);
        }
    }

    #[test]
    fn shutdown_stops_short_read_and_transient_continuations() {
        for result in [4, -libc::EINTR, -libc::EAGAIN] {
            let (mut p, _rx, _sem) = pending();
            assert_cancelled(reap_read(&mut p, 1, result, true));
        }
    }

    #[test]
    fn closed_receiver_without_cancel_still_continues_positioned_reads() {
        for result in [4, -libc::EINTR, -libc::EAGAIN] {
            let (mut p, rx, sem) = pending();
            drop(rx);
            assert!(matches!(reap_read(&mut p, 1, result, false), ReapStep::Resubmit(_)));
            assert_eq!(sem.available_permits(), 0);
        }
    }

    #[test]
    fn current_position_short_read_survives_cancel_and_shutdown_race() {
        let (mut p, _rx, _sem) = pending();
        p.offset = CURRENT_POSITION;
        p.cancel_requested = true;
        match reap_read(&mut p, 1, 4, true) {
            ReapStep::Finish(Ok(bytes)) => assert_eq!(bytes, [0, 1, 2, 3]),
            _ => panic!("stream short read must keep its read(2) result"),
        }
    }

    #[test]
    fn successful_complete_read_wins_cancel_race() {
        let (mut p, _rx, _sem) = pending();
        p.cancel_requested = true;
        match reap_read(&mut p, 1, 16, true) {
            ReapStep::Finish(Ok(bytes)) => assert_eq!(bytes, (0..16).collect::<Vec<_>>()),
            _ => panic!("complete read must retain success"),
        }
    }

    #[test]
    fn eof_after_prefix_preserves_success_during_shutdown() {
        let (mut p, _rx, _sem) = pending();
        p.nread = 4;
        p.cancel_requested = true;
        match reap_read(&mut p, 1, 0, true) {
            ReapStep::Finish(Ok(bytes)) => assert_eq!(bytes, [0, 1, 2, 3]),
            _ => panic!("observed EOF must preserve the completed prefix"),
        }
    }

    #[test]
    fn transient_retry_keeps_prefix_and_exhausts_budget() {
        let (mut p, _rx, sem) = pending();
        p.nread = 4;
        for _ in 0..MAX_TRANSIENT_RETRIES {
            assert!(matches!(reap_read(&mut p, 1, -libc::EAGAIN, false), ReapStep::Resubmit(_)));
            assert_eq!(p.nread, 4);
            assert_eq!(sem.available_permits(), 0);
        }
        match reap_read(&mut p, 1, -libc::EAGAIN, false) {
            ReapStep::Finish(Err(error)) => assert_eq!(error.raw_os_error(), Some(libc::EAGAIN)),
            _ => panic!("transient retry budget must remain bounded"),
        }
    }

    #[test]
    fn current_position_transient_error_never_retries() {
        let (mut p, _rx, _sem) = pending();
        p.offset = CURRENT_POSITION;
        p.cancel_requested = true;
        match reap_read(&mut p, 1, -libc::EINTR, true) {
            ReapStep::Finish(Err(error)) => assert_eq!(error.raw_os_error(), Some(libc::EINTR)),
            _ => panic!("stream errors preserve read(2) semantics"),
        }
    }

    #[test]
    fn closed_receiver_skips_direct_result_copy_and_materialization() {
        let (mut p, rx, _sem) = pending();
        p.pad = 2;
        p.head = 3;
        p.want = 4;
        p.region_len = 8;
        p.align = 4;
        let before = p.buf.clone();
        let ptr = p.buf.as_ptr();
        drop(rx);
        match reap_read(&mut p, 1, 8, false) {
            ReapStep::Finish(Ok(bytes)) => assert_eq!(bytes.capacity(), 0),
            _ => panic!("terminal direct read must finish"),
        }
        assert_eq!(p.buf, before, "orphan result must not be memmoved or truncated");
        assert_eq!(p.buf.as_ptr(), ptr);
    }

    #[test]
    fn live_receiver_gets_exact_direct_result_range() {
        let (mut p, _rx, _sem) = pending();
        p.pad = 2;
        p.head = 3;
        p.want = 4;
        p.region_len = 8;
        p.align = 4;
        match reap_read(&mut p, 1, 8, false) {
            ReapStep::Finish(Ok(bytes)) => assert_eq!(bytes, [5, 6, 7, 8]),
            _ => panic!("direct result must contain exactly the logical range"),
        }
    }

    #[test]
    fn eventfd_retries_interruption_until_transfer_or_would_block() {
        for terminal in [Ok(8), Err(io::Error::from_raw_os_error(libc::EAGAIN))] {
            let mut calls = [
                Err(io::Error::from_raw_os_error(libc::EINTR)),
                Err(io::Error::from_raw_os_error(libc::EINTR)),
                terminal,
            ]
            .into_iter();
            eventfd_transfer(|| calls.next().expect("unexpected retry")).expect("transfer or readiness satisfied");
            assert!(calls.next().is_none());
        }
    }

    #[test]
    fn eventfd_propagates_unexpected_error_and_short_transfer() {
        let error = eventfd_transfer(|| Err(io::Error::from_raw_os_error(libc::EBADF))).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
        assert!(eventfd_transfer(|| Ok(4)).is_err());
    }

    #[test]
    fn saturated_eventfd_signal_keeps_readiness_and_drain_clears_it() {
        let event = EventFd::new().expect("eventfd");
        let value = u64::MAX - 1;
        // SAFETY: event owns the fd; value is an initialized eight-byte counter.
        assert_eq!(unsafe { libc::write(event.as_raw(), (&value as *const u64).cast(), 8) }, 8);
        event.signal(); // EAGAIN: the already readable saturated fd is sufficient.
        assert!(!event.error_logged.load(Ordering::Relaxed));
        event.drain();
        let mut read = 0_u64;
        // SAFETY: event owns the fd; read is a valid eight-byte output buffer.
        assert_eq!(unsafe { libc::read(event.as_raw(), (&mut read as *mut u64).cast(), 8) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EAGAIN));
        event.drain(); // Empty EAGAIN is normal too.
        assert!(!event.error_logged.load(Ordering::Relaxed));
    }
}
