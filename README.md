# rustfs-uring

[![CI](https://github.com/rustfs/uring/actions/workflows/ci.yml/badge.svg)](https://github.com/rustfs/uring/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/rustfs/uring/blob/main/LICENSE)
[![crates](https://img.shields.io/crates/v/rustfs-uring.svg)](https://crates.io/crates/rustfs-uring)
[![docs.rs](https://docs.rs/rustfs-uring/badge.svg)](https://docs.rs/rustfs-uring/)

Cancel-safe async `io_uring` read backend for [RustFS](https://github.com/rustfs/rustfs) storage.

When a caller drops the future of an in-flight read (an erasure-code quorum was reached, a timeout, a disconnect), the kernel may still write into the read buffer until the CQE — so freeing it at future-drop is a use-after-free. This crate owns each buffer and file handle in the driver's pending table from submission until the CQE, reclaims only at the CQE, drains in-flight ops to zero on shutdown (with a bounded leak-over-UAF escape hatch for a hung disk), and aborts rather than free in-flight buffers on a driver-thread panic. The per-invariant rationale lives inline in [`src/driver.rs`](src/driver.rs) and on [docs.rs](https://docs.rs/rustfs-uring/).

> **Status:** read path only, Linux only (an empty stub on other targets). Wired into `rustfs/rustfs` behind a runtime probe, **off by default** (`RUSTFS_IO_URING_READ_ENABLE`). See [`CHANGELOG.md`](CHANGELOG.md).

```toml
[target.'cfg(target_os = "linux")'.dependencies]
rustfs-uring = "0.2.2"
```

## Usage

```rust
use std::fs::File;
use std::sync::Arc;
use rustfs_uring::UringDriver;

# async fn demo() -> std::io::Result<()> {
// Probe a real IORING_OP_READ before accepting work. On a restricted host,
// `ProbeFailure::is_expected_restriction()` identifies the expected fallback.
let driver = match UringDriver::probe_and_start(64) {
    Ok(driver) => driver,
    Err(err) if err.is_expected_restriction() => return Ok(()), // use std backend
    Err(err) => return Err(std::io::Error::other(err)),
};
let file = Arc::new(File::open("/data/object")?);

// Positioned read (whole-range: short reads are resubmitted). Dropping the
// returned future before it completes is safe — the driver owns the buffer.
let bytes = driver.read_at(Arc::clone(&file), 0, 65536).await?;

let snapshot = driver.shutdown();
assert_eq!(snapshot.delivered + snapshot.orphan_reclaimed, snapshot.submitted);
# Ok(())
# }
```

- `read_at(file, offset, len)` — positioned (pread) read, whole-range.
- `read_at_direct(file, offset, len, align)` — the same for an `O_DIRECT` fd; `offset`/`len` need not be aligned (the driver reads a block-aligned superset and returns exactly the requested range).
- `read_current(file, len)` — `read(2)` semantics from the current position, for pipes and other non-seekable fds (a short read is a valid final result).
- `probe_and_start_sharded(entries, shards)` — several independent rings per disk (each ring caps at one core's memory bandwidth for cache-hit reads); `probe_and_start(entries)` equals `..._sharded(entries, 1)`.
- `probe_and_start_with_limits(entries, shards, ReadLimits { max_read_len, max_in_flight_bytes })` — optional logical read-size and driver-wide read-buffer limits. Both fields default to `None`, preserving existing constructor behavior.
- `probe_and_start_with_shared_budget(entries, shards, limits, &pool)` — reserve the driver's whole configured byte quota from a cloneable `SharedReadBudget` before startup. See [shared reservation ownership](docs/shared-read-budget.md); this is not dynamic per-read sharing or an RSS limit.
- `with_shard_policy(ShardPolicy::CapacityAware)` — opt-in capacity-aware routing for positioned reads. Constructors keep `ShardPolicy::RoundRobin` by default.
- `request_shutdown()` — close admission and request cancellation/drain without joining; `is_finished()` reports advisory thread completion, not a clean drain.
- `shutdown_async()` — with the default-off `tokio-runtime` feature, transfer consuming cleanup to Tokio's blocking pool at method call time. See [shutdown ownership and runtime boundaries](docs/shutdown.md).

### Shard selection

Round-robin binds each read to the next shard, even if that shard is busy or
closed. Capacity-aware selection starts at the same cursor and tries each shard's
count permit at most once, skipping closed count semaphores. It uses actual
permit acquisition, not a free-capacity snapshot. If all healthy shards are busy,
the handle waits on the first healthy candidate using Tokio's fair semaphore.
The wait is local to that shard; it does not rebalance when another shard frees.

A shared byte-budget shortage waits on the first shard whose count permit was
available, returning that temporary count reservation before constructing the
waiter. A closed shared byte budget rejects admission globally. Once a read is
accepted or deferred, its read, wakeup, retry, and cancel retain the same owning
shard. `read_current` always keeps the original round-robin behavior; concurrent
stream reads still require caller serialization when ordering matters.

Enable the policy explicitly on the constructed driver before sharing it:

```rust,ignore
let driver = UringDriver::probe_and_start_sharded(128, 4)?
    .with_shard_policy(rustfs_uring::ShardPolicy::CapacityAware);
```

This policy has additional admission work under contention. Throughput, CPU cost,
and tail-latency acceptance remain pending target-hardware measurements; it is
not enabled by default.

### Read allocation admission

With `max_in_flight_bytes: Some(budget)`, all shards share one byte budget.
Buffered reads reserve `len` bytes; direct reads reserve the block-aligned
superset length plus `align - 1` bytes of allocation padding, including for
zero-length direct reads. A request whose allocation exceeds the entire budget,
or whose logical length exceeds `max_read_len`, returns `InvalidInput` before
allocation. A byte budget of zero or above `tokio::sync::Semaphore::MAX_PERMITS`
is rejected at construction. `max_read_len: Some(0)` allows only zero-length reads.

Admission acquires the shard's count permit before its byte permits. Saturated
handles wait asynchronously, holding no read buffer; a byte waiter may hold a
count permit, and Tokio's fair byte semaphore can put small reads behind a large
waiter. Dropping a waiting handle returns all partial reservations. After enqueue,
both permits travel with the read until its terminal CQE, even if its caller is
canceled. Short-read retries retain the same reservation. A leaked read retains
its charge. Shutdown or any shard-thread exit closes both the shared byte
semaphore and every shard's count semaphore when byte limits are enabled.
This rejects further admission and wakes waiters at either acquisition stage,
even when another shard has a hung read or takes a bounded-drain escape.
Registration and terminal closure are synchronized at startup/shutdown; ordinary
read admission does not take a registry lock.

This limits reserved driver read-buffer allocation bytes, **not process RSS**.
It excludes queued handle/FD metadata, allocator overhead, result copies and
completed `Vec` results retained in channels or by callers. The caller must bound
its task fan-out and result queue separately. Completion releases admission even
when the returned result remains alive.

## API contract

The public API is intentionally small and read-only:

- `UringDriver::probe_and_start` performs both ring setup and a real read
  round-trip. Treat `ProbeFailure` as a startup decision: expected restriction
  errors (`EACCES`, `EPERM`, `ENOSYS`, `EINVAL`, `EOPNOTSUPP`) select the std
  backend; other errors should be logged and investigated.
- `read_at` uses positioned (`pread`) semantics and resubmits short reads until
  the requested range is complete or EOF. `read_current` follows `read(2)`:
  one short read is a successful result and is not resubmitted.
- `read_at_direct` requires an `O_DIRECT` descriptor and a power-of-two block
  size. The `offset` and `len` arguments may be unaligned; the returned vector
  always has exactly the logical range (never alignment padding).
- Invalid offsets, lengths, and alignments are reported through the awaited
  `io::Result`; they do not panic. In particular, `u64::MAX` is reserved for
  `read_current` and is rejected by positioned APIs.
- A `ReadHandle` may be dropped at any time. Dropping an in-flight handle
  abandons only its result; the driver retains the buffer and file descriptor
  until the completion event. Call `without_cancel_on_drop` when best-effort
  cancellation is not desired.
- Call `shutdown` when the driver is no longer needed. It cancels and drains
  all shards before unmapping rings. A hung device may trigger the bounded
  leak-over-UAF escape hatch; inspect `StatsSnapshot::in_flight` to distinguish
  that degraded outcome from a clean drain.

`StatsSnapshot` is a point-in-time diagnostic view. The conservation identity
`submitted == delivered + orphan_reclaimed` holds after all completions have
been reaped; `in_flight == 0` indicates a clean shutdown.

The optional `diagnostics` feature exposes sampled stage histograms through
`UringDriver::diagnostics()` and `shard_diagnostics()`. It is off by default.
See [the measurement guide](docs/benchmarking.md#sampled-diagnostics) for sampling,
stage overlap, cancellation, and instrumentation-overhead boundaries.

## Testing

Benchmark configuration, CSV schema, timing boundaries, and performance gates
are documented in [the benchmarking guide](docs/benchmarking.md).
See [implementation and acceptance status](docs/optimization-status.md) for
completed correctness work and the still-open performance/integration gates.
Application wiring has separate [RustFS integration prerequisites](docs/rustfs-integration.md).
The [ordered-prefetch example](docs/ordered-prefetch.md) is a bounded consumer
contract experiment, not a production streaming API or performance result.

Linux only; on other hosts `cargo check` builds the empty stub.

```bash
# On a Linux host with io_uring available:
cargo test -- --nocapture --test-threads=1

# Two legs in Docker (also on macOS via Docker Desktop / OrbStack):
#   leg 1 — io_uring blocked by an explicit seccomp profile → ring-dependent
#           tests gracefully skip; kernel-independent unit tests still run;
#   leg 2 — seccomp=unconfined → real io_uring, and NO test may skip.
./run-docker.sh
```

The harness fails on a non-degrading leg 1 or a vacuous-pass leg 2, so a skipped suite can never masquerade as coverage. The cancel-safety contract is pinned by the acceptance tests in `tests/cancel.rs`; the `fault-injection` feature (test-only) drives the panic-abort, bounded-drain-leak, and probe-failure escape hatches in `tests/fault_injection.rs`.

For bounded buffered read groups, see [explicit batch reads](docs/batch-reads.md).
`read_at_batch` shares eager notifications per owning shard while keeping each
read's admission, result and cancellation independent.

## License

Apache-2.0. See [LICENSE](LICENSE).
