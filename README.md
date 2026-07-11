# rustfs-uring

[![CI](https://github.com/rustfs/uring/actions/workflows/ci.yml/badge.svg)](https://github.com/rustfs/uring/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/rustfs/uring/blob/main/LICENSE)
[![crates](https://img.shields.io/crates/v/rustfs-uring.svg)](https://crates.io/crates/rustfs-uring)
[![docs.rs](https://docs.rs/rustfs-uring/badge.svg)](https://docs.rs/rustfs-uring/)

Cancel-safe async `io_uring` read backend for [RustFS](https://github.com/rustfs/rustfs) storage.

This crate is the io_uring integration that RustFS's read path is built on. It lives in its own repository so it can be
verified in isolation — with a real io_uring CI leg that the main `rustfs/rustfs` workspace cannot run — before being
wired into the storage layer behind a runtime probe.

> **Status:** read path only, Linux only. On any other target the crate compiles to an empty stub. The read path is
> wired into `rustfs/rustfs` behind a runtime probe and is **off by default** (`RUSTFS_IO_URING_READ_ENABLE`). What has
> landed is in [`CHANGELOG.md`](CHANGELOG.md); the per-invariant rationale lives in the module and function docs on
> [docs.rs](https://docs.rs/rustfs-uring/) and inline in `src/`.

```toml
[target.'cfg(target_os = "linux")'.dependencies]
rustfs-uring = "0.2.0"
```

## The ownership model it enforces

When a caller drops the future of an in-flight read (EC quorum reached, timeout, disconnect), the kernel may still write
into the read buffer at any point until the CQE. Freeing the buffer at future-drop is a use-after-free. This crate
enforces the invariants any production io_uring integration must follow:

- **The buffer and the file handle are owned by the driver's pending (orphan) table** from SQE submission until the
  CQE — never by the caller's future.
- **Dropping the future abandons only the result**; reclamation always happens at the CQE, optionally accelerated by an
  `IORING_OP_ASYNC_CANCEL` sent on drop.
- **Shutdown drains in-flight ops to zero** before the ring is unmapped, with a bounded escape hatch for a hung disk:
  on timeout it fails the stranded callers with an error and *leaks* the ring and its buffers rather than free memory
  the kernel may still touch (leak over UAF).
- A driver-thread panic **aborts the process before freeing in-flight buffers**; backpressure permits are released at
  the CQE, not at future drop; a short read on a positioned read is resubmitted to satisfy the whole-range contract;
  the probe file is opened via `O_TMPFILE`.

It also has a **degradation contract** — the reason it can ship default-on-probe safely:

- The startup probe runs a real `IORING_OP_READ` under a wall-clock timeout. A restricted environment
  (seccomp / gVisor / old kernel) fails with a `ProbeFailure` whose `is_expected_restriction()` tells the caller to
  degrade to the std backend quietly; an unexpected failure is surfaced, not hidden.
- A persistently failing `io_uring_enter` is classified (not retried forever in silence): the shard shuts down so
  callers fall back, and the failure count is visible in `StatsSnapshot`.
- Transient CQE errnos (`EINTR`/`EAGAIN`) are retried; the whole-range and EOF contracts are honoured even on stacked
  filesystems by disambiguating a short read with `fstat` rather than assuming EOF.

Each invariant holds **per shard** (see below), because a shard is an independent instance of the same driver. Every
one is pinned by an acceptance test (see [Testing](#testing)).

## Usage

```rust
use std::fs::File;
use std::sync::Arc;
use rustfs_uring::UringDriver;

# async fn demo() -> std::io::Result<()> {
    // Probe a real IORING_OP_READ before accepting work. On a restricted host the
    // ProbeFailure's `is_expected_restriction()` tells you to degrade quietly.
    let driver = UringDriver::probe_and_start(64).expect("io_uring available");

    let file = Arc::new(File::open("/data/object")?);

    // Positioned read (pread semantics, whole-range: short reads are resubmitted).
    let bytes = driver.read_at(Arc::clone(&file), 0, 65536).await?;

    // Dropping the returned future before it completes is safe — the driver owns
    // the buffer until the CQE.

    let snapshot = driver.shutdown();
    assert_eq!(snapshot.delivered + snapshot.orphan_reclaimed, snapshot.submitted);
    # Ok(())
    #
}
```

Three read entry points, all returning an awaitable `ReadHandle`:

- `read_at(file, offset, len)` — positioned (pread) read, whole-range: short reads are resubmitted until the range is
  satisfied or a real EOF.
- `read_at_direct(file, offset, len, align)` — the same, for an fd opened with `O_DIRECT` (see below).
- `read_current(file, len)` — reads from the fd's current position with `read(2)` semantics: a short read is a valid
  final result and is *not* resubmitted. Use it for pipes and other non-seekable fds.

`ReadHandle::without_cancel_on_drop()` opts a handle out of the drop-time `ASYNC_CANCEL` when you know the op will
complete on its own; and `driver.stats()` returns a `StatsSnapshot` at any time.

### Sharded rings

A buffered read that hits the page cache completes *inline* inside `io_uring_enter`, so the thread driving a ring
performs that read's `memcpy`. One ring is therefore capped at a single core's memory bandwidth (~5 GB/s measured).
Give a disk several rings when its reads hit the cache:

```rust
# use rustfs_uring::UringDriver;
// Four independent rings, each with `entries` SQ slots and its own driver thread.
// In-flight is capped per shard, so the driver admits up to `shards * entries` reads.
let driver = UringDriver::probe_and_start_sharded(64, 4)?;
# Ok::<_, rustfs_uring::ProbeFailure>(())
```

`probe_and_start(entries)` is exactly `probe_and_start_sharded(entries, 1)`, so nobody grows threads by upgrading.
Rings stay per-disk: a stalled disk cannot starve another disk's rings.

### `O_DIRECT`

Open the fd with `O_DIRECT`, pass the device's logical block size, and let the driver do the alignment. `offset` and
`len` need **not** be aligned — it reads a block-aligned superset into a block-aligned buffer and hands back exactly
the range you asked for. Padding, the bytes before the range, and the block-aligned tail never escape.

```rust
# use rustfs_uring::UringDriver;
# use std::{fs::File, sync::Arc};
# async fn demo(driver: &UringDriver, file: Arc<File>) -> std::io::Result<()> {
// `file` was opened with O_DIRECT; 4096 is the probed logical block size.
let bytes = driver.read_at_direct(file, 8_191, 100, 4096).await?;
assert_eq!(bytes.len(), 100);
# Ok(())
# }
```

## When this crate helps — and when it does not

These numbers come from the harnesses in this repository and from end-to-end profiling of RustFS
([rustfs/backlog#1159](https://github.com/rustfs/backlog/issues/1159)). They are reported as measured, including the
cases where io_uring loses.

| workload | result |
| --- | --- |
| **Many concurrent positioned reads on one disk** (erasure-coded shard reads) | **Where it wins.** With sharded rings and a cached fd: 64 KiB at concurrency 128 → 361k IOPS vs 125k for a blocking-pool baseline, and p999 3.0 ms vs 13.5 ms. |
| **A single sequential stream** | **It loses.** Kernel readahead already does what pipelining would buy. Cold reads are device-bound; on a warm page cache io_uring reaches only 11–41% of a buffered read. Streaming reads should stay on the std backend. |
| **One read at a time (low concurrency)** | **It loses.** Per-op submission overhead exceeds a page-cache `memcpy`. |
| **End-to-end S3 GET** | **Roughly neutral today (−7% … +4%).** The disk read is not the bottleneck: a cached 1 MiB GET spends ~25% of CPU in `memcpy` and ~10% in `memset`, and 0% on device reads. Optimising the read path further only pays once those copies are gone. |

Two traps this crate's own benchmarking fell into, documented so others do not repeat them:

- A `76×` apparent speedup turned out to be a **behaviour regression**, not a win: the io_uring path had silently
  stopped honouring RustFS's `fadvise(DONTNEED)` page-cache reclaim policy, so one leg served everything from cache
  while the other read the device. Always check `disk_read` and page-cache deltas, not just throughput.
- Microbenchmarks of the read path measured a page-cache-hit regime that production *deliberately avoids* for large
  reads. Isolated-path gains do not transfer end-to-end for free.

## Testing

This is a Linux-only crate; on a non-Linux host `cargo check` only builds the empty stub.

```bash
# Native, on a Linux host with io_uring available:
cargo test -- --nocapture --test-threads=1

# Two legs in Docker (also runs on macOS via Docker Desktop / OrbStack):
#   leg 1 — io_uring blocked by an explicit seccomp profile → the suite MUST
#           degrade to a graceful skip (reproduces a restricted environment);
#   leg 2 — seccomp=unconfined → real io_uring, and NO test may skip.
./run-docker.sh
```

The harness fails on either a non-degrading leg 1 or a vacuous-pass leg 2, so a skipped suite can never masquerade as
real coverage. The cancel-safety contract is pinned by 16 acceptance tests in `tests/cancel.rs` — buffer conservation
under a mixed drop/keep stress across shards, an orphaned op reclaimed only at its CQE, sharded cancel routed to the
ring that owns the op, bounded shutdown drain, `O_DIRECT` returning exact unaligned ranges, and backpressure deferring
rather than blocking a runtime worker. Five deterministic fault-injection tests in `tests/fault_injection.rs` (behind
the test-only `fault-injection` feature, never compiled into a release build) drive the escape hatches: the
panic-abort barrier, the bounded-drain leak-and-error path, a forced probe-drain failure, and a driver-thread spawn
failure that must degrade rather than panic.

## Benchmarks

Both harnesses refuse to overwrite or follow a symlink at a caller-supplied path (the sweeps run as root), fill their
files with an offset-addressable pattern, and check every delivered byte against it under `BENCH_VERIFY=1` — throughput
alone cannot tell a correct strategy from one reading the wrong offsets.

```bash
# Sequential whole-file read: buffered vs O_DIRECT vs pipelined io_uring, warm and cold cache.
./bench-streaming.sh

# Many concurrent positioned reads on one disk — the shape shard reads actually serve.
# Isolates the cost of the per-read open and of the spawn_blocking hop.
./bench-concurrent-pread.sh
```

## Roadmap

- **Write path.** Untouched today; PUT still goes through the blocking pool, and profiling suggests the win there may
  exceed the read path's.
- **`register_files`.** Would remove the per-op fd lookup. Lower value now that the consumer caches descriptors.
- **`SQPOLL`.** Eliminates `io_uring_enter` under sustained load, at the cost of a kernel polling thread per ring —
  which multiplies by shards and by disks. Only for high-end deployments.

Closed by measurement, not built (see the CHANGELOG): streaming reads through io_uring (NO-GO), `AsyncFd` reaping
without a driver thread (would break the public API), a process-wide singleton ring (conflicts with per-disk
isolation), and registered buffers (conflicts with the `Vec<u8>` ownership model, and the bottleneck is elsewhere).

## License

Apache-2.0. See [LICENSE](LICENSE).
