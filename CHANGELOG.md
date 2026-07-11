<!--
Copyright 2024 RustFS Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Changelog

All notable changes to `rustfs-uring` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Docs

- Rewrote the README from the current public API — all three read entry points
  (`read_at`, `read_at_direct`, `read_current`) and the degradation contract —
  and removed the `docs/DESIGN.md` design notes. The per-invariant rationale now
  lives in the module and function docs and in the README, so there is a single
  source of truth.

## [0.2.0] - 2026-07-11

Hardening pass from the rustfs/backlog#1160 audit. All changes are on the read
path and preserve the cancel-safety ownership model; both `run-docker.sh` legs
(seccomp-blocked degradation and real io_uring) pass.

### Added

- **Test-only fault-injection seams** for the cancel-safety escape hatches
  (driver-thread panic, stuck bounded drain, forced probe-drain failure), gated
  behind the `fault-injection` feature and never present in a default build.
  ([#11], backlog#1103)
- `StatsSnapshot::submit_errors` — count of non-transient `ring.submit()`
  failures, so a persistently failing `io_uring_enter` is observable.
  (backlog#1162)

### Fixed

- **Bounded-drain bailout no longer hangs awaited handles.** It now fails every
  stranded caller with a driver-gone error before leaking the pending table, and
  leaks the ring-registered eventfd alongside the ring so the "cq_efd outlives
  the ring" invariant holds on that exit too. (backlog#1161, #1167)
- **Persistent `ring.submit()` errors are classified** instead of retried
  forever in silence: EINTR/EBUSY stay transient; any other errno is counted and,
  after a bounded run, shuts the shard down so callers fall back. (backlog#1162)
- **Wakeup gaps closed:** short-read resubmits are flushed the same turn, and a
  drop-cancel now signals the wake eventfd (after closing the receiver so the
  reclaim is still counted as an orphan), removing up-to-50 ms stalls.
  (backlog#1163)
- **Driver-thread spawn failure degrades** to a `ProbeFailure` instead of
  panicking out of disk init. (backlog#1164)
- **The startup probe is time-bounded** (EXT_ARG timeout with a pre-5.11
  fallback) and runs on the first shard only. (backlog#1165)
- **EINTR/EAGAIN completions are retried** (bounded) rather than surfaced as the
  read's final error, and the submit offset guard also rejects an aligned end
  crossing `i64::MAX`. (backlog#1166)
- **CQ overflow is reported as a NODROP backpressure warning**, not fatal loss,
  and AsyncCancel SQEs are deduplicated per op. (backlog#1167)
- **O_DIRECT tail short reads are disambiguated with `fstat`** instead of assuming
  any non-block-multiple read is EOF, so a stacked filesystem cannot cause a
  silent truncation. (backlog#1168)
- **Idle churn cut**: the loop skips `io_uring_enter` on an empty SQ and uses a
  longer heartbeat when idle; each ring caps its io-wq bounded workers.
  (backlog#1169)

### Changed

- **Internal driver-loop refactor** folding the duplicated submit,
  cancel-enqueue, and resubmit-SQE paths into single helpers (`submit_ring`,
  `queue_cancel`, `Pending::resubmit_sqe`, `flush_backlog`). No behavior change;
  the perf-sensitive submit → reap → conditional re-flush order is kept so a
  cache-hit read is still reaped the same turn it completes inline.
  (backlog#1160)

### Tests

- Deterministic sharded cancel-routing test proving a drop-cancel reaches the
  ring that accepted the op. (backlog#1180)

## [0.1.0] - 2026-07-11

### Added

- **Sharded rings** — `UringDriver::probe_and_start_sharded(entries, shards)` runs `shards`
  independent rings, each with its own driver thread, pending table, backpressure semaphore,
  and eventfd. `probe_and_start(entries)` is unchanged and equals `..._sharded(entries, 1)`.
  ([#6], `719b245`)

  A buffered read that hits the page cache completes *inline* inside `io_uring_enter`, so the
  thread driving a ring performs that read's `memcpy`. A single-ring driver is therefore capped
  at one core's memory bandwidth. Measured on a 16-core host: one thread pinned at 100% CPU,
  throughput flat at ~5 GB/s regardless of read size, while a blocking-pool baseline reached
  50 GB/s. Independent rings scaled near-linearly (1/2/4 rings → 4890/8969/15806 MB/s), which is
  what identified the driver thread as the ceiling. With 8 shards, 1 MiB reads went from
  4911 MB/s to 47361 MB/s, and 64 KiB reads at concurrency 32 from 124k to 345k IOPS — while
  *keeping* io_uring's tail-latency advantage rather than trading it away.

  In-flight ops are capped at `entries` **per shard**, so the invariant that makes CQ overflow
  structurally unreachable holds per ring; the driver admits up to `shards * entries`
  concurrent reads. Every cancel-safety invariant holds per shard: a `ReadHandle` carries the
  `tx`/`wake` of the shard that accepted it, so a cancel or a deferred submission necessarily
  routes back to the ring whose pending table holds the op.

- **Native aligned `O_DIRECT` positioned read** — `UringDriver::read_at_direct(file, offset, len, align)`.
  The caller opens the fd with `O_DIRECT` and passes the device's logical block size; `offset`
  and `len` need **not** be aligned. The driver reads a block-aligned superset range into a
  block-aligned buffer and hands back exactly `[offset, offset + len)`. ([#3], `f577ba0`)

  Alignment padding, the bytes preceding the range, and the block-aligned tail never escape — a
  `BitrotReader` expecting an exact shard length would flag padded output as corruption. The
  buffer remains a `Vec<u8>`, over-allocated by `align - 1` with the read region starting at the
  first aligned byte inside the allocation, so the cancel-safety ownership model is untouched.
  Buffered reads are the degenerate `align == 1` case of the same geometry.

- **Benchmark harnesses** (`examples/` + runner scripts), both hardened so they cannot pass
  vacuously: files are created only when absent (`create_new` + `O_NOFOLLOW`, never truncating a
  caller-supplied path even though the sweeps run as root), content is offset-addressable, and
  `BENCH_VERIFY=1` checks every delivered byte at its claimed offset.

  - `streaming_bench` + `bench-streaming.sh` — sequential whole-file read across
    `std_buffered` / `std_odirect` / pipelined `uring_read_at` / `uring_read_at_direct`, in warm
    and cold page-cache legs. ([#4], `4299b92`)
  - `concurrent_pread_bench` + `bench-concurrent-pread.sh` — many concurrent positioned reads on
    one disk, the shape erasure-coded shard reads actually serve. Four strategies isolate the
    cost of the per-read `open` and of the `spawn_blocking` hop. ([#5], `d03977a`)

### Changed

- **Async backpressure** — the driver's synchronous backpressure gate became a tokio
  `Semaphore`. `submit` takes a permit with `try_acquire_owned()` on the fast path and, when
  saturated, hands the acquire future to the `ReadHandle` to await on its first poll instead of
  parking the calling runtime worker. ([#2], `2b7cae4`)

  The permit is stored in the pending entry, so it is released exactly when that entry is
  dropped at the final CQE — never at future drop. Public API is unchanged: `read_at`'s
  signature and its "in flight the moment it returns" behaviour hold on the fast path.

- **eventfd-driven reaping** — the 200 µs busy-poll loop was replaced by a `poll(2)` on two
  eventfds: one registered with the ring so the kernel signals every CQE, one signalled by
  `submit`/shutdown so a new message wakes the loop immediately. A bounded heartbeat still runs
  a loop turn so the drain deadline is checked and the NODROP overflow list is flushed.
  ([#1], `ea6b920`)

### Fixed

- `aligned_geometry` rejects an `align` above the kernel read cap, foreclosing the
  `align_offset == usize::MAX` path that would have made the driver's pointer arithmetic
  undefined, and the `region_len + align - 1` allocation overflow. The driver's aligned-buffer
  setup replaced a release-mode-only `debug_assert!` with a checked allocation size and a
  runtime guard: an unsatisfiable alignment now fails the read instead of doing UB pointer
  arithmetic. ([#3], `f577ba0`)
- `deliver()` skips the full-buffer `copy_within` on the buffered path (`align == 1`, where the
  logical range already starts at byte 0), so a buffered read no longer pays a per-read
  `memmove`. ([#3], `f577ba0`)
- The fast-path `submit` now surfaces an explicit driver-gone error through the result channel
  when the driver has exited, instead of letting the caller infer one from a dropped oneshot.
  ([#2], `2b7cae4`)

## Pre-history

Before this repository existed, the code was the Spike 0 cancel-safety prototype in
`rustfs/rustfs` under `experiments/`. It was audited line by line
([rustfs/backlog#1051]) and every confirmed finding was remediated there. The library was then
promoted into this repository with its history preserved (`60993c9`), given a real-io_uring CI
leg that the main workspace cannot run (`d5b28fc`), and had its crate metadata and README
polished (`39018c0`).

The cancel-safety invariants that survived that audit — the driver's pending table owning the
buffer and fd from SQE submission until the CQE, drop-abandons-result-only, bounded shutdown
drain, abort-before-free on a driver panic — are the ones every change above is measured
against. See the [design notes](https://github.com/rustfs/uring/blob/0.1.0/docs/DESIGN.md).

## Decisions recorded, not implemented

These were on the roadmap and were **closed by measurement or by design conflict** rather than
built. They are listed so nobody re-opens them without new evidence.

- **Three read shapes / streaming reads through io_uring** — **NO-GO** ([rustfs/backlog#1144]).
  io_uring's lever on a *sequential* stream is pipelining; kernel readahead already has it. Cold
  reads are device-bound, so io_uring at best ties; on a warm page cache it *loses badly*
  (11–41% of a buffered read) because a single sequential stream exploits neither batched
  submission nor the absence of a blocking thread. Streaming reads stay on the std backend.
- **`AsyncFd` reaping without a driver thread** — **not done**. `Drop` cannot `await`, so the
  bounded shutdown drain would have to break the public API. The dedicated driver thread plus
  eventfd reaping already removed the busy-poll, which was the actual cost.
- **Process-wide singleton ring** — **redefined**. A singleton conflicts with per-disk
  isolation: a stalled disk must not starve another disk's ring. Rings stay per-disk, and
  sharding ([#6]) scales them within a disk instead.
- **Registered buffers (`register_buffers`)** — **deprioritized**. It conflicts with the
  `Vec<u8>` ownership model the cancel-safety proof rests on, and end-to-end profiling showed
  the S3 GET bottleneck is userspace copies, not the disk read
  ([rustfs/backlog#1159]). See "When this crate helps" in the README.

[#1]: https://github.com/rustfs/uring/pull/1
[#2]: https://github.com/rustfs/uring/pull/2
[#3]: https://github.com/rustfs/uring/pull/3
[#4]: https://github.com/rustfs/uring/pull/4
[#5]: https://github.com/rustfs/uring/pull/5
[#6]: https://github.com/rustfs/uring/pull/6
[#11]: https://github.com/rustfs/uring/pull/11
[rustfs/backlog#1051]: https://github.com/rustfs/backlog/issues/1051
[rustfs/backlog#1144]: https://github.com/rustfs/backlog/issues/1144
[rustfs/backlog#1159]: https://github.com/rustfs/backlog/issues/1159
[Unreleased]: https://github.com/rustfs/uring/compare/0.2.0...HEAD
[0.2.0]: https://github.com/rustfs/uring/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/rustfs/uring/releases/tag/0.1.0
