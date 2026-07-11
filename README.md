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
rustfs-uring = "0.2.1"
```

## Usage

```rust
use std::fs::File;
use std::sync::Arc;
use rustfs_uring::UringDriver;

# async fn demo() -> std::io::Result<()> {
// Probe a real IORING_OP_READ before accepting work. On a restricted host the
// ProbeFailure's `is_expected_restriction()` says to degrade to the std backend.
let driver = UringDriver::probe_and_start(64).expect("io_uring available");
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

## Testing

Linux only; on other hosts `cargo check` builds the empty stub.

```bash
# On a Linux host with io_uring available:
cargo test -- --nocapture --test-threads=1

# Two legs in Docker (also on macOS via Docker Desktop / OrbStack):
#   leg 1 — io_uring blocked by an explicit seccomp profile → every test MUST
#           degrade to a graceful skip;
#   leg 2 — seccomp=unconfined → real io_uring, and NO test may skip.
./run-docker.sh
```

The harness fails on a non-degrading leg 1 or a vacuous-pass leg 2, so a skipped suite can never masquerade as coverage. The cancel-safety contract is pinned by the acceptance tests in `tests/cancel.rs`; the `fault-injection` feature (test-only) drives the panic-abort, bounded-drain-leak, and probe-failure escape hatches in `tests/fault_injection.rs`.

## License

Apache-2.0. See [LICENSE](LICENSE).
