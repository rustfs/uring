# rustfs-uring

[![CI](https://github.com/rustfs/uring/actions/workflows/ci.yml/badge.svg)](https://github.com/rustfs/uring/actions/workflows/ci.yml)

Cancel-safe async `io_uring` read backend for [RustFS](https://github.com/rustfs/rustfs) storage.

This crate is the io_uring integration that RustFS's P2 read path is built on. It started as the Spike 0 cancel-safety prototype ([rustfs/backlog#894](https://github.com/rustfs/backlog/issues/894)) and was hardened per the [#1048/#1051 audit](https://github.com/rustfs/backlog/issues/1051). It lives in its own repository so it can be verified in isolation — with a real io_uring CI leg that the main `rustfs/rustfs` workspace cannot run — before being wired into the storage layer.

> **Status:** read path only, Linux only. Not yet published to crates.io — depend on it via git until the P2 work (write path, eventfd/`AsyncFd` reaping, O_DIRECT, `LocalIoBackend` integration) lands. See [`docs/DESIGN.md`](docs/DESIGN.md).

## The ownership model it enforces

When a caller drops the future of an in-flight read (EC quorum reached, timeout, disconnect), the kernel may still write into the read buffer at any point until the CQE. Freeing the buffer at future-drop is a use-after-free. This crate proves and enforces the invariants any production io_uring integration must follow:

- **The buffer and the file handle are owned by the driver's pending (orphan) table** from SQE submission until the CQE — never by the caller's future.
- **Dropping the future abandons only the result**; reclamation always happens at the CQE (optionally accelerated by `IORING_OP_ASYNC_CANCEL`).
- **Shutdown drains in-flight ops to zero** (with a bounded escape hatch for hung disks) before the ring is unmapped.
- A driver-thread panic **aborts before freeing in-flight buffers** (leak over UAF); backpressure permits are released at the CQE, not at future drop; short reads on positioned reads are resubmitted to satisfy the whole-range contract; the probe file is opened via `O_TMPFILE`.

The full invariant list, the corrected fd-reuse mechanism, and the P2 design constraints are in [`docs/DESIGN.md`](docs/DESIGN.md).

## Usage

```rust
use std::fs::File;
use std::sync::Arc;
use rustfs_uring::UringDriver;

# async fn demo() -> std::io::Result<()> {
// Probe a real IORING_OP_READ before accepting work; a restricted environment
// (seccomp/gVisor/old kernel) returns a ProbeFailure you degrade to the std
// backend on.
let driver = UringDriver::probe_and_start(64).expect("io_uring available");

let file = Arc::new(File::open("/data/object")?);

// Positioned read (pread semantics, whole-range).
let bytes = driver.read_at(Arc::clone(&file), 0, 65536).await?;

// Dropping the returned future before it completes is safe — the driver owns
// the buffer until the CQE.

driver.shutdown();
# Ok(())
# }
```

## Testing

This is a Linux-only crate; on a non-Linux host `cargo check` only builds the empty stub.

```bash
# Native, on a Linux host with io_uring available:
cargo test -- --nocapture --test-threads=1

# Two legs in Docker (also runs on macOS via Docker Desktop / OrbStack):
#   leg 1 — io_uring blocked by an explicit seccomp profile → the suite MUST
#           degrade to a graceful skip (reproduces the #4313 restricted env);
#   leg 2 — seccomp=unconfined → real io_uring, and NO test may skip.
./run-docker.sh
```

The harness fails on either a non-degrading leg 1 or a vacuous-pass leg 2, so a skipped suite can never masquerade as real coverage.

## Roadmap (P2)

- eventfd + tokio `AsyncFd` reaping to replace the poll loop (must still flush the NODROP overflow list).
- O_DIRECT aligned buffers and the three read shapes.
- `LocalIoBackend` integration in `rustfs/rustfs` behind runtime probing.
- Per-disk probe cache and runtime errno degradation latch.
- Registered buffers (P3) with the content-hygiene invariant, and the write path (P4).

## License

Apache-2.0. See [LICENSE](LICENSE).
