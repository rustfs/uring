# Completion progress and direct-read integrity

The driver skips `io_uring_enter` only when the submission queue is empty and
neither `IORING_SQ_CQ_OVERFLOW` nor `IORING_SQ_TASKRUN` requests kernel work.
The io-uring 0.7.15 submitter supplies `GETEVENTS` for these flags. After reaping,
the driver attempts submission again so newly freed CQ space can receive the
kernel's NODROP overflow entries even when no new reads arrive.

Each driver turn makes at most two submission attempts. Partial submission,
zero progress, EINTR and EBUSY do not start an unbounded retry loop. The next
event or heartbeat drives further progress. Pending buffers, file descriptors
and admission permits retain their existing final-CQE ownership rules.

CQ-overflow warnings are emitted when the observed kernel counter changes to a
nonzero value, not on every turn while the cumulative counter remains nonzero.
The snapshot continues to report the kernel counter, including its u32 wrap:
a wrap to zero updates the snapshot silently, and a later nonzero value warns
again. This prevents repeated log output without changing completion recovery.

A non-block-aligned O_DIRECT short read that does not cover the logical range
cannot be resumed from its unaligned endpoint. The driver checks file metadata:
confirmed EOF returns the initialized logical prefix; a mid-file endpoint
returns an error; metadata failure propagates its original I/O error. A metadata
failure is not evidence of EOF. Concurrent file mutation retains ordinary read
semantics, without providing snapshot isolation.

## Validation

On Linux, run:

```sh
cargo test --all-features --lib fault_recovery_tests -- --nocapture
cargo test --all-features --lib submit_result_tests -- --nocapture
```

The metadata-result tests inject a completed prefix and metadata outcomes into
the production decision helper. They do not simulate a kernel short read or a
real `fstat` failure. The NOP overflow test creates a two-entry CQ, overflows it,
drains it, and verifies the empty-SQ submission path recovers the third CQE. It
uses actual kernel CQEs but no user buffers. The backlog test covers SQ-capacity
backpressure, not forced partial acceptance by `io_uring_enter`.

Submit-result tests inject `io::Result<usize>` into the production classification
helper: partial positive acceptance, zero progress, EINTR/EBUSY and persistent
nontransient errors. They verify counter reset, the exact shutdown threshold,
one shutdown transition, deduplicated cancels and retained pending buffer/FD/
count-permit/byte-permit ownership. They also check the loop's retry decision.
This models syscall return handling without inducing a partial return or failure
from a real kernel; it is not native kernel partial-submission evidence.

Kernel tests print `SKIP` when setup returns an expected restriction error;
such a run is not evidence of overflow recovery. Run on an unrestricted Linux
host for kernel coverage. Advanced taskrun ring modes remain disabled by default
and are not enabled or kernel-validated by these tests.
