# Read-driver optimization status

Tracking: [rustfs/backlog#2647](https://github.com/rustfs/backlog/issues/2647).
Updated 2026-09-23. Implementation, correctness, CI, performance and application
integration are separate gates; a checked code item does not close the roadmap.

| Step | Implementation and review | Remaining acceptance |
| --- | --- | --- |
| 1.1–1.4: benchmark schema, timing, diagnostics, direct positive gate | Merged in [#15](https://github.com/rustfs/uring/pull/15), CI passed | Diagnostics overhead and real LocalIoBackend baseline |
| 1.5: ABBA evidence tooling | Implemented; 10 gate/cleanup tests pass | Stable calibration, valid comparison and application integration |
| 2.1–2.4: completion recovery, direct integrity, byte admission, guarantee boundaries | Implemented and independently reviewed; native regression suite passed | Hardening PR CI/merge; application-wide limits remain separate |
| 3.1–3.3: bounded turns, explicit batch notifications, cancellation efficiency | Implemented and independently reviewed; native regression suite passed | CPU/syscall and tail-latency comparison; PR CI/merge |
| 4.1: capacity-aware routing | Implemented and independently reviewed, opt-in; native tests passed | Controlled slow-shard/mixed-load performance; PR CI/merge |
| 4.2: system-wide budgets and probe offload | Not implemented in this crate | Fresh application-path review and coordinated integration |
| 5: owned buffers and direct FD cache | Not implemented | Profile evidence, lease/pool lifetime design and cache invalidation integration |
| 6: ordered streaming prefetch | Not implemented | Consumer/backpressure contract and end-to-end evidence |
| 7: advanced ring/runtime modes | Not enabled or implemented | Earlier gates, capability/fallback and isolated benefit evidence |

## Correctness and review evidence

The integrated code revision `7e2f1aa` passed 104 tests on unrestricted Linux:
71 unit, 4 admission, 21 read/cancellation, 6 fault and 2 shard-policy tests.
No test skipped; the mandatory `DIRECT_OK` marker was observed. All-feature
Clippy, formatting, 10 Python evidence-runner tests and the instrumented
eight-strategy benchmark correctness smoke also passed. The default-feature
native suite passed another 90 tests with no skips and the same direct marker.
A Linux-target
default/all-feature Clippy check and warning-denying rustdoc passed locally;
cross-compilation is not additional native execution evidence.

Independent reviews covered buffer/FD/permit ownership, global closure and
count-stage waiters, cancellation races, final-owner routing, batch unwind and
notifications, fairness/progress, test validity and documentation. Review found
and fixed a global-close waiter gap and benchmark orphan-child cleanup. No
blocking finding remained in the completed component and combined-patch reviews.

Kernel CQ-overflow and direct-read tests actually execute on Linux. Partial
submit/error classification and metadata failures use deterministic production
decision seams; they do not prove forced real-kernel partial acceptance, hung
syscall recovery, or advanced taskrun-mode behavior.

## Performance status: not accepted

The same-binary warm-cache A/A control used three A1/B1/B2/A2 rounds, 10 million
operations per leg and unchanged drift gates (3% throughput, 5% p99). The third
round's p99 baseline drift was **6.35%**, so the whole calibration was rejected
and candidate/diagnostics-overhead comparisons were not started. An earlier
shorter calibration was rejected for insufficient measurement duration; neither
run is performance evidence. The baseline executable predates this hardening
patch: these controls do not measure the new driver's speed.

Do not infer throughput, latency, CPU or S3 improvements from correctness tests,
eventfd-count assertions, or rejected calibration. Keep diagnostics disabled
and round-robin routing as defaults. Before further performance-dependent work,
establish stable calibration without loosening gates after observing results.

## Contract references

- [Public read and admission contracts](../README.md)
- [Completion recovery and direct integrity](fault-recovery.md)
- [Cancellation and eventfd behavior](cancellation-efficiency.md)
- [Driver turn fairness](driver-fairness.md)
- [Explicit buffered batches](batch-reads.md)
- [Measurement boundaries and evidence gates](benchmarking.md)
