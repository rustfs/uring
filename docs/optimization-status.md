# Read-driver optimization status

Tracking: [rustfs/backlog#2647](https://github.com/rustfs/backlog/issues/2647).
Updated 2026-09-23. Implementation, correctness, CI, performance and application
integration are separate gates; a checked code item does not close the roadmap.

| Step | Implementation and review | Remaining acceptance |
| --- | --- | --- |
| 1.1–1.4: benchmark schema, timing, diagnostics, direct positive gate | Merged in [#15](https://github.com/rustfs/uring/pull/15), CI passed | Diagnostics overhead and real LocalIoBackend baseline |
| 1.5: ABBA evidence tooling | Explicit same-binary calibration implemented/reviewed; 20 gate/cleanup tests pass | Stable native calibration, valid comparison and application integration |
| 2.1–2.4: completion recovery, direct integrity, byte admission, guarantee boundaries | Implemented/reviewed; native regression suite passed; #16 merged | Application-wide limits remain separate |
| 2.4 API follow-up: shutdown control and runtime adapter | Implemented/reviewed; native CI #55 passed; #16 merged | No hard cleanup deadline or performance claim |
| 3.1–3.3: bounded turns, explicit batch notifications, cancellation efficiency | Implemented/reviewed; native regression suite passed; #16 merged | CPU/syscall and tail-latency comparison |
| 4.1: capacity-aware routing | Implemented/reviewed, opt-in; native tests passed; #16 merged | Controlled slow-shard/mixed-load performance |
| 4.2: system-wide budgets and probe offload | Application probe offload, driver-thread budget and logical chunk limit implemented/reviewed; #8080 merged into #8074, then #8074 into #8072 | #8072/#8076 final CI and application-main integration; physical byte/result/io-wq budgets remain open |
| 4.2 pool prerequisite | Library whole-driver shared quota implemented/reviewed; native CI #57 passed; #16 merged | Application dependency/fallback/result ownership wiring |
| 4.2 io-wq registration evidence | Per-shard startup query reviewed; both [PR #17 CI runs](https://github.com/rustfs/uring/actions/runs/35809115447) passed and #17 merged | Actual worker count and process-wide budget remain unproven |
| 5: owned buffers and direct FD cache | Dual-mode exact invalidation is in open application PR #8075; a prior head passed CI, but direct caching and owned buffers are not enabled | Final-head CI/merge, profile evidence, lease/pool lifetime and complete invalidation integration |
| 6: ordered streaming prefetch | [Example-only contract experiment](ordered-prefetch.md) implemented/reviewed; 11 portable tests and native CLI CI #54 passed | Production consumer contract, bitrot/S3 and performance evidence |
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

The following documentation head `80a7af9` passed all three jobs in
[CI #53](https://github.com/rustfs/uring/actions/runs/35767797346), including
restricted/unrestricted Docker tests and both benchmark smoke modes. Later
follow-up commits need their own CI; the live PR and tracking issue record
final-head CI and merge status separately.

## Follow-up evidence and examples

The [io-wq startup record](iowq-startup.md) captures the existing registration
result without changing its requested `[16, 0]` values, fallback, or read path.
It records kernel-returned *previous* limits on success and error kind/errno on
failure. Deterministic injected tests and a two-shard native test are added;
two independent reviews passed. [CI at `193ab21`](https://github.com/rustfs/uring/actions/runs/35808804770)
passed all three jobs: 147 native all-feature tests with no skips, the positive
O_DIRECT marker, ordered-prefetch and both benchmark CLI smokes, and restricted/unrestricted
Docker coverage. The new native test read exact bytes from two shards and
observed stable startup records; it did not force io-wq worker execution. The
snapshot does not measure effective worker counts or establish a process-wide
cap. The documentation-only head `2b79358` also passed [all three CI jobs](https://github.com/rustfs/uring/actions/runs/35809115447), and [#17](https://github.com/rustfs/uring/pull/17) merged as `fafd405d`.

`3931124` implements `SharedReadBudget` whole-driver reservations, with public
tests in `7e94f36`. Seven deterministic private tests and nine public/native
tests cover concurrent accounting, large `usize` quotas, message/deferred
ownership, independent shutdown, retained results and leaked whole quotas.
Two independent code/test/document reviews found no blocking issue. Local
Linux-target default/all-feature all-target checks, all-feature Clippy,
default/all-feature warning-denying rustdoc, formatting and diff checks pass;
the default library/dependency graph remains Tokio sync-only. The initial RED
was missing-API compilation, not a behavioral run. Subsequently, [CI #57](https://github.com/rustfs/uring/actions/runs/35796196402)
at `b09b966` passed all three jobs: 143 native all-feature tests plus one compiled
`no_run` documentation example, mandatory O_DIRECT, ordered-prefetch and both
benchmark CLI smokes, lint, and restricted/unrestricted Docker legs. All seven
private and nine public shared-budget tests ran successfully, including the
isolated whole-quota leak and greater-than-`u32` reservation cases. Later
evidence-only documentation commits have their own workflow status. Its
[contract](shared-read-budget.md) documents the
later-shard startup-failure coverage boundary and conservative idle/retirement
reservation costs. Application wiring and performance remain separate gates.

Application work is tracked in [RustFS #8072](https://github.com/rustfs/rustfs/pull/8072),
[#8074](https://github.com/rustfs/rustfs/pull/8074),
[#8075](https://github.com/rustfs/rustfs/pull/8075),
[#8076](https://github.com/rustfs/rustfs/pull/8076), and
[#8080](https://github.com/rustfs/rustfs/pull/8080). #8080 merged into the #8074
branch and #8074 merged into the #8072 branch; none of those changes reached
application main. The open #8072, #8075 and #8076 PRs now target
`improve/uring`, created at the then-current application main commit.
#8076 still has failing full-CI jobs, while final-head checks on #8072 and #8075
remain separate acceptance gates. These partial results do not establish
successful application integration.

`c7bb321` adds non-joining shutdown requests, advisory thread completion and the
default-off Tokio shutdown adapter. Count and byte admission close before a
request returns; the adapter transfers ownership before its result future is
polled. The [shutdown contract](shutdown.md) distinguishes thread completion,
successful join, clean drain, and runtime-shutdown failure boundaries. Tests in
`be66d8e` include real pending reads and an isolated nonclean-drain subprocess.
Two independent code reviews found no blocking issue. Linux-target default
all-target checking, all-feature Clippy, both feature-state rustdoc builds with
warnings denied, formatting and diff checks pass locally. These are compilation
checks, not native execution. Subsequently, [CI #55](https://github.com/rustfs/uring/actions/runs/35794075169)
at `a942ac1` passed all three jobs: 127 all-feature native tests (76 unit, 4
admission, 21 read/cancel, 6 fault, 11 prefetch, 2 shard-policy, 7 shutdown), the
mandatory O_DIRECT marker, ordered-prefetch and both benchmark CLI smokes, lint
and 20 Python tests. Docker also passed restricted degradation and unrestricted
native legs; its fault-injection-only build does not enable `tokio-runtime`.
Later documentation-only commits do not change this tested implementation;
their own workflow status remains separate. No throughput or hard cleanup
deadline is claimed.

`6927bb0` adds explicit same-binary calibration: identical executable content,
zero diagnostics interval on every leg, unchanged endpoint drift thresholds,
and a separate drift check for each middle leg against the endpoint mean.
Calibration never emits candidate-attribution fields; 20 Python tests pass,
including synthetic three-round execution and early-stop cases. These tests do
not establish a stable native calibration.

`f135f1d` adds an example-local ordered reader, not a public streaming API.
Eleven portable tests pass, including actual Tokio semaphore reservation order,
cancelled `next`, EOF/error ordering, logical-byte accounting and slow-consumer
boundaries. The CLI uses an immutable fixture and positioned std oracle outside
the executor; the new CI smoke requires successful native io_uring byte checks
and bounds each child process duration. No production driver code or default
behavior changes in these follow-ups.

Both additions and the current-main application integration plan received
independent review. Dedicated-host native execution is deferred while an existing
CI worker is active; the worker was not stopped and no performance run was made.
Portable checks and Linux-target compilation are not substitutes for the new
native CLI gate. Final-head CI/execution results are tracked in the issue/PR.

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
- [Shutdown ownership and Tokio adapter](shutdown.md)
- [Shared whole-driver read-budget reservations](shared-read-budget.md)
- [Io-wq startup registration evidence](iowq-startup.md)
- [Completion recovery and direct integrity](fault-recovery.md)
- [Cancellation and eventfd behavior](cancellation-efficiency.md)
- [Driver turn fairness](driver-fairness.md)
- [Explicit buffered batches](batch-reads.md)
- [Measurement boundaries and evidence gates](benchmarking.md)
- [Current RustFS application integration prerequisites](rustfs-integration.md)
- [Ordered-prefetch contract experiment](ordered-prefetch.md)
