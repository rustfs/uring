# Driver turn fairness

Each driver turn processes at most 64 input messages and 64 completion entries.
Intake also ends once the turn has allocated 8 MiB of read buffers, including
alignment padding. These private constants bound batches; they are not claimed
to be optimal throughput settings. Public configuration and ring flags remain
unchanged.

An individual accepted read may exceed 8 MiB: it is allocated once and then
intake yields to submission and reap. Consequently the allocation threshold may
be exceeded by one allocation. These limits do not preempt a single allocation,
direct-read copy, metadata lookup, syscall, or shutdown's cancellation sweep.
Request-count and optional byte admission limits continue to govern ownership.

The driver alternates bounded intake, submission, bounded reap, and a second
submission. This prevents a continuously replenished input or completion queue
from starving the other phase or shutdown-deadline checks. Messages remain FIFO;
this does not move cancellation or shutdown ahead of previously queued reads.

Hitting either phase budget remembers that work may remain and starts the next
turn without waiting for another eventfd edge. This matters because eventfd has
already been drained even when input/CQ entries remain. A ready CQ also prevents
waiting. A partial successful submission continues immediately while queued SQ
work remains; failed or zero-progress submissions alone do not prevent waiting.
The existing active/idle heartbeats and two bounded submission attempts remain.
An exact budget boundary with no actual remaining work costs one empty turn,
after which normal idle waiting resumes.

Single-read producer notifications are not coalesced; explicit bounded groups
can share notifications through [read_at_batch](batch-reads.md). No async-only
eventfd registration is introduced. Messages arriving after the driver's queue
check retain their eventfd notification; work
left because of a budget retains the explicit continuation flag. Buffers, FDs
and permits retain the existing final-CQE ownership rules.

## Validation

```sh
cargo test --all-features --lib loop_budget_tests -- --nocapture
```

Tests exercise a real eventfd plus an input queue whose wake signal is drained
before the intake limit, allocation fairness including an oversized read,
completion budget continuation, idle boundaries and submission retry decisions.
Submission-error/partial-submit decisions and CQ batches are deterministic
models, not injected kernel failures. Native read/cancellation/fault suites
remain necessary integration coverage. Throughput and tail-latency changes
require isolated before/after measurements and are currently unmeasured.
