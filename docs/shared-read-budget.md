# Shared driver read-budget reservations

`SharedReadBudget` is an opt-in pool for **whole-driver reservations**. It bounds
the sum of configured in-flight read-buffer quotas across drivers using the same
pool. It is not a dynamic shared per-read byte queue, returned-result limit, or
process RSS limit.

## Admission

Create a pool with `SharedReadBudget::new(total)` and pass a reference to
`UringDriver::probe_and_start_with_shared_budget(entries, shards, limits, &pool)`.
Cloning the pool shares its capacity; constructing a separate pool does not.
The new constructor requires an explicit `limits.max_in_flight_bytes = Some(B)`.
It validates the local limit and reserves all `B` bytes before ring startup.

Zero total capacity, missing/invalid local byte limits, or `B > total` are
`InvalidInput`. Insufficient currently available capacity returns `WouldBlock`
without starting a driver; it is not an environment restriction and must not
poison a disk's unsupported-io_uring cache. Admission does not wait, queue a
blocking task, or partially reserve a driver. The caller decides when to retry
or use a separately budgeted fallback. Budgets use `usize`; whole-driver
reservations must not truncate through a per-read `u32` permit count.

`capacity()` returns the fixed pool size. `available()` is an advisory snapshot
of capacity not reserved by drivers or their surviving resource owners. Do not
check it and then assume creation must succeed; another constructor can reserve
the same capacity before this caller's atomic reservation.

Once admitted, a driver keeps the existing local count and byte semaphores.
Buffered reads charge logical length; direct reads charge the aligned superset
plus allocation padding. Read admission does not acquire from the global pool.
Each driver therefore retains its full reservation even while idle, and cannot
borrow unused capacity already reserved by another driver. This deliberately
trades utilization for a smaller shutdown/cancellation contract.

## Ownership and retirement

The same reservation receipt follows driver-owned and deferred read resources
through queued messages and pending kernel operations. Canceling the caller does
not refund a reservation while the kernel can still access a buffer. Normal
terminal CQEs release their owners; the whole block returns only after its last
owner disappears. A deferred handle retained after driver shutdown can therefore
keep the entire block reserved until its ownership is released. Drop unused
handles during retirement instead of treating `is_finished()` or a clean
snapshot as a pool-refund signal.

A bounded-drain escape retains the receipt with the leaked pending resources,
so the entire `B` stays reserved, even if the leaked read is much smaller. A
replacement driver cannot reuse that capacity. There is no forced refund API.
Creating a new independent pool to bypass retained reservations also bypasses
the aggregate bound; the application must keep a stable shared pool across
reconnects and generations.

Closing one driver closes only its local admission and wakes its local waiters.
It does not close the pool or reject reads on another admitted driver. Startup
failures roll back the reservation after resource cleanup; existing probe/setup
failure and synchronous cleanup contracts still apply.

## Scope and cost

This accounts configured driver read-buffer capacity, including direct padding
and retained pending reads. It excludes probe buffers, rings, kernel/io-wq
resources, allocator overhead, handle metadata, completion/result copies,
caller-held results and application std fallback allocations. Startup concurrency
and these other resources require separate controls. It does not constrain
drivers created without this pool.

Pool atomics run at reservation/final release and explicit observation. Opt-in
reads retain a shared receipt; this is not a performance improvement claim.
Existing constructors and default limits keep their behavior. No application
dependency source or production pool wiring is changed by this library API.

## Verification boundaries

Deterministic tests cover concurrent weighted reservations, `usize` arithmetic,
receipt ownership in eager messages and deferred handles, and independent local
closure. Native tests cover multi-driver admission and retirement, retained
results, deferred count/byte cancellation, and a subprocess-injected leak that
keeps the entire quota. Restricted-environment tests also check that failed setup
refunds its reservation before reporting a capability skip.

Failure after one or more earlier shards have started is covered by the existing
RAII cleanup path and code review, not a new later-shard fault-injection test.
The simulated stuck completion does not prove recovery from a real hung syscall.
See [acceptance status](optimization-status.md) for execution evidence; compiled
tests alone are not native I/O coverage.

See [read admission](../README.md#read-allocation-admission),
[shutdown ownership](shutdown.md), and [application integration](rustfs-integration.md).
