# Shutdown ownership and runtime integration

`request_shutdown`, `is_finished`, `shutdown`, and the optional `shutdown_async`
have different contracts. None can kill a blocked kernel syscall or promise a
hard cleanup deadline.

## Request, observe, join

`request_shutdown(&self)` closes every shard's count admission and the shared
byte admission, if configured, before returning. It wakes admission waiters and
sends each driver thread a shutdown request. It does not join or wait for reads.
Repeated and concurrent calls are safe; they can send repeated notifications.
Call this at lifecycle transitions, not in a polling loop.

Valid reads, including zero-length reads, started after the request cannot gain
admission. Invalid inputs retain their validation errors. A read racing the
request may already hold admission: shutdown still
owns and settles its queued or submitted state. Neither admission closure nor
dropping a read handle releases a kernel-owned buffer early. Submitted buffers,
file descriptors and permits remain owned until the terminal read CQE, or are
retained by the leak-over-use-after-free escape path.

`is_finished()` is an advisory query over the driver thread join handles. It is
not a join, a synchronization barrier, or proof of a clean drain. It can become
true just before final thread teardown, and can be true with nonzero
`stats().in_flight` after a bounded-drain leak. Do not return process-wide memory
credits merely because it is true.

`shutdown(self)` requests all shards to stop, then synchronously joins them and
returns the final snapshot. `Drop` uses the same synchronous cleanup ordering.
The bounded drain only advances while the driver loop runs: a syscall that does
not return can also prevent the join from returning. A returned snapshot with
nonzero `in_flight` records retained kernel-owned resources, not clean recovery.

## Optional Tokio adapter

Enable `features = ["tokio-runtime"]` for `shutdown_async(self)`. Default builds
still require only Tokio synchronization support; read handles can be driven by
other executors. The adapter does not move each read onto Tokio's blocking pool.

Call the method from an entered, live Tokio runtime. At **method call time**, it
requests shutdown and moves the driver into one blocking-pool task that performs
the consuming synchronous shutdown. This is deliberately an ordinary function
returning a `Send` future, not an `async fn` whose body starts at first poll.

```rust
// `driver` is an exclusively owned UringDriver; tokio-runtime is enabled.
let shutdown = driver.shutdown_async(); // ownership transferred here
let stats = shutdown.await?;
if stats.in_flight != 0 {
    // Report degraded cleanup and retain any external memory reservation.
}
```

Dropping or timing out the returned future, even before its first poll, detaches
the join handle during normal runtime operation; cleanup continues. It does not
abort a started blocking task or recover leaked buffers. Keep the runtime alive
until cleanup completes if that completion is required. Concurrent driver
retirement still needs application-level limits: the runtime's blocking queue
is not an admission budget.

Calling without an entered runtime panics and drops the driver through its
synchronous cleanup path. A runtime shutting down may reject or discard queued
blocking work and synchronously drop its captured driver. Thus the adapter does
not make runtime teardown, panic unwinding, or arbitrary `Drop` nonblocking.
An `Ok` snapshot means the blocking task joined successfully, not necessarily
that `in_flight` is zero. A join error is returned as an `io::Error`.

## Validation boundaries

Private mock-thread tests exercise immediate count/byte closure, concurrent
requests, advisory completion, and eager ownership handoff with an occupied
blocking pool. Native tests exercise pending pipe reads, registered count/byte
waiters, current-thread Tokio cleanup, and dropped unpolled shutdown futures.
An isolated fault-injection subprocess verifies that finished threads can retain
nonzero in-flight resources. These are correctness tests, not performance or
real hung-device recovery evidence.

See [completion recovery](fault-recovery.md), [integration prerequisites](rustfs-integration.md),
and [acceptance status](optimization-status.md) for the remaining gates.
