# Ordered buffered prefetch: correctness experiment

This example explores step 6 of [the optimization roadmap](optimization-status.md).
It does not add a public reader API, change production defaults, integrate an
object stream, or establish a performance improvement. Keep it as an example
and test harness until the consumer contract and performance gates are accepted.

## Run against an existing immutable fixture

```sh
cargo run --example ordered_prefetch -- FILE OFFSET LENGTH CHUNK WINDOW MAX_BYTES
```

All geometry arguments are decimal integers. `CHUNK` is 1 byte through 8 MiB,
`WINDOW` is 1 through 64, and `MAX_BYTES` is between `CHUNK` and 512 MiB.
`OFFSET + LENGTH` must fit `i64::MAX`. Zero length is supported. The file must
already exist and be regular; symlinks are refused. The example never creates,
truncates or modifies the fixture. Keep it immutable during verification:
independent positioned reads do not provide snapshot isolation.

The example compares every returned chunk, including a short final chunk,
against a synchronous positioned std read outside the async executor. It checks
contiguous output, premature EOF and final driver drain conservation. Successful
execution prints `ORDERED_PREFETCH_OK bytes=N chunks=N`. Invalid input, unavailable
io_uring, read/reference mismatch or incomplete drain returns a nonzero status
without that marker. Non-Linux execution also fails explicitly.

The driver uses one shard, two count permits and a one-chunk byte budget to
exercise deferred admission; the consumer window uses the supplied geometry.
This configuration and the reference reads are deliberately for correctness,
not a performance comparison. No new dependency or per-chunk task is introduced.

## State and backpressure contract

The example-local `OrderedReader::new(Config, source)` stores ordered slots of
`Pending(future)` or `Ready(result)`. The source must implement whole requested
range-or-EOF positioned reads, as `UringDriver::read_at` does. Arbitrary stream
short reads must not be substituted: the reader treats a successful short range
as EOF. Sources must return no more bytes than requested; excess output becomes
`InvalidData`. The generic source exists for deterministic tests, not as a
published production abstraction.

`next(&mut self)` fills only available slots and logical bytes at the start of
a consumer poll, then polls all active slots, retaining out-of-order results.
It returns only the front slot and never refills after yielding that chunk.
No consumer polls means no replacement submissions. The constructor submits
nothing. A short result or error stops future scheduling and drops later slots;
earlier results remain ordered. A nonempty EOF prefix is yielded once, then
`None`; an empty EOF returns `None`; an ordered error is returned once, then
the reader is fused at `None`.

Slots remain inside the reader across `Poll::Pending`. Dropping a pending
`next()` future neither advances the delivered offset nor drops the front
handle. Dropping the reader drops all its handles, retaining their normal
driver cancellation and final-CQE ownership rules. Earlier speculative reads
may already have completed, and cancellation is not guaranteed to win that race.

Both pending and ready slots reserve their original requested logical length
until removal. Thus their combined requested lengths never exceed `MAX_BYTES`,
and their combined count never exceeds `WINDOW`. This is not an RSS or
whole-process memory bound: it excludes allocator overhead/excess Vec capacity,
driver structures, the example's reference buffer and already returned chunks
retained by the caller. Dropped kernel-visible buffers can also remain in the
driver until CQE; dropping a slot does not free them early. A terminal boundary
stops replenishment after dropping its speculative tail.

## Admission and liveness

Polling only the front is safe from this particular admission cycle only when
later handles have never been polled: eager later reads progress on the driver
and release permits at CQE, independently of consuming their results. Such a
simple reader can lose prefetch depth after saturation because deferred tails
remain unpolled.

Once a deferred tail has been polled, switching to front-only polling can
deadlock an active consumer. For example, the head waits for a count permit on
shard A while a tail on shard B waits for shared bytes. Tokio may assign newly
released bytes to the tail before A's count becomes available. The head then
waits for bytes reserved by the tail; the tail must be repolled to submit and
eventually release them. The state machine polls every remaining active slot
on each consumer poll, including tails when the head is pending. Ready futures
are never polled again. Work per poll is bounded by the 64-slot limit; there is
no wait/spin loop or background producer.

This does not promise global admission fairness while a consumer is paused.
Already-polled deferred handles can retain partial or newly assigned permits
until the consumer polls again or drops the reader. Application integration
must define stall timeouts and drop the reader when abandoning a stalled
consumer; merely dropping a pending `next()` preserves the reader and its
reservations intentionally. Paused consumer tests prove no refill, not continued
background permit release or unlimited progress by unrelated readers.

## Deterministic coverage and remaining gates

```sh
cargo test --test ordered_prefetch
```

The portable tests use controlled futures and actual Tokio count/byte semaphores
for the deferred-tail reservation race. They cover out-of-order completions,
ready-result byte accounting, slow-consumer refill boundaries, cancellation of
`next`, whole-reader drop, early/empty EOF, ordered errors, final range geometry,
invalid/overflow inputs and an oversized source result. These are state-machine
and admission tests, not native kernel stall or filesystem truncation injection.
Run the example on unrestricted Linux for actual io_uring byte correctness.

Before any public API or RustFS integration, define whole-range versus exact
object-length EOF policy, stall timeout, quorum abandonment, error mapping and
file-generation consistency. Bitrot verification and checksum framing belong to
the application: raw byte equality in this example does not implement bitrot,
and arbitrary chunk boundaries cannot be assumed to match its verification
units. Consumer-retained results need a separate ownership/budget policy.

Only after stable calibration should a measurement harness compare a sequential
buffered/readahead baseline and small ordered windows with matched consumer
backpressure, provenance and ABBA drift gates. Measure TTFB, p99/stall behavior,
throughput, CPU, syscall counts and memory residency. The existing unordered
JoinSet benchmark is not evidence for ordered-consumer behavior. Actual S3
streaming, bitrot/quorum behavior, performance acceptance and production enablement
remain unimplemented/unverified by this experiment.
