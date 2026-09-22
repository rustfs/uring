# Explicit buffered read batches

`UringDriver::read_at_batch(Vec<ReadRequest>)` accepts up to `MAX_BATCH_READS`
(64) buffered positioned reads and returns one `ReadHandle` per input, in input
order. More than 64 requests fails before any submission; an empty batch sends
no wakeup. Invalid individual offsets or lengths become normal asynchronous
handle errors and do not reject other requests.

Eagerly admitted reads share one eventfd notification per distinct final owning
shard after handle construction. The capacity-aware policy, when selected, can
route several inputs to the same owner, which still receives one notification.
Requests awaiting count or byte admission signal their owner individually when
polled and admitted. No tasks are spawned and existing single-read, direct-read
and stream APIs retain their notification behavior.

This is notification batching, not an atomic multi-read operation or snapshot.
Kernel submission/completion order may differ from input order; each handle
keeps independent cancellation and result ownership. Construction performs at
most 64 submissions and at most 64-by-64 pointer identity comparisons for wake
deduplication. It does not await capacity. If construction unwinds, already built
handles are dropped, queue their normal cancels, and wake the owning shards.
Driver-owned buffers, FDs and admission permits still survive until final CQE.

Unit tests count real eventfd notifications using threadless driver queues, and
cover rejection, mixed valid/invalid requests, final-owner routing, deferred byte
admission, dropping handles and interrupted construction. Native integration
tests verify byte-exact results and cancellation/drain conservation. Threadless
tests model CQE resource release without submitting to a kernel. Notification
reduction is verified deterministically; throughput and latency are unmeasured.

```sh
cargo test --all-features --lib batch_read_tests -- --nocapture
cargo test --all-features --test cancel batch_ -- --nocapture
```
