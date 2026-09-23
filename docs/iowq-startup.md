# Io-wq registration at driver startup

Each io_uring shard makes one best-effort `IORING_REGISTER_IOWQ_MAX_WORKERS`
request after the ring and its completion eventfd are ready. The request remains
`[16, 0]`: sixteen bounded workers per NUMA node, and zero to leave the
unbounded-worker limit unchanged. A kernel that rejects this registration still
allows the driver to start and serve reads.

`UringDriver::shard_iowq_setup()` returns one immutable `IoWqSetup` per shard in
shard order. It copies startup records and does not call the kernel or add work
to the read path. Every record contains the requested values and an
`IoWqRegistration` result:

- `Registered { previous_limits }` means the request succeeded. The
  `io-uring` registration call overwrites its input array with the limits from
  **before** this request. These values are historical; they are not a query of
  the resulting limit or active worker count.
- `Failed { error_kind, raw_os_error }` records why registration failed. Some
  errors have no errno. A failed best-effort setting is distinct from the
  real-read startup probe and does not automatically disable io_uring or poison
  an application's unsupported-disk cache.

This snapshot can explain whether an attempted bounded-worker setting was
accepted on a particular ring. It does not establish a process-wide thread cap,
current kernel worker count, per-disk CPU share, or performance benefit. Shards
and other applications may share kernel io-wq resources; the effective limit
can also differ from the requested value. Operational validation must combine
this startup record with runtime process/kernel observations.

Unit tests inject success with a rewritten previous-limit array and failure
with and without errno. A native test creates two shards, reads byte-exact data,
and checks that their startup records remain stable. It accepts either
registration outcome because kernel support and policy vary; it does not force
an io-wq worker to run. See [acceptance status](optimization-status.md) for the
latest CI result.
