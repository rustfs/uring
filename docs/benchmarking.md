# Read-backend benchmarks

The examples measure I/O mechanisms, not a complete RustFS `LocalIoBackend` or
S3 request. In particular, the std pread strategies do not model every mmap,
metadata, cache, or reclaim policy used by an application. The streaming example
measures a task-per-chunk completion pipeline, not ordered delivery to a slow
consumer. Compare application backends separately before making rollout choices.

## Output contract

Each example prints its CSV header with `--header`. Sweep scripts obtain that
header from the executable rather than maintaining a second copy. Schema version
2 adds `schema_version`, `mode`, explicit setup/teardown times, and configuration
fields. Existing positional CSV consumers must migrate; do not parse v2 rows
using the previous header. `mode=verify` is a correctness run and must never be
included in performance results. Sweeps reject verification rows.

`secs` measures the workload after preparation and optional warmup, before
driver/runtime teardown. `startup_secs` includes runtime/driver creation, probe,
opening reusable descriptors, generating offsets, and allocating reusable std
buffers. Dataset creation happens before all reported intervals.
`shutdown_secs` includes dropping these prepared resources and the runtime.
Warmup time is excluded from all three fields.

Task creation/joining, per-operation allocations, error/length checks, result
collection, and buffer disposal remain inside the workload interval. Reported
operation latencies end at the result, before content verification or buffer
drop; total throughput measures the whole loop. Verification changes workload
cost even though its byte checks happen outside an individual latency sample.

The std streaming strategies reuse a single buffer. io_uring allocates one per
read. These results compare the current implementations including that
difference; they do not isolate syscall overhead or prove buffer-pool benefits.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `BENCH_WORKERS` | Available parallelism | Tokio workers, 1–1024 |
| `BENCH_RING_ENTRIES` | 128 | Power-of-two SQ depth per shard, 1–32768 |
| `BENCH_WARMUP_OPS` | 0 | Concurrent benchmark operations before measurement |
| `BENCH_WARMUP_RUNS` | 0 | Full streaming passes before measurement |
| `BENCH_VERIFY` | Off | `1` or `true` enables byte-exact verification |
| `SHARD_COUNTS` | 1 | Space-separated shard counts for the concurrent sweep |

Ring depth is independent of concurrency so saturation can be exercised.
Concurrent positional arguments still accept an optional shard count (1–64).
Std rows report zero ring entries/shards; std streaming also reports zero Tokio
workers because it uses no runtime. `workers` in concurrent std rows remains
nonzero because the tasks dispatch work through Tokio's blocking pool.

Warmup defaults to zero to avoid relabeling warmed data as cold. Cold sweep
legs require zero warmup. A cache drop describes the initial state only: reads
can repopulate the cache during a run. A warm preload alone does not prove that
the whole working set remains resident. Never clear global page caches on a
shared or production host.

## Correctness and smoke validation

On Linux with real io_uring and an O_DIRECT-capable test filesystem:

```sh
bash scripts/test-benchmark-cli.sh
URING_REQUIRE_DIRECT=1 cargo test --locked --test cancel \
  direct_read_returns_exact_unaligned_ranges -- --nocapture --test-threads=1
```

The smoke checks all eight strategies, schema/field alignment, warmup counts,
small-ring saturation, invalid configuration, and byte-exact unaligned ranges.
It is intentionally small and is not a performance result. The direct test
prints `DIRECT_OK direct_read_returns_exact_unaligned_ranges` only after its
native direct assertions execute. Without `URING_REQUIRE_DIRECT=1`, unsupported
filesystems may still omit the direct assertions; a passing general test suite
alone is not proof of O_DIRECT coverage.

## Performance acceptance

Use release builds and record source revisions, runtime configuration, filesystem,
cache policy, resource allocation, and actual backend execution. Keep the total
operation/byte budget constant when comparing shard counts. Use separate startup
and steady-state results. Do not compare old whole-process timings to v2 `secs`.

The existing sweeps are exploratory parameter sweeps, not an ABBA acceptance
harness. Run A1/B1/B2/A2 on isolated target hardware, reject baseline drift, and
measure throughput, CPU/op, tail latency, RSS, fallback/errors, and scheduler
signals. The default 512–4096 operations are smoke-sized: p99.9 is not robust
with that sample count. Increase the population and report uncertainty. Closed
loop concurrency also hides overload queueing; evaluate a controlled arrival
rate separately when studying tail latency.

Tracking and implementation status: [rustfs/backlog#2647](https://github.com/rustfs/backlog/issues/2647).
