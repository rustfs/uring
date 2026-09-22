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
| `BENCH_DIAGNOSTICS` | Off | `1` reports sampled driver-stage histograms to stderr; requires the `diagnostics` feature |
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

### Sampled diagnostics

Build with `--features diagnostics` to collect one sample per 64 handle
constructions **per shard**, starting with each shard's first handle. Sampling
has a separate sequence per shard so round-robin selection cannot bias every
sample toward shard zero. Invalid requests can consume a sampling position
without recording stages. Deterministic sampling is diagnostic, not an unbiased
estimate for every possible periodic workload.

`UringDriver::diagnostics()` aggregates shards;
`UringDriver::shard_diagnostics()` preserves shard identity. Each stage has a
count, total nanoseconds, and 64 log2 nanosecond buckets. Bucket zero covers
0–1 ns; bucket i > 0 covers `[2^i, 2^(i+1))`. Counts and sums wrap modulo 2^64.
Snapshots are approximate during concurrent updates; take quiescent snapshots
and use `since()` on the same driver to exclude warmup without resetting it.

| Stage | Boundary |
| --- | --- |
| `admission` | Handle timing starts to permit acquisition; includes an unpolled saturated handle's inactivity |
| `driver_queue` | Immediately before send through driver intake |
| `preparation` | Intake through initial local SQE backlog insertion |
| `driver_lifetime` | Intake through final read CQE reap; includes preparation, retries and reaper delay |
| `cqe_processing` | Each read CQE's handling and range adjustment, before removal/send |
| `completion_to_poll` | Immediately before result send through caller ready poll, including receiver inactivity |

These stages overlap; do not sum them or label lifetime as disk latency or
completion-to-poll as Tokio schedule latency. Short reads can generate several
CQE samples. Cancel CQEs themselves are excluded. Abandoned receivers do not
create completion-to-poll samples. Rejection, driver disappearance, and the
bounded-drain leak path can leave some stages unrecorded; do not infer a terminal
CQE from an error delivered by shutdown.

`BENCH_DIAGNOSTICS=1` makes the examples report **measurement-interval deltas**
to stderr, outside their measured interval. Sweep scripts build with the feature
automatically when this variable is 1. In a manually built binary the variable
does not enable/disable instrumentation; it controls reporting only. CSV
`diagnostics_interval` is 64 whenever the feature was compiled, otherwise zero,
even on std strategies (which do not use the instrumented driver).

The default build compiles out timing fields, clock reads, histogram storage and
sample allocations. Enabled builds add a per-shard atomic sampling counter per
handle and an Arc/timestamps/histogram updates for sampled operations. Measure
that overhead on target hardware with separate feature-off/feature-on artifacts;
do not assume it is free. Runtime schedule-latency and blocking-pool metrics must
still be correlated in the application, which owns the Tokio runtime.

### Target-hardware acceptance

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

### ABBA driver runner

`scripts/bench-abba.py` (Python 3.11+, Linux, GNU time, taskset) runs three or
more A1/B1/B2/A2 rounds against a **pre-created, byte-verified** warm-cache
dataset. Build two release artifacts from the same source with diagnostics off
and on, using separate target directories. Supply explicit binaries, source
revision, CPU affinity, and a reservation description. The tool never stops
services, changes their configuration, clears global caches, or overwrites an
existing result directory.

```sh
python3 scripts/bench-abba.py \
  --baseline /test/target-off/release/examples/concurrent_pread_bench \
  --candidate /test/target-on/release/examples/concurrent_pread_bench \
  --data-file /test/verified-data.bin --run-dir /test/results/new-run \
  --source-revision COMMIT --reservation-note 'reserved benchmark window' \
  --cpus 0-7 --workers 4 --shards 2 --entries 64 \
  --ops 1000000 --warmup-ops 10000 --rounds 3 --dry-run
```

Remove `--dry-run` only after reserving resources. If a CI runner or another
service can introduce load, supply `--require-inactive-unit UNIT`; the runner
refuses to start or continue unless that unit is inactive. Any service stop or
restore is an explicit operator action outside this tool. A reservation note and
process checks are evidence aids, not a substitute for exclusive resources.
Perform A/A calibration first by passing the baseline binary in both positions
and `--candidate-interval 0`; then use the diagnostics candidate and the default
interval of 64. Keep the workload and thresholds fixed between experiments.

Every leg must run at least five measured seconds by default. If it is too
short, increase operations in a **new** experiment. The tool validates geometry,
feature state, sample count, schema, finite values, derived throughput, process
resource reports, binary identity and dataset metadata. Active build/load/CI
workers, errors, per-leg deadlines, or failed baseline drift invalidate the run.
It stops after the first invalid round rather than expanding the matrix.

Output contains provenance, each leg's CSV/stderr and whole-process CPU/RSS/
context-switch report, parsed leg JSON, and a summary. `valid-comparison` means
the evidence passed these gates, **not** that the candidate improved, passed an
overhead budget, or proved a RustFS/S3 benefit. Review signed candidate changes
and resource reports against the application SLO separately. Duration histograms
with too few sampled operations are not reliable tail estimates.

Runner gate tests: `python3 -m unittest discover -s scripts -p 'test_bench_abba.py'`.

Tracking and implementation status: [rustfs/backlog#2647](https://github.com/rustfs/backlog/issues/2647).
