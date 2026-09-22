# RustFS application integration prerequisites

Tracking: [rustfs/backlog#2647](https://github.com/rustfs/backlog/issues/2647),
steps 4.2 and 5.2. This is a source-backed implementation plan, **not completed
application integration**. The reviewed RustFS main revision is
[`1880b42`](https://github.com/rustfs/rustfs/tree/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad)
(2026-09-23). Recheck the application head before implementation.

## Current production boundary

The application's [ecstore dependency](https://github.com/rustfs/rustfs/blob/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad/crates/ecstore/Cargo.toml#L230)
still uses registry `rustfs-uring = "0.2.2"`; it does not yet consume the new
`ReadLimits`, batch or shard-policy APIs. The [local backend](https://github.com/rustfs/rustfs/blob/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad/crates/ecstore/src/disk/local.rs#L1087)
keeps io_uring opt-in, a depth of 128 per shard and a 128 MiB read chunk cap.
Configured shard counts multiply across disks; driver-local limits are not a
machine-wide budget.

| Concern | Verified current behavior | Required integration |
| --- | --- | --- |
| Initialization | `LocalDisk::new` is async but calls synchronous probe/start | Bounded initialization offload plus explicit resource ownership |
| Teardown | Backend Drop already uses a blocking worker | Preserve it; account for retiring instances until real completion |
| Byte limits | Application has no `ReadLimits` wiring | Coordinate chunks, direct padding, per-driver allocation and fallback |
| Buffered descriptors | Existing FD cache with generation fencing | Reuse; do not replace invalidation with TTL-only behavior |
| Direct descriptors | Open/stat on each read; alignment is cached | Separate direct entries with all invalidation paths covered |
| Reclaim | Synchronous fadvise inside async read paths | Measure first; any offload must retain error and ordering semantics |

Relevant paths: [backend lifecycle](https://github.com/rustfs/rustfs/blob/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad/crates/ecstore/src/disk/local.rs#L4186),
[async construction](https://github.com/rustfs/rustfs/blob/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad/crates/ecstore/src/disk/local.rs#L5291),
[direct reads](https://github.com/rustfs/rustfs/blob/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad/crates/ecstore/src/disk/local.rs#L4561).

## Required implementation order

1. **Deliver the dependency and wire resource-aware chunks.** Use a released or
   otherwise explicitly pinned uring version containing the new APIs. Validate
   logical read size, actual aligned allocation charge and byte quota together;
   the existing 128 MiB chunk cannot remain unconditional under a smaller quota.
   Audit the [fallback path](https://github.com/rustfs/rustfs/blob/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad/crates/ecstore/src/disk/local.rs#L4676):
   it currently retries any uring error through the std backend. Merely setting
   a smaller driver limit would cause fallback, not enforce application-wide
   memory admission. An application-level budget must also cover fallback and
   retained results; changing fallback/error classification needs its own tests.
2. **Budget and offload initialization.** Bound startup/reconnect concurrency
   before scheduling blocking probe work. Account driver slots, rings and OS
   threads across all disks, including retiring/leaked instances. A timeout
   stops waiting but cannot kill an already running blocking syscall. Do not
   immediately release its capacity and admit unlimited replacement probes.
   Current `ReadLimits` covers one driver only: use conservatively allocated
   per-driver quotas or design an explicitly shared application admission layer.
   The existing best-effort per-ring io-wq setting is not a process thread cap.
   The library's [shared reservation pool](shared-read-budget.md) offers the
   conservative whole-driver quota option: pass the same pool across reconnects
   and generations; leaked pending reads keep the entire reservation. It does
   not wire the application's dependency, std fallback, result assembly, or
   retained results into any budget. A temporary `WouldBlock` reservation error
   must not enter the permanent unsupported-disk cache.
3. **Fix invalidation before adding direct cache hits.**
   [Exact invalidation](https://github.com/rustfs/rustfs/blob/1880b42169bf26b15d8ca6bea3d8f1e4203b1bad/crates/ecstore/src/disk/local.rs#L3991)
   currently builds a buffered-only key; invalidate both variants before
   inserting direct entries. Preserve generation fencing, inode/length and
   permission checks, alignment, FD capacity, prefix/volume invalidation and
   open-to-insert race protection. Do not reuse a buffered descriptor as direct.
   Specify permission freshness explicitly: today's direct path opens/checks on
   every read, whereas buffered cache hits may skip some checks within the TTL.
   Adopting its cache policy must not silently weaken the direct-read contract.
4. **Measure reclaim and validate the real backend.** If fadvise offload is
   justified, use bounded blocking work and await its result; fire-and-forget
   changes both errors and page-cache timing. Compare real LocalIoBackend cache
   hit/miss, buffered/direct and fallback behavior after correctness gates,
   rather than extrapolating from this crate's read microbenchmark.

## Acceptance checklist (not yet completed)

- [ ] Current application dependency contains the intended reviewed driver code.
- [ ] Below-quota/at-quota/over-quota and direct-padding cases; zero accidental
  budget bypass through fallback; returned-result residency explicitly covered.
- [ ] Many-disk startup, reconnect storms, retiring instances, failed/timed-out
  probes and shutdown retain correct capacity accounting and task responsiveness.
- [ ] Direct/buffered exact invalidation, heal same-path replacement, both rename
  endpoints, delete, volume removal, open/insert races and permission failures.
- [ ] Real direct I/O executes; unavailable capability is not mistaken for a
  passing direct test. Existing bitrot, EOF/range and error semantics preserved.
- [ ] Stable resource-isolated calibration precedes before/after comparisons;
  CPU, tail latency, descriptors and memory remain within agreed bounds.

The ordered-prefetch example in this repository cannot satisfy application
bitrot, quorum cancellation, stall policy or end-to-end stream acceptance.
Owned-buffer pooling and advanced ring modes remain evidence-gated separately.
