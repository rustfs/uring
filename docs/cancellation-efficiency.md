# Cancellation and eventfd efficiency

Tracking: [rustfs/backlog#2647](https://github.com/rustfs/backlog/issues/2647), step 3 (partial).

## Implemented scope

- [x] Retry interrupted eventfd signal/drain syscalls. Treat `EAGAIN` as an
  already-pending wake on write, or an already-empty counter on read. Log an
  unexpected error at most once per eventfd; the existing heartbeat remains.
- [x] Drain one successful counter read per turn. A concurrent later signal
  stays readable; intake and completion processing still run after draining.
- [x] Record explicit drop-cancel intent separately from receiver closure.
  After the read CQE arrives, stop positioned short-read and transient-error
  continuations on explicit cancellation or shutdown with `ECANCELED`.
- [x] Preserve successful complete reads, EOF results, and `read_current`
  short-read semantics when they race with cancellation.
- [x] Preserve `without_cancel_on_drop`: closing the receiver alone does not
  stop positioned continuation. At the terminal completion, a closed receiver
  skips result memmove/truncation/materialization, including direct-I/O padding.
- [x] Keep fd, buffer, and permit ownership until read completion and pending
  removal. AsyncCancel CQEs still only update cancel statistics.
- [ ] Intake/reap budgets and wake coalescing (separate liveness work).
- [ ] Native Linux execution of the new tests and the full regression suite.
- [ ] Isolated before/after benchmark evidence; no measured speedup is claimed.

## Review and validation

The completion decision now lives in `reap_read` so deterministic tests can
provide short reads and transient errnos without regular-file timing races.
Tests cover explicit cancellation, shutdown, abandoned receivers without
cancellation, stream semantics, successful cancel races, direct result ranges,
unchanged orphan allocations, and resource ownership at terminal decisions.
Eventfd tests cover interrupted retries, saturated counters, empty drains, and
unexpected errors. They do not depend on io_uring availability or accept a skip.

Local `cargo fmt --all --check`, `git diff --check`, and Linux-target
`cargo check` / `cargo clippy` with all targets and features passed. Cross-checks
compile the Linux-only tests but do not execute them. Native execution remains
an integration gate for this batch.

Review confirms that no buffer is reclaimed at a cancel CQE or merely because
the receiver closed. The optimization only observes cancellation at a read
completion, with no live SQE writing into that buffer. A receiver closing after
the final `is_closed` check may still incur a copy; this is an accepted race,
not an ownership or correctness failure. Current direct-I/O `fstat` handling
is unchanged and is tracked by the separate integrity work.
