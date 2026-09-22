// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// One in every 64 handle constructions per shard is sampled when `diagnostics`
/// is enabled, starting with that shard's first handle.
pub const DIAGNOSTICS_SAMPLE_INTERVAL: u64 = 64;

/// Cumulative sampled duration histogram. Concurrent snapshots are approximate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencyHistogram {
    /// Number of recorded durations, including error outcomes reaching this stage.
    pub count: u64,
    /// Sum of recorded durations in nanoseconds; not an average or percentile.
    pub total_nanos: u64,
    /// Log2 nanosecond buckets: bucket 0 holds 0..=1 ns, bucket i > 0 holds
    /// `[2^i, 2^(i+1))` ns. Bucket 63 includes all larger durations.
    pub buckets: [u64; 64],
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            count: 0,
            total_nanos: 0,
            buckets: [0; 64],
        }
    }
}

impl LatencyHistogram {
    fn merge(&mut self, other: &Self) {
        self.count = self.count.wrapping_add(other.count);
        self.total_nanos = self.total_nanos.wrapping_add(other.total_nanos);
        for (value, addition) in self.buckets.iter_mut().zip(other.buckets) {
            *value = value.wrapping_add(addition);
        }
    }

    fn since(&self, earlier: &Self) -> Self {
        let mut delta = Self {
            count: self.count.wrapping_sub(earlier.count),
            total_nanos: self.total_nanos.wrapping_sub(earlier.total_nanos),
            ..Self::default()
        };
        for ((out, current), previous) in delta.buckets.iter_mut().zip(self.buckets).zip(earlier.buckets) {
            *out = current.wrapping_sub(previous);
        }
        delta
    }
}

/// Sampled driver-stage durations, with no object/path labels or exporter locks.
///
/// Counts can differ: cancellation can stop a request before admission or after
/// completion, and short reads can produce several CQEs per logical operation.
/// The stages overlap as documented and must not be blindly summed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticsSnapshot {
    /// Sample creation during handle construction to permit acquisition, including time before a
    /// saturated handle's first poll. Not solely semaphore queue time.
    pub admission: LatencyHistogram,
    /// Just before message send to driver intake, including channel send cost.
    pub driver_queue: LatencyHistogram,
    /// Driver intake to placing the initial SQE in the local backlog.
    pub preparation: LatencyHistogram,
    /// Driver intake to reaping the final read CQE, including preparation,
    /// backlog, retries, kernel/device time and reaper delay. Not disk latency.
    pub driver_lifetime: LatencyHistogram,
    /// Processing time for each sampled read CQE, including range adjustment
    /// but excluding result send and pending removal. May overlap lifetime.
    pub cqe_processing: LatencyHistogram,
    /// Just before result send to the caller's ready poll. This includes
    /// receiver inactivity and is not a substitute for runtime schedule latency.
    pub completion_to_poll: LatencyHistogram,
}

impl DiagnosticsSnapshot {
    pub(crate) fn merge(&mut self, other: &Self) {
        self.admission.merge(&other.admission);
        self.driver_queue.merge(&other.driver_queue);
        self.preparation.merge(&other.preparation);
        self.driver_lifetime.merge(&other.driver_lifetime);
        self.cqe_processing.merge(&other.cqe_processing);
        self.completion_to_poll.merge(&other.completion_to_poll);
    }

    /// Subtract an earlier snapshot from the same driver without resetting
    /// counters. Prefer quiescent boundaries; concurrent snapshots need not
    /// satisfy cross-field conservation identities. Counters wrap modulo 2^64.
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            admission: self.admission.since(&earlier.admission),
            driver_queue: self.driver_queue.since(&earlier.driver_queue),
            preparation: self.preparation.since(&earlier.preparation),
            driver_lifetime: self.driver_lifetime.since(&earlier.driver_lifetime),
            cqe_processing: self.cqe_processing.since(&earlier.cqe_processing),
            completion_to_poll: self.completion_to_poll.since(&earlier.completion_to_poll),
        }
    }
}

#[derive(Clone, Copy)]
enum Stage {
    Admission,
    Queue,
    Prepare,
    Lifetime,
    Cqe,
    Resume,
}

struct Histogram {
    count: AtomicU64,
    total: AtomicU64,
    buckets: [AtomicU64; 64],
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            total: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Histogram {
    fn record(&self, nanos: u64) {
        let bucket = 63 - nanos.max(1).leading_zeros() as usize;
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencyHistogram {
        LatencyHistogram {
            count: self.count.load(Ordering::Relaxed),
            total_nanos: self.total.load(Ordering::Relaxed),
            buckets: std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
        }
    }
}

#[derive(Default)]
pub(crate) struct Diagnostics {
    stages: [Histogram; 6],
    requests: AtomicU64,
}

impl Diagnostics {
    fn record(&self, stage: Stage, nanos: u64) {
        self.stages[stage as usize].record(nanos);
    }

    pub(crate) fn snapshot(&self) -> DiagnosticsSnapshot {
        DiagnosticsSnapshot {
            admission: self.stages[Stage::Admission as usize].snapshot(),
            driver_queue: self.stages[Stage::Queue as usize].snapshot(),
            preparation: self.stages[Stage::Prepare as usize].snapshot(),
            driver_lifetime: self.stages[Stage::Lifetime as usize].snapshot(),
            cqe_processing: self.stages[Stage::Cqe as usize].snapshot(),
            completion_to_poll: self.stages[Stage::Resume as usize].snapshot(),
        }
    }
}

pub(crate) struct Trace {
    start: Instant,
    diagnostics: Arc<Diagnostics>,
    queued: AtomicU64,
    entered: AtomicU64,
    sent: AtomicU64,
}

impl Trace {
    pub(crate) fn sample(diagnostics: &Arc<Diagnostics>) -> Option<Arc<Self>> {
        // A global id modulo 64 would sample only shard zero for power-of-two
        // round-robin sharding. Each shard therefore owns its sampling sequence.
        let sequence = diagnostics.requests.fetch_add(1, Ordering::Relaxed);
        sequence.is_multiple_of(DIAGNOSTICS_SAMPLE_INTERVAL).then(|| {
            Arc::new(Self {
                start: Instant::now(),
                diagnostics: Arc::clone(diagnostics),
                queued: AtomicU64::new(0),
                entered: AtomicU64::new(0),
                sent: AtomicU64::new(0),
            })
        })
    }

    pub(crate) fn now(&self) -> u64 {
        // Reserve zero for "no result sent". Saturate only at ~584 years.
        (self.start.elapsed().as_nanos().min(u128::from(u64::MAX - 1)) as u64) + 1
    }

    pub(crate) fn enqueue(&self) {
        let now = self.now();
        self.queued.store(now, Ordering::Relaxed);
        self.diagnostics.record(Stage::Admission, now - 1);
    }

    pub(crate) fn enter(&self) {
        let now = self.now();
        self.entered.store(now, Ordering::Relaxed);
        self.diagnostics
            .record(Stage::Queue, now.saturating_sub(self.queued.load(Ordering::Relaxed)));
    }

    pub(crate) fn prepared(&self) {
        self.diagnostics
            .record(Stage::Prepare, self.now().saturating_sub(self.entered.load(Ordering::Relaxed)));
    }

    pub(crate) fn reaped(&self, started: u64, final_cqe: bool) {
        self.diagnostics.record(Stage::Cqe, self.now().saturating_sub(started));
        if final_cqe {
            self.diagnostics
                .record(Stage::Lifetime, started.saturating_sub(self.entered.load(Ordering::Relaxed)));
        }
    }

    pub(crate) fn sending(&self) {
        self.sent.store(self.now(), Ordering::Release);
    }

    pub(crate) fn received(&self) {
        let sent = self.sent.load(Ordering::Acquire);
        if sent != 0 {
            self.diagnostics.record(Stage::Resume, self.now().saturating_sub(sent));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_boundaries_and_delta_preserve_samples() {
        let histogram = Histogram::default();
        for nanos in [0, 1, 2, 3, 4, 7, 8] {
            histogram.record(nanos);
        }
        let before = histogram.snapshot();
        assert_eq!(&before.buckets[..4], &[2, 2, 2, 1]);
        assert_eq!(before.count, 7);
        assert_eq!(before.total_nanos, 25);
        histogram.record(16);
        let delta = histogram.snapshot().since(&before);
        assert_eq!(delta.count, 1);
        assert_eq!(delta.total_nanos, 16);
        assert_eq!(delta.buckets[4], 1);
        assert_eq!(delta.buckets.iter().sum::<u64>(), 1);
    }

    #[test]
    fn unsampled_handles_do_not_retain_trace_references() {
        let diagnostics = Arc::new(Diagnostics::default());
        let count = (0..128).filter(|_| Trace::sample(&diagnostics).is_some()).count();
        assert_eq!(count, 2);
        assert_eq!(Arc::strong_count(&diagnostics), 1);
    }

    #[test]
    fn unsent_results_do_not_record_receiver_latency() {
        let diagnostics = Arc::new(Diagnostics::default());
        let trace = Trace::sample(&diagnostics).expect("first handle is sampled");
        trace.received();
        assert_eq!(diagnostics.snapshot().completion_to_poll.count, 0);
    }

    #[test]
    fn largest_bucket_and_wrapping_delta_are_defined() {
        let histogram = Histogram::default();
        histogram.record(u64::MAX);
        let before = histogram.snapshot();
        assert_eq!(before.buckets[63], 1);
        histogram.record(2);
        let delta = histogram.snapshot().since(&before);
        assert_eq!(delta.count, 1);
        assert_eq!(delta.total_nanos, 2);
        assert_eq!(delta.buckets[1], 1);
    }
}
