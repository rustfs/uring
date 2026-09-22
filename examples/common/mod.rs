// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::env;

pub fn diagnostics_interval() -> u64 {
    #[cfg(feature = "diagnostics")]
    {
        rustfs_uring::DIAGNOSTICS_SAMPLE_INTERVAL
    }
    #[cfg(not(feature = "diagnostics"))]
    {
        0
    }
}

pub fn setting(name: &str, default: usize, min: usize, max: usize) -> Result<usize, String> {
    let value = match env::var(name) {
        Ok(raw) => raw.parse::<usize>().map_err(|_| format!("{name} must be an integer"))?,
        Err(env::VarError::NotPresent) => default,
        Err(err) => return Err(format!("{name}: {err}")),
    };
    if !(min..=max).contains(&value) {
        return Err(format!("{name} must be in {min}..={max}"));
    }
    Ok(value)
}

pub fn workers() -> Result<usize, String> {
    if env::var("BENCH_DIAGNOSTICS").as_deref() == Ok("1") && !cfg!(feature = "diagnostics") {
        return Err("BENCH_DIAGNOSTICS=1 requires building with --features diagnostics".into());
    }
    let default = std::thread::available_parallelism().map(usize::from).unwrap_or(1);
    setting("BENCH_WORKERS", default, 1, 1024)
}

#[cfg(feature = "diagnostics")]
pub fn report_diagnostics(snapshot: &rustfs_uring::DiagnosticsSnapshot) {
    if env::var("BENCH_DIAGNOSTICS").as_deref() != Ok("1") {
        return;
    }
    for (stage, histogram) in [
        ("admission", &snapshot.admission),
        ("driver_queue", &snapshot.driver_queue),
        ("preparation", &snapshot.preparation),
        ("driver_lifetime", &snapshot.driver_lifetime),
        ("cqe_processing", &snapshot.cqe_processing),
        ("completion_to_poll", &snapshot.completion_to_poll),
    ] {
        eprintln!(
            "DIAGNOSTICS stage={stage} interval={} count={} total_ns={} buckets={:?}",
            rustfs_uring::DIAGNOSTICS_SAMPLE_INTERVAL,
            histogram.count,
            histogram.total_nanos,
            histogram.buckets,
        );
    }
}

pub fn ring_entries(default: usize) -> Result<u32, String> {
    let entries = setting("BENCH_RING_ENTRIES", default, 1, 32768)?;
    if !entries.is_power_of_two() {
        return Err("BENCH_RING_ENTRIES must be a power of two".into());
    }
    Ok(entries as u32)
}
