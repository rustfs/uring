// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::env;

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
    let default = std::thread::available_parallelism().map(usize::from).unwrap_or(1);
    setting("BENCH_WORKERS", default, 1, 1024)
}

pub fn ring_entries(default: usize) -> Result<u32, String> {
    let entries = setting("BENCH_RING_ENTRIES", default, 1, 32768)?;
    if !entries.is_power_of_two() {
        return Err("BENCH_RING_ENTRIES must be a power of two".into());
    }
    Ok(entries as u32)
}
