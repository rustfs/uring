// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Concurrent positioned-read benchmark — the shape ecstore actually serves.
//!
//! `streaming_bench` measured a single sequential stream and (correctly) found
//! io_uring loses to kernel readahead. That is not io_uring's lever. Its levers
//! are (a) batching many independent IOs into one `io_uring_enter`, and (b)
//! keeping the IO off a blocking thread. Both only appear when *many concurrent
//! positioned reads* hit one disk — exactly what erasure-coded shard reads
//! (`pread_bytes`) do under load.
//!
//! Four strategies isolate where the time actually goes:
//!
//! | strategy            | models                                                    |
//! |---------------------|-----------------------------------------------------------|
//! | `std_open_pread`    | today's StdBackend: spawn_blocking{open; pread}            |
//! | `std_cached_pread`  | spawn_blocking{pread} on a pre-opened fd                   |
//! | `uring_open_read`   | today's UringBackend: spawn_blocking{open;stat} + read_at  |
//! | `uring_cached_read` | pre-opened fd + read_at, NO spawn_blocking                 |
//!
//! `std_open_pread` vs `uring_open_read` answers "is today's io_uring wiring
//! worth anything". `*_cached_*` vs `*_open_*` prices the per-read open.
//! `uring_cached_read` is the ceiling an fd cache + registered files would buy.
//!
//! Usage:
//!   concurrent_pread_bench <strategy> <file> <file_size> <read_size> <concurrency> <total_ops>
//!
//! The file is created (filled + fsync'd) only if it does not exist. An existing
//! path is never truncated, overwritten, or followed through a symlink, so a
//! mistyped path cannot destroy data even though the sweep runs as root.
//!
//! Correctness: IOPS alone cannot tell a correct strategy from one that reads
//! the wrong offsets, so the file is filled with an offset-addressable pattern
//! (`byte[i] = splitmix64(i / 8)[i % 8]`). Setting `BENCH_VERIFY=1` checks every
//! byte each strategy delivers against that pattern at its claimed offset.
//! Verification is skipped entirely when unset, so it never perturbs a timed
//! run; a `BENCH_VERIFY=1` run is a correctness check, not a measurement.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustfs_uring::UringDriver;

/// Keeps `(concurrency * 2).next_power_of_two()` ring entries under the
/// kernel's 32768-entry limit and the arithmetic far from overflow.
const MAX_CONCURRENCY: usize = 4096;
const MAX_READ_SIZE: usize = 1 << 30;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Strategy {
    StdOpenPread,
    StdCachedPread,
    UringOpenRead,
    UringCachedRead,
}

impl Strategy {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "std_open_pread" => Ok(Self::StdOpenPread),
            "std_cached_pread" => Ok(Self::StdCachedPread),
            "uring_open_read" => Ok(Self::UringOpenRead),
            "uring_cached_read" => Ok(Self::UringCachedRead),
            other => Err(format!(
                "unknown strategy {other:?}; expected one of std_open_pread, std_cached_pread, uring_open_read, uring_cached_read"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::StdOpenPread => "std_open_pread",
            Self::StdCachedPread => "std_cached_pread",
            Self::UringOpenRead => "uring_open_read",
            Self::UringCachedRead => "uring_cached_read",
        }
    }

    /// Strategies that reuse one pre-opened fd across every read.
    fn uses_cached_fd(self) -> bool {
        matches!(self, Self::StdCachedPread | Self::UringCachedRead)
    }

    fn uses_uring(self) -> bool {
        matches!(self, Self::UringOpenRead | Self::UringCachedRead)
    }
}

#[derive(Clone)]
struct Config {
    strategy: Strategy,
    file: String,
    file_size: u64,
    read_size: usize,
    concurrency: usize,
    total_ops: usize,
    verify: bool,
}

fn parse_args() -> Result<Config, String> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 7 {
        return Err(
            "usage: concurrent_pread_bench <strategy> <file> <file_size> <read_size> <concurrency> <total_ops>".to_string(),
        );
    }
    let parse = |name: &str, raw: &str| -> Result<u64, String> {
        raw.parse::<u64>()
            .map_err(|_| format!("{name}: {raw:?} is not a non-negative integer"))
    };
    let cfg = Config {
        strategy: Strategy::parse(&a[1])?,
        file: a[2].clone(),
        file_size: parse("file_size", &a[3])?,
        read_size: parse("read_size", &a[4])? as usize,
        concurrency: parse("concurrency", &a[5])? as usize,
        total_ops: parse("total_ops", &a[6])? as usize,
        verify: matches!(std::env::var("BENCH_VERIFY").as_deref(), Ok("1") | Ok("true")),
    };
    validate(&cfg)?;
    Ok(cfg)
}

/// Reject geometry the strategies cannot honour, before any I/O runs.
fn validate(cfg: &Config) -> Result<(), String> {
    if cfg.read_size == 0 || cfg.read_size > MAX_READ_SIZE {
        return Err(format!("read_size must be in 1..={MAX_READ_SIZE}, got {}", cfg.read_size));
    }
    if cfg.file_size > i64::MAX as u64 {
        return Err("file_size exceeds i64::MAX (the kernel's signed loff_t)".to_string());
    }
    // Offsets are drawn from `file_size - read_size` rounded down to 4 KiB, so
    // the file must hold at least one whole read plus a block to pick from.
    if cfg.file_size < cfg.read_size as u64 + 4096 {
        return Err(format!(
            "file_size ({}) must exceed read_size ({}) by at least 4096 bytes",
            cfg.file_size, cfg.read_size
        ));
    }
    if cfg.concurrency == 0 || cfg.concurrency > MAX_CONCURRENCY {
        return Err(format!("concurrency must be in 1..={MAX_CONCURRENCY}, got {}", cfg.concurrency));
    }
    if cfg.total_ops == 0 {
        return Err("total_ops must be > 0".to_string());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Offset-addressable file content (shared shape with streaming_bench)
// ---------------------------------------------------------------------------

fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The expected 8-byte little-endian word covering byte offsets
/// `idx * 8 .. idx * 8 + 8`. Content is a pure function of absolute offset, so a
/// read at any offset can be checked without reading anything before it.
fn expected_word(idx: u64) -> [u8; 8] {
    splitmix64(idx).to_le_bytes()
}

/// Check that `data` is exactly what the file holds at `offset`. Catches a
/// strategy that reads the right *number* of bytes from the wrong offsets — a
/// length-only assertion cannot.
fn verify_range(offset: u64, data: &[u8]) -> Result<(), String> {
    let (mut cached_idx, mut word) = (u64::MAX, [0u8; 8]);
    for (i, &got) in data.iter().enumerate() {
        let pos = offset + i as u64;
        let idx = pos / 8;
        if idx != cached_idx {
            cached_idx = idx;
            word = expected_word(idx);
        }
        let want = word[(pos % 8) as usize];
        if got != want {
            return Err(format!("content mismatch at byte {pos}: got {got:#04x}, want {want:#04x}"));
        }
    }
    Ok(())
}

/// Create the bench file if missing. An existing path is left untouched: never
/// truncated, never followed through a symlink (`create_new` implies `O_EXCL`;
/// `symlink_metadata` does not resolve one). A wrong-size or non-regular path is
/// an error rather than an overwrite, because the sweep runs as root and the
/// path is caller-supplied.
fn ensure_file(path: &str, size: u64) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_file() => Err(format!(
            "{path} exists and is not a regular file ({:?}); refusing to touch it",
            meta.file_type()
        )),
        Ok(meta) if meta.len() != size => Err(format!(
            "{path} exists with {} bytes but {size} were requested; refusing to truncate it — delete it or pick another path",
            meta.len()
        )),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => create_file(path, size).map_err(|e| {
            // A half-written file would silently become the "wrong size" error
            // on the next run; leave nothing behind.
            let _ = std::fs::remove_file(path);
            format!("create bench file {path}: {e}")
        }),
        Err(e) => Err(format!("stat {path}: {e}")),
    }
}

fn create_file(path: &str, size: u64) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let mut buf = vec![0u8; 1 << 20];
    let mut written = 0u64;
    while written < size {
        // `written` is always a multiple of 8 (the buffer is), so word indices
        // line up with absolute offsets.
        let base = written / 8;
        for (w, word) in buf.chunks_exact_mut(8).enumerate() {
            word.copy_from_slice(&expected_word(base + w as u64));
        }
        let n = ((size - written) as usize).min(buf.len());
        f.write_all(&buf[..n])?;
        written += n as u64;
    }
    f.sync_all()
}

/// O_NOFOLLOW closes the window between `ensure_file`'s stat and this open.
fn open_read(path: &str) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}

/// Deterministic block-aligned offsets, so every strategy reads the exact same
/// blocks and the comparison is not confounded by a different access pattern.
fn offsets(cfg: &Config) -> Vec<u64> {
    let blocks = (cfg.file_size - cfg.read_size as u64) / 4096;
    let mut state = 0x2545_f491_4f6c_dd1du64;
    (0..cfg.total_ops)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ((state >> 33) % blocks) * 4096
        })
        .collect()
}

fn percentile(sorted: &[Duration], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx].as_micros()
}

async fn run(cfg: &Config) -> Result<Vec<Duration>, String> {
    let offs = Arc::new(offsets(cfg));
    let path = Arc::new(cfg.file.clone());

    // A shared fd is safe for the cached strategies: pread is positional and
    // never touches the file description's shared offset.
    let cached: Option<Arc<File>> = if cfg.strategy.uses_cached_fd() {
        Some(Arc::new(open_read(&cfg.file).map_err(|e| format!("open cached fd: {e}"))?))
    } else {
        None
    };
    let driver = if cfg.strategy.uses_uring() {
        let depth = (cfg.concurrency.max(64) * 2).next_power_of_two() as u32;
        Some(Arc::new(
            UringDriver::probe_and_start(depth).map_err(|e| format!("probe io_uring: {e:?}"))?,
        ))
    } else {
        None
    };

    let (strategy, read_size, verify) = (cfg.strategy, cfg.read_size, cfg.verify);
    let per_task = cfg.total_ops.div_ceil(cfg.concurrency);
    let mut set = tokio::task::JoinSet::new();
    for t in 0..cfg.concurrency {
        let (offs, path, cached, driver) = (offs.clone(), path.clone(), cached.clone(), driver.clone());
        let start_idx = (t * per_task).min(offs.len());
        let end_idx = (start_idx + per_task).min(offs.len());
        set.spawn(async move {
            let mut lats = Vec::with_capacity(end_idx - start_idx);
            for &off in &offs[start_idx..end_idx] {
                let t0 = Instant::now();
                let bytes: Vec<u8> = match strategy {
                    // Today's StdBackend: a blocking-pool hop that opens then preads.
                    Strategy::StdOpenPread => {
                        let p = path.clone();
                        tokio::task::spawn_blocking(move || {
                            let f = open_read(&p)?;
                            let mut buf = vec![0u8; read_size];
                            f.read_exact_at(&mut buf, off)?;
                            io::Result::Ok(buf)
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                        .map_err(|e| format!("open+pread: {e}"))?
                    }
                    // Blocking pread on a pre-opened fd: prices the open away.
                    Strategy::StdCachedPread => {
                        let f = cached.clone().expect("cached fd");
                        tokio::task::spawn_blocking(move || {
                            let mut buf = vec![0u8; read_size];
                            f.read_exact_at(&mut buf, off)?;
                            io::Result::Ok(buf)
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                        .map_err(|e| format!("pread: {e}"))?
                    }
                    // Today's UringBackend: still a blocking-pool hop for open+stat.
                    Strategy::UringOpenRead => {
                        let p = path.clone();
                        let file = tokio::task::spawn_blocking(move || {
                            let f = open_read(&p)?;
                            f.metadata()?;
                            io::Result::Ok(f)
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                        .map_err(|e| format!("open+stat: {e}"))?;
                        let d = driver.clone().expect("driver");
                        d.read_at(Arc::new(file), off, read_size)
                            .await
                            .map_err(|e| format!("uring read: {e}"))?
                    }
                    // The ceiling: no blocking pool at all on the read path.
                    Strategy::UringCachedRead => {
                        let d = driver.clone().expect("driver");
                        let f = cached.clone().expect("cached fd");
                        d.read_at(f, off, read_size).await.map_err(|e| format!("uring read: {e}"))?
                    }
                };
                let elapsed = t0.elapsed();
                if bytes.len() != read_size {
                    return Err(format!("short read at {off}: {} of {read_size}", bytes.len()));
                }
                if verify {
                    verify_range(off, &bytes)?;
                }
                lats.push(elapsed);
            }
            Ok::<_, String>(lats)
        });
    }

    let mut all = Vec::with_capacity(cfg.total_ops);
    while let Some(r) = set.join_next().await {
        all.extend(r.map_err(|e| format!("task panicked: {e}"))??);
    }
    Ok(all)
}

fn main() -> ExitCode {
    let cfg = match parse_args() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    if let Err(e) = ensure_file(&cfg.file, cfg.file_size) {
        eprintln!("error: {e}");
        return ExitCode::FAILURE;
    }

    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: build runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    let start = Instant::now();
    let mut lats = match rt.block_on(run(&cfg)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let secs = start.elapsed().as_secs_f64();

    if cfg.verify {
        eprintln!("{}: verified byte-exact across {} reads", cfg.strategy.name(), lats.len());
    }

    lats.sort_unstable();
    let ops = lats.len();
    let iops = ops as f64 / secs;
    let mbps = (ops as f64 * cfg.read_size as f64 / (1024.0 * 1024.0)) / secs;
    // CSV: strategy,file_size,read_size,concurrency,ops,secs,IOPS,MBps,p50_us,p99_us,p999_us
    println!(
        "{},{},{},{},{},{:.6},{:.0},{:.1},{},{},{}",
        cfg.strategy.name(),
        cfg.file_size,
        cfg.read_size,
        cfg.concurrency,
        ops,
        secs,
        iops,
        mbps,
        percentile(&lats, 0.50),
        percentile(&lats, 0.99),
        percentile(&lats, 0.999),
    );
    ExitCode::SUCCESS
}
