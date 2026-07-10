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

//! Streaming-read A/B benchmark for rustfs/backlog#1144 (the 3c go/no-go).
//!
//! Reads one file end-to-end with a chosen strategy and prints a single CSV
//! row (throughput is the decision metric). One measurement per invocation so
//! the runner script can control page-cache state (cold vs warm) between runs.
//!
//! The question it answers: does routing ecstore's *sequential* streaming reads
//! through io_uring beat StdBackend's buffered read (which rides kernel
//! readahead)? io_uring's only lever on a sequential stream is *pipelining*
//! (queue depth > 1), so the io_uring strategies take a `qd` and keep that many
//! reads in flight. A single-depth io_uring read is expected to lose; the
//! interesting comparison is deep-pipelined io_uring vs buffered+readahead.
//!
//! Usage:
//!   streaming_bench <strategy> <file> <size_bytes> <chunk_bytes> <qd> <align>
//!
//! strategy ∈ { std_buffered, std_odirect, uring_read_at, uring_read_at_direct }
//!
//! The file is created (filled + fsync'd) only if it does not exist. An existing
//! path is never truncated, overwritten, or followed through a symlink: a wrong
//! size or a non-regular file is a hard error, so a mistyped path cannot destroy
//! data even though the sweep runs as root. Once created, the file is reused
//! across runs so the runner can drop caches without rewriting it.
//!
//! Correctness: throughput alone cannot tell a correct strategy from one that
//! reads the wrong offsets, so the file is filled with an offset-addressable
//! pattern (`byte[i] = splitmix64(i / 8)[i % 8]`). Setting `BENCH_VERIFY=1`
//! checks every byte a strategy delivers against that pattern, at its claimed
//! offset. Verification is skipped entirely when the variable is unset, so it
//! never perturbs a timed run; a `BENCH_VERIFY=1` run is a correctness check,
//! not a measurement, and its CSV row must be discarded.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use rustfs_uring::UringDriver;

/// Queue depth ceiling. `probe_and_start` gets `(qd * 2).next_power_of_two()`
/// entries, so this keeps the ring under the kernel's 32768-entry limit and the
/// arithmetic far from overflow.
const MAX_QD: usize = 4096;
/// Smallest logical block size any device reports.
const MIN_ALIGN: usize = 512;
const MAX_ALIGN: usize = 1 << 20;
/// One chunk is one in-flight buffer; `qd` of them are live at once.
const MAX_CHUNK: usize = 1 << 30;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Strategy {
    StdBuffered,
    StdODirect,
    UringReadAt,
    UringReadAtDirect,
}

impl Strategy {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "std_buffered" => Ok(Self::StdBuffered),
            "std_odirect" => Ok(Self::StdODirect),
            "uring_read_at" => Ok(Self::UringReadAt),
            "uring_read_at_direct" => Ok(Self::UringReadAtDirect),
            other => Err(format!(
                "unknown strategy {other:?}; expected one of std_buffered, std_odirect, uring_read_at, uring_read_at_direct"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::StdBuffered => "std_buffered",
            Self::StdODirect => "std_odirect",
            Self::UringReadAt => "uring_read_at",
            Self::UringReadAtDirect => "uring_read_at_direct",
        }
    }

    fn is_direct(self) -> bool {
        matches!(self, Self::StdODirect | Self::UringReadAtDirect)
    }
}

#[derive(Clone)]
struct Config {
    strategy: Strategy,
    file: String,
    size: usize,
    chunk: usize,
    qd: usize,
    align: usize,
    verify: bool,
}

fn parse_usize(name: &str, raw: &str) -> Result<usize, String> {
    raw.parse()
        .map_err(|_| format!("{name}: {raw:?} is not a non-negative integer"))
}

fn parse_args() -> Result<Config, String> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 7 {
        return Err("usage: streaming_bench <strategy> <file> <size_bytes> <chunk_bytes> <qd> <align>".to_string());
    }
    let cfg = Config {
        strategy: Strategy::parse(&a[1])?,
        file: a[2].clone(),
        size: parse_usize("size_bytes", &a[3])?,
        chunk: parse_usize("chunk_bytes", &a[4])?,
        qd: parse_usize("qd", &a[5])?,
        align: parse_usize("align", &a[6])?,
        verify: matches!(std::env::var("BENCH_VERIFY").as_deref(), Ok("1") | Ok("true")),
    };
    validate(&cfg)?;
    Ok(cfg)
}

/// Reject geometry the strategies cannot honour, before any I/O runs. Without
/// this, `chunk == 0` panics in `step_by`, `align == 0` panics in
/// `next_multiple_of`/`align_offset`, and a large `qd` overflows the ring-entry
/// arithmetic.
fn validate(cfg: &Config) -> Result<(), String> {
    if cfg.size == 0 {
        return Err("size_bytes must be > 0".to_string());
    }
    if cfg.size as u64 > i64::MAX as u64 {
        return Err("size_bytes exceeds i64::MAX (the kernel's signed loff_t)".to_string());
    }
    if cfg.chunk == 0 || cfg.chunk > MAX_CHUNK {
        return Err(format!("chunk_bytes must be in 1..={MAX_CHUNK}, got {}", cfg.chunk));
    }
    if cfg.qd == 0 || cfg.qd > MAX_QD {
        return Err(format!("qd must be in 1..={MAX_QD}, got {}", cfg.qd));
    }
    // `align_offset` and the kernel's O_DIRECT contract both require a power of
    // two; validate it for every strategy so the sweep cannot pass one geometry
    // to the baselines and another to io_uring.
    if !cfg.align.is_power_of_two() || cfg.align < MIN_ALIGN || cfg.align > MAX_ALIGN {
        return Err(format!("align must be a power of two in {MIN_ALIGN}..={MAX_ALIGN}, got {}", cfg.align));
    }
    if cfg.strategy.is_direct() && !cfg.chunk.is_multiple_of(cfg.align) {
        return Err(format!(
            "O_DIRECT strategies need chunk_bytes ({}) to be a multiple of align ({}), so every read offset is block-aligned",
            cfg.chunk, cfg.align
        ));
    }
    if cfg.chunk.checked_add(cfg.align).is_none() {
        return Err("chunk_bytes + align overflows usize".to_string());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Offset-addressable file content
// ---------------------------------------------------------------------------

fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The expected 8-byte little-endian word at word index `idx`, i.e. covering
/// byte offsets `idx * 8 .. idx * 8 + 8`. Content is a pure function of absolute
/// offset, so any chunk can be checked without reading the ones before it.
fn expected_word(idx: u64) -> [u8; 8] {
    splitmix64(idx).to_le_bytes()
}

/// Check that `data` is exactly what the file holds at `offset`. Catches a
/// strategy that reads the right *number* of bytes from the wrong offsets, or
/// repeats a chunk — both of which pass a length-only assertion.
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

/// Create the bench file if it is missing. An existing path is left untouched:
/// this never truncates, and never follows a symlink (`create_new` implies
/// `O_EXCL`, which fails on one; `symlink_metadata` does not resolve one). A
/// wrong-size or non-regular path is an error rather than an overwrite, because
/// the sweep runs as root and `BENCH_DIR` is caller-supplied.
fn ensure_file(path: &str, size: usize) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_file() => Err(format!(
            "{path} exists and is not a regular file ({:?}); refusing to touch it",
            meta.file_type()
        )),
        Ok(meta) if meta.len() != size as u64 => Err(format!(
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

fn create_file(path: &str, size: usize) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let mut buf = vec![0u8; 1 << 20];
    let mut written = 0usize;
    while written < size {
        // `written` is always a multiple of 8 here (the buffer is), so word
        // indices line up with absolute offsets.
        let base = (written / 8) as u64;
        for (w, word) in buf.chunks_exact_mut(8).enumerate() {
            word.copy_from_slice(&expected_word(base + w as u64));
        }
        let n = (size - written).min(buf.len());
        f.write_all(&buf[..n])?;
        written += n;
    }
    f.sync_all()
}

fn open_read(cfg: &Config, direct: bool) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true);
    // O_NOFOLLOW closes the window between `ensure_file`'s stat and this open.
    let mut flags = libc::O_NOFOLLOW | libc::O_CLOEXEC;
    if direct {
        flags |= libc::O_DIRECT;
    }
    opts.custom_flags(flags).open(&cfg.file)
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Sequential buffered read: the StdBackend baseline that rides kernel readahead.
fn run_std_buffered(cfg: &Config) -> Result<(usize, usize), String> {
    let mut f = open_read(cfg, false).map_err(|e| format!("open: {e}"))?;
    let mut buf = vec![0u8; cfg.chunk];
    let (mut total, mut ops) = (0usize, 0usize);
    loop {
        let n = f.read(&mut buf).map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        if cfg.verify {
            verify_range(total as u64, &buf[..n])?;
        }
        total += n;
        ops += 1;
    }
    Ok((total, ops))
}

/// Heap buffer whose start is aligned to `align` (for O_DIRECT).
fn aligned_buf(len: usize, align: usize) -> (Vec<u8>, usize) {
    let v = vec![0u8; len + align];
    let pad = v.as_ptr().align_offset(align);
    assert!(pad + len <= v.len());
    (v, pad)
}

/// Sequential O_DIRECT read: page-cache-bypassing baseline.
fn run_std_odirect(cfg: &Config) -> Result<(usize, usize), String> {
    let f = open_read(cfg, true).map_err(|e| format!("open O_DIRECT: {e}"))?;
    // `validate` already requires chunk % align == 0; this is the identity.
    let chunk = cfg.chunk;
    let (mut buf, pad) = aligned_buf(chunk, cfg.align);
    let (mut total, mut ops, mut off) = (0usize, 0usize, 0u64);
    while (off as usize) < cfg.size {
        let n = f
            .read_at(&mut buf[pad..pad + chunk], off)
            .map_err(|e| format!("O_DIRECT read_at: {e}"))?;
        if n == 0 {
            break;
        }
        // The kernel returns whole blocks; count only the logical remainder.
        let logical = n.min(cfg.size - off as usize);
        if cfg.verify {
            verify_range(off, &buf[pad..pad + logical])?;
        }
        total += logical;
        ops += 1;
        off += n as u64;
    }
    Ok((total, ops))
}

/// Pipelined io_uring read at depth `qd`. `direct` selects read_at_direct.
async fn run_uring(cfg: &Config, direct: bool) -> Result<(usize, usize), String> {
    // `validate` caps qd at MAX_QD, so this cannot overflow u32.
    let depth = (cfg.qd * 2).next_power_of_two() as u32;
    let driver = Arc::new(UringDriver::probe_and_start(depth).map_err(|e| format!("probe io_uring: {e:?}"))?);
    let file = Arc::new(open_read(cfg, direct).map_err(|e| format!("open: {e}"))?);

    let offsets: Vec<(u64, usize)> = (0..cfg.size)
        .step_by(cfg.chunk)
        .map(|o| (o as u64, cfg.chunk.min(cfg.size - o)))
        .collect();

    let (mut total, mut ops) = (0usize, 0usize);
    let mut set = tokio::task::JoinSet::new();
    let mut idx = 0usize;
    let align = cfg.align;
    let verify = cfg.verify;
    let spawn = |set: &mut tokio::task::JoinSet<io::Result<usize>>, idx: usize| {
        let (off, len) = offsets[idx];
        let d = driver.clone();
        let f = file.clone();
        set.spawn(async move {
            let bytes = if direct {
                d.read_at_direct(f, off, len, align).await?
            } else {
                d.read_at(f, off, len).await?
            };
            if verify {
                if bytes.len() != len {
                    return Err(io::Error::other(format!(
                        "short read at offset {off}: got {} bytes, want {len}",
                        bytes.len()
                    )));
                }
                verify_range(off, &bytes).map_err(io::Error::other)?;
            }
            Ok(bytes.len())
        });
    };
    while set.len() < cfg.qd && idx < offsets.len() {
        spawn(&mut set, idx);
        idx += 1;
    }
    while let Some(res) = set.join_next().await {
        let n = res.map_err(|e| format!("join: {e}"))?.map_err(|e| format!("read: {e}"))?;
        total += n;
        ops += 1;
        if idx < offsets.len() {
            spawn(&mut set, idx);
            idx += 1;
        }
    }
    Ok((total, ops))
}

fn run(cfg: &Config) -> Result<(usize, usize, f64), String> {
    let start = Instant::now();
    let (total, ops) = match cfg.strategy {
        Strategy::StdBuffered => run_std_buffered(cfg)?,
        Strategy::StdODirect => run_std_odirect(cfg)?,
        Strategy::UringReadAt | Strategy::UringReadAtDirect => {
            let direct = cfg.strategy == Strategy::UringReadAtDirect;
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("runtime: {e}"))?;
            rt.block_on(run_uring(cfg, direct))?
        }
    };
    let secs = start.elapsed().as_secs_f64();
    if total != cfg.size {
        return Err(format!("strategy {} read {total} of {} bytes", cfg.strategy.name(), cfg.size));
    }
    Ok((total, ops, secs))
}

pub(super) fn main() -> ExitCode {
    let cfg = match parse_args().and_then(|cfg| ensure_file(&cfg.file, cfg.size).map(|()| cfg)) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("streaming_bench: {e}");
            return ExitCode::from(2);
        }
    };

    let (total, ops, secs) = match run(&cfg) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("streaming_bench: {e}");
            return ExitCode::FAILURE;
        }
    };

    if cfg.verify {
        eprintln!(
            "streaming_bench: verify=ok strategy={} size={} chunk={} qd={} align={} (timing below is NOT a measurement)",
            cfg.strategy.name(),
            cfg.size,
            cfg.chunk,
            cfg.qd,
            cfg.align
        );
    }
    let mbps = (total as f64 / (1024.0 * 1024.0)) / secs;
    // CSV: strategy,size,chunk,qd,align,bytes,secs,MBps,ops
    println!(
        "{},{},{},{},{},{},{:.6},{:.1},{}",
        cfg.strategy.name(),
        cfg.size,
        cfg.chunk,
        cfg.qd,
        cfg.align,
        total,
        secs,
        mbps,
        ops
    );
    ExitCode::SUCCESS
}
