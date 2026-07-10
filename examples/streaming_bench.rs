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
//! The file is created (filled + fsync'd) if missing or the wrong size, then
//! reused across runs so the runner can drop caches without rewriting it.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::sync::Arc;
use std::time::Instant;

use rustfs_uring::UringDriver;

#[derive(Clone)]
struct Config {
    strategy: String,
    file: String,
    size: usize,
    chunk: usize,
    qd: usize,
    align: usize,
}

fn parse_args() -> Config {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 7 {
        eprintln!("usage: streaming_bench <strategy> <file> <size_bytes> <chunk_bytes> <qd> <align>");
        std::process::exit(2);
    }
    Config {
        strategy: a[1].clone(),
        file: a[2].clone(),
        size: a[3].parse().expect("size_bytes"),
        chunk: a[4].parse().expect("chunk_bytes"),
        qd: a[5].parse().expect("qd"),
        align: a[6].parse().expect("align"),
    }
}

/// Create the file with `size` deterministic bytes if it is missing or a
/// different length; otherwise leave it untouched so a cache drop is the only
/// thing that changes between runs.
fn ensure_file(path: &str, size: usize) {
    if let Ok(meta) = std::fs::metadata(path)
        && meta.len() == size as u64
    {
        return;
    }
    let mut f = File::create(path).expect("create bench file");
    let mut buf = vec![0u8; 1 << 20];
    let mut written = 0usize;
    let mut state = 0x9e3779b97f4a7c15u64;
    while written < size {
        for b in buf.iter_mut() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            *b = (state >> 33) as u8;
        }
        let n = (size - written).min(buf.len());
        f.write_all(&buf[..n]).expect("fill bench file");
        written += n;
    }
    f.sync_all().expect("fsync bench file");
}

/// Sequential buffered read: the StdBackend baseline that rides kernel readahead.
fn run_std_buffered(cfg: &Config) -> (usize, usize) {
    let mut f = File::open(&cfg.file).expect("open");
    let mut buf = vec![0u8; cfg.chunk];
    let (mut total, mut ops) = (0usize, 0usize);
    loop {
        let n = f.read(&mut buf).expect("read");
        if n == 0 {
            break;
        }
        total += n;
        ops += 1;
    }
    (total, ops)
}

/// Heap buffer whose start is aligned to `align` (for O_DIRECT).
fn aligned_buf(len: usize, align: usize) -> (Vec<u8>, usize) {
    let v = vec![0u8; len + align];
    let pad = v.as_ptr().align_offset(align);
    assert!(pad + len <= v.len());
    (v, pad)
}

/// Sequential O_DIRECT read: page-cache-bypassing baseline.
fn run_std_odirect(cfg: &Config) -> (usize, usize) {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(&cfg.file)
        .expect("open O_DIRECT");
    let chunk = cfg.chunk.next_multiple_of(cfg.align);
    let (mut buf, pad) = aligned_buf(chunk, cfg.align);
    let (mut total, mut ops, mut off) = (0usize, 0usize, 0u64);
    while (off as usize) < cfg.size {
        let n = f.read_at(&mut buf[pad..pad + chunk], off).expect("O_DIRECT read_at");
        if n == 0 {
            break;
        }
        // The kernel returns whole blocks; count only the logical remainder.
        let logical = n.min(cfg.size - off as usize);
        total += logical;
        ops += 1;
        off += n as u64;
    }
    (total, ops)
}

/// Pipelined io_uring read at depth `qd`. `direct` selects read_at_direct.
async fn run_uring(cfg: &Config, direct: bool) -> (usize, usize) {
    let depth = (cfg.qd.max(1) * 2).next_power_of_two() as u32;
    let driver = Arc::new(UringDriver::probe_and_start(depth).expect("probe io_uring"));
    let file = if direct {
        Arc::new(
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(&cfg.file)
                .expect("open O_DIRECT"),
        )
    } else {
        Arc::new(File::open(&cfg.file).expect("open"))
    };

    let offsets: Vec<(u64, usize)> = (0..cfg.size)
        .step_by(cfg.chunk)
        .map(|o| (o as u64, cfg.chunk.min(cfg.size - o)))
        .collect();

    let (mut total, mut ops) = (0usize, 0usize);
    let mut set = tokio::task::JoinSet::new();
    let mut idx = 0usize;
    let align = cfg.align;
    let spawn = |set: &mut tokio::task::JoinSet<std::io::Result<usize>>, idx: usize| {
        let (off, len) = offsets[idx];
        let d = driver.clone();
        let f = file.clone();
        set.spawn(async move {
            let bytes = if direct {
                d.read_at_direct(f, off, len, align).await?
            } else {
                d.read_at(f, off, len).await?
            };
            Ok(bytes.len())
        });
    };
    while set.len() < cfg.qd.max(1) && idx < offsets.len() {
        spawn(&mut set, idx);
        idx += 1;
    }
    while let Some(res) = set.join_next().await {
        let n = res.expect("join").expect("read");
        total += n;
        ops += 1;
        if idx < offsets.len() {
            spawn(&mut set, idx);
            idx += 1;
        }
    }
    (total, ops)
}

fn main() {
    let cfg = parse_args();
    ensure_file(&cfg.file, cfg.size);

    let start = Instant::now();
    let (total, ops) = match cfg.strategy.as_str() {
        "std_buffered" => run_std_buffered(&cfg),
        "std_odirect" => run_std_odirect(&cfg),
        "uring_read_at" | "uring_read_at_direct" => {
            let direct = cfg.strategy == "uring_read_at_direct";
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(run_uring(&cfg, direct))
        }
        other => {
            eprintln!("unknown strategy: {other}");
            std::process::exit(2);
        }
    };
    let secs = start.elapsed().as_secs_f64();
    assert_eq!(total, cfg.size, "strategy {} read {total} of {} bytes", cfg.strategy, cfg.size);
    let mbps = (total as f64 / (1024.0 * 1024.0)) / secs;
    // CSV: strategy,size,chunk,qd,align,bytes,secs,MBps,ops
    println!(
        "{},{},{},{},{},{},{:.6},{:.1},{}",
        cfg.strategy, cfg.size, cfg.chunk, cfg.qd, cfg.align, total, secs, mbps, ops
    );
}
