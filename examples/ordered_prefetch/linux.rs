// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::reader::{Config, OrderedReader};
use rustfs_uring::{ReadLimits, UringDriver};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::sync::Arc;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn number<T: std::str::FromStr>(name: &str, value: &str) -> io::Result<T> {
    value.parse().map_err(|_| invalid(format!("invalid {name}: {value}")))
}

fn reference_read(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; len];
    let mut read = 0;
    while read < len {
        match file.read_at(&mut bytes[read..], offset + read as u64) {
            Ok(0) => break,
            Ok(count) => read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    bytes.truncate(read);
    Ok(bytes)
}

pub fn run() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 6 {
        return Err(invalid("usage: ordered_prefetch FILE OFFSET LENGTH CHUNK WINDOW MAX_BYTES"));
    }
    let cfg = Config {
        offset: number("OFFSET", &args[1])?,
        length: number("LENGTH", &args[2])?,
        chunk: number("CHUNK", &args[3])?,
        window: number("WINDOW", &args[4])?,
        max_bytes: number("MAX_BYTES", &args[5])?,
    };
    let end = cfg.validate()?;
    // Never create/write/truncate the fixture or block opening a FIFO. Validate
    // the opened descriptor, and refuse symlinks to keep fixture identity clear.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&args[0])?;
    if !file.metadata()?.is_file() {
        return Err(invalid("FILE must be an existing regular file"));
    }
    let file = Arc::new(file);
    // Intentionally tight admission exercises deferred reads. This executable
    // verifies correctness; it is not a throughput baseline or production tuning.
    let driver = UringDriver::probe_and_start_with_limits(
        2,
        1,
        ReadLimits {
            max_read_len: Some(cfg.chunk),
            max_in_flight_bytes: Some(cfg.chunk),
        },
    )
    .map_err(io::Error::other)?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let mut reader = OrderedReader::new(cfg, |offset, len| driver.read_at(Arc::clone(&file), offset, len))?;
    let result = (|| {
        let mut delivered = 0u64;
        let mut chunks = 0u64;
        // Synchronous reference I/O runs outside the async executor. Pausing
        // consumption for verification must not refill the reader's window.
        while let Some(chunk) = runtime.block_on(reader.next())? {
            let expected_offset = cfg.offset + delivered;
            if chunk.offset != expected_offset {
                return Err(io::Error::other("non-contiguous output offsets"));
            }
            let want = (end - expected_offset).min(cfg.chunk as u64) as usize;
            if chunk.bytes != reference_read(&file, expected_offset, want)? {
                return Err(io::Error::other("byte mismatch against positioned std read"));
            }
            delivered += chunk.bytes.len() as u64;
            chunks += 1;
        }
        if cfg.offset + delivered < end && !reference_read(&file, cfg.offset + delivered, 1)?.is_empty() {
            return Err(io::Error::other("ordered reader terminated before reference EOF"));
        }
        Ok((delivered, chunks))
    })();
    drop(reader);
    let stats = driver.shutdown();
    let (bytes, chunks) = result?;
    if stats.in_flight != 0 || stats.submitted != stats.delivered + stats.orphan_reclaimed {
        return Err(io::Error::other("driver did not drain with read conservation intact"));
    }
    println!("ORDERED_PREFETCH_OK bytes={bytes} chunks={chunks}");
    Ok(())
}
