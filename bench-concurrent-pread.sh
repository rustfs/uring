#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Concurrent positioned-read sweep — the shape ecstore's erasure-coded shard
# reads actually serve (many independent preads per disk, under load).
#
# Sweeps strategy × read_size × concurrency. Caches are dropped before every
# timed run so reads hit the device (a large file + random offsets means the
# page cache would otherwise skew results run-to-run). Needs root for
# drop_caches; the bench host azure-20780104 runs as root.
set -euo pipefail
cd "$(dirname "$0")"

DIR="${BENCH_DIR:-/data/rustfs/uring-bench}"
REPEAT="${REPEAT:-3}"
BIN="${CARGO_TARGET_DIR:-target}/release/examples/concurrent_pread_bench"

FILE_SIZE="${FILE_SIZE:-4294967296}" # 4 GiB, >> page cache reuse for random reads
READ_SIZES=(${READ_SIZES:-65536 1048576})
CONCURRENCIES=(${CONCURRENCIES:-1 8 32 128})
# Bound each cold run's transfer instead of fixing the op count: a cold 1 MiB
# read costs ~16x a 64 KiB one, so a fixed op count would make the large-read
# legs dominate wall-clock for no extra signal.
TOTAL_BYTES="${TOTAL_BYTES:-268435456}" # 256 MiB per run
MIN_OPS="${MIN_OPS:-512}"               # enough samples for a p999
MAX_OPS="${MAX_OPS:-4096}"
STRATS=(std_open_pread std_cached_pread uring_open_read uring_cached_read)

ops_for() { # read_size -> op count, clamped
    local n=$((TOTAL_BYTES / $1))
    ((n < MIN_OPS)) && n=$MIN_OPS
    ((n > MAX_OPS)) && n=$MAX_OPS
    echo "$n"
}

mkdir -p "$DIR"
cargo build --release --example concurrent_pread_bench >&2

# Correctness preflight (untimed; output discarded). IOPS cannot distinguish a
# strategy that reads the right *number* of bytes from one that reads the wrong
# offsets, so every strategy first replays a small workload under BENCH_VERIFY=1,
# which checks each delivered byte against the file's offset-addressable pattern.
# A mismatch aborts before any measurement is taken.
preflight_verify() {
    local vdir="$DIR/verify.$$" vfile strat
    rm -rf "$vdir"
    mkdir -p "$vdir"
    # shellcheck disable=SC2064
    trap "rm -rf '$vdir'" RETURN
    vfile="$vdir/verify.bin"
    for strat in "${STRATS[@]}"; do
        # Unaligned read size on purpose: exercises the offset bookkeeping.
        BENCH_VERIFY=1 "$BIN" "$strat" "$vfile" $((8 * 1024 * 1024)) 65537 8 64 >/dev/null
    done
    echo "preflight: all strategies verified byte-exact" >&2
}
preflight_verify

FILE="$DIR/pread_${FILE_SIZE}.bin"
# Create once, untimed, so every cold run below is genuinely cold.
"$BIN" std_cached_pread "$FILE" "$FILE_SIZE" 65536 1 1 >/dev/null

echo "cache,strategy,file_size,read_size,concurrency,ops,secs,IOPS,MBps,p50_us,p99_us,p999_us"
for cache in ${CACHES:-cold warm}; do
    for read_size in "${READ_SIZES[@]}"; do
        ops=$(ops_for "$read_size")
        for conc in "${CONCURRENCIES[@]}"; do
            for strat in "${STRATS[@]}"; do
                for _ in $(seq 1 "$REPEAT"); do
                    if [ "$cache" = cold ]; then
                        sync
                        echo 3 >/proc/sys/vm/drop_caches
                    else
                        # Warm takes the device out of the picture, isolating the
                        # software cost (open, blocking-pool hop, submission).
                        # On a throughput-throttled disk the cold leg saturates
                        # and hides exactly the overhead we are pricing.
                        cat "$FILE" >/dev/null
                    fi
                    echo "$cache,$("$BIN" "$strat" "$FILE" "$FILE_SIZE" "$read_size" "$conc" "$ops")"
                done
            done
        done
    done
done
