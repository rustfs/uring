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

# Streaming-read A/B sweep for rustfs/backlog#1144 (the 3c go/no-go).
#
# Runs the streaming_bench example across strategies × sizes × chunks × queue
# depths, in two cache legs:
#   warm  — the file is pre-read into the page cache before each timed run
#           (favors buffered; O_DIRECT is unaffected by definition).
#   cold  — `drop_caches` before each timed run, so every read hits the device.
#
# Emits one CSV to stdout. Cold mode needs root (drop_caches); the bench host
# azure-20780104 runs as root. REPEAT>1 runs each config N times so the caller
# can take the median.
#
# BENCH_DIR only ever holds files this script created: the example refuses to
# truncate, overwrite, or follow a symlink at an existing path, so a mistyped
# BENCH_DIR fails loudly instead of destroying data even though the sweep runs
# as root.
set -euo pipefail
cd "$(dirname "$0")"

DIR="${BENCH_DIR:-/data/rustfs/uring-bench}"
REPEAT="${REPEAT:-3}"
ALIGN="${ALIGN:-4096}"
BIN="${CARGO_TARGET_DIR:-target}/release/examples/streaming_bench"
STRATEGIES=(std_buffered std_odirect uring_read_at uring_read_at_direct)

# Sizes: metadata-ish, mid object, large object.
SIZES=(${SIZES:-65536 16777216 268435456})
CHUNKS=(${CHUNKS:-131072 1048576})
QDS=(${QDS:-1 4 16})

mkdir -p "$DIR"
cargo build --release --example streaming_bench >&2

# Correctness preflight (untimed; its output is discarded). Throughput cannot
# distinguish a strategy that reads the right *number* of bytes from one that
# reads the wrong offsets or repeats a chunk, so every strategy first replays
# the boundary geometries under BENCH_VERIFY=1, which checks each delivered byte
# against the file's offset-addressable pattern:
#   - a size that is a block multiple but not a chunk multiple (partial tail);
#   - a size that is neither (O_DIRECT tail inside a partially valid block);
#   - queue depths 1 and 4, covering the pipelined path's offset bookkeeping.
# A mismatch aborts the script before any measurement is taken.
preflight_verify() {
    local vdir="$DIR/verify.$$" chunk=131072 size strat qd
    rm -rf "$vdir"
    mkdir -p "$vdir"
    # shellcheck disable=SC2064
    trap "rm -rf '$vdir'" RETURN
    for size in $((1048576 + 4096)) $((1048576 + 4097)); do
        for strat in "${STRATEGIES[@]}"; do
            for qd in 1 4; do
                BENCH_VERIFY=1 "$BIN" "$strat" "$vdir/verify_${size}.bin" "$size" "$chunk" "$qd" "$ALIGN" >/dev/null
            done
        done
    done
    echo "preflight: all strategies verified byte-exact" >&2
}
preflight_verify

# Pre-create every file once (untimed) so cold runs are genuinely cold: the
# example's ensure_file becomes a no-op and the runner's drop_caches is the only
# thing controlling cache state.
for size in "${SIZES[@]}"; do
    "$BIN" std_buffered "$DIR/bench_${size}.bin" "$size" 1048576 1 "$ALIGN" >/dev/null
done

drop_or_warm() { # cache size chunk
    local cache=$1 file="$DIR/bench_$2.bin"
    if [ "$cache" = cold ]; then
        sync
        echo 3 >/proc/sys/vm/drop_caches
    else
        cat "$file" >/dev/null # prime the page cache
    fi
}

run() { # strategy size chunk qd cache
    local strat=$1 size=$2 chunk=$3 qd=$4 cache=$5
    local file="$DIR/bench_${size}.bin"
    for _ in $(seq 1 "$REPEAT"); do
        drop_or_warm "$cache" "$size" "$chunk"
        echo "$cache,$("$BIN" "$strat" "$file" "$size" "$chunk" "$qd" "$ALIGN")"
    done
}

echo "cache,strategy,size,chunk,qd,align,bytes,secs,MBps,ops"
for cache in warm cold; do
    for size in "${SIZES[@]}"; do
        for chunk in "${CHUNKS[@]}"; do
            run std_buffered "$size" "$chunk" 1 "$cache"
            run std_odirect "$size" "$chunk" 1 "$cache"
            for qd in "${QDS[@]}"; do
                run uring_read_at "$size" "$chunk" "$qd" "$cache"
                run uring_read_at_direct "$size" "$chunk" "$qd" "$cache"
            done
        done
    done
done
