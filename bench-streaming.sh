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
set -euo pipefail
cd "$(dirname "$0")"

DIR="${BENCH_DIR:-/data/rustfs/uring-bench}"
REPEAT="${REPEAT:-3}"
ALIGN="${ALIGN:-4096}"
BIN="${CARGO_TARGET_DIR:-target}/release/examples/streaming_bench"

# Sizes: metadata-ish, mid object, large object.
SIZES=(${SIZES:-65536 16777216 268435456})
CHUNKS=(${CHUNKS:-131072 1048576})
QDS=(${QDS:-1 4 16})

mkdir -p "$DIR"
cargo build --release --example streaming_bench >&2

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
