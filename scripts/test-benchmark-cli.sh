#!/usr/bin/env bash
# Copyright 2024 RustFS Team
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${BENCH_SKIP_BUILD:-0}" != 1 ]]; then
    build_features=()
    if [[ "${BENCH_DIAGNOSTICS:-0}" == 1 ]]; then build_features=(--features diagnostics); fi
    cargo build --locked --examples "${build_features[@]}"
fi
bin="${BENCH_BIN_DIR:-${CARGO_TARGET_DIR:-target}/debug/examples}"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

check_row() {
    local header=$1 row=$2 strategy=$3
    awk -F, -v header="$header" -v strategy="$strategy" '
        BEGIN { n = split(header, names, ",") }
        {
            if (NF != n) exit 1
            for (i = 1; i <= NF; i++) value[names[i]] = $i
            if (value["schema_version"] != 2 || value["mode"] != "verify" || value["strategy"] != strategy) exit 1
            if (value["secs"] <= 0 || value["startup_secs"] < 0 || value["shutdown_secs"] < 0) exit 1
            if (strategy ~ /^uring_/ && value["ring_entries"] != 4) exit 1
            if (strategy ~ /^std_/ && value["ring_entries"] != 0) exit 1
            if ("warmup_ops" in value && (value["warmup_ops"] != 8 || value["ops"] != 32 || value["workers"] != 2)) exit 1
            if ("warmup_runs" in value && (value["warmup_runs"] != 1 || value["bytes"] != 1048583)) exit 1
        }
    ' <<<"$row"
}

concurrent="$bin/concurrent_pread_bench"
streaming="$bin/streaming_bench"
header=$("$concurrent" --header)
[[ ",$header," == *,shards,* ]]
for strategy in std_open_pread std_cached_pread uring_open_read uring_cached_read; do
    row=$(BENCH_VERIFY=1 BENCH_WORKERS=2 BENCH_RING_ENTRIES=4 BENCH_WARMUP_OPS=8 \
        "$concurrent" "$strategy" "$scratch/random.bin" 1048576 4097 8 32 2)
    check_row "$header" "$row" "$strategy"
done

header=$("$streaming" --header)
for strategy in std_buffered std_odirect uring_read_at uring_read_at_direct; do
    row=$(BENCH_VERIFY=1 BENCH_WORKERS=2 BENCH_RING_ENTRIES=4 BENCH_WARMUP_RUNS=1 \
        "$streaming" "$strategy" "$scratch/stream.bin" 1048583 65536 8 4096)
    check_row "$header" "$row" "$strategy"
done

if [[ "${BENCH_DIAGNOSTICS:-0}" == 1 ]]; then
    # Warmup samples must be excluded. Each of two shards then sees 128 measured
    # reads, giving four final-CQE samples in total rather than six.
    BENCH_VERIFY=1 BENCH_WORKERS=2 BENCH_RING_ENTRIES=4 BENCH_WARMUP_OPS=128 \
        "$concurrent" uring_cached_read "$scratch/random.bin" 1048576 4097 8 256 2 \
        >/dev/null 2>"$scratch/diagnostics.log"
    awk '
        /^DIAGNOSTICS / {
            split($2, stage, "="); split($4, count, "=")
            if (stage[2] == "cqe_processing") { if (count[2] < 4) exit 1 }
            else if (count[2] != 4) exit 1
            seen++
        }
        END { if (seen != 6) exit 1 }
    ' "$scratch/diagnostics.log"
fi

if BENCH_RING_ENTRIES=3 "$concurrent" std_cached_pread "$scratch/random.bin" 1048576 4096 1 1; then
    echo "non-power-of-two ring depth was accepted" >&2
    exit 1
fi
if "$concurrent" uring_cached_read "$scratch/random.bin" 1048576 4096 1 1 0; then
    echo "zero shards were accepted" >&2
    exit 1
fi
echo "benchmark schemas, warmup, saturation and byte-exact strategies passed"
