#!/usr/bin/env bash
# Copyright 2024 RustFS Team
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${BENCH_SKIP_BUILD:-0}" != 1 ]]; then
    cargo build --locked --example ordered_prefetch --example streaming_bench
fi
bin="${BENCH_BIN_DIR:-${CARGO_TARGET_DIR:-target}/debug/examples}"
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT

# Reuse the existing deterministic fixture generator/untimed byte verifier.
# This smoke checks contracts; no output is performance evidence.
BENCH_VERIFY=1 "$bin/streaming_bench" std_buffered "$scratch/data.bin" \
    1048583 65536 1 4096 >/dev/null

check_range() {
    local expected=$1
    shift
    local output
    output=$(timeout --kill-after=5s 30s "$bin/ordered_prefetch" "$scratch/data.bin" "$@")
    if [[ "$output" != "ORDERED_PREFETCH_OK bytes=$expected chunks="* ]]; then
        echo "missing ordered-prefetch positive marker: $output" >&2
        exit 1
    fi
}

# A non-aligned range spanning many windows, with a tighter byte than slot cap.
check_range 65543 17 65543 4097 4 8194
# Early EOF must deliver only real bytes and terminate the ordered tail.
check_range 13 1048570 1000 7 8 56
check_range 0 0 0 1024 4 4096

reject() {
    local expected=$1
    shift
    local code=0
    timeout --kill-after=5s 30s "$bin/ordered_prefetch" "$scratch/data.bin" "$@" \
        >"$scratch/rejected.stdout" 2>"$scratch/rejected.stderr" || code=$?
    if [[ "$code" != 1 || -s "$scratch/rejected.stdout" ]] ||
        ! grep -Fqx "ordered_prefetch: $expected" "$scratch/rejected.stderr"; then
        echo "invalid geometry did not return the normal CLI error (exit=$code)" >&2
        cat "$scratch/rejected.stderr" >&2
        exit 1
    fi
}
reject 'chunk must be 1..=8 MiB and window 1..=64' 0 100 0 4 4096
reject 'chunk must be 1..=8 MiB and window 1..=64' 0 100 16 0 4096
reject 'max_bytes must be at least chunk and at most 512 MiB' 0 100 16 4 0
reject 'range end must fit signed file offsets' 18446744073709551615 2 16 4 4096
echo "ordered-prefetch range, byte budget, EOF and validation smoke passed"
