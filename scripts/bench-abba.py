#!/usr/bin/env python3
# Copyright 2024 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Warm-cache driver ABBA runner. Does not stop services or clear global caches."""

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


GEOMETRY = ("shards", "file_size", "read_size", "concurrency", "ops", "workers", "ring_entries", "warmup_ops")


def parse_row(header, output, expected, interval):
    names = next(csv.reader([header]))
    if len(names) != len(set(names)):
        raise ValueError("duplicate CSV column")
    rows = list(csv.reader(io.StringIO(output)))
    if len(rows) != 1 or len(rows[0]) != len(names):
        raise ValueError("expected exactly one row matching the executable header")
    row = dict(zip(names, rows[0]))
    required = {*GEOMETRY, "diagnostics_interval", "secs", "IOPS", "MBps", "p50_us", "p99_us", "p999_us",
                "startup_secs", "shutdown_secs", "schema_version", "mode", "strategy"}
    if not required.issubset(row):
        raise ValueError("missing required CSV columns")
    if row.get("schema_version") != "2" or row.get("mode") != "measure":
        raise ValueError("not a schema-v2 measurement")
    if row.get("strategy") != "uring_cached_read":
        raise ValueError("unexpected backend")
    for key in (*GEOMETRY, "diagnostics_interval"):
        wanted = interval if key == "diagnostics_interval" else expected[key]
        if int(row[key]) != wanted:
            raise ValueError(f"configuration mismatch: {key}")
    for key in ("secs", "IOPS", "MBps", "p50_us", "p99_us", "p999_us", "startup_secs", "shutdown_secs"):
        value = float(row[key])
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid numeric field: {key}")
        row[key] = value
    if row["secs"] <= 0 or row["IOPS"] <= 0:
        raise ValueError("empty measurement")
    computed_iops = expected["ops"] / row["secs"]
    if abs(row["IOPS"] - computed_iops) > max(1.0, computed_iops * 0.00001):
        raise ValueError("IOPS does not match operation count and workload duration")
    if abs(row["MBps"] - computed_iops * expected["read_size"] / (1024 * 1024)) > 0.1:
        raise ValueError("throughput does not match read geometry")
    return row


def percent_change(after, before):
    if before == 0:
        if after == 0:
            return 0.0
        raise ValueError("zero baseline prevents percentage attribution")
    result = 100.0 * (after / before - 1.0)
    if not math.isfinite(result):
        raise ValueError("non-finite percentage change")
    return result


def evaluate_round(rows, throughput_drift, tail_drift):
    if [row["leg"] for row in rows] != ["A1", "B1", "B2", "A2"]:
        raise ValueError("incomplete or reordered ABBA round")
    a1, b1, b2, a2 = [row["measurement"] for row in rows]
    drift = {
        "iops_pct": percent_change(a2["IOPS"], a1["IOPS"]),
        "p99_pct": percent_change(a2["p99_us"], a1["p99_us"]),
    }
    valid = abs(drift["iops_pct"]) <= throughput_drift and abs(drift["p99_pct"]) <= tail_drift
    result = {"valid": valid, "baseline_drift": drift}
    # Do not calculate candidate attribution after a failed baseline gate.
    if valid:
        result["candidate_change_pct"] = {
            key: percent_change((b1[key] + b2[key]) / 2, (a1[key] + a2[key]) / 2)
            for key in ("IOPS", "p99_us", "p999_us")
        }
    return result


def environment_guard(unit):
    if unit:
        state = subprocess.run(["systemctl", "is-active", unit], text=True, capture_output=True, check=False)
        if state.stdout.strip() != "inactive":
            raise RuntimeError("required service is not inactive; no service state was changed")
    processes = subprocess.check_output(["ps", "-eo", "comm="], text=True).splitlines()
    conflicts = {"rustfs", "warp", "cargo", "rustc", "fio", "samply", "perf", "Runner.Worker"}
    if any(name.strip() in conflicts for name in processes):
        raise RuntimeError("another build, load generator, service or CI worker is active")


def execute(command, env, stdout, stderr, timeout, unit):
    with stdout.open("w") as out, stderr.open("w") as err:
        process = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    code = process.wait(timeout=0.5)
                    break
                except subprocess.TimeoutExpired:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("benchmark exceeded the per-leg deadline")
                    environment_guard(unit)
            environment_guard(unit)
            if code != 0:
                raise RuntimeError(f"benchmark failed with exit code {code}; inspect the leg stderr")
        except BaseException:
            # The time wrapper can exit before its benchmark child. The owned
            # process group can therefore still need cleanup after wait/poll
            # has reaped its leader.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                process.wait()
            raise


def sha256(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def data_identity(path):
    stat = path.stat()
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def parse_resources(output):
    values = next(csv.reader([output.strip()]))
    keys = ("user_seconds", "system_seconds", "wall_seconds", "max_rss_kib", "voluntary_switches", "involuntary_switches")
    if len(values) != len(keys):
        raise ValueError("incomplete process resource report")
    result = dict(zip(keys, map(float, values)))
    if any(not math.isfinite(value) or value < 0 for value in result.values()):
        raise ValueError("invalid process resource report")
    if result["wall_seconds"] <= 0 or result["max_rss_kib"] <= 0:
        raise ValueError("empty process resource report")
    return result


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--data-file", type=Path, required=True, help="pre-created, byte-verified benchmark dataset")
    parser.add_argument("--run-dir", type=Path, required=True, help="new output directory; existing paths are refused")
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--reservation-note", required=True)
    parser.add_argument("--require-inactive-unit")
    parser.add_argument("--cpus", default="0-7")
    parser.add_argument("--read-size", type=int, default=32768)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--shards", type=int, default=2)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--entries", type=int, default=64)
    parser.add_argument("--ops", type=int, default=1_000_000)
    parser.add_argument("--warmup-ops", type=int, default=10000)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--candidate-interval", type=int, choices=(0, 64), default=64,
                        help="0 supports A/A calibration; 64 compares diagnostics on against off")
    parser.add_argument("--throughput-drift-pct", type=float, default=3)
    parser.add_argument("--p99-drift-pct", type=float, default=5)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--cooldown", type=float, default=1)
    parser.add_argument("--min-seconds", type=float, default=5)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not (100000 <= args.ops <= 10_000_000) or args.rounds < 3:
        parser.error("acceptance requires 100000..10000000 operations and at least three rounds")
    if any(not math.isfinite(value) or value < 0 for value in (
        args.throughput_drift_pct, args.p99_drift_pct, args.timeout, args.cooldown, args.min_seconds
    )) or args.timeout == 0 or args.min_seconds == 0:
        parser.error("invalid gate or duration")
    if not (1 <= args.workers <= 1024 and 1 <= args.shards <= 64 and 1 <= args.concurrency <= 4096):
        parser.error("invalid worker/shard/concurrency count")
    if not (1 <= args.entries <= 32768) or args.entries & (args.entries - 1):
        parser.error("entries must be a power of two in 1..32768")
    if not (1 <= args.read_size <= 1 << 30 and 0 <= args.warmup_ops <= 10_000_000):
        parser.error("invalid read size or warmup count")
    if args.data_file.is_symlink() or not args.data_file.is_file():
        parser.error("data-file must be a regular, pre-created file, not a symlink")
    if args.data_file.stat().st_size < args.read_size + 4096:
        parser.error("dataset is too small")
    return args


def main():
    args = arguments()
    binaries = {"A": args.baseline.resolve(), "B": args.candidate.resolve()}
    headers = {key: subprocess.check_output([str(path), "--header"], text=True).strip() for key, path in binaries.items()}
    if headers["A"] != headers["B"]:
        raise ValueError("baseline and candidate schema differ")
    expected = dict(shards=args.shards, file_size=args.data_file.stat().st_size, read_size=args.read_size,
                    concurrency=args.concurrency, ops=args.ops, workers=args.workers,
                    ring_entries=args.entries, warmup_ops=args.warmup_ops)
    plan = [(round_id, leg) for round_id in range(1, args.rounds + 1) for leg in ("A1", "B1", "B2", "A2")]
    if args.dry_run:
        print(json.dumps({"plan": plan, "geometry": expected, "candidate_interval": args.candidate_interval}))
        return 0
    environment_guard(args.require_inactive_unit)
    args.run_dir.mkdir(mode=0o700)
    provenance = {"source_revision": args.source_revision, "reservation": args.reservation_note,
                  "cache": "warm-preload", "geometry": expected, "cpus": args.cpus,
                  "data_identity": data_identity(args.data_file),
                  "binaries": {key: {"path": str(path), "sha256": sha256(path)} for key, path in binaries.items()},
                  "gates": {"throughput_drift_pct": args.throughput_drift_pct, "p99_drift_pct": args.p99_drift_pct,
                            "min_seconds": args.min_seconds}, "candidate_interval": args.candidate_interval}
    (args.run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    summary = {"status": "incomplete", "rounds": []}
    try:
        current_round = []
        for round_id, leg in plan:
            environment_guard(args.require_inactive_unit)
            if sha256(binaries[leg[0]]) != provenance["binaries"][leg[0]]["sha256"]:
                raise RuntimeError("binary changed during the experiment")
            if data_identity(args.data_file) != provenance["data_identity"]:
                raise RuntimeError("dataset changed during the experiment")
            time.sleep(args.cooldown)
            preload_deadline = time.monotonic() + args.timeout
            with args.data_file.open("rb") as file:
                while file.read(8 << 20):
                    if time.monotonic() >= preload_deadline:
                        raise RuntimeError("dataset preload exceeded the deadline")
            prefix = args.run_dir / f"round-{round_id}-{leg}"
            env = dict(os.environ, BENCH_WORKERS=str(args.workers), BENCH_RING_ENTRIES=str(args.entries),
                       BENCH_WARMUP_OPS=str(args.warmup_ops), BENCH_VERIFY="0",
                       BENCH_DIAGNOSTICS="1" if leg[0] == "B" and args.candidate_interval else "0")
            command = ["/usr/bin/time", "-f", "%U,%S,%e,%M,%w,%c", "-o", str(prefix) + ".resources.csv",
                       "taskset", "-c", args.cpus, str(binaries[leg[0]]), "uring_cached_read", str(args.data_file),
                       str(expected["file_size"]), str(args.read_size), str(args.concurrency), str(args.ops), str(args.shards)]
            execute(command, env, Path(str(prefix) + ".csv"), Path(str(prefix) + ".stderr"),
                    args.timeout, args.require_inactive_unit)
            interval = args.candidate_interval if leg[0] == "B" else 0
            row = parse_row(headers[leg[0]], Path(str(prefix) + ".csv").read_text(), expected, interval)
            resources = parse_resources(Path(str(prefix) + ".resources.csv").read_text())
            if data_identity(args.data_file) != provenance["data_identity"]:
                raise RuntimeError("dataset changed during measurement")
            if row["secs"] < args.min_seconds:
                raise RuntimeError("measurement too short; increase operations in a new experiment")
            if row["secs"] > resources["wall_seconds"] + 0.02:
                raise RuntimeError("workload duration exceeds the process resource interval")
            record = {"round": round_id, "leg": leg, "measurement": row, "resources": resources}
            Path(str(prefix) + ".json").write_text(json.dumps(record, indent=2, allow_nan=False) + "\n")
            current_round.append(record)
            print(f"round={round_id} leg={leg} IOPS={row['IOPS']:.0f} p99_us={row['p99_us']:.0f}", flush=True)
            if leg == "A2":
                result = evaluate_round(current_round, args.throughput_drift_pct, args.p99_drift_pct)
                summary["rounds"].append(result)
                current_round = []
                if not result["valid"]:
                    raise RuntimeError("baseline drift failed; stopped before expanding the experiment")
        summary["status"] = "valid-comparison"
        summary["scope"] = "driver-only; resource CSV is whole-process CPU/RSS, not steady-state-only"
        return 0
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        summary["status"] = "invalid"
        summary["reason"] = str(error)
        print(str(error), file=sys.stderr)
        return 1
    finally:
        (args.run_dir / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
