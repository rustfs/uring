# Copyright 2024 RustFS Team
# SPDX-License-Identifier: Apache-2.0
import importlib.util
import os
from pathlib import Path
import select
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("bench_abba", Path(__file__).with_name("bench-abba.py"))
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


class Gates(unittest.TestCase):
    def round(self, a2_iops=100, a2_p99=20):
        return [
            {"leg": leg, "measurement": dict(IOPS=iops, p99_us=p99, p999_us=30)}
            for leg, iops, p99 in [("A1", 100, 20), ("B1", 90, 22), ("B2", 90, 22), ("A2", a2_iops, a2_p99)]
        ]

    def test_stable_baseline_reports_regression_not_improvement(self):
        result = BENCH.evaluate_round(self.round(), 3, 5)
        self.assertTrue(result["valid"])
        self.assertAlmostEqual(result["candidate_change_pct"]["IOPS"], -10)
        self.assertAlmostEqual(result["candidate_change_pct"]["p99_us"], 10)

    def test_drift_rejects_both_directions_without_attribution(self):
        for iops in (90, 110):
            result = BENCH.evaluate_round(self.round(a2_iops=iops), 3, 5)
            self.assertFalse(result["valid"])
            self.assertNotIn("candidate_change_pct", result)

    def test_tail_drift_alone_invalidates_round(self):
        self.assertFalse(BENCH.evaluate_round(self.round(a2_p99=22), 3, 5)["valid"])

    def test_incomplete_or_reordered_round_is_rejected(self):
        for rows in (self.round()[:3], list(reversed(self.round()))):
            with self.assertRaises(ValueError):
                BENCH.evaluate_round(rows, 3, 5)

    def test_zero_baseline_is_not_a_valid_percentage(self):
        with self.assertRaises(ValueError):
            BENCH.percent_change(1, 0)

    def test_missing_or_nonfinite_resource_reports_are_rejected(self):
        self.assertEqual(BENCH.parse_resources("1,2,5,1000,3,4")["max_rss_kib"], 1000)
        for invalid in ("1,2", "1,2,nan,3,4,5", "1,2,3,0,0,0"):
            with self.assertRaises(ValueError):
                BENCH.parse_resources(invalid)

    def test_verification_and_geometry_drift_are_rejected(self):
        expected = dict.fromkeys(BENCH.GEOMETRY, 1)
        names = ["schema_version", "mode", "strategy", *BENCH.GEOMETRY, "diagnostics_interval",
                 "secs", "IOPS", "MBps", "p50_us", "p99_us", "p999_us", "startup_secs", "shutdown_secs"]
        values = ["2", "measure", "uring_cached_read", *(["1"] * len(BENCH.GEOMETRY)), "64", *(["1"] * 8)]
        values[names.index("MBps")] = "0.0"
        header = ",".join(names)
        BENCH.parse_row(header, ",".join(values), expected, 64)
        with self.assertRaises(ValueError):
            BENCH.parse_row("schema_version,mode", "2,measure", expected, 64)
        for key, replacement in (("mode", "verify"), ("shards", "2"), ("IOPS", "nan"),
                                 ("IOPS", "100"), ("diagnostics_interval", "0")):
            invalid = list(values)
            invalid[names.index(key)] = replacement
            with self.assertRaises(ValueError):
                BENCH.parse_row(header, ",".join(invalid), expected, 64)

    def test_timeout_terminates_the_owned_process_group(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(BENCH, "environment_guard"):
            out = Path(directory) / "out"
            err = Path(directory) / "err"
            command = [sys.executable, "-c", "import os,time; print(os.getpid(), flush=True); time.sleep(30)"]
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                BENCH.execute(command, os.environ.copy(), out, err, 0.01, None)
            pid = int(out.read_text().strip())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_failed_leader_does_not_leave_its_child_running(self):
        # The child holds a FIFO writer open before allowing the leader to exit.
        # EOF proves it exited without depending on orphan/zombie reaping timing.
        child = """
import os, sys, time
print(os.getpid(), flush=True)
ready_read, ready_write = os.pipe()
if os.fork() == 0:
    os.close(ready_read)
    writer = os.open(sys.argv[1], os.O_WRONLY)
    os.write(writer, b"ready")
    os.write(ready_write, b"ready")
    time.sleep(30)
    os._exit(0)
os.close(ready_write)
os.read(ready_read, 5)
os._exit(7)
"""
        with tempfile.TemporaryDirectory() as directory, patch.object(BENCH, "environment_guard"):
            out = Path(directory) / "out"
            err = Path(directory) / "err"
            fifo = Path(directory) / "child-lifetime"
            os.mkfifo(fifo)
            reader = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
            try:
                with self.assertRaisesRegex(RuntimeError, "exit code 7"):
                    BENCH.execute([sys.executable, "-c", child, str(fifo)], os.environ.copy(), out, err, 5, None)
                self.assertEqual(os.read(reader, 5), b"ready")
                readable, _, _ = select.select([reader], [], [], 5)
                self.assertEqual(readable, [reader], "benchmark descendant still holds the FIFO open")
                self.assertEqual(os.read(reader, 1), b"")
            finally:
                os.close(reader)
                # Also clean up when this regression is run against broken code.
                if out.exists() and out.read_text().strip():
                    try:
                        os.killpg(int(out.read_text().strip()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_missing_process_group_preserves_the_benchmark_failure(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(BENCH, "environment_guard"), \
                patch.object(BENCH.os, "killpg", side_effect=ProcessLookupError) as killpg:
            out = Path(directory) / "out"
            err = Path(directory) / "err"
            with self.assertRaisesRegex(RuntimeError, "exit code 7"):
                BENCH.execute([sys.executable, "-c", "raise SystemExit(7)"], os.environ.copy(), out, err, 5, None)
            killpg.assert_called_once()


if __name__ == "__main__":
    unittest.main()
