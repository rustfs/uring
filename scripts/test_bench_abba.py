# Copyright 2024 RustFS Team
# SPDX-License-Identifier: Apache-2.0
import importlib.util
import io
import json
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


class CalibrationGates(unittest.TestCase):
    HEADER = ("schema_version,mode,strategy,shards,file_size,read_size,concurrency,ops,secs,IOPS,MBps,"
              "p50_us,p99_us,p999_us,startup_secs,shutdown_secs,workers,ring_entries,warmup_ops,diagnostics_interval")

    def rows(self):
        return [{"leg": leg, "measurement": dict(IOPS=100, p99_us=20, p999_us=30)}
                for leg in ("A1", "B1", "B2", "A2")]

    def argv(self, root, *extra):
        (root / "baseline").write_bytes(b"identical executable content")
        (root / "candidate").write_bytes(b"identical executable content")
        (root / "data").write_bytes(bytes(36864))
        return ["bench-abba.py", "--baseline", str(root / "baseline"), "--candidate", str(root / "candidate"),
                "--data-file", str(root / "data"), "--run-dir", str(root / "results"),
                "--source-revision", "test-revision", "--reservation-note", "unit test; no benchmark executed",
                "--calibration", "--candidate-interval", "0", *extra]

    def test_calibration_success_has_no_candidate_attribution(self):
        result = BENCH.evaluate_round(self.rows(), 3, 5, calibration=True)
        self.assertTrue(result["valid"])
        self.assertEqual(set(result["middle_drift"]), {"B1", "B2"})
        self.assertNotIn("candidate_change_pct", result)

    def test_each_middle_leg_is_checked_without_opposite_noise_cancellation(self):
        for metric, low, high in (("IOPS", 90, 110), ("p99_us", 18, 22)):
            with self.subTest(metric=metric):
                rows = self.rows()
                rows[1]["measurement"][metric] = low
                rows[2]["measurement"][metric] = high
                result = BENCH.evaluate_round(rows, 3, 5, calibration=True)
                self.assertFalse(result["valid"])
                self.assertNotIn("candidate_change_pct", result)

    def test_middle_gates_use_endpoint_mean(self):
        rows = self.rows()
        for row, iops in zip(rows, (100, 103, 103, 102)):
            row["measurement"]["IOPS"] = iops
        result = BENCH.evaluate_round(rows, 3, 5, calibration=True)
        self.assertTrue(result["valid"])
        self.assertAlmostEqual(result["middle_drift"]["B1"]["iops_pct"], 100 * (103 / 101 - 1))

    def test_calibration_endpoint_drift_still_invalidates_without_attribution(self):
        for metric, changed in (("IOPS", 110), ("p99_us", 22)):
            with self.subTest(metric=metric):
                rows = self.rows()
                rows[3]["measurement"][metric] = changed
                result = BENCH.evaluate_round(rows, 3, 5, calibration=True)
                self.assertFalse(result["valid"])
                self.assertNotIn("candidate_change_pct", result)

    def test_calibration_requires_zero_interval_and_three_rounds(self):
        for extra in (("--candidate-interval", "64"), ("--rounds", "2")):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with patch.object(sys, "argv", self.argv(root, *extra)), patch("sys.stderr", new_callable=io.StringIO):
                    with self.assertRaises(SystemExit) as error:
                        BENCH.arguments()
                self.assertEqual(error.exception.code, 2)
                self.assertFalse((root / "results").exists())

    def test_default_mode_still_compares_different_feature_states(self):
        with tempfile.TemporaryDirectory() as directory:
            argv = self.argv(Path(directory))
            argv.remove("--calibration")
            del argv[-2:]  # Preserve the default candidate interval of 64.
            with patch.object(sys, "argv", argv):
                args = BENCH.arguments()
            self.assertFalse(args.calibration)
            self.assertEqual(args.candidate_interval, 64)

    def test_different_binary_hashes_reject_calibration_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = self.argv(root)
            (root / "candidate").write_bytes(b"different executable content")
            with patch.object(sys, "argv", argv), patch.object(BENCH.subprocess, "check_output") as command:
                with self.assertRaisesRegex(ValueError, "same binary"):
                    BENCH.main()
            command.assert_not_called()
            self.assertFalse((root / "results").exists())

    def test_calibration_dry_run_validates_identity_and_reports_its_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(sys, "argv", self.argv(root, "--dry-run")), \
                    patch.object(BENCH.subprocess, "check_output", return_value=self.HEADER), \
                    patch("sys.stdout", new_callable=io.StringIO) as output:
                self.assertEqual(BENCH.main(), 0)
            plan = json.loads(output.getvalue())
            self.assertEqual(plan["mode"], "calibration")
            self.assertEqual(len(plan["plan"]), 12)
            self.assertFalse((root / "results").exists())

    def run_main_with_measurements(self, root, invalid_leg=None):
        executed = []

        def execute(command, env, stdout, stderr, timeout, unit):
            executed.append(stdout.stem)
            self.assertEqual(env["BENCH_DIAGNOSTICS"], "0")
            iops = 90000 if stdout.stem == invalid_leg else 100000
            secs = 1_000_000 / iops
            stdout.write_text(f"2,measure,uring_cached_read,2,36864,32768,32,1000000,{secs:.6f},{iops},"
                              f"{iops / 32:.1f},10,20,30,0.1,0.1,4,64,10000,0\n")
            stderr.write_text("")
            Path(command[4]).write_text(f"1,1,{secs + 1},1000,3,4\n")

        with patch.object(sys, "argv", self.argv(root)), \
                patch.object(BENCH.subprocess, "check_output", return_value=self.HEADER), \
                patch.object(BENCH, "environment_guard"), patch.object(BENCH.time, "sleep"), \
                patch.object(BENCH, "execute", side_effect=execute), \
                patch("sys.stdout", new_callable=io.StringIO), patch("sys.stderr", new_callable=io.StringIO):
            code = BENCH.main()
        summary = json.loads((root / "results" / "summary.json").read_text())
        provenance = json.loads((root / "results" / "provenance.json").read_text())
        return code, summary, provenance, executed

    def test_three_valid_rounds_report_valid_calibration(self):
        with tempfile.TemporaryDirectory() as directory:
            code, summary, provenance, executed = self.run_main_with_measurements(Path(directory))
        self.assertEqual(code, 0)
        self.assertEqual(summary["mode"], "calibration")
        self.assertEqual(summary["status"], "valid-calibration")
        self.assertEqual(len(summary["rounds"]), 3)
        self.assertEqual(len(executed), 12)
        self.assertEqual(provenance["mode"], "calibration")
        self.assertEqual(provenance["binaries"]["A"]["sha256"], provenance["binaries"]["B"]["sha256"])
        self.assertNotIn("candidate_change_pct", json.dumps(summary))

    def test_middle_failure_stops_calibration_without_attribution_from_earlier_rounds(self):
        with tempfile.TemporaryDirectory() as directory:
            code, summary, _, executed = self.run_main_with_measurements(Path(directory), "round-2-B1")
        self.assertEqual(code, 1)
        self.assertEqual(summary["status"], "invalid")
        self.assertEqual(len(summary["rounds"]), 2)
        self.assertEqual(len(executed), 8)
        self.assertNotIn("candidate_change_pct", json.dumps(summary))


if __name__ == "__main__":
    unittest.main()
