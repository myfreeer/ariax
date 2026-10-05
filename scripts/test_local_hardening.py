import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import local_hardening as hardening


class RecordedCommands(unittest.TestCase):
    def test_success_and_nonzero_exit_preserve_logs_and_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            for status in (0, 7):
                with self.subTest(status=status):
                    path = Path(temporary) / str(status)
                    record = hardening.run([sys.executable, "-c", f"print('retained'); raise SystemExit({status})"],
                                           path, timeout=5)
                    self.assertEqual(record["passed"], status == 0)
                    self.assertEqual(record["exitCode"], status)
                    self.assertIn("retained", (path / "stdout.log").read_text())
                    self.assertEqual(record["stdout.logSha256"], hardening.digest(path / "stdout.log"))
                    self.assertEqual(record, json.loads((path / "result.json").read_text()))
                    with self.assertRaises(FileExistsError):
                        hardening.run([sys.executable], path, timeout=1)

    def test_expected_count_rejects_missing_zero_failed_ignored_and_wrong_results(self):
        self.assertTrue(hardening.test_counts("test result: ok. 1 passed; 0 failed; 0 ignored;", 1))
        for output in ("", "test result: ok. 0 passed; 0 failed; 0 ignored;",
                       "test result: FAILED. 1 passed; 1 failed; 0 ignored;",
                       "test result: ok. 1 passed; 0 failed; 1 ignored;",
                       "test result: ok. 2 passed; 0 failed; 0 ignored;"):
            self.assertFalse(hardening.test_counts(output, 1))
        with tempfile.TemporaryDirectory() as temporary:
            record = hardening.run([sys.executable, "-c", "pass"], Path(temporary) / "missing",
                                   timeout=5, expected_tests=1)
            self.assertFalse(record["passed"])

    def test_child_and_parent_results_require_the_explicit_combined_count(self):
        output = "test result: ok. 1 passed; 0 failed; 0 ignored;\n" * 2
        self.assertTrue(hardening.test_counts(output, 2))
        self.assertFalse(hardening.test_counts(output, 1))
        self.assertFalse(hardening.test_counts(output.replace("ok.", "FAILED.", 1), 2))

    def test_timeout_terminates_child_and_records_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            record = hardening.run([sys.executable, "-c", "import time; time.sleep(60)"],
                                   Path(temporary) / "timeout", timeout=0.1)
            self.assertFalse(record["passed"])
            self.assertIsNotNone(record["exitCode"])
            self.assertIn("terminated", record["cleanup"])

    @unittest.skipIf(os.name == "nt", "POSIX session cleanup")
    def test_interruption_still_terminates_the_owned_group(self):
        with tempfile.TemporaryDirectory() as temporary:
            original = subprocess.Popen.wait
            first = True

            def interrupt_once(process, *args, **kwargs):
                nonlocal first
                if first:
                    first = False
                    raise KeyboardInterrupt()
                return original(process, *args, **kwargs)

            with mock.patch.object(subprocess.Popen, "wait", interrupt_once):
                record = hardening.run([sys.executable, "-c", "import time; time.sleep(60)"],
                                       Path(temporary) / "interrupted", timeout=5)
            self.assertFalse(record["passed"])
            self.assertEqual(record["error"], "KeyboardInterrupt")
            self.assertEqual(record["cleanup"], "owned-process-group-terminated")

    @unittest.skipIf(os.name == "nt", "POSIX process groups")
    def test_cleanup_error_cannot_produce_a_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(hardening.os, "killpg", side_effect=PermissionError("cleanup refused")):
                record = hardening.run([sys.executable, "-c", "pass"], Path(temporary) / "cleanup", timeout=5)
            self.assertFalse(record["passed"])
            self.assertEqual(record["cleanup"], "failed")


class FuzzAccounting(unittest.TestCase):
    def test_counts_separate_initialization_and_mutation(self):
        value = hardening.fuzz_counts("#9 INITED cov: 12\n#64 DONE cov: 19\nDone 64 runs in 0 second(s)", 64)
        self.assertEqual(value, {"executions": 64, "initializationExecutions": 9,
                                 "mutationExecutions": 55, "coverage": 19})

    def test_missing_instrumentation_short_runs_and_initialization_only_fail(self):
        for output in ("Done 64 runs in 0 second(s)",
                       "#9 INITED cov: 12\n#63 DONE cov: 19\nDone 63 runs in 0 second(s)",
                       "#64 INITED cov: 12\n#64 DONE cov: 19\nDone 64 runs in 0 second(s)"):
            with self.assertRaises(ValueError):
                hardening.fuzz_counts(output, 64)

    def test_campaign_retains_failed_process_without_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            seeds = root / "seeds"
            seeds.mkdir()
            (seeds / "seed").write_bytes(b"hello")
            with mock.patch.object(hardening, "run", return_value={"passed": False, "exitCode": 7}) as run:
                result = hardening.fuzz(Path(sys.executable), seeds, root / "campaign")
            self.assertFalse(result["passed"])
            self.assertEqual(len(result["attempts"]), 1)
            self.assertEqual(result["acceptedExecutions"], 0)
            run.assert_called_once()

    def test_exhausted_budget_is_incomplete(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = hardening.fuzz(Path(sys.executable), Path(temporary), Path(temporary) / "campaign", seconds=0)
            self.assertFalse(result["passed"])
            self.assertEqual(result["acceptedExecutions"], 0)
            self.assertIn("budget exhausted", result["error"])


if __name__ == "__main__":
    unittest.main()
