"""Regressions for failure propagation and benchmark acceptance boundaries."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import ci


class AggregateTests(unittest.TestCase):
    def test_main_requires_benchmark_success(self):
        self.assertTrue(ci.aggregate_success("push", "refs/heads/main", "success", "success", "success"))
        for result in ("failure", "cancelled", "skipped", None):
            with self.subTest(result=result):
                self.assertFalse(ci.aggregate_success("push", "refs/heads/main", "success", "success", result))

    def test_pull_request_requires_functional_success_and_expected_benchmark_skip(self):
        self.assertTrue(ci.aggregate_success("pull_request", "refs/pull/1/merge", "success", "success", "skipped"))
        for preflight, validation in (("failure", "skipped"), ("success", "cancelled"), ("success", "skipped")):
            with self.subTest(preflight=preflight, validation=validation):
                self.assertFalse(ci.aggregate_success("pull_request", "refs/pull/1/merge", preflight, validation, "skipped"))

    def test_unknown_event_is_not_a_success(self):
        self.assertFalse(ci.aggregate_success(None, None, "success", "success", "skipped"))


class CommandTests(unittest.TestCase):
    def test_output_capture_retains_nonzero_exit_and_stops_following_work(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "command.log"
            reached = False
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                ci.checked_command([sys.executable, "-c", "print('retained failure'); raise SystemExit(7)"], log,
                                   cwd=temporary, capture=True)
                reached = True
            self.assertEqual(caught.exception.returncode, 7)
            self.assertFalse(reached)
            self.assertIn("retained failure", log.read_text())

    def test_successful_output_is_captured(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = ci.checked_command([sys.executable, "-c", "print('ok')"], Path(temporary) / "command.log",
                                        cwd=temporary, capture=True)
            self.assertEqual(result.strip(), "ok")


@unittest.skipIf(os.name == "nt", "Unix temporary-directory aliases")
class TemporaryDirectoryTests(unittest.TestCase):
    def test_runner_resolves_system_alias_before_creating_fixture_descendants(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            actual = root / "actual"
            actual.mkdir(mode=0o700)
            alias = root / "alias"
            alias.symlink_to(actual, target_is_directory=True)
            with mock.patch.object(ci, "ROOT", root), \
                    mock.patch.object(ci, "resolve_toolchain", return_value=root), \
                    mock.patch.object(ci.tempfile, "gettempdir", return_value=str(alias)):
                runner = ci.Runner("macos")
            self.assertEqual(runner.env["TMPDIR"], str(actual))
            self.assertEqual(runner.env["RUST_TEST_NOCAPTURE"], "1")

    def test_missing_temporary_parent_fails_before_commands_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            with mock.patch.object(ci, "ROOT", root), \
                    mock.patch.object(ci, "resolve_toolchain", return_value=root), \
                    mock.patch.object(ci.tempfile, "gettempdir", return_value=str(root / "missing")):
                with self.assertRaises(FileNotFoundError):
                    ci.Runner("macos")


class BenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        evidence = ci.ROOT / "performance-evidence/phase5-windows-gnu-2026-09-15.json"
        cls.reports = json.loads(evidence.read_text())["scenarios"]

    def report(self, scenario="http"):
        result = copy.deepcopy(next(report for report in self.reports if report["scenario"] == scenario))
        result["os"] = "linux"
        return result

    def test_complete_transport_and_administrative_shapes_pass(self):
        for scenario in ci.SCENARIOS:
            with self.subTest(scenario=scenario):
                ci.validate_benchmark(self.report(scenario), scenario)

    def test_missing_false_or_out_of_bounds_evidence_is_rejected(self):
        changes = {"complete": False, "passed": False, "os": "windows", "samples": 19_999,
                   "ranges": 999, "verificationCalls": 999, "maxBurstCalls": 1_001,
                   "maxBurstMs": 501, "cooldownMs": 249, "p99Us": 50_001,
                   "elapsedScenarioMs": 90_001, "renewedBarrierAfterWarmup": False,
                   "perStatusRangeCheck": False, "operations": {}, "maxSampledRssBytes": 1 << 60,
                   "controlCalls": 999, "shutdownDrainBoundary": None, "shutdownAcknowledgementUs": 50_001}
        for field, value in changes.items():
            with self.subTest(field=field):
                report = self.report()
                report[field] = value
                with self.assertRaises(RuntimeError):
                    ci.validate_benchmark(report, "http")
        report = self.report()
        del report["maxBurstMs"]
        with self.assertRaises(RuntimeError):
            ci.validate_benchmark(report, "http")

    def test_slow_operation_is_rejected_even_when_aggregate_passes(self):
        report = self.report()
        next(iter(report["operations"].values()))["p99Us"] = 50_001
        with self.assertRaises(RuntimeError):
            ci.validate_benchmark(report, "http")

    def test_missing_mutation_measurement_is_rejected(self):
        for remove in (True, False):
            report = self.report()
            if remove:
                del report["operations"]["pause"]
            else:
                report["operations"]["pause"]["calls"] -= 1
            with self.assertRaises(RuntimeError):
                ci.validate_benchmark(report, "http")

    def test_incomplete_administration_is_rejected(self):
        for mutate in (lambda r: r["operations"].pop(),
                       lambda r: r["operations"][0].update(completed=False),
                       lambda r: r["operations"][0].update(maxQueryBurstUs=500_001),
                       lambda r: r.update(resultSetupRemovals=127)):
            report = self.report("administrative")
            mutate(report)
            with self.assertRaises(RuntimeError):
                ci.validate_benchmark(report, "administrative")

    def test_administrative_target_and_result_counts_are_exact(self):
        for method, field in (("aria2.unpauseAll", "resumedTasks"),
                              ("aria2.pauseAll", "pausedTasks"),
                              ("aria2.purgeDownloadResult", "deletedTasks")):
            for changed in ("targets", field):
                with self.subTest(method=method, field=changed):
                    report = self.report("administrative")
                    operation = next(item for item in report["operations"] if item["method"] == method)
                    operation[changed] += 1
                    with self.assertRaises(RuntimeError):
                        ci.validate_benchmark(report, "administrative")


if __name__ == "__main__":
    unittest.main()
