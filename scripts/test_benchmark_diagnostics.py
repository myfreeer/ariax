"""Rejection boundaries and outlier accounting for local timing diagnostics."""
import copy
import json
import unittest

import benchmark_diagnostics
import ci


class DiagnosticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        reports = json.loads((ci.ROOT / "performance-evidence/phase5-windows-gnu-2026-09-15.json").read_text(encoding="utf-8"))
        cls.base = next(report for report in reports["scenarios"] if report["scenario"] == "http")

    def attempt(self, latency=1_000, *, compiler=False):
        report = copy.deepcopy(self.base)
        report.update(measurementKind="diagnostic-small", acceptanceEligible=False,
                      stalledConsumerCleanupVerified=True, ranges=16, samples=1_600,
                      controlCalls=80, verificationCalls=80, elapsedScenarioMs=4_000,
                      p99Us=latency, passed=latency <= 50_000)
        for operation in report["operations"].values():
            operation["calls"] = operation["calls"] * 80 // 1_000
            operation["p99Us"] = latency
        return {"report": report, "exitCode": 0 if report["passed"] else 1,
                "compilerProcessesObserved": [{"name": "cc"}] if compiler else []}

    def test_small_reports_validate_but_cannot_satisfy_full_acceptance(self):
        attempt = self.attempt()
        self.assertTrue(ci.validate_diagnostic_benchmark(attempt["report"], "http"))
        for marker in ("measurementKind", "acceptanceEligible"):
            full = copy.deepcopy(self.base)
            full[marker] = attempt["report"][marker]
            with self.assertRaisesRegex(RuntimeError, "diagnostic benchmarks"):
                ci.validate_benchmark(full, "http", expected_os="windows")

    def test_incomplete_wrong_geometry_and_missing_cleanup_are_rejected(self):
        for field, value in (("complete", False), ("ranges", 1_000), ("samples", 20_000),
                             ("controlCalls", 79), ("verificationCalls", 79),
                             ("stalledConsumerCleanupVerified", False), ("stalledCreditBytes", 0),
                             ("stalledEventCreditBytes", 0), ("acceptanceEligible", True),
                             ("measurementKind", "full"), ("os", "linux")):
            with self.subTest(field=field):
                report = self.attempt()["report"]
                report[field] = value
                with self.assertRaises(RuntimeError):
                    ci.validate_diagnostic_benchmark(report, "http")

    def test_fixed_filter_removes_slow_tail_and_preserves_compiler_attribution(self):
        attempts = [self.attempt(compiler=index == 0) for index in range(6)] + [self.attempt(20_000)]
        result = benchmark_diagnostics.summarize(attempts, "http")
        self.assertEqual(result["excludedTimingAttempts"], [7])
        self.assertEqual(result["filteredTimings"]["runs"], 6)
        self.assertEqual(result["filteredTimings"]["medianP99Us"], 1_000)
        self.assertEqual(result["quietFilteredTimings"]["runs"], 5)
        self.assertEqual(result["compilerOverlapRuns"], 1)
        self.assertFalse(result["acceptanceEligible"])

    def test_insufficient_repeats_and_cutoff_boundary_are_not_filtered(self):
        result = benchmark_diagnostics.summarize([self.attempt()] * 3 + [self.attempt(20_000)], "http")
        self.assertIsNone(result["upperCutoffUs"])
        self.assertEqual(result["excludedTimingAttempts"], [])
        result = benchmark_diagnostics.summarize([self.attempt()] * 4 + [self.attempt(2_000)], "http")
        self.assertEqual(result["upperCutoffUs"], 2_000)
        self.assertEqual(result["excludedTimingAttempts"], [])

    def test_filter_never_hides_functional_or_latency_gate_failures(self):
        attempts = [self.attempt()] * 5 + [self.attempt(60_000)]
        attempts += [{"error": "consumer cleanup failed", "compilerProcessesObserved": [{"name": "cc"}]}]
        result = benchmark_diagnostics.summarize(attempts, "http")
        self.assertEqual(result["excludedTimingAttempts"], [6])
        self.assertEqual(result["rawLatencyGateFailures"], 1)
        self.assertEqual(result["failureCount"], 1)
        self.assertFalse(result["allFunctionalChecksPassed"])
        self.assertFalse(result["allRawTimingGatesPassed"])
        self.assertTrue(result["failedAttempts"][0]["compilerOverlap"])

    def test_inconsistent_exit_status_and_claimed_success_are_rejected(self):
        first = self.attempt()
        first["exitCode"] = 1
        second = self.attempt(60_000)
        second["report"]["passed"] = True
        result = benchmark_diagnostics.summarize([first, second], "http")
        self.assertEqual(result["failureCount"], 2)
        self.assertIsNone(result["filteredTimings"])


if __name__ == "__main__":
    unittest.main()
