"""Validate new burst summaries without discarding historical benchmark evidence."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import ci


class BurstTimingTests(unittest.TestCase):
    def report(self):
        archive = ci.ROOT / "performance-evidence/phase6-benchmarks-rerun-2026-10-05.json"
        report = json.loads(archive.read_text())["scenarios"]["mixed-bt"]["report"]
        # Synthetic timing geometry for validator boundaries, not measurements.
        report.update(measuredBurstUs=1_000_000, measuredRoundTripUs=600_000)
        step = {"operation": "bt.changeOption", "phase": "verification", "sampleIndex": 1,
                "startUs": 110_000, "durationUs": 350_000}
        report["burstTiming"] = {
            "version": 1, "primaryUs": 600_000, "verificationUs": 350_000, "otherUs": 50_000,
            "worstCompletedBurst": {"burst": 2, "elapsedNs": 486_000_000, "elapsedUs": 486_000,
                                    "startedUnixNs": 1_791_187_200_000_000_000,
                                    "firstSampleIndex": 1, "primaryCalls": 1, "verificationCalls": 1,
                                    "primaryUs": 100_000, "verificationUs": 350_000, "otherUs": 36_000,
                                    "last": step, "slowest": copy.deepcopy(step)}}
        return report

    def test_full_and_small_reports_validate_present_diagnostics(self):
        report = self.report()
        ci.validate_benchmark(report, "mixed-bt", require_burst_timing=True)
        # Both public validators validate diagnostics when supplied. The small
        # fixture uses its real geometry through the existing diagnostic tests.
        import test_benchmark_diagnostics
        test_benchmark_diagnostics.DiagnosticTests.setUpClass()
        small = test_benchmark_diagnostics.DiagnosticTests().attempt()["report"]
        small.update(burstTiming=report["burstTiming"], measuredBurstUs=1_000_000,
                     measuredRoundTripUs=600_000, maxBurstMs=486)
        for name in ("last", "slowest"):
            small["burstTiming"]["worstCompletedBurst"][name]["operation"] = "pause"
        self.assertTrue(ci.validate_diagnostic_benchmark(small, "http", require_burst_timing=True))

    def test_historical_reports_remain_readable_but_fresh_measurements_require_diagnostics(self):
        report = self.report()
        del report["burstTiming"]
        ci.validate_benchmark(report, "mixed-bt")
        with self.assertRaisesRegex(RuntimeError, "missing completed-burst"):
            ci.validate_benchmark(report, "mixed-bt", require_burst_timing=True)

    def test_anchor_is_required_for_fresh_reports_and_validated_when_present(self):
        report = self.report()
        del report["burstTiming"]["worstCompletedBurst"]["startedUnixNs"]
        ci.validate_benchmark(report, "mixed-bt")
        with self.assertRaisesRegex(RuntimeError, "startedUnixNs"):
            ci.validate_benchmark(report, "mixed-bt", require_burst_timing=True)
        for value in (None, True, 0, -1, "1791187200000000000"):
            report["burstTiming"]["worstCompletedBurst"]["startedUnixNs"] = value
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, "startedUnixNs"):
                ci.validate_benchmark(report, "mixed-bt")

    def test_missing_malformed_and_unknown_versions_fail_closed(self):
        for value in (None, [], {}, {"version": 2}, {"version": True}):
            with self.subTest(value=value):
                report = self.report()
                report["burstTiming"] = value
                with self.assertRaises(RuntimeError):
                    ci.validate_benchmark(report, "mixed-bt")
        for field in self.report()["burstTiming"]:
            report = self.report()
            del report["burstTiming"][field]
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                ci.validate_benchmark(report, "mixed-bt")

    def test_timing_composition_and_cross_report_mismatches_reject(self):
        for field, value in (("primaryUs", 599_999), ("verificationUs", 349_997),
                             ("otherUs", 50_001), ("primaryUs", True)):
            report = self.report()
            report["burstTiming"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(RuntimeError):
                ci.validate_benchmark(report, "mixed-bt")
        changes = {"elapsedNs": 500_000_001, "elapsedUs": 486_001, "burst": 0,
                   "primaryUs": 100_001, "verificationUs": 350_001, "otherUs": 35_997,
                   "firstSampleIndex": 20_000, "primaryCalls": 1001, "verificationCalls": 2,
                   "last": None, "slowest": None}
        for field, value in changes.items():
            report = self.report()
            report["burstTiming"]["worstCompletedBurst"][field] = value
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                ci.validate_benchmark(report, "mixed-bt")

    def test_steps_require_known_operations_positions_phases_and_contained_durations(self):
        changes = {"operation": "unknown", "phase": "other", "sampleIndex": 0,
                   "startUs": 200_000, "durationUs": 350_001}
        for name in ("last", "slowest"):
            for field, value in changes.items():
                report = self.report()
                report["burstTiming"]["worstCompletedBurst"][name][field] = value
                with self.subTest(name=name, field=field), self.assertRaises(RuntimeError):
                    ci.validate_benchmark(report, "mixed-bt")
        report = self.report()
        report["burstTiming"]["worstCompletedBurst"]["slowest"]["durationUs"] = 1
        with self.assertRaises(RuntimeError):
            ci.validate_benchmark(report, "mixed-bt")

    def test_empty_burst_and_submicrosecond_rounding_are_supported(self):
        report = self.report()
        report.update(maxBurstMs=0, measuredBurstUs=5, measuredRoundTripUs=1)
        report["burstTiming"].update(primaryUs=1, verificationUs=1, otherUs=1)
        worst = report["burstTiming"]["worstCompletedBurst"]
        worst.update(elapsedUs=2, elapsedNs=2700, primaryUs=0, verificationUs=0, otherUs=0)
        for name in ("last", "slowest"):
            worst[name].update(startUs=0, durationUs=0)
        ci.validate_benchmark(report, "mixed-bt")
        worst.update(primaryCalls=0, verificationCalls=0, last=None, slowest=None,
                     elapsedUs=1, elapsedNs=1500, otherUs=1)
        ci.validate_benchmark(report, "mixed-bt")

    def test_one_nanosecond_past_limit_rejects_even_when_milliseconds_truncate_to_500(self):
        report = self.report()
        report["maxBurstMs"] = 500
        report["burstTiming"].update(otherUs=64_000)
        report["measuredBurstUs"] += 14_000
        worst = report["burstTiming"]["worstCompletedBurst"]
        worst.update(elapsedNs=500_000_000, elapsedUs=500_000, otherUs=50_000)
        ci.validate_benchmark(report, "mixed-bt")
        worst['elapsedNs'] += 1
        with self.assertRaisesRegex(RuntimeError, "worst.elapsedNs"):
            ci.validate_benchmark(report, "mixed-bt")

    def test_live_collector_rejects_a_report_missing_current_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "fake-benchmark"
            binary.write_bytes(b"fixture")
            runner = mock.Mock(directory=root, env={})
            report = self.report()
            del report["burstTiming"]
            process = mock.Mock(returncode=0)
            process.poll.return_value = 0

            def launch(command, **kwargs):
                kwargs["stdout"].write((json.dumps(report) + '\n').encode())
                return process

            with mock.patch.object(ci, "compiler_processes", return_value=[]), \
                    mock.patch.object(ci.subprocess, "Popen", side_effect=launch), \
                    mock.patch.object(ci.os, "killpg", create=True), \
                    mock.patch.object(ci.signal, "SIGKILL", 9, create=True), \
                    mock.patch.object(ci.host_telemetry, "Sampler") as sampler:
                sampler.return_value.close.return_value = {"samples": 0}
                with self.assertRaisesRegex(RuntimeError, "missing completed-burst"):
                    ci.measure_scenario(runner, binary, "mixed-bt", True)
            self.assertFalse(json.loads((root / 'mixed-bt/run.json').read_text())['passed'])


if __name__ == "__main__":
    unittest.main()
