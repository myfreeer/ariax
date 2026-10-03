"""Regressions for failure propagation and benchmark acceptance boundaries."""
import copy
import contextlib
import io
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
    def test_manual_full_validation_requires_success_and_no_automatic_benchmark(self):
        self.assertTrue(ci.aggregate_success("workflow_dispatch", "refs/heads/fix", "success", "success", "skipped"))
        self.assertFalse(ci.aggregate_success("workflow_dispatch", "refs/heads/main", "success", "failure", "skipped"))
        self.assertFalse(ci.aggregate_success("workflow_dispatch", "refs/heads/main", "success", "success", "failure"))

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
    def test_unicode_diagnostics_preserve_output_and_exit_status_on_legacy_consoles(self):
        # Rust renders MSVC linker carriage returns as U+240D. Later output
        # exceeds a pipe buffer so the relay must keep draining after that line.
        warning = "warning: LNK4099: debug symbols unavailable\u240d\n"
        stderr = "stderr: \u4e2d\U0001f980\n"
        tail = "remaining output " * 8192 + "done without newline"
        expected = warning + stderr + tail
        script = ("import sys; "
                  f"sys.stdout.buffer.write({warning.encode('utf-8')!r}); sys.stdout.flush(); "
                  f"sys.stderr.buffer.write({stderr.encode('utf-8')!r}); sys.stderr.flush(); "
                  "sys.stdout.buffer.write(b'remaining output ' * 8192 + b'done without newline'); "
                  "sys.stdout.flush(); sys.exit(int(sys.argv[1]))")
        for encoding in ("cp1252", "ascii", "utf-8"):
            for exit_code in (0, 7):
                with self.subTest(encoding=encoding, exit_code=exit_code), \
                        tempfile.TemporaryDirectory() as temporary:
                    log = Path(temporary) / "command.log"
                    raw = io.BytesIO()
                    with io.TextIOWrapper(raw, encoding=encoding, errors="strict", newline="") as console:
                        with contextlib.redirect_stdout(console):
                            command = [sys.executable, "-c", script, str(exit_code)]
                            if exit_code:
                                with self.assertRaises(subprocess.CalledProcessError) as caught:
                                    ci.checked_command(command, log, cwd=temporary, capture=True)
                                self.assertEqual(caught.exception.returncode, exit_code)
                                self.assertEqual(caught.exception.cmd, command)
                            else:
                                captured = ci.checked_command(command, log, cwd=temporary, capture=True)
                                self.assertEqual(captured, expected)
                        console.flush()
                        displayed = raw.getvalue().decode(encoding)
                        self.assertTrue(displayed.endswith(
                            expected.encode(encoding, errors="backslashreplace").decode(encoding)))
                    self.assertEqual(log.read_text(encoding="utf-8"), expected)

    def test_malformed_utf8_is_visible_without_losing_later_output(self):
        with tempfile.TemporaryDirectory() as temporary, contextlib.redirect_stdout(io.StringIO()):
            log = Path(temporary) / "command.log"
            output = ci.checked_command([sys.executable, "-c",
                "import sys; sys.stdout.buffer.write(b'bad: \\xff\\nlast line')"],
                log, cwd=temporary, capture=True)
            self.assertEqual(output, "bad: \ufffd\nlast line")
            self.assertEqual(log.read_text(encoding="utf-8"), output)

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


class MatrixTests(unittest.TestCase):
    def test_routine_pushes_and_pull_requests_keep_linux_and_msvc(self):
        for event, ref in (("push", "refs/heads/fix"), ("pull_request", "refs/pull/1/merge")):
            matrix = ci.validation_matrix(event, ref)["include"]
            self.assertEqual({job["check"] for job in matrix}, {"linux", "windows-msvc"})

    def test_main_tags_and_manual_runs_keep_every_full_coverage_group(self):
        for event, ref in (("push", "refs/heads/main"), ("push", "refs/tags/candidate"),
                           ("workflow_dispatch", "refs/heads/fix")):
            with self.subTest(event=event, ref=ref):
                matrix = ci.validation_matrix(event, ref)["include"]
                self.assertEqual({job["check"] for job in matrix}, {
                    "linux", "windows-msvc", "macos", "windows-gnu", "msrv-linux",
                    "msrv-windows-gnu", "feature-bundles", "bt-safety"})
                native = {job["check"]: job["native"] for job in matrix}
                self.assertEqual(native["linux"], native["msrv-linux"])
                self.assertEqual(native["linux"], native["feature-bundles"])
                self.assertEqual(native["windows-gnu"], native["msrv-windows-gnu"])
                self.assertNotEqual(native["windows-msvc"], native["windows-gnu"])
                self.assertNotEqual(native["bt-safety"], native["linux"])

    def test_missing_or_unknown_context_cannot_select_a_smaller_matrix(self):
        for event, ref in ((None, None), ("schedule", "refs/heads/main"), ("push", None),
                           ("workflow_dispatch", "main")):
            with self.subTest(event=event, ref=ref), self.assertRaises(RuntimeError):
                ci.validation_matrix(event, ref)


class NativeCacheTests(unittest.TestCase):
    def test_cache_save_is_authorized_only_after_native_verification(self):
        for fail in (False, True):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as temporary:
                environment = Path(temporary) / "github-env"
                runner = mock.Mock()
                runner.tool.return_value = Path("rustc")
                runner.run.side_effect = ["host: x86_64-unknown-linux-gnu\n", "",
                    RuntimeError("verification failed") if fail else ""]
                with mock.patch.dict(os.environ, {"GITHUB_ENV": str(environment)}):
                    if fail:
                        with self.assertRaisesRegex(RuntimeError, "verification failed"):
                            ci.provision_bt(runner)
                        self.assertFalse(environment.exists())
                    else:
                        ci.provision_bt(runner)
                        self.assertEqual(environment.read_text(), "ARIAX_BT_NATIVE_VERIFIED=1\n")


class FocusedValidationTests(unittest.TestCase):
    def test_non_native_hosts_fail_before_any_check_or_provisioning(self):
        for system, release in (("win32", "10"), ("linux", "4.4.0-Microsoft")):
            with self.subTest(system=system, release=release):
                runner = mock.Mock()
                with mock.patch.object(ci.sys, "platform", system), \
                        mock.patch.object(ci.platform, "release", return_value=release), \
                        mock.patch.object(ci, "provision_bt") as provision:
                    with self.assertRaises(RuntimeError):
                        ci.focused(runner)
                    runner.run.assert_not_called()
                    runner.cargo.assert_not_called()
                    provision.assert_not_called()

    def test_checks_gate_measurements_and_every_stage_propagates_failure(self):
        for failure in (None, "precheck", "native", "test", "benchmark"):
            with self.subTest(failure=failure):
                runner = mock.Mock()
                runner.env = {}
                prefix = Path("native-install")
                if failure == "precheck":
                    runner.run.side_effect = RuntimeError("precheck failed")

                def cargo(*args):
                    if failure == "test" and args[0] == "test":
                        raise RuntimeError("test failed")

                runner.cargo.side_effect = cargo
                with mock.patch.object(ci, "require_native_linux"), \
                        mock.patch.object(ci, "actionlint", return_value=Path("actionlint")), \
                        mock.patch.object(ci, "provision_bt", return_value=prefix) as provision, \
                        mock.patch.object(ci, "native_security", side_effect=(
                            RuntimeError("native failed") if failure == "native" else None)) as native, \
                        mock.patch.object(ci, "benchmark", side_effect=(
                            RuntimeError("benchmark failed") if failure == "benchmark" else None)) as benchmark:
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, failure + " failed"):
                            ci.focused(runner)
                    else:
                        ci.focused(runner)
                    if failure == "precheck":
                        provision.assert_not_called()
                        native.assert_not_called()
                    else:
                        provision.assert_called_once_with(runner)
                        native.assert_called_once_with(runner, prefix)
                    if failure in {"precheck", "native", "test"}:
                        benchmark.assert_not_called()
                    else:
                        benchmark.assert_called_once_with(runner, (
                            "mixed-bt", "http", "websocket", "content-length", "ndjson"), provision=False)
                        self.assertEqual(runner.env["ARIAX_BT_NATIVE_DIR"], str(prefix))
                        self.assertEqual(runner.env["RUST_TEST_THREADS"], "1")
                        tests = [call.args for call in runner.cargo.call_args_list if call.args[0] == "test"]
                        self.assertTrue(tests)
                        for args in tests:
                            self.assertIn("--locked", args)
                            self.assertIn("--release", args)
                            self.assertIn("--test-threads=1", args)
                            self.assertNotIn("--workspace", args)

    def test_focused_reports_do_not_replace_full_reports_but_reuse_release_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            with mock.patch.object(ci, "ROOT", root), \
                    mock.patch.object(ci, "resolve_toolchain", return_value=root):
                focused = ci.Runner("focused")
                full = ci.Runner("benchmarks")
            self.assertEqual(focused.target, full.target)
            self.assertNotEqual(focused.directory, full.directory)
            self.assertEqual(focused.env["CARGO_TARGET_DIR"], str(full.target))

    def test_benchmark_selection_preserves_full_default_and_stops_after_failure(self):
        for focused, fail in ((False, False), (True, False), (True, True)):
            with self.subTest(focused=focused, fail=fail), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                binary = directory / "benchmark"
                binary.write_bytes(b"fixture executable")
                runner = mock.Mock()
                runner.directory = directory
                runner.cargo.return_value = json.dumps({
                    "reason": "compiler-artifact", "target": {"name": "rpc_active_profile"},
                    "executable": str(binary)})
                measured = []

                def measure(_runner, _binary, scenario, _metalink):
                    measured.append(scenario)
                    if fail and scenario == "http":
                        raise RuntimeError("measurement failed")

                with mock.patch.object(ci, "require_native_linux"), \
                        mock.patch.object(ci, "provision_bt") as provision, \
                        mock.patch.object(ci.platform, "platform", return_value="Linux fixture"), \
                        mock.patch.object(ci.subprocess, "check_output", side_effect=[b"", "a" * 40]), \
                        mock.patch.object(ci.time, "sleep"), \
                        mock.patch.object(ci, "measure_scenario", side_effect=measure):
                    if not focused:
                        ci.benchmark(runner)
                        provision.assert_called_once_with(runner)
                    elif fail:
                        with self.assertRaisesRegex(RuntimeError, "measurement failed"):
                            ci.benchmark(runner, ci.FOCUSED_SCENARIOS, provision=False)
                        provision.assert_not_called()
                    else:
                        ci.benchmark(runner, ci.FOCUSED_SCENARIOS, provision=False)
                        provision.assert_not_called()
                expected = ci.FOCUSED_SCENARIOS if focused else ci.SCENARIOS
                self.assertEqual(measured, ["mixed-bt", "http"] if fail else list(expected))
                manifest = json.loads((directory / "manifest.json").read_text())
                self.assertEqual(manifest["scenarios"], list(expected))


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


class PlatformNativeTests(unittest.TestCase):
    def test_windows_helper_failure_stops_before_native_provisioning(self):
        for name in ("windows-msvc", "windows-gnu"):
            with self.subTest(platform=name):
                runner = mock.Mock()
                runner.name = name
                runner.run.side_effect = subprocess.CalledProcessError(7, ["helper tests"])
                with mock.patch.object(ci, "provision_bt") as provision:
                    with self.assertRaises(subprocess.CalledProcessError):
                        ci.validate(runner)
                runner.run.assert_called_once_with([sys.executable, "-B", "-m", "unittest",
                    "discover", "-s", "scripts", "-p", "test_*.py"])
                provision.assert_not_called()
                runner.cargo.assert_not_called()

    def test_grouped_bundles_keep_all_tests_and_release_builds_and_provision_once(self):
        runner = mock.Mock()
        runner.name = "feature-bundles"
        events = []
        runner.cargo.side_effect = lambda *args: events.append((args[0], args[args.index("--features")+1]))
        with mock.patch.object(ci, "provision_bt", side_effect=lambda _: events.append(("native", "full"))):
            ci.validate(runner)
        self.assertEqual(events, [("test", "minimal"), ("build", "minimal"),
            ("test", "standard"), ("build", "standard"), ("native", "full"),
            ("test", "full"), ("build", "full"), ("test", "compat"), ("build", "compat")])
        for call in runner.cargo.call_args_list:
            self.assertIn("--no-default-features", call.args)
            if call.args[0] == "build":
                self.assertEqual(call.args[-2:], ("--profile", "release-cli"))

    def test_failed_bundle_stops_later_bundles_and_native_provisioning(self):
        runner = mock.Mock()
        runner.name = "feature-bundles"
        runner.cargo.side_effect = [None, None, RuntimeError("standard failed")]
        with mock.patch.object(ci, "provision_bt") as provision:
            with self.assertRaisesRegex(RuntimeError, "standard failed"):
                ci.validate(runner)
            provision.assert_not_called()
        self.assertEqual(runner.cargo.call_count, 3)

    def test_every_platform_runs_native_probes_and_a_failure_stops_workspace_work(self):
        for name in ("linux", "macos", "windows-msvc", "windows-gnu"):
            for fail in (False, True):
                with self.subTest(platform=name, fail=fail):
                    runner = mock.Mock()
                    runner.name = name
                    prefix = Path("native-install")
                    error = RuntimeError("native probe failed") if fail else None
                    with mock.patch.object(ci, "provision_bt", return_value=prefix), \
                            mock.patch.object(ci, "native_security", side_effect=error) as native:
                        if fail:
                            with self.assertRaisesRegex(RuntimeError, "native probe failed"):
                                ci.validate(runner)
                            runner.cargo.assert_not_called()
                        else:
                            ci.validate(runner)
                            self.assertTrue(runner.cargo.called)
                        native.assert_called_once_with(runner, prefix)

    def test_feature_and_msrv_jobs_do_not_duplicate_platform_probes(self):
        for name in ("feature-minimal", "feature-standard", "feature-full", "feature-compat",
                     "msrv-linux", "msrv-windows-gnu"):
            with self.subTest(check=name):
                runner = mock.Mock()
                runner.name = name
                with mock.patch.object(ci, "provision_bt") as provision, \
                        mock.patch.object(ci, "native_security") as native:
                    ci.validate(runner)
                    native.assert_not_called()
                    self.assertEqual(provision.called, name not in {"feature-minimal", "feature-standard"})
                    self.assertTrue(runner.cargo.called)

    def test_native_generators_match_platform_and_ctest_uses_the_built_configuration(self):
        for name, generator in (("linux", "Unix Makefiles"), ("macos", "Unix Makefiles"),
                                ("windows-msvc", "NMake Makefiles"), ("windows-gnu", "MinGW Makefiles")):
            with self.subTest(platform=name):
                runner = mock.Mock()
                runner.name = name
                runner.target = Path("target")
                ci.native_security(runner, Path("native-install"))
                configure, build, test = [call.args[0] for call in runner.run.call_args_list]
                self.assertEqual(configure[configure.index("-G") + 1], generator)
                self.assertEqual(build[build.index("--config") + 1],
                                 test[test.index("--build-config") + 1])
                self.assertIn("-DCMAKE_BUILD_TYPE=" + build[build.index("--config") + 1], configure)


class BitTorrentSafetyTests(unittest.TestCase):
    def test_native_checks_precede_fuzzing_and_uninstrumented_results_fail(self):
        for output, accepted in (("#123 DONE cov: 42\n", True), ("Done without coverage", False)):
            with self.subTest(accepted=accepted), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                seeds = root / "fuzz/seeds/bittorrent_metadata"
                seeds.mkdir(parents=True)
                (seeds / "seed").write_bytes(b"de")
                runner = mock.Mock()
                runner.env = {}
                runner.directory = root / "reports"
                runner.directory.mkdir()
                runner.target = root / "target"
                runner.run.return_value = output
                with mock.patch.object(ci, "ROOT", root), \
                        mock.patch.object(ci, "provision_bt", return_value=root / "native") as provision, \
                        mock.patch.object(ci, "native_security") as security:
                    if accepted:
                        ci.bt_safety(runner)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "coverage instrumentation"):
                            ci.bt_safety(runner)
                    provision.assert_called_once_with(runner, "address")
                    security.assert_called_once_with(runner, root / "native", True)
                self.assertEqual(runner.cargo.call_args_list[0].args[0], "test")
                fuzz_build = runner.cargo.call_args_list[1]
                self.assertEqual(fuzz_build.args[0], "build")
                self.assertEqual(fuzz_build.kwargs["env"]["RUSTC_BOOTSTRAP"], "1")
                self.assertNotIn("RUSTC_BOOTSTRAP", runner.env)
                self.assertTrue((runner.directory / "corpus/seed").is_file())


class BenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        evidence = ci.ROOT / "performance-evidence/phase5-windows-gnu-2026-09-15.json"
        cls.reports = json.loads(evidence.read_text(encoding="utf-8"))["scenarios"]

    def report(self, scenario="http"):
        result = copy.deepcopy(next(report for report in self.reports if report["scenario"] == scenario))
        result["os"] = "linux"
        return result

    def test_complete_transport_and_administrative_shapes_pass(self):
        # Historical native reports predate the additional mixed-BT scenario.
        for scenario in ("http", "websocket", "content-length", "ndjson", "administrative"):
            with self.subTest(scenario=scenario):
                ci.validate_benchmark(self.report(scenario), scenario)

    def mixed_report(self):
        # A validator fixture, not claimed native performance evidence.
        report = self.report()
        report.update(scenario="mixed-bt", verificationCalls=2_000, btPeers=1_000,
                      btPeerProjection=1_000, btControlCalls=1_000, perStatusPeerCheck=True,
                      peerFixtureMemoryExcluded=True, btRenewedBarriers=report["bursts"],
                      btDownloadedBytes=(report["bursts"] + 1) * 1_000 * 16_384)
        operations = report["operations"]
        latency = operations.pop("tellStatus")
        operations.pop("getUris")
        operations["tellWaiting"]["calls"] = 2_000
        for name, calls in (("http.tellStatus", 6_000), ("bt.tellStatus", 6_000),
                            ("bt.getPeers", 1_000), ("bt.getFiles", 1_000), ("bt.changeOption", 1_000)):
            operations[name] = dict(latency, calls=calls)
        return report

    def test_mixed_bt_requires_both_payload_barriers_and_native_mutations(self):
        self.assertIn("mixed-bt", ci.SCENARIOS)
        ci.validate_benchmark(self.mixed_report(), "mixed-bt")
        changes = {"btPeers": 999, "btPeerProjection": 999, "btControlCalls": 999,
                   "verificationCalls": 1_000, "btRenewedBarriers": 0, "btDownloadedBytes": 0,
                   "perStatusPeerCheck": False, "peerFixtureMemoryExcluded": False,
                   "rssLimit": 2 * 1024**3, "residentLimit": 897 * 1024**2}
        for field, value in changes.items():
            with self.subTest(field=field):
                report = self.mixed_report()
                report[field] = value
                with self.assertRaises(RuntimeError):
                    ci.validate_benchmark(report, "mixed-bt")
        for name in ("bt.tellStatus", "bt.getPeers", "bt.getFiles", "bt.changeOption"):
            report = self.mixed_report()
            del report["operations"][name]
            with self.assertRaises(RuntimeError):
                ci.validate_benchmark(report, "mixed-bt")

    def test_old_transport_evidence_cannot_substitute_for_mixed_bt(self):
        report = self.report()
        report["scenario"] = "mixed-bt"
        with self.assertRaises(RuntimeError):
            ci.validate_benchmark(report, "mixed-bt")

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
