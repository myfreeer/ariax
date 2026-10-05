import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

import bt_tsan as driver


class DriverTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        for name in (*driver.NATIVE_INPUTS.values(), "Cargo.toml", "rust-toolchain.toml"):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n")
        self.original = self.root / driver.TEST
        self.original.parent.mkdir(parents=True)
        self.source = '#![cfg(feature = "native")]\n\n' + ''.join(
            '#[test]\nfn ' + case + '() {\n    assert!(true);\n}\n' for case in driver.CASES)
        self.original.write_text(self.source)
        fixtures = self.original.parent / "fixtures"
        fixtures.mkdir()
        (fixtures / "payload.bin").write_bytes(b"payload")
        (self.root / "Cargo.lock").write_text('version = 4\n[[package]]\nname = "dep"\nversion = "1.0.0"\n')
        self.output = Path(self.temporary.name) / "prior"
        self.project = self.output / "project"
        (self.project / "src").mkdir(parents=True)
        (self.project / "src/main.rs").write_text(driver.driver_source(self.source))
        (self.project / "Cargo.toml").write_text(driver.manifest(self.root))
        (self.project / "Cargo.lock").write_text((self.root / "Cargo.lock").read_text()
                                                + '\n[[package]]\nname = "' + driver.NAME + '"\nversion = "0.0.0"\n')
        shutil.copytree(fixtures, self.project / "src/fixtures")
        self.binary = self.output / "binary"
        self.binary.write_bytes(b"recorded executable")
        self.record = {
            "passed": True, "sourceHashes": driver.source_hashes(self.root),
            "originalSourceSha256": driver.evidence.digest(self.original),
            "driverSourceSha256": driver.evidence.digest(self.project / "src/main.rs"),
            "lockSha256": driver.evidence.digest(self.project / "Cargo.lock"),
            "build": {"passed": True, "environment": {"ARIAX_BT_SANITIZER": "thread", "RUSTFLAGS": "-fsanitize=thread"}},
            "tests": [{"test": case, "passed": True, "caseReportVerified": True,
                       "command": [str(self.binary), case], "binarySha256": driver.evidence.digest(self.binary)}
                      for case in driver.CASES],
        }
        self.record_path = self.output / "result.json"
        self.save()

    def save(self):
        driver.evidence.save(self.record_path, self.record)

    def test_source_adaptation_preserves_bodies_and_rejects_inventory_drift(self):
        adapted = driver.driver_source(self.source)
        self.assertTrue(adapted.startswith(self.source.removeprefix('#![cfg(feature = "native")]\n').replace('#[test]\n', '')))
        for changed in (self.source.replace('#[test]\n', '', 1), self.source.replace('native_rejection', 'renamed_rejection'),
                        self.source + '#[test]\nfn extra() {}\n', self.source.replace('cfg(feature', 'cfg(any(feature')):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                driver.driver_source(changed)

    def test_reuse_verifies_source_project_lock_and_binary(self):
        prior, project, binary = driver.verify_reuse(self.root, self.record_path)
        self.assertTrue(prior['passed'])
        self.assertEqual(project, self.project)
        self.assertEqual(binary, self.binary)

    def test_legacy_reuse_requires_matching_snapshot(self):
        snapshot = self.output / "snapshot.json"
        driver.evidence.save(snapshot, {"files": self.record.pop("sourceHashes")})
        self.save()
        with self.assertRaisesRegex(ValueError, "requires a source snapshot"):
            driver.verify_reuse(self.root, self.record_path)
        driver.verify_reuse(self.root, self.record_path, snapshot)
        driver.evidence.save(snapshot, {"files": {}})
        with self.assertRaisesRegex(ValueError, "stale driver build sources"):
            driver.verify_reuse(self.root, self.record_path, snapshot)

    def test_later_partial_snapshot_completes_legacy_provenance(self):
        sources = self.record.pop('sourceHashes')
        old = dict(sources, **{'Cargo.lock': 'old'})
        first, second = self.output / 'first.json', self.output / 'second.json'
        driver.evidence.save(first, {'files': old})
        driver.evidence.save(second, {'files': {'Cargo.lock': sources['Cargo.lock']}})
        self.save()
        with self.assertRaisesRegex(ValueError, 'stale driver build sources'):
            driver.verify_reuse(self.root, self.record_path, [second])
        driver.verify_reuse(self.root, self.record_path, [first, second])

    def test_reuse_rejects_each_changed_input(self):
        for path in (self.original, self.root / "Cargo.toml", self.root / "Cargo.lock",
                     self.root / driver.CRATE / "src/bridge.cc", self.project / "src/main.rs",
                     self.project / "Cargo.toml", self.project / "Cargo.lock",
                     self.project / "src/fixtures/payload.bin", self.binary):
            with self.subTest(path=path):
                previous = path.read_bytes() if path.exists() else None
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"changed\n")
                try:
                    with self.assertRaises((ValueError, KeyError)):
                        driver.verify_reuse(self.root, self.record_path)
                finally:
                    if previous is None:
                        path.unlink()
                    else:
                        path.write_bytes(previous)

    def test_lock_rejects_new_cached_dependency_even_with_updated_digest(self):
        lock = self.project / "Cargo.lock"
        lock.write_text(lock.read_text().replace('version = "1.0.0"', 'version = "1.0.1"'))
        self.record['lockSha256'] = driver.evidence.digest(lock)
        self.save()
        with self.assertRaisesRegex(ValueError, "differ from repository lock"):
            driver.verify_reuse(self.root, self.record_path)

    def test_reuse_rejects_removed_source(self):
        fixture = self.original.parent / 'fixtures/payload.bin'
        fixture.unlink()
        (self.project / 'src/fixtures/payload.bin').unlink()
        with self.assertRaisesRegex(ValueError, 'stale driver build sources'):
            driver.verify_reuse(self.root, self.record_path)

    def test_reuse_rejects_failed_incomplete_and_unsanitized_records(self):
        baseline = copy.deepcopy(self.record)
        for change in (lambda r: r.update(passed=False), lambda r: r['build'].update(passed=False),
                       lambda r: r.update(tests=r['tests'][:1]),
                       lambda r: r['tests'][0].update(command=[str(self.binary), driver.CASES[1]]),
                       lambda r: r['tests'][0].update(caseReportVerified=False),
                       lambda r: r['build']['environment'].update(ARIAX_BT_SANITIZER='none')):
            self.record = copy.deepcopy(baseline)
            change(self.record)
            self.save()
            with self.assertRaises(ValueError):
                driver.verify_reuse(self.root, self.record_path)

    def test_short_build_uses_workspace_lock_then_locked_offline_build(self):
        output = Path(self.temporary.name) / 'new'
        commands = []

        def run(command, *args, **kwargs):
            commands.append(command)
            if command[1] == 'update':
                shutil.copyfile(self.project / 'Cargo.lock', output / 'project/Cargo.lock')
            return {'passed': True}

        with mock.patch.object(driver.evidence, 'run', side_effect=run):
            driver.build(self.root, output, Path('/selected/cargo'), {'CARGO_TARGET_DIR': '/selected/target'})
        self.assertEqual(commands[0][1:4], ['update', '--offline', '--workspace'])
        self.assertEqual(commands[1][1:4], ['build', '--locked', '--offline'])

    def test_failed_lock_preparation_never_builds(self):
        with mock.patch.object(driver.evidence, 'run', return_value={'passed': False}) as run:
            with self.assertRaisesRegex(ValueError, 'lock preparation failed'):
                driver.build(self.root, Path(self.temporary.name) / 'new', Path('/selected/cargo'), {})
        run.assert_called_once()

    def test_runtime_report_sanitizer_and_fixture_failures_stop_without_retry(self):
        for failure in ('none', 'exit', 'missing', 'sanitizer', 'fixture'):
            output = Path(self.temporary.name) / failure
            (output / 'tmp').mkdir(parents=True)

            def run(command, directory, **kwargs):
                directory.mkdir()
                (directory / 'stdout.log').write_text('' if failure == 'missing' else f'case {command[1]}: passed\n')
                (directory / 'stderr.log').write_text('WARNING: ThreadSanitizer: data race' if failure == 'sanitizer' else '')
                if failure == 'fixture':
                    (output / 'tmp/ariax-bt-leftover').mkdir(exist_ok=True)
                return {'passed': failure != 'exit'}

            with mock.patch.object(driver.evidence, 'run', side_effect=run):
                results = driver.run_cases(self.binary, output, 1, {}, self.root)
            self.assertEqual(len(results), 2 if failure == 'none' else 1)
            self.assertEqual(all(r['passed'] for r in results), failure == 'none')


if __name__ == '__main__':
    unittest.main()
