"""Package drafts preserve covered source, reject drift, and never imply release approval."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import release_manifest as rm


class ReleaseManifestTests(unittest.TestCase):
    def setUp(self):
        self.catalog = json.loads((rm.ROOT / rm.CATALOG).read_text())
        # Keep synthetic staging fixtures small; real full/compat identities are
        # checked by the catalog test and their runtime contract below.
        for package in self.catalog['packages']:
            if package['bundle'] in {'full', 'compat'}:
                package.update(status='planned', binary=None, runtime=None, remaining=['fixture'])

    def test_all_eight_plans_validate_without_requiring_unbuilt_artifacts(self):
        catalog = json.loads((rm.ROOT / rm.CATALOG).read_text())
        rm.validate(catalog)
        self.assertEqual(len(catalog['packages']), 8)

    def test_safe_paths_reject_traversal_windows_aliases_and_reserved_names(self):
        self.assertEqual(rm.relative('runtime/libstdc++-6.dll').name, 'libstdc++-6.dll')
        self.assertEqual(rm.relative('source-data/public-suffix-list.dat').as_posix(),
                         'source-data/public-suffix-list.dat')
        for name in ('../escape', '/absolute', 'a/../escape', 'a//b', 'a\\b', 'C:foo',
                     '.', 'a/./b', 'a.', 'NUL', 'licenses/con.txt'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                rm.relative(name)

    def test_missing_notice_covered_source_and_platform_notice_reject(self):
        for missing in ('source-data/public-suffix-list.dat', 'LICENSE', 'licenses/rust-compiler-builtins.txt'):
            catalog = copy.deepcopy(self.catalog)
            catalog['commonFiles'] = [item for item in catalog['commonFiles'] if item['destination'] != missing]
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, 'missing required'):
                rm.validate(catalog)
        self.catalog['packages'][0]['files'].pop()
        with self.assertRaisesRegex(ValueError, 'missing platform'):
            rm.validate(self.catalog)

    def test_case_collisions_reserved_outputs_and_gpl_source_artifacts_reject(self):
        for destination in ('license', 'manifest.json', 'SHA256SUMS', 'ariax.exe'):
            catalog = copy.deepcopy(self.catalog)
            item = copy.deepcopy(catalog['commonFiles'][0])
            item['destination'] = destination
            catalog['commonFiles'].append(item)
            with self.subTest(destination=destination), self.assertRaisesRegex(ValueError, 'duplicate or reserved'):
                rm.validate(catalog)
        self.catalog['commonFiles'][0]['source'] = 'generated/aria2_options.json'
        with self.assertRaisesRegex(ValueError, 'source-only GPL'):
            rm.validate(self.catalog)

    def test_lock_inventory_and_file_digest_drift_reject(self):
        for field in ('dependencyLockSha256', 'noticeInventorySha256'):
            catalog = copy.deepcopy(self.catalog)
            catalog[field] = '0' * 64
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'drift'):
                rm.validate(catalog)
        self.catalog['commonFiles'][0]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'hash or size mismatch'):
            rm.validate(self.catalog)

    def test_explicit_inventory_binds_lock_and_packaged_notice_collection(self):
        catalog = copy.deepcopy(self.catalog)
        source = 'performance-evidence/phase7-release-license-inventory-2026-10-06.json'
        inventory = json.loads((rm.ROOT / source).read_text())
        catalog.update(noticeInventorySource=source,
                       noticeInventorySha256=rm.digest(rm.ROOT / source),
                       dependencyLockSha256=rm.digest(rm.ROOT / 'Cargo.lock'))
        catalog['commonFiles'] = [item for item in catalog['commonFiles']
                                  if item['destination'] not in {
                                      'license-inventory.json', 'THIRD-PARTY-NOTICES.txt'}]
        for name, destination in ((source, 'license-inventory.json'),
                                  (inventory['noticeArtifact']['path'], 'THIRD-PARTY-NOTICES.txt')):
            path = rm.ROOT / name
            catalog['commonFiles'].append({'source': name, 'destination': destination,
                                           'sha256': rm.digest(path), 'bytes': path.stat().st_size})
        rm.validate(catalog)
        for destination, message in (('license-inventory.json', 'packaged inventory'),
                                     ('THIRD-PARTY-NOTICES.txt', 'notice collection drift')):
            modified = copy.deepcopy(catalog)
            modified['commonFiles'] = [item for item in modified['commonFiles']
                                       if item['destination'] != destination]
            with self.subTest(destination=destination), self.assertRaisesRegex(ValueError, message):
                rm.validate(modified)
        modified = copy.deepcopy(catalog)
        modified['noticeInventorySource'] = '../outside.json'
        with self.assertRaisesRegex(ValueError, 'unsafe path'):
            rm.validate(modified)
        for field, message in (('cargoLockSha256', 'inventory lock drift'),
                               ('noticeArtifact', 'notice collection drift')):
            stale = copy.deepcopy(inventory)
            if field == 'noticeArtifact':
                stale[field]['sha256'] = '0' * 64
            else:
                stale[field] = '0' * 64
            with patch.object(rm.json, 'loads', return_value=stale):
                with self.assertRaisesRegex(ValueError, message):
                    rm.validate(catalog)

    def test_unreviewed_runtime_dependencies_and_approval_reject(self):
        for target, field, value in (
                ('windows-gnu', 'systemLibraries', sorted(rm.WINDOWS_SYSTEM | {'libstdc++-6.dll'})),
                ('linux-gnu', 'minimumGlibc', '2.17'),
                ('windows-gnu', 'additionalRuntimeFiles', ['libwinpthread-1.dll'])):
            catalog = copy.deepcopy(self.catalog)
            package = next(p for p in catalog['packages'] if p['target'].endswith(target) and p['binary'])
            package['runtime'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                rm.validate(catalog)
        self.catalog['releaseApproved'] = True
        with self.assertRaises(ValueError):
            rm.validate(self.catalog)

    def test_duplicate_identity_and_fabricated_planned_binary_reject(self):
        catalog = copy.deepcopy(self.catalog)
        catalog['packages'][-1] = catalog['packages'][0]
        with self.assertRaisesRegex(ValueError, 'package set'):
            rm.validate(catalog)
        planned = next(p for p in self.catalog['packages'] if p['status'] == 'planned')
        planned['binary'] = {'artifactPath': 'invented'}
        with self.assertRaisesRegex(ValueError, 'planned package'):
            rm.validate(self.catalog)

    def test_full_runtime_review_rejects_missing_closure_collisions_and_changes(self):
        files = rm.reviewed_runtime_files()
        runtime = {'systemLibraries': sorted(rm.WINDOWS_BT_SYSTEM), 'additionalRuntimeFiles': files}
        names = {'licenses/' + name.casefold() for name in rm.WINDOWS_NOTICES}
        rm.validate_runtime(runtime, True, 'full', set(names))
        for modified in (files[:-1], [dict(files[0], sha256='0' * 64), *files[1:]]):
            with self.assertRaisesRegex(ValueError, 'matching review'):
                rm.validate_runtime(dict(runtime, additionalRuntimeFiles=modified), True, 'full', set(names))
        with self.assertRaisesRegex(ValueError, 'collision'):
            rm.validate_runtime(runtime, True, 'compat', names | {'libstdc++-6.dll'})
        with self.assertRaisesRegex(ValueError, 'missing redistributed'):
            rm.validate_runtime(runtime, True, 'full', set())

    def fixture_artifacts(self, root):
        # Synthetic bytes test the copy/manifest contract; never execute them.
        for package in self.catalog['packages']:
            binary = package['binary']
            if binary is None:
                continue
            data = ('fixture: ' + package['id']).encode()
            binary.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
            path = root / binary['artifactPath']
            path.parent.mkdir(parents=True)
            path.write_bytes(data)
            binary['pathAudit'] = rm.audit_binary_paths(path)

    def test_staging_preserves_files_hashes_and_unapproved_historical_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture_artifacts(root)
            output = root / 'drafts'
            result = rm.stage(self.catalog, root, output)
            self.assertEqual(len(result['staged']), 4)
            self.assertEqual(len(result['planned']), 4)
            for package_id in result['staged']:
                directory = output / package_id
                manifest = json.loads((directory / 'manifest.json').read_text())
                self.assertFalse(manifest['releaseApproved'])
                self.assertEqual(manifest['status'], 'staged-retained-draft')
                for line in (directory / 'SHA256SUMS').read_text().splitlines():
                    expected, name = line.split('  ', 1)
                    self.assertEqual(rm.digest(directory / name), expected)
            with self.assertRaises(FileExistsError):
                rm.stage(self.catalog, root, output)

    def test_stale_artifacts_fail_before_creating_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture_artifacts(root)
            path = root / self.catalog['packages'][0]['binary']['artifactPath']
            path.write_bytes(b'stale')
            output = root / 'drafts'
            with self.assertRaisesRegex(ValueError, 'hash or size mismatch'):
                rm.stage(self.catalog, root, output)
            self.assertFalse(output.exists())

    def test_staging_copies_the_reviewed_runtime_and_rejects_missing_or_modified_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture_artifacts(root)
            template = next(p for p in self.catalog['packages'] if p['target'].endswith('windows-gnu') and p['binary'])
            full = next(p for p in self.catalog['packages'] if p['target'].endswith('windows-gnu') and p['bundle'] == 'full')
            full.update(status='draft-retained', binary=copy.deepcopy(template['binary']))
            files = rm.reviewed_runtime_files()
            for item in files:
                path = root / item['artifactPath']; path.parent.mkdir(exist_ok=True)
                path.write_bytes(item['destination'].encode())
                item.update(bytes=path.stat().st_size, sha256=rm.digest(path))
            full['runtime'] = {'systemLibraries': sorted(rm.WINDOWS_BT_SYSTEM), 'additionalRuntimeFiles': files}
            with patch.object(rm, 'reviewed_runtime_files', return_value=files):
                output = root / 'staged'
                rm.stage(self.catalog, root, output)
                for item in files:
                    self.assertEqual(rm.digest(output / full['id'] / item['destination']), item['sha256'])
                path = root / files[0]['artifactPath']; path.write_bytes(b'changed')
                with self.assertRaisesRegex(ValueError, 'hash or size'):
                    rm.stage(self.catalog, root, root / 'modified')
                self.assertFalse((root / 'modified').exists())
                path.unlink()
                with self.assertRaisesRegex(ValueError, 'missing'):
                    rm.stage(self.catalog, root, root / 'missing')
                self.assertFalse((root / 'missing').exists())

    def test_dependency_paths_are_reported_and_cannot_be_hidden_in_the_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture_artifacts(root)
            binary = self.catalog['packages'][0]['binary']
            path = root / binary['artifactPath']
            # Synthetic path, split to keep this source file publication-portable.
            data = ('/mnt/' + 'e/temp/cache/registry/dependency.rs').encode()
            path.write_bytes(data)
            binary.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
            with self.assertRaisesRegex(ValueError, 'path audit mismatch'):
                rm.validate(self.catalog, artifacts=root)
            binary['pathAudit'] = rm.audit_binary_paths(path)
            self.assertFalse(binary['pathAudit']['passed'])
            self.assertEqual(binary['pathAudit']['counts']['wslMount'], 1)
            rm.validate(self.catalog, artifacts=root)


if __name__ == '__main__':
    unittest.main()
