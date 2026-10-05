"""Package smoke checks expose host additions without implying a fresh OS."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import release_smoke as rs


class ReleaseSmokeTests(unittest.TestCase):
    def test_reduced_environment_drops_credentials_injection_and_developer_paths(self):
        base = {'PATH': '/developer/bin', 'LD_PRELOAD': '/injected.so', 'ARIAX_RPC_SECRET': 'secret',
                'SSL_CERT_FILE': '/custom-certs', 'SystemRoot': 'C:/Windows'}
        for windows in (False, True):
            env = rs.reduced_environment(base, Path('/fixture/tmp'), windows)
            self.assertTrue(set(env).isdisjoint({'LD_PRELOAD', 'ARIAX_RPC_SECRET', 'SSL_CERT_FILE'}))
            self.assertNotIn('developer', env['PATH'])
        with self.assertRaises(ValueError):
            rs.reduced_environment({}, Path('/fixture/tmp'), True)

    def test_mapping_parser_records_executable_files_and_rejects_missing_data(self):
        text = '00-01 r-xp 00 00:00 1 /usr/lib/libc.so.6\n01-02 rw-p 00 00:00 2 /tmp/data\n'
        self.assertEqual(rs.executable_maps(text), ['/usr/lib/libc.so.6'])
        with self.assertRaises(ValueError):
            rs.executable_maps('00-01 r-xp 00 00:00 0 [vdso]\n')

    def test_host_module_inventory_distinguishes_system_and_extra_modules(self):
        paths = ['E:/packages/ariax.exe', 'C:/Windows/System32/kernel32.dll', 'D:/tools/injected.dll']
        rows = rs.module_inventory(paths, Path('E:/packages/ariax.exe'), True, 'C:/Windows')
        self.assertEqual([r['kind'] for r in rows], ['application', 'system', 'host-extra'])
        with self.assertRaises(ValueError):
            rs.module_inventory(paths[1:], Path('E:/packages/ariax.exe'), True, 'C:/Windows')

    def test_package_inventory_accepts_exact_files_and_rejects_drift_or_extra_dll(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'ariax'; binary.write_bytes(b'synthetic fixture')
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            manifest = {'files': [{'destination': 'ariax'}], 'binary': {'destination': 'ariax', 'sha256': digest}}
            (root / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
            (root / 'SHA256SUMS').write_text(digest + '  ariax\n' + rs.h.digest(root / 'manifest.json') +
                                           '  manifest.json\n', encoding='utf-8')
            self.assertEqual(rs.verify_package(root)[1], binary)
            (root / 'unexpected.dll').write_bytes(b'not in manifest')
            with self.assertRaisesRegex(ValueError, 'unexpected'):
                rs.verify_package(root)
            (root / 'unexpected.dll').unlink(); binary.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                rs.verify_package(root)


if __name__ == '__main__':
    unittest.main()
