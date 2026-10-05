"""Release preparation maps real compiler arguments and rejects mismatched inputs."""
from pathlib import Path, PureWindowsPath
import shlex
import tempfile
import unittest
from unittest.mock import patch

import release_build as rb


class ReleaseBuildTests(unittest.TestCase):
    def roots(self, windows=False):
        base = 'E:/build space/' if windows else '/build space/'
        return {name: base + name for name in ('repo', 'cargo', 'target', 'temp')}

    def test_utf8_cli_output_is_read_without_locale_dependent_decoding(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'stdout.log'
            expected = 'ariax — experimental bounded downloader\nUsage: ariax\n'
            path.write_bytes(expected.encode('utf-8'))
            self.assertEqual(rb.read_utf8(path), expected)
            path.write_bytes(b'ariax\xff')
            with self.assertRaises(UnicodeDecodeError):
                rb.read_utf8(path)

    def test_linux_flags_cover_dependency_and_generated_sources_with_spaces(self):
        flags = rb.remap_flags(self.roots(), False)
        self.assertIn('--remap-path-prefix=/build space/cargo=/ariax-cargo', flags['rust'])
        self.assertIn('-ffile-prefix-map=/build space/target=/ariax-target', flags['native'])
        self.assertEqual(shlex.split(shlex.join(flags['native'])), flags['native'])
        self.assertNotIn('link-self-contained=no', flags['rust'])

    def test_windows_native_spellings_and_runtime_flags_are_preserved(self):
        flags = rb.remap_flags(self.roots(True), True)
        for spelling in ('E:/build space/cargo', str(PureWindowsPath('E:/build space/cargo')),
                         'e:/build space/cargo'):
            self.assertIn('--remap-path-prefix=' + spelling + '=/ariax-cargo', flags['rust'])
        backslash_roots = {key: value.replace('/', '\\') for key, value in self.roots(True).items()}
        self.assertEqual(rb.remap_flags(backslash_roots, True), flags)
        self.assertEqual(shlex.split(shlex.join(flags['native'])), flags['native'])
        self.assertEqual(flags['rust'][-4:], ['-C', 'link-self-contained=no',
                                            '-C', 'link-arg=-Wl,--no-insert-timestamp'])

    def test_more_specific_roots_follow_broad_roots(self):
        roots = self.roots()
        roots.update(repo='/work', cargo='/work/cargo', target='/work/cargo/target')
        flags = rb.remap_flags(roots, False)['rust']
        self.assertLess(flags.index('--remap-path-prefix=/work=/ariax'),
                        flags.index('--remap-path-prefix=/work/cargo=/ariax-cargo'))
        self.assertLess(flags.index('--remap-path-prefix=/work/cargo=/ariax-cargo'),
                        flags.index('--remap-path-prefix=/work/cargo/target=/ariax-target'))

    def test_native_header_root_is_remapped_and_unknown_roots_reject(self):
        roots = dict(self.roots(), native='/native/install')
        self.assertIn('-ffile-prefix-map=/native/install=/ariax-native', rb.remap_flags(roots, False)['native'])
        with self.assertRaises(ValueError):
            rb.remap_flags(dict(roots, arbitrary='/arbitrary'), False)

    def test_native_comparison_preserves_link_bytes_and_allows_remapped_build_locations(self):
        import copy
        first = {'inputs': {'builderSha256': 'builder', 'releasePaths': {
            'flags': ['-ffile-prefix-map=/first=/ariax-native-work'], 'sourceDateEpoch': '123'}},
                 'compiler': 'gcc', 'files': {'lib/' + name: name for name in
                                             ('libcrypto.a', 'libssl.a', 'libtorrent-rasterbar.a')}}
        first['files']['include/openssl/header.h'] = 'header'
        second = copy.deepcopy(first)
        second['inputs']['releasePaths']['flags'][0] = '-ffile-prefix-map=/second=/ariax-native-work'
        self.assertEqual(rb.native_comparison(first), rb.native_comparison(second))
        second['files']['include/openssl/header.h'] = 'changed'
        self.assertNotEqual(rb.native_comparison(first), rb.native_comparison(second))
        del second['files']['lib/libcrypto.a']
        with self.assertRaisesRegex(ValueError, 'incomplete native'):
            rb.native_comparison(second)

    def test_full_runtime_inventory_requires_exact_platform_imports_and_versions(self):
        output = '\n'.join('DLL Name: ' + name for name in rb.rm.WINDOWS_BT_SYSTEM | {'libstdc++-6.dll'})
        self.assertEqual({r['destination'] for r in rb.runtime_inventory(output, True, 'full')['additionalRuntimeFiles']},
                         rb.rm.WINDOWS_BT_RUNTIME)
        with self.assertRaises(ValueError):
            rb.runtime_inventory(output + '\nDLL Name: unexpected.dll', True, 'full')
        output = '\n'.join('Shared library: [' + name + ']' for name in rb.rm.LINUX_BT_SYSTEM)
        output += '\n[Requesting program interpreter: /lib64/ld-linux-x86-64.so.2]\nGLIBC_2.38 GLIBCXX_3.4.30'
        self.assertEqual(rb.runtime_inventory(output, False, 'compat')['minimumGlibcxx'], '3.4.30')
        for changed in (output.replace('3.4.30', '3.4.31'), output.replace('2.38', '2.39'),
                        output.replace('libstdc++.so.6', 'libunknown.so')):
            with self.assertRaises(ValueError):
                rb.runtime_inventory(changed, False, 'full')

    def test_missing_relative_ambiguous_and_duplicate_roots_reject(self):
        for value in ('relative', '/', '/a=b', '/a\nb', '/a\x1fb', '/a\x00b'):
            roots = self.roots(); roots['cargo'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                rb.remap_flags(roots, False)
        roots = self.roots(); roots.pop('cargo')
        with self.assertRaises(ValueError):
            rb.remap_flags(roots, False)
        roots = self.roots(True); roots['target'] = roots['cargo'].lower()
        with self.assertRaises(ValueError):
            rb.remap_flags(roots, True)

    def test_environment_replaces_overrides_and_encodes_arguments(self):
        base = {'PATH': '/native/bin', 'RUSTFLAGS': '-Clink-self-contained=yes',
                'RUSTC_WRAPPER': '/unexpected', 'CARGO_BUILD_TARGET': 'wrong',
                'TARGET_CFLAGS': 'unexpected', 'CFLAGS_x86_64_pc_windows_gnu': 'unexpected'}
        with patch.object(rb.shutil, 'which', side_effect=lambda name, **kwargs: '/native/bin/' + name):
            env, flags = rb.build_environment(base, self.roots(True), Path('/toolchain'), True, '123')
        for name in ('RUSTFLAGS', 'RUSTC_WRAPPER', 'CARGO_BUILD_TARGET', 'TARGET_CFLAGS',
                     'CFLAGS_x86_64_pc_windows_gnu'):
            self.assertNotIn(name, env)
        self.assertEqual(env['CARGO_ENCODED_RUSTFLAGS'].split('\x1f'), flags['rust'])
        self.assertEqual(shlex.split(env['CFLAGS']), flags['native'])
        self.assertEqual(env['PATH'], base['PATH'])
        self.assertEqual(env['SOURCE_DATE_EPOCH'], '123')
        self.assertEqual(base['RUSTC_WRAPPER'], '/unexpected')

    def test_pinned_toolchain_host_and_native_tools_are_required(self):
        output = 'release: 1.97.1\nhost: ' + rb.LINUX + '\n'
        self.assertTrue(rb.toolchain_matches(output, rb.LINUX))
        self.assertFalse(rb.toolchain_matches(output, rb.WINDOWS))
        self.assertFalse(rb.toolchain_matches(output.replace('1.97.1', '1.97.2'), rb.LINUX))
        with patch.object(rb.shutil, 'which', return_value=None), self.assertRaises(ValueError):
            rb.build_environment({}, self.roots(), Path('/toolchain'), False, '123')

    def test_unknown_option_requires_the_cli_usage_exit_code_and_message(self):
        message = 'ariax: unknown argument: --ariax-intentionally-invalid-option\n'
        self.assertTrue(rb.option_rejected({'exitCode': 2}, message))
        for code in (None, 0, 1, -9):
            self.assertFalse(rb.option_rejected({'exitCode': code}, message))
        self.assertFalse(rb.option_rejected({'exitCode': 2}, 'missing runtime library'))

    def test_cache_reuse_allows_only_driver_changes_and_rejects_source_drift(self):
        previous = {'Cargo.lock': 'lock', 'bin/ariax/src/main.rs': 'source', 'scripts/release_build.py': 'old'}
        rb.verify_cache_sources(previous, dict(previous, **{'scripts/release_build.py': 'new'}))
        for changed in (dict(previous, **{'Cargo.lock': 'other'}),
                        dict(previous, **{'bin/ariax/src/main.rs': 'changed'}),
                        {'Cargo.lock': 'lock'}):
            with self.assertRaises(ValueError):
                rb.verify_cache_sources(previous, changed)

    def test_comparison_holds_epoch_and_rejects_incomplete_or_changed_reference(self):
        import copy
        sources = {'Cargo.lock': 'lock', 'scripts/release_build.py': 'current'}
        record = {'passed': True, 'target': rb.LINUX, 'sourceHashes': sources,
                  'sourceDateEpoch': '123', 'builds': [{'bundle': 'minimal'}, {'bundle': 'standard'}]}
        self.assertEqual(rb.comparison_reference(record, rb.LINUX, sources), '123')
        for key, value in (('passed', False), ('target', rb.WINDOWS), ('sourceDateEpoch', 'invalid'),
                           ('builds', [{'bundle': 'minimal'}])):
            changed = copy.deepcopy(record); changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                rb.comparison_reference(changed, rb.LINUX, sources)
        with self.assertRaises(ValueError):
            rb.comparison_reference(record, rb.LINUX, {'Cargo.lock': 'different'})

    def test_comparison_flags_allow_changed_roots_but_not_changed_destinations_or_link_options(self):
        first = rb.remap_flags(self.roots(True), True)
        roots = {k:v.replace('build space', 'independent build') for k,v in self.roots(True).items()}
        repeat = rb.remap_flags(roots, True)
        self.assertEqual(rb.comparison_flags(first), rb.comparison_flags(repeat))
        repeat['rust'][-1] = 'link-arg=-Wl,--insert-timestamp'
        self.assertNotEqual(rb.comparison_flags(first), rb.comparison_flags(repeat))
        repeat = rb.remap_flags(roots, True)
        repeat['native'][0] = repeat['native'][0].replace('=/ariax', '=/different')
        self.assertNotEqual(rb.comparison_flags(first), rb.comparison_flags(repeat))

    def test_windows_inventory_rejects_extra_or_missing_dlls(self):
        output = '\n'.join('DLL Name: ' + name.upper() for name in rb.rm.WINDOWS_SYSTEM)
        self.assertEqual(rb.runtime_inventory(output, True)['additionalRuntimeFiles'], [])
        for changed in ('DLL Name: KERNEL32.dll', output + '\nDLL Name: libstdc++-6.dll'):
            with self.assertRaises(ValueError):
                rb.runtime_inventory(changed, True)

    def test_linux_inventory_requires_reviewed_libraries_loader_and_versions(self):
        output = '\n'.join('Shared library: [' + name + ']' for name in rb.rm.LINUX_SYSTEM)
        output += '\n[Requesting program interpreter: /lib64/ld-linux-x86-64.so.2]\nGLIBC_2.9 GLIBC_2.34'
        self.assertEqual(rb.runtime_inventory(output, False)['minimumGlibc'], '2.34')
        for changed in (output.replace('2.34', '2.35'), output.replace('libm.so.6', 'libother.so'),
                        output.replace('/lib64/', '/unsupported/'), output.split('GLIBC_')[0]):
            with self.assertRaises(ValueError):
                rb.runtime_inventory(changed, False)


if __name__ == '__main__':
    unittest.main()
