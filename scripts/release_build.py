#!/usr/bin/env python3
"""Build and inspect two native CLI drafts with explicit source-path remapping."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import shlex
import shutil
import subprocess
import sys

import local_hardening as h
import release_manifest as rm
import bt_native

ROOT = Path(__file__).resolve().parents[1]
VERSION = '1.97.1'
LINUX = 'x86_64-unknown-linux-gnu'
WINDOWS = 'x86_64-pc-windows-gnu'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_utf8(path):
    return path.read_text(encoding='utf-8')


def remap_flags(roots, windows):
    """Use native spellings; more specific roots take precedence in rustc."""
    expected = {'repo', 'cargo', 'target', 'temp'}
    require(expected <= set(roots) <= expected | {'native'}, 'all four remap roots and optional native root required')
    path_type = PureWindowsPath if windows else PurePosixPath
    mappings = []
    identities = set()
    for name, value in roots.items():
        require(isinstance(value, str) and not any(c in value for c in '\x00\x1f\r\n='),
                'invalid remap root')
        path = path_type(value)
        require(path.is_absolute() and path != path_type(path.anchor), 'absolute non-root path required')
        source = path.as_posix()
        identity = source.casefold() if windows else source
        require(identity not in identities, 'remap roots must be distinct')
        identities.add(identity)
        destination = '/ariax' if name == 'repo' else '/ariax-' + name
        spellings = {source}
        if windows:
            spellings |= {str(path), source[0].lower() + source[1:],
                          str(path)[0].lower() + str(path)[1:]}
        mappings.extend((spelling, destination) for spelling in spellings)
    mappings.sort(key=lambda item: (len(item[0]), item[0]))
    rust = ['--remap-path-prefix=' + source + '=' + destination for source, destination in mappings]
    if windows:
        rust += ['-C', 'link-self-contained=no', '-C', 'link-arg=-Wl,--no-insert-timestamp']
    native = ['-ffile-prefix-map=' + source + '=' + destination for source, destination in mappings]
    return {'rust': rust, 'native': native}


def build_environment(base, roots, toolchain, windows, epoch):
    flags = remap_flags(roots, windows)
    env = dict(base)
    for key in tuple(env):
        if key in {'RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_TARGET'} or (
                key.startswith(('HOST_', 'TARGET_')) and key.endswith(('CFLAGS', 'CXXFLAGS'))) or (
                key.startswith(('CFLAGS_', 'CXXFLAGS_'))):
            env.pop(key)
    suffix = '.exe' if windows else ''
    env.update(RUSTC=str(toolchain / ('rustc' + suffix)), RUSTDOC=str(toolchain / ('rustdoc' + suffix)),
               CARGO_ENCODED_RUSTFLAGS='\x1f'.join(flags['rust']), CARGO_TARGET_DIR=roots['target'],
               CARGO_HOME=roots['cargo'], CARGO_BUILD_JOBS='2', CARGO_INCREMENTAL='0',
               CARGO_NET_OFFLINE='true', CARGO_CACHE_AUTO_CLEAN_FREQUENCY='never',
               SOURCE_DATE_EPOCH=str(epoch), TMPDIR=roots['temp'], TMP=roots['temp'], TEMP=roots['temp'],
               CFLAGS=shlex.join(flags['native']), CXXFLAGS=shlex.join(flags['native']),
               CC_SHELL_ESCAPED_FLAGS='1')
    # Windows PATH is set by MSYS2; putting standalone Rust first mixes GCC DLLs.
    if not windows:
        env['PATH'] = str(toolchain) + os.pathsep + env.get('PATH', '')
    for variable, program in (('CC', 'gcc'), ('CXX', 'g++'), ('AR', 'ar')):
        selected = shutil.which(program + suffix, path=env.get('PATH'))
        require(selected is not None, 'missing native compiler tool: ' + program)
        env[variable] = selected
    return env, flags


def toolchain_matches(output, target):
    return ('release: ' + VERSION) in output.splitlines() and ('host: ' + target) in output.splitlines()


def option_rejected(result, stderr):
    return result['exitCode'] == 2 and 'unknown argument' in stderr


def runtime_inventory(output, windows, bundle='minimal'):
    require(bundle in rm.BUNDLES, 'unknown bundle')
    bt = bundle in {'full', 'compat'}
    if windows:
        libraries = sorted({value.lower() for value in re.findall(r'DLL Name:\s*(\S+)', output)})
        system = rm.WINDOWS_BT_SYSTEM if bt else rm.WINDOWS_SYSTEM
        require(set(libraries) == system | ({'libstdc++-6.dll'} if bt else set()), 'unexpected Windows runtime imports')
        return {'systemLibraries': sorted(system), 'additionalRuntimeFiles': rm.reviewed_runtime_files() if bt else []}
    libraries = sorted(set(re.findall(r'Shared library: \[([^\]]+)\]', output)))
    require(set(libraries) == (rm.LINUX_BT_SYSTEM if bt else rm.LINUX_SYSTEM), 'unexpected Linux runtime imports')
    versions = re.findall(r'GLIBC_([0-9.]+)', output)
    require(bool(versions), 'missing glibc version requirements')
    minimum = max(versions, key=lambda value: tuple(map(int, value.split('.'))))
    match = re.search(r'Requesting program interpreter: ([^\]]+)\]', output)
    require(match is not None and match[1] == '/lib64/ld-linux-x86-64.so.2' and minimum == ('2.38' if bt else '2.34'),
            'unexpected Linux loader or glibc requirement')
    result = {'systemLibraries': libraries, 'additionalRuntimeFiles': [],
              'minimumGlibc': minimum, 'interpreter': match[1]}
    if bt:
        versions = re.findall(r'GLIBCXX_([0-9.]+)', output)
        require(bool(versions), 'missing C++ runtime requirement')
        result['minimumGlibcxx'] = max(versions, key=lambda value: tuple(map(int, value.split('.'))))
        require(result['minimumGlibcxx'] == '3.4.30', 'unexpected C++ runtime requirement')
    return result


def source_hashes(root):
    names = subprocess.check_output(['git', 'ls-files', '-z'], cwd=root).decode().split('\0')
    selected = [name for name in names if name and (name.startswith((
        'crates/', 'bin/', 'vendor/', 'native/', 'assets/', 'compat/', 'generated/', '.cargo/'))
        or name in {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'})]
    selected += ['scripts/release_build.py', 'scripts/release_manifest.py', 'scripts/local_hardening.py', 'scripts/bt_native.py']
    return {name: h.digest(root / name) for name in sorted(set(selected))}


def verify_cache_sources(previous, current):
    # This Python driver is not compiled into the executable. Repairs to its
    # assertions may reuse Cargo's intermediates; retain both driver identities.
    excluded = {'scripts/release_build.py'}
    require({k: v for k, v in previous.items() if k not in excluded} ==
            {k: v for k, v in current.items() if k not in excluded}, 'cached build source drift')


def comparison_flags(flags):
    result = {}
    for key, prefix in (('rust', '--remap-path-prefix='), ('native', '-ffile-prefix-map=')):
        values = flags[key]
        result[key] = {'destinations': sorted(value.rsplit('=', 1)[1] for value in values if value.startswith(prefix)),
                       'options': [value for value in values if not value.startswith(prefix)]}
    return result


def native_comparison(manifest):
    inputs = dict(manifest['inputs'])
    release = dict(inputs['releasePaths'])
    release['flags'] = sorted(value.rsplit('=', 1)[-1] for value in release['flags'])
    inputs['releasePaths'] = release
    files = {name: value for name, value in manifest['files'].items()
             if name.startswith('include/') or name in {
                 'lib/libcrypto.a', 'lib/libssl.a', 'lib/libtorrent-rasterbar.a'}}
    require(all('lib/' + name in files for name in ('libcrypto.a', 'libssl.a', 'libtorrent-rasterbar.a')),
            'incomplete native link inputs')
    return {'inputs': inputs, 'compiler': manifest['compiler'], 'consumedFiles': len(files),
            'filesSha256': hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest(),
            'archives': {name: value for name, value in files.items() if name.startswith('lib/')}}


def comparison_reference(record, target, sources, bundles=('minimal', 'standard')):
    require(record.get('passed') is True and record.get('target') == target, 'passing matching reference required')
    verify_cache_sources(record['sourceHashes'], sources)
    require(re.fullmatch(r'[0-9]+', record.get('sourceDateEpoch', '')) is not None, 'invalid reference epoch')
    builds = record.get('builds', [])
    require(len(builds) == len(bundles) and {row['bundle'] for row in builds} == set(bundles),
            'complete reference bundle set required')
    return record['sourceDateEpoch']


def prepare(args, root=ROOT):
    windows = os.name == 'nt'
    require(windows or sys.platform == 'linux', 'only native Linux and Windows-GNU are supported')
    target = WINDOWS if windows else LINUX
    bundles = getattr(args, 'bundles', ['minimal', 'standard'])
    require(bundles and len(bundles) == len(set(bundles)) and set(bundles) <= rm.BUNDLES, 'invalid bundle set')
    native = getattr(args, 'native_dir', None)
    require(bool(native) == bool(set(bundles) & {'full', 'compat'}), 'full/compat require an explicit native installation')
    require(args.output.is_absolute() and args.toolchain.is_absolute() and args.cargo_home.is_absolute(),
            'absolute output, toolchain and Cargo-home paths required')
    require(args.cargo_home.is_dir() and args.toolchain.is_dir(), 'existing toolchain and cache required')
    require(0 < args.build_timeout <= 1800, 'build timeout must be between 1 and 1800 seconds')
    require(not (args.compare_record and args.reuse_cache), 'independent comparison cannot reuse a target cache')
    require(args.output.parent.is_dir() and not args.output.exists(), 'fresh output directory required')
    minimum_free = (768 if args.reuse_cache else 1536) * 1024 ** 2
    require(shutil.disk_usage(args.output.parent).free >= minimum_free, 'insufficient space for build intermediates')
    roots = {'repo': str(root.resolve()), 'cargo': str(args.cargo_home.resolve()),
             'target': str(args.output / 'target'), 'temp': str(args.output / 'tmp')}
    native_manifest = None
    if native:
        require(native.is_absolute() and native.is_dir(), 'absolute existing native installation required')
        native_manifest = json.loads(read_utf8(native / 'ariax-native.json'))
        expected = {key: h.digest(root / path) for key, path in (
            ('sourcesSha256', 'native/libtorrent/sources.json'), ('patchSha256', 'native/libtorrent/ariax.patch'),
            ('opensslPatchSha256', 'native/libtorrent/openssl.patch'), ('builderSha256', 'scripts/bt_native.py'))}
        release = native_manifest['inputs'].get('releasePaths')
        require(release and release.get('flags'), 'native release path configuration required')
        expected.update(sanitizer='none', releasePaths=release)
        bt_native.verify_manifest(native, target, expected)
        roots['native'] = str(native.resolve())
    previous = None
    if args.reuse_cache:
        require(args.reuse_cache.is_absolute(), 'absolute cache record directory required')
        previous = json.loads(read_utf8(args.reuse_cache / 'result.json'))
        require(previous['target'] == target, 'cached target mismatch')
        verify_cache_sources(previous['sourceHashes'], source_hashes(root))
        for key, variable in (('target', 'CARGO_TARGET_DIR'), ('temp', 'TMPDIR')):
            path = Path(previous['environment'][variable])
            expected = args.reuse_cache / ('target' if key == 'target' else 'tmp')
            require(path.resolve() == expected.resolve() and path.is_dir() and not path.is_symlink(),
                    'cache directory identity mismatch')
            roots[key] = str(path)
    reference = json.loads(read_utf8(args.compare_record)) if args.compare_record else None
    epoch = comparison_reference(reference, target, source_hashes(root), bundles) if reference else subprocess.check_output(
        ['git', 'show', '-s', '--format=%ct', 'HEAD'], cwd=root).decode().strip()
    env, flags = build_environment(os.environ, roots, args.toolchain, windows, epoch)
    if native:
        require(native_manifest['inputs']['releasePaths']['sourceDateEpoch'] == epoch, 'native release epoch drift')
        env.update(ARIAX_BT_NATIVE_DIR=str(native), ARIAX_BT_SANITIZER='none')
    if reference:
        require(comparison_flags(flags) == comparison_flags(reference['flags']), 'reference build option drift')
    if previous:
        require(flags == previous['flags'] and epoch == previous['sourceDateEpoch'] and
                all(env.get(k) == v for k, v in previous['environment'].items()), 'cached build environment drift')
    args.output.mkdir()
    if not previous:
        (args.output / 'tmp').mkdir()
    record = {'schema': 1, 'passed': False, 'releaseApproved': False, 'target': target,
              'sourceCommit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root).decode().strip(),
              'sourceHashes': source_hashes(root), 'flags': flags, 'sourceDateEpoch': epoch,
              'environment': {key: env[key] for key in ('RUSTC', 'RUSTDOC', 'CARGO_HOME', 'CARGO_TARGET_DIR',
                  'CARGO_ENCODED_RUSTFLAGS', 'CFLAGS', 'CXXFLAGS', 'CC_SHELL_ESCAPED_FLAGS',
                  'CC', 'CXX', 'AR', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL', 'TMPDIR')}, 'builds': []}
    if native:
        record['nativeManifestSha256'] = h.digest(native / 'ariax-native.json')
        record['nativeComparison'] = native_comparison(native_manifest)
        record['environment'].update(ARIAX_BT_NATIVE_DIR=str(native), ARIAX_BT_SANITIZER='none')
        if reference:
            require(record['nativeComparison'] == reference.get('nativeComparison'), 'native reference drift')
    suffix = '.exe' if windows else ''
    cargo = args.toolchain / ('cargo' + suffix)
    if previous:
        record['cacheReuse'] = {'recordSha256': h.digest(args.reuse_cache / 'result.json'),
                               'previousDriverSha256': previous['sourceHashes']['scripts/release_build.py'],
                               'scope': 'Same compiled sources, flags and tool identities; Python driver repair only.'}
    if reference:
        record['independentComparison'] = {'referenceRecordSha256': h.digest(args.compare_record),
            'referenceSourceCommit': reference['sourceCommit'], 'referenceDriverSha256': reference['sourceHashes']['scripts/release_build.py'],
            'freshTarget': True, 'freshTemporaryDirectory': True, 'sharedSourceCacheAndToolchain': True,
            'sharedVerifiedNativeInstallation': bool(native) and str(native) == reference['environment'].get('ARIAX_BT_NATIVE_DIR')}
    try:
        def checked(command, name, timeout=20):
            result = h.run(command, args.output / name, timeout=timeout, env=env, cwd=root)
            require(result['passed'], 'command failed: ' + str(name))
            return result
        record['compiler'] = checked([env['RUSTC'], '--version', '--verbose'], 'compiler')
        require(toolchain_matches(read_utf8(args.output / 'compiler/stdout.log'), target),
                'pinned native Rust toolchain required')
        record['nativeCompiler'] = checked([env['CC'], '-dumpmachine'], 'native-compiler')
        machine = read_utf8(args.output / 'native-compiler/stdout.log').strip()
        require(machine == ('x86_64-w64-mingw32' if windows else 'x86_64-linux-gnu'),
                'native compiler target mismatch')
        record['cargoSha256'] = h.digest(cargo)
        for prior in (previous, reference):
            if prior:
                require(record['cargoSha256'] == prior['cargoSha256'] and
                        record['compiler']['binarySha256'] == prior['compiler']['binarySha256'] and
                        record['nativeCompiler']['binarySha256'] == prior['nativeCompiler']['binarySha256'],
                        'compiler identity drift')
        for bundle in bundles:
            item = {'bundle': bundle}
            record['builds'].append(item)
            h.save(args.output / 'result.json', record)
            command = [cargo, 'build', '--locked', '--offline', '-p', 'ariax-cli',
                       '--no-default-features', '--features', bundle, '--profile', 'release-cli']
            item['build'] = checked(command, bundle + '/build', timeout=args.build_timeout)
            binary = args.output / bundle / ('ariax' + suffix)
            shutil.copyfile(Path(roots['target']) / 'release-cli' / binary.name, binary)
            binary.chmod(0o755)
            item.update(binarySha256=h.digest(binary), bytes=binary.stat().st_size,
                        pathAudit=rm.audit_binary_paths(binary))
            if reference:
                prior = next(row for row in reference['builds'] if row['bundle'] == bundle)
                prior_binary = args.compare_record.parent / bundle / binary.name
                require(h.digest(prior_binary) == prior['binarySha256'], 'reference binary drift')
                item['referenceBinarySha256'] = prior['binarySha256']
                item['matchesIndependentBuild'] = item['binarySha256'] == prior['binarySha256']
                require(item['matchesIndependentBuild'], 'independent binary comparison failed')
            require(item['pathAudit']['passed'], 'binary retains absolute source paths')
            inspection = ['objdump', '-p', str(binary)] if windows else [
                'readelf', '-l', '-d', '--version-info', str(binary)]
            item['inspection'] = checked(inspection, bundle + '/imports')
            item['runtime'] = runtime_inventory(read_utf8(args.output / bundle / 'imports/stdout.log'), windows, bundle)
            item['help'] = checked([binary, '--help'], bundle + '/help')
            require('Usage: ariax' in read_utf8(args.output / bundle / 'help/stdout.log'), 'missing help output')
            item['rejection'] = h.run([binary, '--ariax-intentionally-invalid-option'],
                                     args.output / bundle / 'rejection', timeout=20, env=env, cwd=root)
            item['rejectionObserved'] = option_rejected(item['rejection'], read_utf8(
                args.output / bundle / 'rejection/stderr.log'))
            require(item['rejectionObserved'], 'missing unknown-option rejection')
            h.save(args.output / 'result.json', record)
            print(json.dumps({'bundle': bundle, 'target': target, 'pathAuditPassed': True}), flush=True)
        require(record['sourceHashes'] == source_hashes(root), 'source changed during build')
        record['passed'] = True
        if args.prune_target:
            directory = Path(roots['target'])
            inventory = {p.relative_to(directory).as_posix(): p.stat().st_size
                         for p in directory.rglob('*') if p.is_file()}
            h.save(args.output / 'discarded-intermediates.json', inventory)
            shutil.rmtree(directory)
            record['cleanup'] = {'files': len(inventory), 'bytes': sum(inventory.values()),
                                 'inventorySha256': h.digest(args.output / 'discarded-intermediates.json')}
    except (Exception, KeyboardInterrupt) as error:
        record.update(passed=False, error=str(error) or type(error).__name__)
    finally:
        h.save(args.output / 'result.json', record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--toolchain', type=Path, required=True, help='pinned native Rust distribution bin directory')
    parser.add_argument('--cargo-home', type=Path, required=True, help='existing offline Cargo cache')
    parser.add_argument('--output', type=Path, required=True, help='fresh absolute directory on the temporary volume')
    parser.add_argument('--bundles', nargs='+', choices=sorted(rm.BUNDLES), default=['minimal', 'standard'])
    parser.add_argument('--native-dir', type=Path, help='verified release-path native installation for full/compat')
    parser.add_argument('--prune-target', action='store_true')
    parser.add_argument('--reuse-cache', type=Path, help='prior preparation directory with identical compiled inputs and flags')
    parser.add_argument('--compare-record', type=Path, help='passing reference record for an independent fresh-target comparison')
    parser.add_argument('--build-timeout', type=int, default=600, help='per-bundle seconds, up to 1800; not a performance threshold')
    args = parser.parse_args()
    result = prepare(args)
    print(json.dumps({key: result[key] for key in ('passed', 'target', 'releaseApproved')}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
