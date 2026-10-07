#!/usr/bin/env python3
"""Validate and stage explicit retained package drafts; never build or publish."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil

ROOT = Path(__file__).resolve().parents[1]
CATALOG = 'distribution/package-manifests.json'
TARGETS = {'x86_64-unknown-linux-gnu', 'x86_64-pc-windows-gnu'}
BUNDLES = {'minimal', 'standard', 'full', 'compat'}
WINDOWS_SYSTEM = {'iphlpapi.dll', 'kernel32.dll', 'advapi32.dll',
                  'api-ms-win-core-synch-l1-2-0.dll', 'bcrypt.dll',
                  'bcryptprimitives.dll', 'crypt32.dll', 'msvcrt.dll',
                  'ntdll.dll', 'oleaut32.dll', 'ws2_32.dll'}
LINUX_SYSTEM = {'libc.so.6', 'libgcc_s.so.1', 'libm.so.6'}
LINUX_BT_SYSTEM = LINUX_SYSTEM | {'libstdc++.so.6', 'ld-linux-x86-64.so.2'}
WINDOWS_BT_SYSTEM = WINDOWS_SYSTEM | {'user32.dll', 'mswsock.dll'}
WINDOWS_NOTICES = {'winapi-MIT.txt', 'winapi-Apache-2.0.txt', 'mingw-crt.txt', 'mingw-w64.txt',
                   'mingw-runtime.txt', 'winpthread.txt', 'gcc-GPL-3.0.txt', 'gcc-runtime-exception.txt'}
LINUX_NOTICES = {'glibc-copyright.txt', 'libgcc-copyright.txt'}
REQUIRED = {'LICENSE', 'README.txt', 'DATA-NOTICE.txt', 'THIRD-PARTY-NOTICES.txt',
            'license-review.json', 'source-data/public-suffix-list.dat',
            'source-data/public-suffix-list.toml', 'source-data/iana-special-purpose.pin',
            'source-data/iana-ipv4-special-registry.csv', 'source-data/iana-ipv6-special-registry.csv',
            'licenses/MPL-2.0.txt', 'licenses/CC0-1.0.txt', 'licenses/rust-stdlib-1.97.1.html',
            'licenses/rust-compiler-builtins.txt', 'licenses/rust-MIT.txt', 'licenses/rust-Apache-2.0.txt'}
SOURCE_ONLY = {'generated/aria2_options.json', 'generated/aria2_rpc.json',
               'generated/aria2_compat.json', 'distribution/aria2-source-notice.txt',
               'distribution/licenses/aria2-GPL-2.0.txt'}
PATH_PATTERNS = {'wslMount': rb'/mnt/[a-z]/', 'windowsTemp': rb'(?i)[a-z]:[/\\]temp[/\\]',
                 'windowsUser': rb'(?i)[a-z]:[/\\](?:Users|UserData)[/\\]',
                 'unixHome': rb'/(?:home|Users)/[^/\s\x00]+/'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def effective_crypto_backend(bundle, requested):
    """Resolve current build policy; historical manifests keep their recorded backend."""
    require(bundle in BUNDLES and requested in {'default', 'openssl'}, 'unknown build selection')
    return 'openssl' if bundle in {'full', 'compat'} else requested


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def audit_binary_paths(path):
    data = path.read_bytes()
    counts = {name: len(re.findall(pattern, data)) for name, pattern in PATH_PATTERNS.items()}
    return {'passed': not any(counts.values()), 'counts': counts,
            'scope': 'Known absolute workstation/cache path patterns; not a complete reproducibility proof.'}


def reviewed_runtime_files(root=ROOT):
    return json.loads((root / 'distribution/runtime-files.json').read_text(encoding='utf-8'))['files']


def validate_runtime(runtime, windows, bundle, names, root=ROOT):
    bt = bundle in {'full', 'compat'}
    openssl = runtime.get('cryptoBackend', 'default') == 'openssl'
    require(runtime.get('cryptoBackend', 'default') in {'default', 'openssl'}, 'unknown crypto backend')
    expected = (WINDOWS_BT_SYSTEM if bt else WINDOWS_SYSTEM) if windows else (LINUX_BT_SYSTEM if bt else LINUX_SYSTEM)
    if windows and openssl:
        expected = (expected - {'bcrypt.dll'}) | {'user32.dll'}
    libraries = runtime['systemLibraries']
    require(len(libraries) == len(set(libraries)) and set(libraries) == expected,
            'unknown or missing runtime dependency')
    files = runtime['additionalRuntimeFiles']
    require(files == [], 'additional runtime files are forbidden; link runtimes statically')
    if not windows:
        require(runtime['minimumGlibc'] == ('2.38' if bt or openssl else '2.34') and
                runtime['interpreter'] == '/lib64/ld-linux-x86-64.so.2', 'unreviewed Linux runtime requirement')
        if bt:
            require(runtime.get('minimumGlibcxx') == '3.4.30', 'unreviewed C++ runtime requirement')


def relative(value):
    require(isinstance(value, str) and 0 < len(value) <= 240, 'invalid path')
    path = PurePosixPath(value)
    require(bool(path.parts) and not path.is_absolute() and path.as_posix() == value
            and all(re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.+-]*', part)
                    and not part.endswith('.') and not re.fullmatch(
                        r'(?i)(con|prn|aux|nul|com[1-9]|lpt[1-9])', part.split('.')[0])
                    for part in path.parts), 'unsafe path')
    return path


def contained(root, name):
    path = root.joinpath(*relative(name).parts)
    require(path.resolve().is_relative_to(root.resolve()), 'source escapes root')
    require(path.is_file() and not path.is_symlink(), 'missing or symlinked source')
    return path


def verify_file(path, record):
    require(type(record.get('bytes')) is int and record['bytes'] > 0, 'invalid byte count')
    require(re.fullmatch(r'[a-f0-9]{64}', record.get('sha256', '')) is not None, 'invalid digest')
    require(path.stat().st_size == record['bytes'] and digest(path) == record['sha256'],
            'source hash or size mismatch: ' + path.name)


def validate(catalog, root=ROOT, artifacts=None):
    require(type(catalog.get('schema')) is int and catalog['schema'] == 1 and catalog.get('releaseApproved') is False,
            'only unapproved version-1 drafts are supported')
    require(catalog.get('dependencyLockSha256') == digest(root / 'Cargo.lock'), 'dependency lock drift')
    inventory_source = catalog.get('noticeInventorySource',
                                   'performance-evidence/phase7-release-license-inventory-2026-10-05.json')
    inventory = contained(root, inventory_source)
    require(catalog.get('noticeInventorySha256') == digest(inventory), 'notice inventory drift')
    if 'noticeInventorySource' in catalog:
        notices = json.loads(inventory.read_text(encoding='utf-8'))
        require(notices.get('cargoLockSha256') == catalog['dependencyLockSha256'], 'inventory lock drift')
        packaged_inventory = [item for item in catalog['commonFiles']
                              if item['destination'] == 'license-inventory.json']
        require(len(packaged_inventory) == 1 and packaged_inventory[0]['source'] == inventory_source
                and packaged_inventory[0]['sha256'] == catalog['noticeInventorySha256'],
                'missing or mismatched packaged inventory')
        packaged_notices = [item for item in catalog['commonFiles']
                            if item['destination'] == 'THIRD-PARTY-NOTICES.txt']
        require(len(packaged_notices) == 1
                and packaged_notices[0]['sha256'] == notices['noticeArtifact']['sha256'],
                'notice collection drift')
    require(set(catalog.get('excludedSourceArtifacts', [])) == SOURCE_ONLY, 'source exclusion drift')
    expected = {target + '-' + bundle for target in TARGETS for bundle in BUNDLES}
    optional = {identity + '-openssl' for identity in expected}
    ids = [package.get('id') for package in catalog['packages']]
    require(len(ids) == len(set(ids)) and expected <= set(ids) <= expected | optional,
            'incomplete or duplicate package set')
    for package in catalog['packages']:
        backend = package.get('cryptoBackend', 'default')
        require(backend in {'default', 'openssl'}, 'unknown crypto backend')
        require(package['target'] in TARGETS and package['bundle'] in BUNDLES
                and package['id'] == package['target'] + '-' + package['bundle']
                    + ('-openssl' if backend == 'openssl' else ''), 'package identity mismatch')
        require(package.get('releaseApproved') is False, 'release approval is not a manifest operation')
        names = set()
        records = catalog['commonFiles'] + package['files']
        for item in records:
            name = relative(item['destination']).as_posix().casefold()
            require(name not in names and name not in {'ariax', 'ariax.exe', 'manifest.json', 'sha256sums'},
                    'duplicate or reserved destination')
            names.add(name)
            require(item['source'] not in SOURCE_ONLY, 'source-only GPL inventory in binary package')
            verify_file(contained(root, item['source']), item)
        require({name.casefold() for name in REQUIRED} <= names, 'missing required notice or covered source')
        platform_notices = WINDOWS_NOTICES if package['target'].endswith('windows-gnu') else LINUX_NOTICES
        require({'licenses/' + name.casefold() for name in platform_notices} <= names,
                'missing platform runtime notice')
        if package['status'] == 'planned':
            require(package['bundle'] in {'full', 'compat'} and package['binary'] is None
                    and package['runtime'] is None and package.get('remaining'), 'invalid planned package')
            continue
        require(package['status'] in {'draft-retained', 'validated-removed'},
                'unreviewed retained bundle')
        removed = package['status'] == 'validated-removed'
        if removed:
            require(package.get('retention', {}).get('binaryPresent') is False
                    and package['retention'].get('validationEvidence'), 'missing removed-artifact evidence')
        windows = package['target'].endswith('windows-gnu')
        binary, runtime = package['binary'], package['runtime']
        require(binary['destination'] == ('ariax.exe' if windows else 'ariax'), 'wrong binary destination')
        relative(binary['artifactPath'])
        require(runtime.get('cryptoBackend', 'default') == backend, 'runtime backend mismatch')
        validate_runtime(runtime, windows, package['bundle'], names, root)
        if artifacts is not None and not removed:
            path = contained(artifacts, binary['artifactPath'])
            verify_file(path, binary)
            require(audit_binary_paths(path) == binary['pathAudit'], 'binary path audit mismatch')
            for item in runtime['additionalRuntimeFiles']:
                verify_file(contained(artifacts, item['artifactPath']), item)
    return catalog


def stage(catalog, artifacts, output, root=ROOT):
    validate(catalog, root, artifacts)
    require(any(p['status'] == 'draft-retained' for p in catalog['packages']),
            'no retained binaries are available to stage')
    output.mkdir(parents=True, exist_ok=False)
    staged = []
    for package in catalog['packages']:
        if package['status'] != 'draft-retained':
            continue
        directory = output / package['id']
        directory.mkdir()
        files = []
        entries = [(contained(root, item['source']), item) for item in catalog['commonFiles'] + package['files']]
        binary = package['binary']
        entries.append((contained(artifacts, binary['artifactPath']), binary))
        entries += [(contained(artifacts, item['artifactPath']), item)
                    for item in package['runtime']['additionalRuntimeFiles']]
        for source, item in entries:
            destination = directory / item['destination']
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            verify_file(destination, item)
            if item is binary:
                destination.chmod(0o755)
            files.append({key: item[key] for key in ('destination', 'sha256', 'bytes')})
        manifest = dict(package, files=files, status='staged-retained-draft', releaseApproved=False,
                        noticeScope=catalog['noticeScope'], dependencyLockSha256=catalog['dependencyLockSha256'])
        manifest_path = directory / 'manifest.json'
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
        hashes = {item['destination']: item['sha256'] for item in files}
        hashes['manifest.json'] = digest(manifest_path)
        (directory / 'SHA256SUMS').write_text(''.join(f'{value}  {name}\n' for name, value in sorted(hashes.items())),
                                             encoding='utf-8')
        staged.append(package['id'])
    return {'staged': staged, 'planned': [p['id'] for p in catalog['packages'] if p['status'] == 'planned'],
            'removed': [p['id'] for p in catalog['packages'] if p['status'] == 'validated-removed'],
            'binaryPathAuditPassed': all(p['binary']['pathAudit']['passed'] for p in catalog['packages'] if p['binary']),
            'releaseApproved': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    require((args.artifacts is None) == (args.output is None), 'artifacts and output must be supplied together')
    catalog = json.loads((ROOT / CATALOG).read_text(encoding='utf-8'))
    if args.output is None:
        validate(catalog)
        print(f"{len(catalog['packages'])} package manifests validated. No release approval.")
    else:
        print(json.dumps(stage(catalog, args.artifacts, args.output)))


if __name__ == '__main__':
    main()
