#!/usr/bin/env python3
"""Check packaged CLI operation with a reduced environment on the current host."""
import argparse
import ctypes
import json
import os
from pathlib import Path, PureWindowsPath
import platform
import queue
import subprocess
import threading

import local_hardening as h
import release_manifest as rm


def require(condition, message):
    if not condition:
        raise ValueError(message)


def reduced_environment(base, temporary, windows):
    env = {'TMPDIR': str(temporary), 'TMP': str(temporary), 'TEMP': str(temporary)}
    if windows:
        system = next((v for k, v in base.items() if k.casefold() == 'systemroot'), None)
        require(system and PureWindowsPath(system).is_absolute(), 'Windows SystemRoot required')
        env.update(SystemRoot=system, WINDIR=system, PATH=str(PureWindowsPath(system) / 'System32'))
    else:
        env.update(PATH='/usr/bin:/bin', LANG='C.UTF-8', LC_ALL='C.UTF-8', TZ='UTC')
    return env


def executable_maps(text):
    paths = set()
    for line in text.splitlines():
        fields = line.split(None, 5)
        if len(fields) == 6 and 'x' in fields[1] and fields[5].startswith('/'):
            paths.add(fields[5])
    require(bool(paths), 'no executable file mappings observed')
    return sorted(paths)


def loaded_modules(pid, windows):
    if not windows:
        return executable_maps(Path(f'/proc/{pid}/maps').read_text(encoding='utf-8'))
    from ctypes import wintypes as w

    class Module(ctypes.Structure):
        _fields_ = [('size', w.DWORD), ('moduleId', w.DWORD), ('processId', w.DWORD),
                    ('globalUsage', w.DWORD), ('processUsage', w.DWORD),
                    ('baseAddress', ctypes.c_void_p), ('baseSize', w.DWORD), ('module', w.HMODULE),
                    ('name', w.WCHAR * 256), ('path', w.WCHAR * 260)]
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
    kernel.CreateToolhelp32Snapshot.restype = w.HANDLE
    kernel.Module32FirstW.argtypes = [w.HANDLE, ctypes.POINTER(Module)]
    kernel.Module32NextW.argtypes = [w.HANDLE, ctypes.POINTER(Module)]
    kernel.CloseHandle.argtypes = [w.HANDLE]
    snapshot = kernel.CreateToolhelp32Snapshot(0x08 | 0x10, pid)
    require(snapshot not in (None, ctypes.c_void_p(-1).value), 'module snapshot failed')
    try:
        entry = Module(); entry.size = ctypes.sizeof(entry)
        require(kernel.Module32FirstW(snapshot, ctypes.byref(entry)), 'module enumeration failed')
        paths = []
        while True:
            paths.append(entry.path)
            if not kernel.Module32NextW(snapshot, ctypes.byref(entry)):
                require(ctypes.get_last_error() == 18, 'module enumeration incomplete')
                break
        return sorted(set(paths))
    finally:
        kernel.CloseHandle(snapshot)


def module_inventory(paths, binary, windows, system_root=None, runtime=None):
    rows = []
    runtime = runtime or {'additionalRuntimeFiles': [], 'systemLibraries': sorted(rm.LINUX_SYSTEM)}
    packaged = {item['destination'].casefold(): item for item in runtime['additionalRuntimeFiles']}
    observed = set()
    for value in paths:
        if windows:
            path = PureWindowsPath(value)
            application = path == PureWindowsPath(str(binary))
            system = path.is_relative_to(PureWindowsPath(system_root) / 'System32')
        else:
            path = Path(value)
            application = path.resolve() == binary.resolve()
            sonames = set(runtime['systemLibraries']) | {'ld-linux-x86-64.so.2'}
            system = any(path.resolve().is_relative_to(Path(prefix).resolve()) for prefix in ('/lib', '/usr/lib')) and (
                path.name in sonames or any((path.parent / name).resolve() == path.resolve() for name in sonames))
        kind = 'application' if application else 'system' if system else 'host-extra'
        if windows and path.name.casefold() in packaged:
            item = packaged[path.name.casefold()]
            require(path == PureWindowsPath(str(binary)).parent / item['destination'], 'runtime loaded outside package')
            rm.verify_file(Path(value), item)
            observed.add(path.name.casefold())
            kind = 'packaged-runtime'
        rows.append({'path': value, 'kind': kind})
    require(any(row['kind'] == 'application' for row in rows), 'application module missing')
    require(observed == set(packaged), 'packaged runtime module missing')
    return rows


def verify_package(directory, crypto_backend=None):
    manifest = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))
    if crypto_backend is not None:
        require(crypto_backend in {'default', 'openssl'}
                and manifest.get('cryptoBackend', 'default') == crypto_backend,
                'package crypto backend mismatch')
    checked = set()
    for line in (directory / 'SHA256SUMS').read_text(encoding='utf-8').splitlines():
        digest, name = line.split('  ', 1)
        require(name not in checked, 'duplicate checksum path')
        checked.add(name)
        require(h.digest(rm.contained(directory, name)) == digest, 'package checksum mismatch')
    require(checked == {row['destination'] for row in manifest['files']} | {'manifest.json'},
            'incomplete checksum inventory')
    require({p.relative_to(directory).as_posix() for p in directory.rglob('*') if p.is_file()} ==
            checked | {'SHA256SUMS'}, 'unexpected package files')
    binary = rm.contained(directory, manifest['binary']['destination'])
    require(h.digest(binary) == manifest['binary']['sha256'], 'binary identity mismatch')
    return manifest, binary


def verify_version(result, bundle):
    require(result.get('version') == '0.1.0', 'version query failed')
    require(bundle in rm.BUNDLES, 'unknown package bundle')
    expected = {'HTTP', 'HTTPS', 'JSON-RPC', 'Session', 'Async DNS', 'Metalink'}
    if bundle != 'minimal':
        expected |= {'FTP', 'SFTP'}
    if bundle in {'full', 'compat'}:
        expected.add('BitTorrent')
    require(set(result.get('enabledFeatures', [])) == expected, 'packaged feature bundle mismatch')


def rpc_check(binary, work, env, windows, runtime=None, bundle='minimal'):
    for name in ('state', 'control', 'output'):
        (work / name).mkdir()
    args = [str(binary), '--profile=compact', '--rpc-stdio-framing=ndjson',
            '--rpc-stdio-events=false', '--rpc-stdio-eof=shutdown', '--rpc-stdio',
            str(work / 'state/session.db'), str(work / 'control'), str(work / 'output')]
    result = {'command': args, 'passed': False}
    process = None
    messages = queue.Queue()
    try:
        with (work / 'rpc-stderr.log').open('wb') as err, (work / 'rpc-stdout.log').open('wb') as out:
            process = subprocess.Popen(args, cwd=work / 'cwd', env=env, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=err)
            def read_responses():
                try:
                    while True:
                        line = process.stdout.readline(65537)
                        if not line:
                            messages.put(EOFError('RPC output closed')); return
                        out.write(line); out.flush()
                        if len(line) > 65536 or not line.endswith(b'\n'):
                            raise ValueError('oversized or incomplete RPC line')
                        messages.put(line)
                except Exception as error:
                    messages.put(error)
            reader = threading.Thread(target=read_responses, daemon=True); reader.start()
            result['responses'] = []
            methods = ('aria2.getVersion', 'aria2.getGlobalStat', 'aria2.noSuchPackagingMethod')
            for number, method in enumerate(methods, 1):
                request = {'jsonrpc': '2.0', 'id': number, 'method': method, 'params': []}
                process.stdin.write(json.dumps(request).encode() + b'\n'); process.stdin.flush()
                line = messages.get(timeout=30)
                if isinstance(line, Exception):
                    raise line
                response = json.loads(line)
                require(response.get('id') == number and response.get('jsonrpc') == '2.0', 'wrong RPC response')
                if number == 1:
                    verify_version(response.get('result', {}), bundle)
                elif number == 2:
                    require(response.get('result', {}).get('numActive') == '0', 'empty-session query failed')
                else:
                    require(response.get('error', {}).get('code') == -32601, 'unknown RPC method not rejected')
                result['responses'].append(response)
            paths = loaded_modules(process.pid, windows)
            result['loadedModules'] = module_inventory(paths, binary, windows, env.get('SystemRoot'), runtime)
            process.stdin.close()
            result['exitCode'] = process.wait(timeout=30)
            require(result['exitCode'] == 0, 'RPC EOF shutdown failed')
            reader.join(timeout=5)
            require(not reader.is_alive(), 'RPC output reader did not stop')
            result['passed'] = True
    except Exception as error:
        result['error'] = str(error) or type(error).__name__
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=30)
            result['cleanup'] = 'owned CLI process exited; no fixture child processes launched'
        for name in ('rpc-stdout.log', 'rpc-stderr.log'):
            if (work / name).is_file():result[name + 'Sha256'] = h.digest(work / name)
    h.save(work / 'rpc-result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packages', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--bundles', nargs='+', choices=sorted(rm.BUNDLES), default=['minimal', 'standard'])
    parser.add_argument('--crypto-backend', choices=('default', 'openssl'), default='default')
    args = parser.parse_args()
    require(args.packages.is_absolute() and args.output.is_absolute(), 'absolute paths required')
    args.output.mkdir(parents=True, exist_ok=False)
    windows = os.name == 'nt'
    target = 'x86_64-pc-windows-gnu' if windows else 'x86_64-unknown-linux-gnu'
    report = {'schema': 1, 'host': platform.platform(), 'target': target, 'functionalPassed': False,
              'freshOsAcceptance': False, 'releaseApproved': False, 'packages': []}
    if not windows:
        preload = Path('/etc/ld.so.preload')
        report['systemPreload'] = preload.read_text(encoding='utf-8') if preload.exists() else None
    try:
        require(len(args.bundles) == len(set(args.bundles)), 'duplicate bundle')
        for bundle in args.bundles:
            backend = rm.effective_crypto_backend(bundle, args.crypto_backend)
            package = args.packages / (target + '-' + bundle
                                      + ('-openssl' if backend == 'openssl' else ''))
            manifest, binary = verify_package(package, backend)
            work = args.output / bundle; work.mkdir()
            for name in ('tmp', 'cwd'):(work / name).mkdir()
            env = reduced_environment(os.environ, work / 'tmp', windows)
            row = {'bundle': bundle, 'cryptoBackend': backend,
                   'binarySha256': h.digest(binary), 'environment': env}
            report['packages'].append(row)
            row['help'] = h.run([binary, '--help'], work / 'help', timeout=30, env=env, cwd=work / 'cwd')
            require(row['help']['passed'], 'packaged help failed')
            row['rpc'] = rpc_check(binary, work, env, windows, manifest['runtime'], bundle)
            require(row['rpc']['passed'], 'packaged RPC check failed')
            row['reopen'] = h.run([binary, '--check-bootstrap', work / 'state/session.db',
                                  work / 'control', work / 'output'], work / 'reopen', timeout=30,
                                 env=env, cwd=work / 'cwd')
            require(row['reopen']['passed'] and 'bootstrap ok: 0 tasks' in (
                work / 'reopen/stdout.log').read_text(encoding='utf-8'), 'packaged database reopen failed')
            require(h.digest(binary) == row['binarySha256'], 'package binary changed')
            row['onlySystemOrPackagedModulesObserved'] = all(m['kind'] != 'host-extra' for m in row['rpc']['loadedModules'])
            print(json.dumps({'bundle': bundle, 'functionalPassed': True,
                              'onlySystemOrPackagedModulesObserved': row['onlySystemOrPackagedModulesObserved']}), flush=True)
        report['functionalPassed'] = True
    except Exception as error:
        report['error'] = str(error) or type(error).__name__
    finally:
        h.save(args.output / 'result.json', report)
    print(json.dumps({k: report[k] for k in ('functionalPassed', 'freshOsAcceptance', 'releaseApproved')}))
    return 0 if report['functionalPassed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
