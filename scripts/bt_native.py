#!/usr/bin/env python3
"""Build the pinned BitTorrent dependencies for one native Rust target ABI."""

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "native/libtorrent/sources.json"
PATCH = ROOT / "native/libtorrent/ariax.patch"
TARGETS = {
    "x86_64-unknown-linux-gnu": ("Linux", "x86_64", "linux-x86_64"),
    "aarch64-unknown-linux-gnu": ("Linux", "aarch64", "linux-aarch64"),
    "x86_64-apple-darwin": ("Darwin", "x86_64", "darwin64-x86_64-cc"),
    "aarch64-apple-darwin": ("Darwin", "aarch64", "darwin64-arm64-cc"),
    "x86_64-pc-windows-msvc": ("Windows", "x86_64", "VC-WIN64A"),
    "x86_64-pc-windows-gnu": ("Windows", "x86_64", "mingw64"),
}


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def inventory(root):
    return {p.relative_to(root).as_posix(): digest(p)
            for p in sorted(root.rglob("*")) if p.is_file()}


@contextlib.contextmanager
def target_lock(work):
    """A crashed builder releases its OS lock; other target ABIs never share trees."""
    work.mkdir(parents=True, exist_ok=True)
    with (work / ".builder-lock").open("a+b") as lock:
        lock.seek(0)
        lock.write(b"0")
        lock.flush()
        deadline = time.monotonic() + 1800
        while True:
            try:
                lock.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                require(time.monotonic() < deadline, "native builder lock deadline exceeded")
                time.sleep(0.25)
        try:
            yield
        finally:
            lock.seek(0)
            if os.name == "nt":
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def native_target(target, system=None, machine=None):
    require(target in TARGETS, "unsupported native BitTorrent target: " + target)
    system = system or platform.system()
    machine = (machine or platform.machine()).lower()
    machine = {"amd64": "x86_64", "arm64": "aarch64"}.get(machine, machine)
    require(TARGETS[target][:2] == (system, machine), "native target does not match this host")
    if target.endswith("windows-gnu"):
        require(os.environ.get("MSYSTEM") == "MINGW64", "Windows GNU requires MSYS2 MINGW64")
    return TARGETS[target][2]


def extract(archive, destination, headers_only=False):
    """Reject unsafe archive entries before creating the destination tree."""
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        require(len(members) <= 250_000, "native archive has too many entries")
        roots = set()
        selected = []
        total = 0
        for member in members:
            name = PurePosixPath(member.name)
            require(not name.is_absolute() and ".." not in name.parts
                    and "\\" not in member.name and ":" not in member.name,
                    "unsafe native archive path")
            require(bool(name.parts), "empty archive path")
            roots.add(name.parts[0])
            if headers_only and len(name.parts) > 1 and name.parts[1] not in ("boost", "LICENSE_1_0.txt"):
                continue
            require(member.isfile() or member.isdir(), "links and special archive entries are forbidden")
            total += member.size
            require(total <= 2 * 1024**3, "native archive exceeds extracted byte limit")
            selected.append(member)
        require(len(roots) == 1, "native archive needs exactly one source root")
        require(not destination.exists(), "native extraction destination already exists")
        destination.mkdir(parents=True)
        source.extractall(destination, members=selected, filter="data")
    return destination / roots.pop()


def apply_patch(tree, patch):
    """Apply exact unified hunks without fuzz or external patch utilities."""
    lines = patch.read_text().splitlines(keepends=True)
    pos = 0
    pending = []
    while pos < len(lines):
        require(lines[pos].startswith("--- a/"), "invalid native patch header")
        name = lines[pos][6:].strip()
        path = PurePosixPath(name)
        require(not path.is_absolute() and ".." not in path.parts, "unsafe native patch path")
        require(lines[pos + 1] == "+++ b/" + name + "\n", "native patch must modify existing files")
        source = tree / name
        require(source.is_file() and not source.is_symlink(), "invalid native patch source")
        original = source.read_text().splitlines(keepends=True)
        result = []
        cursor = 0
        pos += 2
        while pos < len(lines) and lines[pos].startswith("@@"):
            match = re.fullmatch(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@.*\n", lines[pos])
            require(match, "invalid native patch hunk")
            start = int(match[1]) - 1
            require(start >= cursor, "overlapping native patch hunks")
            before, after = [], []
            pos += 1
            while pos < len(lines) and lines[pos][:1] in (" ", "+", "-") and not lines[pos].startswith("--- a/"):
                line = lines[pos]
                if line[0] in " -":
                    before.append(line[1:])
                if line[0] in " +":
                    after.append(line[1:])
                pos += 1
            require(len(before) == int(match[2] or 1) and len(after) == int(match[4] or 1), "native patch hunk count mismatch")
            require(original[start:start + len(before)] == before, "native patch source does not match")
            result.extend(original[cursor:start])
            result.extend(after)
            cursor = start + len(before)
        result.extend(original[cursor:])
        pending.append((source, "".join(result)))
    for source, content in pending:
        source.write_text(content)


def fetch(spec, archives, supplied):
    archives.mkdir(parents=True, exist_ok=True)
    path = archives / spec["archive"]
    if not path.exists():
        local = supplied / spec["archive"] if supplied else None
        if local and local.is_file():
            require(digest(local) == spec["sha256"], "supplied native archive hash mismatch")
            shutil.copyfile(local, path)
        else:
            temporary = path.with_suffix(path.suffix + ".part")
            request = urllib.request.Request(spec["url"], headers={"User-Agent": "ariax-native-build"})
            with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as output:
                total = 0
                while block := response.read(1024 * 1024):
                    total += len(block)
                    require(total <= 512 * 1024**2, "native archive download exceeds limit")
                    output.write(block)
            require(digest(temporary) == spec["sha256"], "downloaded native archive hash mismatch")
            temporary.replace(path)
    require(digest(path) == spec["sha256"], "cached native archive hash mismatch")
    return path


def source_tree(name, spec, cache, supplied):
    archive = fetch(spec, cache / "archives", supplied)
    directory = cache / "sources" / (name + "-" + spec["sha256"][:16])
    marker = directory / ".complete.json"
    if marker.is_file():
        cached = json.loads(marker.read_text())
        root = directory / cached["root"]
        require(root.is_dir(), "incomplete cached source tree")
        if cached.get("files") == inventory(root):
            return root
    if directory.exists():
        shutil.rmtree(directory)
    root = extract(archive, directory, headers_only=name == "boost")
    marker.write_text(json.dumps({"root": root.name, "sha256": spec["sha256"], "files": inventory(root)}) + "\n")
    return root


def verify_manifest(prefix, target, expected):
    manifest = json.loads((prefix / "ariax-native.json").read_text())
    require(manifest["target"] == target and manifest["inputs"] == expected, "stale or wrong-ABI native installation")
    for name, value in manifest["files"].items():
        path = PurePosixPath(name)
        require(not path.is_absolute() and ".." not in path.parts and ":" not in name and "\\" not in name,
                "unsafe native installation inventory path")
        require(digest(prefix / name) == value, "native installation hash mismatch: " + name)
    return manifest


def build(args):
    openssl_target = native_target(args.target)
    spec = json.loads(SPEC.read_text())
    expected = {"sourcesSha256": digest(SPEC), "patchSha256": digest(PATCH),
                "builderSha256": digest(Path(__file__))}
    work = ROOT / "toolchains/bt-native" / args.target
    cache = work / "cache"
    prefix = work / "install"
    if (prefix / "ariax-native.json").is_file():
        try:
            verify_manifest(prefix, args.target, expected)
            print("Verified native installation for " + args.target, flush=True)
            return
        except ValueError:
            if args.verify:
                raise
    require(not args.verify, "native installation is missing")
    work.mkdir(parents=True, exist_ok=True)
    logs = work / "logs"
    logs.mkdir(exist_ok=True)
    count = 0

    def run(command, cwd=work):
        nonlocal count
        count += 1
        print("Native build: " + str(command[0]) + " " + " ".join(map(str, command[1:3])), flush=True)
        with (logs / f"{count:02}.log").open("wb") as output:
            result = subprocess.run(list(map(str, command)), cwd=cwd, stdout=output, stderr=subprocess.STDOUT)
        require(result.returncode == 0, "native command failed; see " + str(logs / f"{count:02}.log"))

    boost = source_tree("boost", spec["boost"], cache, args.archive_dir)
    upstream = source_tree("openssl", spec["openssl"], cache, args.archive_dir)
    ssl = work / "openssl-source"
    ssl_prefix = work / "openssl-install"
    ssl_marker = work / "openssl-inputs.json"
    ssl_inputs = {"source": spec["openssl"]["sha256"], "builder": expected["builderSha256"], "target": args.target}
    ssl_cached = json.loads(ssl_marker.read_text()) if ssl_marker.is_file() else {}
    if ssl_cached.get("inputs") != ssl_inputs or ssl_cached.get("files") != inventory(ssl_prefix):
        if ssl.exists():
            shutil.rmtree(ssl)
        shutil.copytree(upstream, ssl)
        run(["perl", "Configure", openssl_target, "no-shared", "no-tests", "no-apps", "no-docs",
             "no-module", "no-legacy", "no-engine", "no-zlib", "no-asm", "--libdir=lib",
             "--prefix=" + str(ssl_prefix), *([] if os.name == "nt" else ["-fPIC"])], cwd=ssl)
        make = "nmake" if args.target.endswith("msvc") else "make"
        parallel = [] if make == "nmake" else ["-j" + str(args.jobs)]
        run([make, *parallel, "build_libs"], cwd=ssl)
        run([make, "install_dev"], cwd=ssl)
        ssl_marker.write_text(json.dumps({"inputs": ssl_inputs, "files": inventory(ssl_prefix)}) + "\n")
    if args.dependencies_only:
        print("Built pinned Boost headers and OpenSSL for " + args.target, flush=True)
        return
    upstream = source_tree("libtorrent", spec["libtorrent"], cache, args.archive_dir)
    source = work / "libtorrent-source"
    patch_marker = source / ".ariax-patch.sha256"
    if not patch_marker.is_file() or patch_marker.read_text().strip() != expected["patchSha256"]:
        if source.exists():
            shutil.rmtree(source)
        shutil.copytree(upstream, source)
        apply_patch(source, PATCH)
        patch_marker.write_text(expected["patchSha256"] + "\n")
    build_key = hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()[:12]
    build_dir = work / ("libtorrent-build-" + build_key)
    generator = "NMake Makefiles" if args.target.endswith("msvc") else "MinGW Makefiles" if os.name == "nt" else "Unix Makefiles"
    command = ["cmake", "-S", source, "-B", build_dir, "-G", generator,
               "-DCMAKE_INSTALL_PREFIX=" + str(prefix), "-DCMAKE_INSTALL_LIBDIR=lib",
               "-DBoost_INCLUDE_DIR=" + str(boost), "-DBOOST_ROOT=" + str(boost),
               "-DOPENSSL_ROOT_DIR=" + str(ssl_prefix)]
    command += ["-D" + key + "=" + value for key, value in spec["settings"].items()]
    run(command)
    run(["cmake", "--build", build_dir, "--config", "Release", "--parallel", str(args.jobs)])
    run(["cmake", "--install", build_dir, "--config", "Release"])
    # The bridge consumes only inventoried installation headers, including Boost.
    shutil.copytree(boost / "boost", prefix / "include/boost", dirs_exist_ok=True)
    shutil.copytree(ssl_prefix / "include", prefix / "include", dirs_exist_ok=True)
    shutil.copytree(ssl_prefix / "lib", prefix / "lib", dirs_exist_ok=True)
    licenses = prefix / "licenses"
    licenses.mkdir(exist_ok=True)
    for name, path in (("libtorrent", upstream / "LICENSE"), ("boost", boost / "LICENSE_1_0.txt"),
                       ("openssl", ssl / "LICENSE.txt")):
        shutil.copyfile(path, licenses / (name + ".txt"))
    compiler = "cl" if args.target.endswith("msvc") else os.environ.get("CXX", "c++")
    result = subprocess.run([compiler, "/Bv" if compiler == "cl" else "--version"], capture_output=True, text=True)
    files = {str(p.relative_to(prefix)).replace("\\", "/"): digest(p)
             for p in sorted(prefix.rglob("*")) if p.is_file() and p.name != "ariax-native.json"}
    manifest = {"target": args.target, "inputs": expected, "versions": {n: spec[n]["version"] for n in ("libtorrent", "boost", "openssl")},
                "settings": spec["settings"], "compiler": (result.stdout + result.stderr).strip(),
                "files": files}
    (prefix / "ariax-native.json").write_text(json.dumps(manifest, indent=2) + "\n")
    verify_manifest(prefix, args.target, expected)
    print("Built and verified native installation for " + args.target, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=TARGETS)
    parser.add_argument("--archive-dir", type=Path)
    parser.add_argument("--jobs", type=int, default=min(4, os.cpu_count() or 1))
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--dependencies-only", action="store_true")
    args = parser.parse_args()
    require(1 <= args.jobs <= 64, "invalid native build parallelism")
    with target_lock(ROOT / "toolchains/bt-native" / args.target):
        build(args)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, tarfile.TarError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
