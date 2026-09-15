#!/usr/bin/env python3
"""Check the pinned protocol forks and their bounded production entry points."""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import re
import tarfile

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "vendor"
NAMES = ("russh-sftp", "suppaftp")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def inventory(files):
    return {name: {"bytes": len(data), "sha256": sha(data)} for name, data in sorted(files.items())}


def tree_hash(files):
    return sha(json.dumps(files, sort_keys=True, separators=(",", ":")).encode())


def sources(name):
    return {str(path.relative_to(VENDOR / name)).replace("\\", "/"): path.read_bytes()
            for path in sorted((VENDOR / name).rglob("*")) if path.is_file()}


def source_contracts():
    ftp = (ROOT / "crates/ariax-engine/src/ftp.rs").read_text()
    for token in ("connect_with_stream(", "connect_secure_implicit_with_stream(",
                  "passive_stream_builder(", "active_listener(", "connect_protocol("):
        assert token in ftp, f"missing owned FTP path: {token}"
    for token in ("TcpStream::connect", "connect_secure_implicit(", "::connect(", "nat_workaround("):
        assert token not in ftp, f"unowned FTP entry point: {token}"
    tokio_ftp = (VENDOR / "suppaftp/src/async_ftp/tokio_ftp.rs").read_text().split("#[cfg(test)]")[0]
    data_path = tokio_ftp[tokio_ftp.index("async fn data_command("):]
    assert data_path.index("if predicate(address)") < data_path.index(".connect(domain, stream)")
    assert "rejected >= 32" in tokio_ftp
    assert "MAX_CONTROL_LINE.saturating_sub(line.len())" in tokio_ftp
    assert tokio_ftp.index("MAX_CONTROL_LINE.saturating_sub(line.len())") < tokio_ftp.index("line.extend_from_slice")
    assert "self.reader.get_mut().shutdown().await" in tokio_ftp
    allowed = {
        'trace!("FTP command bytes={}", command.len());',
        'trace!("FTP reply status={} bytes={} lines={}", number, self.body.len(), self.lines);',
    }
    for path in (VENDOR / "suppaftp/src").rglob("*.rs"):
        source = path.read_text().split("#[cfg(test)]")[0]
        for match in re.findall(r"(?<!\w)(?:trace|debug|info|warn|error)!\(.*?\);", source, re.S):
            normalized = re.sub(r"\s+", " ", match)
            assert normalized in allowed, f"unreviewed FTP log at {path.relative_to(ROOT)}"
    framing = (VENDOR / "russh-sftp/src/utils.rs").read_text()
    assert framing.index("if length > max_length") < framing.index("vec![0; length as usize]")
    raw = (VENDOR / "russh-sftp/src/client/rawsession.rs").read_text()
    runtime = (VENDOR / "russh-sftp/src/client/mod.rs").read_text()
    for token in ("state.pending.len() >= MAX_PENDING_REQUESTS", "close_session", "pending_requests"):
        assert token in raw
    for token in ("mpsc::channel::<Bytes>(MAX_PENDING_REQUESTS)", "handler.closed(failure).await", "drop(rd)", "rc.cancel()"):
        assert token in runtime
    assert "pub handle: Vec<u8>" in (VENDOR / "russh-sftp/src/protocol/handle.rs").read_text()
    assert "handle: String" not in (VENDOR / "russh-sftp/examples/server.rs").read_text()


def refresh(archive_dir):
    packages = []
    for name in NAMES:
        upstream = json.loads((VENDOR / f"{name}-upstream.json").read_text())
        archive = archive_dir / f"{name}-{upstream['version']}.crate"
        assert sha(archive.read_bytes()) == upstream["archive_sha256"], f"wrong archive: {name}"
        prefix = f"{name}-{upstream['version']}/"
        original = {}
        with tarfile.open(archive) as package:
            for member in package.getmembers():
                if member.isdir():
                    continue
                assert member.isfile() and member.name.startswith(prefix)
                relative = member.name[len(prefix):]
                assert ".." not in Path(relative).parts and relative not in original
                original[relative] = package.extractfile(member).read()
        assert inventory(original) == upstream["files"], f"upstream inventory differs: {name}"
        patched = sources(name)
        lines = []
        for path in sorted(original.keys() | patched.keys()):
            before, after = original.get(path, b""), patched.get(path, b"")
            if before != after:
                lines.extend(difflib.unified_diff(before.decode().splitlines(keepends=True), after.decode().splitlines(keepends=True),
                    fromfile=f"a/{path}" if path in original else "/dev/null", tofile=f"b/{path}" if path in patched else "/dev/null"))
        patch = "".join(lines).encode()
        (VENDOR / f"{name}.patch").write_bytes(patch)
        files = inventory(patched)
        packages.append({"name": name, "version": upstream["version"], "archive_sha256": upstream["archive_sha256"],
            "upstream_vcs": upstream["upstream_vcs"], "upstream_manifest_sha256": sha((VENDOR / f"{name}-upstream.json").read_bytes()),
            "patch_sha256": sha(patch), "patched_tree_sha256": tree_hash(files), "files": files})
    (VENDOR / "protocol-patches.json").write_text(json.dumps({"schema": 1, "packages": packages}, indent=2, sort_keys=True) + "\n")


def verify():
    manifest = json.loads((VENDOR / "protocol-patches.json").read_text())
    assert manifest["schema"] == 1 and [p["name"] for p in manifest["packages"]] == list(NAMES)
    for package in manifest["packages"]:
        name = package["name"]
        files = inventory(sources(name))
        assert files == package["files"], f"patched files changed: {name}"
        assert tree_hash(files) == package["patched_tree_sha256"]
        assert sha((VENDOR / f"{name}.patch").read_bytes()) == package["patch_sha256"]
        assert sha((VENDOR / f"{name}-upstream.json").read_bytes()) == package["upstream_manifest_sha256"]
    for license in json.loads((VENDOR / "suppaftp-licenses.json").read_text()):
        assert sha((VENDOR / "suppaftp" / license["path"]).read_bytes()) == license["sha256"]
    assert (VENDOR / "russh-sftp/LICENSE").is_file()
    source_contracts()
    print("Verified protocol fork inventories, patches, licenses and bounded source entry points.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", type=Path, metavar="PINNED_ARCHIVE_DIRECTORY")
    args = parser.parse_args()
    if args.refresh:
        refresh(args.refresh)
    verify()
