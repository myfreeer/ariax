#!/usr/bin/env python3
"""Reject workstation paths and local-only files in a tree or publication history."""
import argparse
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
WORKSTATION_PATH = re.compile(
    rb"(?:/mnt/[a-z]/|[a-z]:[/\\](?:Users|UserData)[/\\]|/[U]sers/[^/\s]+/)", re.IGNORECASE)
LOCAL_PREFIXES = (b"toolchains/", b"target/", b".codex/", b".claude/")


def local_only(name):
    return name.startswith(LOCAL_PREFIXES) or name == b".env" or name.startswith(b".env.")


def git(root, *arguments):
    return subprocess.check_output(["git", *arguments], cwd=root)


def audit(root=ROOT, history=False):
    issues = []
    if history:
        names = git(root, "log", "--format=", "--name-only", "--no-renames", "-z", "HEAD").split(b"\0")
        objects = [row.split(b" ", 1)[0] for row in
                   git(root, "rev-list", "--objects", "HEAD").splitlines()]
        # Include commit messages as well as every reachable historical blob.
        with subprocess.Popen(["git", "cat-file", "--batch"], cwd=root,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE) as process:
            for object_id in objects:
                process.stdin.write(object_id + b"\n")
                process.stdin.flush()
                header = process.stdout.readline().split()
                if len(header) != 3:
                    raise RuntimeError("cannot read publication object")
                content = process.stdout.read(int(header[2]))
                process.stdout.read(1)
                if header[1] in {b"commit", b"blob"} and WORKSTATION_PATH.search(content):
                    issues.append(object_id.decode() + ": workstation path")
            process.stdin.close()
            if process.wait():
                raise RuntimeError("publication object scan failed")
    else:
        names = git(root, "ls-files", "-z").split(b"\0")
        for name in names:
            if name and not local_only(name) and WORKSTATION_PATH.search((Path(root) / name.decode()).read_bytes()):
                issues.append(name.decode() + ": workstation path")
    for name in sorted(set(names)):
        name = name.lstrip(b"\n")
        if local_only(name):
            issues.append(name.decode() + ": local-only file")
    return issues


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args()
    issues = audit(history=args.history)
    for issue in issues:
        print(issue)
    if not issues:
        print("Publication history is portable." if args.history else "Tracked files are portable.")
    return bool(issues)


if __name__ == "__main__":
    raise SystemExit(main())
