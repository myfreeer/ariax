#!/usr/bin/env python3
"""Resolve optional workstation tools without committing workstation paths."""
import argparse
import json
import os
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]
CONFIGURATION = ROOT / "toolchains/local-paths.json"


def configured_directory(key, variable):
    value = os.environ.get(variable)
    if value is None and CONFIGURATION.exists():
        settings = json.loads(CONFIGURATION.read_text(encoding="utf-8"))
        if not isinstance(settings, dict):
            raise RuntimeError("local-paths.json must contain an object")
        value = settings.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value or any(c in value for c in "\r\n\0"):
        raise RuntimeError(f"invalid {variable} directory")
    directory = Path(value)
    if not directory.is_absolute():
        directory = ROOT / directory
    if not directory.is_dir():
        raise RuntimeError(f"{variable} must name an existing directory")
    return directory


def executable(path):
    if not path.is_file() or not os.access(path, os.X_OK):
        raise RuntimeError(f"native executable is unavailable: {path.name}")
    return path


def msys2_env():
    directory = configured_directory("msys2_root", "ARIAX_MSYS2_ROOT")
    if directory is None:
        candidate = shutil.which("env.exe")
        if candidate:
            directory = Path(candidate).resolve().parents[2]
    if directory is None:
        raise RuntimeError("set ARIAX_MSYS2_ROOT or msys2_root in toolchains/local-paths.json")
    executable(directory / "usr/bin/bash.exe")
    executable(directory / "mingw64/bin/gcc.exe")
    return executable(directory / "usr/bin/env.exe")


def windows_tool(name):
    if name not in {"whoami.exe", "icacls.exe"}:
        raise RuntimeError("unsupported Windows fixture tool")
    directory = configured_directory("windows_system32", "ARIAX_WINDOWS_SYSTEM32")
    if directory is not None:
        return executable(directory / name)
    candidate = shutil.which(name)
    if candidate:
        return executable(Path(candidate))
    raise RuntimeError("set ARIAX_WINDOWS_SYSTEM32 or windows_system32 in toolchains/local-paths.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool", choices=("msys2-env", "whoami.exe", "icacls.exe"))
    args = parser.parse_args()
    try:
        print(msys2_env() if args.tool == "msys2-env" else windows_tool(args.tool))
    except (RuntimeError, ValueError, OSError) as error:
        parser.exit(1, f"local tool configuration: {error}\n")


if __name__ == "__main__":
    main()
