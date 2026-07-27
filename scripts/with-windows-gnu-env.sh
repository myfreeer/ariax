#!/usr/bin/env bash
set -euo pipefail

[[ $# -ge 1 ]] || {
    printf 'usage: %s <command> [arguments...]\n' "$0" >&2
    exit 2
}

command -v wslpath >/dev/null 2>&1 || {
    printf 'Windows-GNU environment requires WSL wslpath\n' >&2
    exit 1
}

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
msys_env="${ARIAX_MSYS2_ROOT:?set ARIAX_MSYS2_ROOT}/usr/bin/env.exe"
[[ -x "$msys_env" ]] || {
    printf 'MSYS2 environment launcher is unavailable: %s\n' "$msys_env" >&2
    exit 1
}

exec "$msys_env" \
    MSYSTEM=MINGW64 \
    CHERE_INVOKING=1 \
    ARIAX_REPO_ROOT="$(wslpath -m "$repo_root")" \
    /usr/bin/bash -lc '
        set -euo pipefail
        repo_root=$(cygpath -u "$ARIAX_REPO_ROOT")
        toolchain_root="$repo_root/toolchains/installed/windows-gnu"
        cargo_home="$repo_root/toolchains/cargo-home/windows-gnu"
        target_dir="$repo_root/toolchains/target/windows-gnu"
        test -x "$toolchain_root/bin/cargo.exe"
        test -x "$toolchain_root/bin/rustc.exe"
        mkdir -p -- "$cargo_home" "$target_dir"
        export PATH="$toolchain_root/bin:/mingw64/bin:/usr/local/bin:/usr/bin"
        export RUSTC="$toolchain_root/bin/rustc.exe"
        export RUSTDOC="$toolchain_root/bin/rustdoc.exe"
        export RUSTFMT="$toolchain_root/bin/rustfmt.exe"
        export CLIPPY_DRIVER="$toolchain_root/bin/clippy-driver.exe"
        export CARGO_HOME="$cargo_home"
        export CARGO_TARGET_DIR="$target_dir"
        cd -- "$repo_root"
        command_name=$1
        shift
        case "$command_name" in
            cargo.exe)
                exec "$toolchain_root/bin/cargo.exe" "$@"
                ;;
            rustc.exe)
                exec "$toolchain_root/bin/rustc.exe" "$@"
                ;;
            *)
                exec "$command_name" "$@"
                ;;
        esac
    ' ariax-windows-gnu "$@"
