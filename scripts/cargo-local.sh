#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf 'usage: %s <linux|windows-gnu> <cargo-arguments...>\n' "$0" >&2
    exit 2
}

[[ $# -ge 2 ]] || usage

platform=$1
shift
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)

case "$platform" in
    linux)
        toolchain_root="$repo_root/toolchains/installed/linux"
        cargo_bin="$toolchain_root/bin/cargo"
        rustc_bin="$toolchain_root/bin/rustc"
        cargo_home="$repo_root/toolchains/cargo-home/linux"
        target_dir="$repo_root/toolchains/target/linux"
        mkdir -p -- "$cargo_home" "$target_dir"
        [[ -x "$cargo_bin" && -x "$rustc_bin" ]] || {
            printf 'local Linux Rust toolchain is not installed\n' >&2
            exit 1
        }
        exec env \
            CARGO_HOME="$cargo_home" \
            CARGO_TARGET_DIR="$target_dir" \
            PATH="$toolchain_root/bin:$PATH" \
            RUSTC="$rustc_bin" \
            "$cargo_bin" "$@"
        ;;
    windows-gnu)
        exec "$repo_root/scripts/with-windows-gnu-env.sh" cargo.exe "$@"
        ;;
    *)
        usage
        ;;
esac
