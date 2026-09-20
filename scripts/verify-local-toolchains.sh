#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$repo_root"

sha256sum --check support/toolchains.sha256

linux_rustc=$(toolchains/installed/linux/bin/rustc --version)
windows_rustc=$(toolchains/installed/windows-gnu/bin/rustc.exe --version)
linux_cargo=$(toolchains/installed/linux/bin/cargo --version)
windows_cargo=$(toolchains/installed/windows-gnu/bin/cargo.exe --version)
[[ "$linux_rustc" == rustc\ 1.97.1* ]]
[[ "$windows_rustc" == rustc\ 1.97.1* ]]
[[ "$linux_cargo" == cargo\ 1.97.1* ]]
[[ "$windows_cargo" == cargo\ 1.97.1* ]]

linux_host=$(toolchains/installed/linux/bin/rustc -vV | sed -n 's/^host: //p')
windows_host=$(toolchains/installed/windows-gnu/bin/rustc.exe -vV | sed -n 's/^host: //p' | tr -d '\r')
windows_commit=$(toolchains/installed/windows-gnu/bin/rustc.exe -vV | sed -n 's/^commit-hash: //p' | tr -d '\r')
[[ "$linux_host" == x86_64-unknown-linux-gnu ]]
[[ "$windows_host" == x86_64-pc-windows-gnu ]]
[[ ${#windows_commit} -eq 40 ]]

scripts/cargo-local.sh linux --version
scripts/cargo-local.sh windows-gnu --version
windows_clippy=$(scripts/cargo-local.sh windows-gnu clippy --version | tr -d '\r')
windows_rustfmt=$(scripts/cargo-local.sh windows-gnu fmt --version | tr -d '\r')
[[ "$windows_clippy" == *"${windows_commit:0:10}"* ]]
[[ "$windows_rustfmt" == *"${windows_commit:0:10}"* ]]
if scripts/with-windows-gnu-env.sh >/dev/null 2>&1; then
    printf '%s\n' 'Windows-GNU wrapper accepted a missing command' >&2
    exit 1
fi

windows_environment=$(scripts/with-windows-gnu-env.sh /usr/bin/bash -c '
    printf "rustc=%s\n" "$RUSTC"
    printf "rustdoc=%s\n" "$RUSTDOC"
    printf "rustfmt=%s\n" "$RUSTFMT"
    printf "clippy=%s\n" "$CLIPPY_DRIVER"
    printf "gcc=%s\n" "$(command -v gcc.exe)"
    printf "target=%s\n" "$CARGO_TARGET_DIR"
')
windows_root=$(wslpath -m "$repo_root")
[[ "$windows_environment" == *"rustc=$windows_root/toolchains/installed/windows-gnu/bin/rustc.exe"* ]]
[[ "$windows_environment" == *"rustdoc=$windows_root/toolchains/installed/windows-gnu/bin/rustdoc.exe"* ]]
[[ "$windows_environment" == *"rustfmt=$windows_root/toolchains/installed/windows-gnu/bin/rustfmt.exe"* ]]
[[ "$windows_environment" == *"clippy=$windows_root/toolchains/installed/windows-gnu/bin/clippy-driver.exe"* ]]
[[ "$windows_environment" == *"gcc=/mingw64/bin/gcc.exe"* ]]
[[ "$windows_environment" == *"target=$windows_root/toolchains/target/windows-gnu"* ]]

printf 'verified local Linux and Windows-GNU Rust 1.97.1 toolchains\n'
