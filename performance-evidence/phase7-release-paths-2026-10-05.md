# Phase 7 Release Path Remapping

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-release-paths-2026-10-05.json)

This local slice starts at `473045d` on `local/phase7-hardening`. It addresses
absolute Cargo-cache paths found in all four earlier Linux/Windows-GNU
minimal/standard binaries. Production Rust sources and dependency versions are
unchanged. No full workspace build, benchmark, sanitizer campaign, native
provisioning, CI dispatch, push or tag is part of this slice.

## Maintained Build Preparation

`scripts/release_build.py` builds the two supported local bundles with the
pinned native Rust 1.97.1 distribution, locked offline dependencies, two workers
and a ten-minute timeout per bundle. It selects the matching native GCC tools,
records 345 source hashes and tool identities, and rejects source drift during
preparation. Windows runs through MSYS2 with MinGW64 first on `PATH`.

Rust receives encoded arguments mapping repository, Cargo registry/git-cache,
target and temporary roots to stable prefixes. Windows mappings include normalized and platform-native spellings, plus
drive-case variants. MinGW Python represents native paths with forward slashes. More specific mappings follow broader
ones. C/C++ builds receive shell-quoted `-ffile-prefix-map` arguments through
`CFLAGS` and `CXXFLAGS`, with `CC_SHELL_ESCAPED_FLAGS=1`. Windows retains
`-C link-self-contained=no` and the deterministic PE timestamp option.

The helper inspects the actual binary paths and imports, checks `--help`, and
requires the documented unknown-option exit code and message. Successful runs
may prune only their own compilation intermediates, retaining binaries, logs
and a per-file size inventory. Full/compat native builds remain separate work.

## Artifact Results

All four rebuilt drafts pass the known absolute-path audit. An additional
inspection confirms the declared original roots are absent in UTF-8 and
UTF-16, and that remapped dependency paths occur in each executable. These
checks address the observed path leak; they are not an exhaustive disclosure
or reproducibility proof.

| Target | Bundle | Original Path Matches | Rebuilt Path Matches |
| --- | --- | ---: | ---: |
| Linux | Minimal | 319 | 0 |
| Linux | Standard | 507 | 0 |
| Windows-GNU | Minimal | 323 | 0 |
| Windows-GNU | Standard | 513 | 0 |

Linux still imports `libc.so.6`, `libgcc_s.so.1` and `libm.so.6`, uses the same
loader and requires glibc symbols through 2.34. Windows still imports only the
reviewed system DLLs, with no extra MinGW DLL requirement. Help and invalid
option checks pass for both bundles on both local platforms.

The current package catalog binds the new binary identities and inspection
records. All four staged drafts pass independent integrity verification across
96 files, retaining notices, MPL-covered source, CC0 data and explicit source
artifact exclusions. The prior manifests and failed path observations remain
in Git history and the earlier packaging evidence.

Each bundle has one fresh build. The Linux minimal follow-up reused the exact
completed compilation; it is not an independent reproducibility run. The new
catalog therefore leaves repeat-build identity and comparison results unset.
Old reproducibility results do not transfer to changed binaries.

## Helper Correction And Focused Validation

The initial Linux minimal compilation, path scan, import inspection and help
check passed. The new helper then rejected the CLI's correct unknown-option
exit code 2 because its assertion incorrectly expected 1. The binary behavior
was unchanged. The failed preparation record, command logs and original helper
source remain available; the recovered source matches its recorded hash.

The corrected helper accepts only exit code 2 with the expected error text.
Explicit cache reuse verifies compiled-source hashes, flags, epoch and compiler
identities before allowing another immutable attempt. Only the uncompiled
Python driver may differ, and both identities remain recorded. The repeated
Cargo invocation for Linux minimal takes 0.48 seconds and reuses the same
binary; standard then builds normally.

The initial Windows minimal build also passed its path and import audits, but
the helper failed while decoding UTF-8 help output with the system GBK locale.
Explicit UTF-8 log decoding fixes the helper without changing the executable.
A fresh attempt reuses the verified compilation. Native helper tests also
exposed an assumption that Python's Windows path representation always uses
backslashes; the test now covers MinGW's representation and backslash input.
Both failed records and the pre-fix helper source remain retained.

All eleven helper tests pass under native Windows, including the UTF-8 case.
All 131 Python helper tests pass, including eleven new release-build tests for
path spellings, spaces, ordering, native-runtime flags, toolchain mismatches,
unknown imports, UTF-8 output, option rejection and cache source drift. Package, documentation,
publication and whitespace checks pass. Machine-readable evidence retains
source identities, command results, log hashes, binary comparisons and cleanup
inventories. Historical failures are not relabeled as passes.
An initial publication scan caught two unnormalized MSYS2 tool paths in the
evidence JSON; portable placeholders replace them, with the failed scan retained.

## Remaining Release Gates

The observed dependency-path blocker is resolved for these four local drafts.
Independent builds using the final remap configuration, clean-host acceptance,
full/compat runtime inventories, other platform coverage and the applicable
candidate test matrix remain open. Existing mixed-burst stability/attribution,
race/kernel and hardware-recovery gates also remain. These packages are not
approved release candidates; benchmarks stay manually triggered.
