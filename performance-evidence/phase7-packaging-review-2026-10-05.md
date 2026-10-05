# Phase 7 Package And License Review

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-packaging-review-2026-10-05.json)

This slice starts at `ecd2123` on `local/phase7-hardening`. It resolves the
remaining winapi provenance and IANA/aria2 terms questions, prepares explicit
package layouts, and inspects retained artifacts. It performs no Rust build,
application execution, benchmark, native provisioning, CI dispatch or push.
The retained generator checks contracts; Python checks package preparation.
This completes the requested preparation, with release acceptance still open.

## License Findings

The [review](../distribution/license-review.json) retains 17 original license
materials with their origins and hashes. Ariax-owned code remains MIT.

- All 1,419 published payload files in locked
  `winapi-x86_64-pc-windows-gnu` 0.4.0, including 1,416 import libraries, match
  upstream Git blob identities at `9497609ef44cc9bcd16cd2411c0ee6ccaf5483aa`.
  MIT and Apache license texts come from that snapshot. Its date is later than
  publication: this proves content equivalence, not an unrecorded publishing
  commit. Registry-normalized `Cargo.toml` is covered by the locked archive
  checksum; `Cargo.toml.orig` matches upstream.
- The [IANA/IETF statement](https://www.iana.org/help/licensing-terms), dated
  November 10, 2021, dedicates applicable protocol-registry rights under CC0-1.0.
  Both address registries appear in the protocol registry index. The pin and
  generated policy metadata now cite that specific dedication. CSV bytes,
  classifier behavior and policy overrides are unchanged.
- The aria2 option inventory contains extracted C++ expressions and manual
  directives. Extracted upstream material in the option, RPC and compatibility
  JSON inventories retains GPL-2.0-or-later terms and original notices from
  `9e7273583f83e881e3ec067b523ba88724088d2f`. A
  [source-distribution notice](../distribution/aria2-source-notice.txt) and GPL
  text preserve that scope. These JSON artifacts are not embedded in the CLI
  and are excluded from binary packages.

The previous dependency inventory and notice collection remain intact as
historical evidence. The new review records the resolutions and supplements
their conservative union with runtime and data notices.

## Package Integrity And Runtime Inventory

The [catalog](../distribution/package-manifests.json) contains eight target and
bundle layouts. Four retained Linux/Windows-GNU minimal/standard drafts are
staged with explicit file allowlists, manifests and `SHA256SUMS`. Independent
verification checks all 96 files, exact membership, hashes, lengths, the MPL
source against its pin, the CC0 reference, source-artifact exclusions and
non-binary publication paths. All four packages pass these integrity checks.

Each package supplies the exact MPL-covered public-suffix snapshot, its
provenance, MPL text and source-availability notice. Pinned IANA data, CC0,
project and dependency licenses, Rust runtime notices and applicable system
runtime notices are present. The conservative notice union may include unused
build/source materials; final notices need the exact candidate artifact review.

Fresh `readelf` and `objdump` inspections of the retained binaries confirm:

- Linux minimal/standard import `libc.so.6`, `libgcc_s.so.1` and `libm.so.6`,
  use `/lib64/ld-linux-x86-64.so.2`, and require glibc symbols up to 2.34.
- Windows minimal/standard import only the recorded Windows system DLLs;
  those retained binaries need no additional MinGW DLLs.
- Rust 1.97.1 standard-library notices are identical on both installed targets.
  Compiler-builtins license text comes from the exact recorded Rust revision.

Manifests bind binary hashes and historical first/repeat build records. Package
metadata does not make those binaries builds of the current source tree.
Full/compat layouts have no binary or runtime record yet. Their actual native
imports and platform-specific obligations require separate inspection.

## Absolute Dependency Paths Block Release

The first independent package audit found Cargo-cache source paths inside a
retained Linux executable. Inspection then found them in all four binaries:

| Target | Bundle | Absolute Path Matches | Audit |
| --- | --- | ---: | --- |
| Linux | Minimal | 319 | Failed |
| Linux | Standard | 507 | Failed |
| Windows-GNU | Minimal | 323 | Failed |
| Windows-GNU | Standard | 513 | Failed |

These are pattern-match counts, not distinct-file counts. The paths refer to
dependency source files. Earlier builds remapped repository and output paths
but missed Cargo-cache sources. Byte-for-byte reproducibility on that setup
does not establish portability. The audit covers known workstation/cache path
patterns; an absence of matches would not prove complete reproducibility.

Every draft records this failure, and staging verifies that the observed audit
matches its manifest. The final verifier reports `integrityPassed: true`,
`binaryPathAuditPassed: false` and `releaseApproved: false`. No binary bytes are
patched and no acceptance failure is converted into a passing path result.

Candidate builds must remap Cargo registry/git-cache source roots alongside
repository/output roots and inspect fresh artifacts. Preserve the Windows
`-C link-self-contained=no` choice. Native C/C++ sources need equivalent
file/debug-prefix maps when relevant, with native provenance checks intact.
Clean-host installation and the applicable final candidate matrix remain open.

## Focused Checks And Retained Failures

All 120 Python helper tests pass, including ten package tests covering normal
staging, unsafe paths, missing source/notices, digest drift, unknown runtimes,
invalid identities, stale binaries and inconsistent path audits. The initial
119-test attempt exposed acceptance of `.` as a relative path; the fix rejects
that empty path and the final suite passes. No Rust code changed.

Retained generator checks pass before and after the IANA metadata update.
Documentation, publication, catalog and whitespace checks pass. Source hashes
and raw result/log hashes are retained in the machine-readable evidence.
`Cargo.lock`, native build tooling, runtime classifier and data bytes are
unchanged from the baseline.

Initial upstream URL misses, the first failed package path audit and the
subsequent disk-full staging attempt remain in the local evidence. Disk
exhaustion left an empty result JSON; its stderr remains available and the
attempt is recorded as incomplete, not a passing run. Final staging and
verification use new output directories.

The user's requested cleanup removed superseded package copies (about 54 MiB)
and 1,602 regenerable release `.rlib`/`.rmeta` files totaling 1,791,183,996 bytes.
Original binaries, source caches, logs, debug caches and native installations
are preserved. The per-file deletion record stays with the local evidence.
After that cache cleanup, 2,521,833,472 bytes were free, before final staging.

Phase 7 remains active. Native Linux mixed-burst attribution/stability, remaining
race/kernel and recovery evidence, platform coverage and release approval retain
their existing gates. Benchmarks stay manually triggered.
