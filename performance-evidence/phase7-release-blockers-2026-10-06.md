# Phase 7 Local Release-Blocker Fixes

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-release-blockers-2026-10-06.json)

The full/compat native-path, runtime-packaging and independent-build blockers
are resolved on the existing WSL1 and native Windows-GNU hosts. All four rebuilt
CLI binaries match independent builds, and four extracted archives pass package
operation checks. All eight catalog entries now describe retained drafts.
Release approval and fresh-OS acceptance remain false.

This follow-up starts at `d6a8afc` on `local/phase7-hardening`, after the
[bounded campaign](phase7-local-campaign-2026-10-05.md). It records that base
commit plus the changed helper/native-document source hashes. Rust production
code, native source patches and `Cargo.lock` are unchanged. Builds retain
`SOURCE_DATE_EPOCH=1791224426`, pinned Rust 1.97.1, existing verified source
caches and installed compilers, and at most two compiler workers. Build stages
run serially. No full workspace rebuild, toolchain installation, global setting
change, CI dispatch, push, tag or release is part of this work.

## Native Paths And Independent Builds

The earlier full/compat binaries contain 20 native-dependency path matches per
Linux binary and 37 per Windows binary. Their failures remain retained.
`scripts/bt_native.py` now supports separate release work directories and
read-only reuse of verified native source caches. It rejects source/output
overlap, changed cache contents and unsupported sanitizer targets. Native C/C++
and installed bridge-header paths receive compiler prefix maps.

OpenSSL previously embedded build locations in its compiler description and
default directories. A relative response file supplies the real compiler maps
without putting their absolute arguments in that description. Its default
configuration directory is `/etc/ssl` on Linux and
`C:/Program Files/Common Files/SSL` on Windows; disabled engine/module locations
also use fixed system paths. Native manifests preserve maps, epoch and inputs.
Finished binaries are not edited to remove paths.

Each platform has two separate native build/install directories and two fresh
CLI target/temp directories. All 16,772 consumed headers and archives match
between that platform's native installations, including OpenSSL and libtorrent
archives. Normalized build inputs and compiler identities also match. Cargo
and native source caches and installed toolchains are shared and verified.
These comparisons establish reproducibility on the same host with independent
compilation outputs; they do not establish reproduction on another host.

| Platform | Bundle | Independent Native/CLI Match | Known Absolute Path Matches |
| --- | --- | --- | ---: |
| WSL Linux | full | Pass | 0 |
| WSL Linux | compat | Pass | 0 |
| Windows-GNU | full | Pass | 0 |
| Windows-GNU | compat | Pass | 0 |

Each binary retains positive canonical `/ariax-native` and `/ariax-cargo` path
markers. Actual runtime imports, help and unknown-option rejection pass. The
machine-readable record preserves full binary/archive hashes, source and tool
identities, commands, exits and raw-record digests. Path scanning covers known
workstation/cache patterns, rather than proving the absence of every possible
absolute path. Older minimal/standard binaries retain their existing identities
and independent-build evidence; they were not rebuilt here.

## Runtime And Archive Verification

Linux full/compat require glibc 2.38, GLIBCXX 3.4.30, the GNU loader, libc,
libm, libgcc and libstdc++. Host system libraries are not bundled. The installed
libstdc++ copyright exactly matches the existing packaged GCC copyright text,
including libstdc++ and its runtime exception.

Windows full/compat carry exactly `libstdc++-6.dll`, `libgcc_s_seh-1.dll` and
`libwinpthread-1.dll`. The [runtime review](../distribution/runtime-files.json)
records their bytes, hashes, versions, direct imports, source references and
license notices. GCC runtime-exception and winpthread notices accompany that
closure. Native OpenSSL, Boost and libtorrent notices remain included. These
Linux/Windows reviews do not establish macOS/MSVC licensing or runtime details.

`scripts/release_manifest.py` verifies the complete reviewed runtime closure,
rejects unknown imports, missing or modified DLLs, missing notices and path
collisions, and copies only approved files. Its filename grammar accepts the
literal `+` in `libstdc++-6.dll` while retaining path traversal and Windows alias
rejections. Notice comparisons use the same case folding as package paths.

All eight drafts were staged with exact file/checksum inventories. Four new
full/compat archives were assembled: Linux `.tar.gz` and Windows `.zip` for
each bundle. Extracted contents match their inventories; isolated copies with
missing or modified runtime DLLs reject verification. Those negative fixtures
were inventoried and removed. Checksums and package contents were verified
again after execution and before duplicate staging cleanup.

`scripts/release_smoke.py --bundles full compat` ran against the extracted
packages on each native platform. Both bundles on both hosts pass help, live
NDJSON version and empty-session statistics, exact advertised features including
BitTorrent, unknown-method rejection, EOF shutdown and SQLite reopening.
Windows uses a system-only `PATH`; all three reviewed DLLs are observed loading
from the package directory with matching bytes. Linux recognizes the versioned
libstdc++ file through its system SONAME link.

WSL1 also loads the pre-existing `/usr/local/lib/libnanosleep.so` configured in
`/etc/ld.so.preload`; neither is changed. The Linux report therefore explicitly
records a host addition. These checks cover exercised operations on current
development hosts, including Windows 10.0.17763. They do not prove every delayed
library load, clean-host operation or minimum-OS support.

## Focused Tests And Preserved Findings

| Verification | Linux | Windows |
| --- | ---: | ---: |
| Native-builder Python tests | 18 passed | 18 passed |
| Release-helper Python tests | 35 passed | 35 passed |
| Rust native integration tests | 2 passed | 2 passed |
| Native CTest probes | 3 passed | 4 passed |
| Extracted full/compat package checks | 2 passed | 2 passed |

Rust tests cover v1/v2/hybrid transfer, storage hold and checkpoint behavior,
plus bounded/redacted rejection and callback ownership. Native probes cover
bounded output, OpenSSL callbacks/AES/certificate parsing and rejection, and
destination policy; Windows also runs private-storage checks. No ignored or
filtered entries count toward these totals. Strict Unix filesystem coverage
remains assigned to a supporting native system.

Documentation, package-catalog and tracked-file publication checks pass. Final
integrity verification matches 93 raw-record hashes and 142 direct command-log
hashes against their recorded values.

Windows CTest passed all four named tests with exit zero but used a newer
summary format. The external report wrapper initially rejected that wording.
The original failed assessment and driver are retained. A corrected parser
requires the exact named passing rows, exit zero and either supported summary,
with success and rejection checks. It reassessed unchanged raw output; no
native test was rerun to obtain a pass.

Other retained intermediate findings include the initial plus-sign and notice
case-folding helper regressions, a misplaced test assertion, and an external
runtime-inspection parser's case mismatch. Focused helper tests passed after
repairs; successful immutable inspection output was reused where only parsing
changed. An initial successful Linux native build predates directory-safety and
Windows-quoting refinements. Its manifest and builder remain retained, and its
three archives exactly match the refreshed installation.

## Cleanup And Remaining Gates

Process inspection found no remaining processes referencing the owned run
directory on either host. The Windows read-only query ran inline because the
host disables script-file execution; no policy setting changed. Package
processes exited, and all 346 recorded source files still matched their build
identities before evidence collection.

Cleanup removed 622 inventoried disposable or duplicate files totaling
430,142,173 bytes: smoke state/temp data, combined package inputs, duplicate
staging, superseded native build output and proven identical archive copies.
It retained independent binaries, current native installations, useful target
and source caches, raw logs, failures, four archives and extracted packages.
Cleanup inventories and their hashes remain in the local evidence directory.

The Windows `changeUri` failure remains **100.250 ms against the unchanged
50 ms limit**. Saved maximum owner lock wait of 2 microseconds and owner turn
of 479 microseconds do not locate the delay in queueing, persistence, transport
or host scheduling. No production timing repair or host-load attribution is
supported by those aggregates; no threshold relaxation or retry-until-green
was performed.

Native Linux mixed-BT timing and `io_uring`, strict Unix filesystem tests, fully
instrumented Rust standard-library/harness TSan, macOS/MSVC, fresh/minimum OS,
the final candidate matrix and hardware power-loss acceptance remain open.
Reviewed compatibility handler coverage remains 54/207. Ariax remains parallel
to aria2; release approval, migration and replacement are not implied by this
local packaging work.
