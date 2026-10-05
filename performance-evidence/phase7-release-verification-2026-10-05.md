# Phase 7 Local Release Verification

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-release-verification-2026-10-05.json)

This slice starts at `bd40afc` on `local/phase7-hardening`. All four remapped
Linux/Windows-GNU minimal/standard binaries match independent local builds.
Reduced-environment package operation passes on both current development hosts.
These results do not establish fresh-OS acceptance or approve a release.

## Independent Binary Comparisons

Each platform uses a new target and temporary directory, locked offline Cargo
sources, the pinned native Rust 1.97.1 toolchain and two compiler workers.
The maintained helper verifies compiled-source and compiler executable hashes,
holds the reference `SOURCE_DATE_EPOCH` at `1791193771`, and preserves equivalent
remap destinations and link options. The reference binary hash must still match
its original record. Target-cache reuse and independent comparison cannot be
combined. The installed toolchain and Cargo source cache remain shared.

The reference production source commit is `473045d`; the new build records
identify `bd40afc` and the updated uncompiled Python driver. Compiled inputs
are unchanged. Both preparation identities and per-bundle build records are
retained. This is same-host reproducibility with separate compilation outputs,
not reproduction on another machine or a frozen final release candidate.

| Platform | Bundle | Build Seconds | Binary Comparison |
| --- | --- | ---: | --- |
| Linux | minimal | 438.8 | Match |
| Linux | standard | 418.8 | Match |
| Windows | minimal | 591.5 | Match |
| Windows | standard | 592.7 | Match |

Elapsed times are observations under a 1,200-second per-bundle build budget.
Host activity can change them without invalidating an exact binary comparison;
they are not performance acceptance results. Path audits, actual import checks,
help and unknown-option rejection pass for all four new binaries. Linux still
requires glibc symbols through 2.34; Windows imports only the reviewed system
DLLs. The catalog retains the original binary paths/hashes and adds independent
comparison records. Prior path failures remain historical.

## Package Operation On The Current Hosts

`scripts/release_smoke.py` verifies package inventory and checksums before
execution. It gives each CLI an isolated working/temp/state directory and a
small environment with system-only `PATH`, omitting inherited credentials,
library-injection environment variables and custom certificate configuration.
Each minimal/standard package passes help, live NDJSON version and empty-session
statistics queries, unknown-method rejection, EOF shutdown and reopening the
created SQLite database with zero tasks. Binary hashes remain unchanged.

WSL1 reports Linux 4.4.0-17763-Microsoft and glibc 2.41. Both live processes map
the executable, loader, libc, libm and libgcc, plus
`/usr/local/lib/libnanosleep.so`. That extra library is configured in
`/etc/ld.so.preload`; environment filtering does not remove a system preload.
The record includes configuration/library hashes. Neither was modified.

Native Windows reports version 10.0.17763. Both live processes load the
application and 22 DLLs under Windows directories, with no developer-directory
DLL observed. This does not prove an unmodified OS. Module snapshots cover the
exercised operations only; deferred loads and other protocol paths remain
outside this smoke check. Both reports explicitly set fresh-OS acceptance and
release approval to false. The current host observations do not explain the
historical mixed-burst failure.

## Focused Validation And Retention

All 137 Python tests pass. The 27 release-helper tests also pass under native
Windows, covering accepted and rejected source/reference records, remap/link
option drift, reduced environments, executable-map parsing, module inventory
and package tampering. Package staging verifies all 96 files, checksums,
notices, covered source and source-only exclusions. Restaging changes metadata
and notices only; smoke checks exercised the same four binary hashes.

The first build preflight rejected insufficient temporary-volume free space
before compilation. Pruning removed only 1,234 regenerable Rust `.rmeta` files
(1,133,322,329 bytes) from earlier debug targets. Executables, `.rlib` objects,
source caches, logs and native installations remain. Successful builds prune
only their owned targets, retaining size inventories, binaries and logs. All
new temporary/build/package output stays on the designated temporary volume.

Documentation, catalog, publication and whitespace checks pass. Production
Rust/native sources, dependency locks, toolchain configuration and CI workflows
are unchanged. Machine-readable evidence retains command results, raw-record
hashes, source identities, exact binary comparisons and host limitations;
workstation paths are normalized only in the published copy. No full workspace
build, benchmark, native provisioning, CI dispatch, push or tag was performed.

## Remaining Release Gates

These four local drafts now have independent reproducibility evidence. Fresh-OS
and minimum-OS acceptance, full/compat binary/runtime inventories, macOS/MSVC,
the final candidate matrix and compatibility decisions remain open. Existing
mixed-burst stability/attribution, race/kernel and hardware-recovery gates are
unchanged. Reviewed option-handler coverage remains 54/207; Ariax stays parallel
to aria2. Benchmarks remain manually triggered.
