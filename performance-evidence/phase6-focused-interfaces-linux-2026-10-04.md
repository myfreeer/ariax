# Phase 6 Focused Interface And Feature Linux Validation

The supplied `ci-focused-linux.zip` records a passing native Linux validation
command for `a1f8f4f5087150e6150be34d111c9fc1e0b38a29` on the temporary branch
`local/removable-storage-mixed-bt-20261003`. All 15 commands passed, including
70 Python helper tests, 21 Rust test executions and all four CLI feature graphs.
No selected Rust test failed or was ignored.

The command took 468.687 seconds including compilation; this is not the elapsed
time of the entire workflow. It built and ran no benchmark. The selected
interface/feature coverage passes; full Phase-6 acceptance remains open.

## Executed Coverage

| Test Selection | Feature Selection | Passed | Test Seconds |
| --- | --- | ---: | ---: |
| Engine `native_api::bittorrent::tests::` | All features | 2 | 0.04 |
| Engine `native_api::bittorrent::tests::` | No default features | 1 | 0.00 |
| CLI `rpc_interfaces` | `minimal` | 4 | 0.57 |
| CLI `rpc_interfaces` | `standard` | 4 | 0.60 |
| CLI `rpc_interfaces` | `full` | 5 | 0.85 |
| CLI `rpc_interfaces` | `compat` | 5 | 0.83 |

These are executions across feature selections, not 21 distinct test functions.
The engine filters exclude 425 and 390 other tests. Each CLI bundle runs in a
separate Cargo invocation with `--no-default-features` and its explicit bundle.

Typed Rust tests cover BT option validation, feature-disabled errors, admission,
status/files/peers, acknowledged option changes, JSON projections and retained
reply ownership. Smaller CLI bundles reject torrent admission without creating
database, control or output state. Full bundles admit v1/v2/hybrid torrents,
reopen through the Rust API, and compare RPC status, file and option projections;
strict output-permission rejection also passes.

Every bundle passes combined HTTP/stdio ownership and EOF cases, JSON catalog
and compatibility checks, and rejection of service-only options. Resolved
dependency checks verify that `minimal`/`standard` exclude native BT and that
`full`/`compat` include the pinned native graph and approved crypto dependencies.
This is narrower than the complete feature-bundle test and release-build matrix.

Tests use Rust 1.97.1, the locked dependency graph, release mode and one test
thread. Documentation, workflow syntax, publication paths, whitespace and
formatting pass. The host is
`Linux-6.17.0-1022-azure-x86_64-with-glibc2.39`.

## Artifact And Native Provenance

The [machine-readable record](phase6-focused-interfaces-linux-2026-10-04.json)
retains original command metadata and named test outcomes for each feature
selection. Review matched the source commit, all 15 command-log SHA-256 values,
and the native source, patch and builder digests against Git.

CI verified the cached libtorrent 2.1.1, Boost 1.91.0 and OpenSSL 3.6.3
installation in sanitizer mode `none`, without rebuilding those dependencies.
The manifest contains 16,786 installed-file entries. Only its manifest is in
the archive; local review verifies its inputs and the recorded CI verification,
rather than rehashing installed libraries.

- Archive SHA-256: `35b5b9ddfef54c93a8e69b87fdb5a9fcde6ae960d9867eebe9eebc4ee9b566d6`.
- Original `result.json` SHA-256: `32f0c5e5a250fd7193a60c1e5a412ca9d37c7315b9dca43677946426a7ff4f2d`.
- Native manifest SHA-256: `0a11f7f24fd1fa043e703afcfc27a89a59bfd630e759858d8fac3f1fc40a93eb`.

The original archive and extracted logs are retained locally. Review used the
supplied artifact without GitHub API polling, a push or workflow dispatch.

## Remaining Acceptance

This adds interface evidence to the earlier
[RPC/setup slice](phase6-focused-linux-2026-10-04.md) and
[BT transfer/security/recovery slice](phase6-focused-bt-linux-2026-10-04.md).
Each record remains bound to its source commit and selected tests.

Current-source sanitizer/fuzz evidence, complete platform/MSRV and feature-bundle
tests/builds, remaining workspace coverage and all six performance scenarios
remain required. The next prepared slice targets the native ASan/UBSan probes,
bridge/adapter tests and bounded parser fuzzing. Preparation is not a passing
result. The [Phase 6 gates](../docs/project/implementation-readiness.md#phase-6-local-implementation-and-open-acceptance)
remain open.
