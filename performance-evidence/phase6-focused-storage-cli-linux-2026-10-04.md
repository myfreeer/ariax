# Phase 6 Focused Storage And CLI Linux Validation

The supplied `ci-focused-linux.zip` records a passing native Linux validation
command for `6dab0a547f73719e03286cd0b19a97add86540ce` on the temporary branch
`local/removable-storage-mixed-bt-20261003`. All 18 commands passed, including
70 Python helper tests, 354 top-level Rust test executions, four resolved CLI
feature graphs and all four `release-cli` builds. No selected test failed.

The validation command took 937.483 seconds including compilation; this is not
the elapsed time of the entire workflow. It ran no benchmark. The complete
storage/CLI and Linux release-bundle slice passes; full Phase-6 acceptance
remains open.

## Storage And Recovery Coverage

The unfiltered `ariax-storage --all-features` suite reports 231 unit-test passes
and five permission-policy integration passes. Its empty doc-test harness adds
no tests. Five entries are marked ignored in the top-level unit harness because
they are child-process helpers invoked by passing parent tests with `--ignored`.
They cover forced journal exit/kill, hot backup publication, hot rollback-journal
recovery, corrupt main-header recovery and cross-process owner locking.

The log records 23 child-process starts across nine helper functions, including
the five ignored entries. The other four helper entry points also appear in the
normal top-level pass count. Child-process executions and the nested owner-lock
summary are excluded from the 236 storage total. Expected crash exits and
process termination are checked by the parent fixtures.

Coverage includes torn journal recovery and rotation, no-clobber backup
publication, WAL and rollback-journal recovery, atomic SQLite/JSON v3 state,
BT checkpoint crashes, mixed imports, owner shutdown and resource ownership,
metadata/path rejection and permission policy. The session-owner panic diagnostic
is intentional fault injection in
`owner_panic_closes_completions_and_is_reported_on_join`; that regression passes.
The parent harness reports zero filtered tests and 7.48 seconds; integration
tests take 0.03 seconds.

## CLI Feature Tests And Builds

| Bundle | Unit | Smoke | Permissions | Removable Storage | RPC Interfaces | Total | Release Build |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `minimal` | 8 | 14 | 2 | 1 | 4 | 29 | Pass, 2m 48s |
| `standard` | 8 | 14 | 2 | 1 | 4 | 29 | Pass, 3m 08s |
| `full` | 8 | 14 | 2 | 1 | 5 | 30 | Pass, 3m 01s |
| `compat` | 8 | 14 | 2 | 1 | 5 | 30 | Pass, 1m 28s |

The CLI total is 118 test executions across feature selections, not 118 distinct
functions. Every bundle runs all CLI tests with default features disabled and
its explicit feature selection; none failed, was ignored or was filtered out.
Tests use the `release` profile. Each bundle's tests precede its separate
`release-cli` build, with build durations reported by Cargo as shown above.

Tests verify startup validation and secret redaction, session input/export,
HTTP transfer and durable resume, corrupt-payload rejection, permissions,
HTTP/stdio ownership and EOF handling, JSON catalog and compatibility behavior.
Smaller bundles reject BT admission without creating state; `full` and `compat`
verify v1/v2/hybrid admission, restart parity and strict output permissions.
All four resolved dependency graphs pass the protocol/crypto policy checks.

Rust is 1.97.1, with locked dependencies and one test thread. Documentation,
workflow syntax, publication paths, whitespace and formatting checks pass.
The host is `Linux-6.17.0-1022-azure-x86_64-with-glibc2.39`.

## Artifact And Native Provenance

The [machine-readable record](phase6-focused-storage-cli-linux-2026-10-04.json)
retains original command metadata, named tests, harness summaries, child-process
counts, release-build outcomes and native provenance. Review matched the source
commit, all 18 command-log SHA-256 values, and native source, patch and builder
digests against Git. Archive paths and integrity were checked before extraction.

CI verified the cached libtorrent 2.1.1, Boost 1.91.0 and OpenSSL 3.6.3
installation once with sanitizer mode `none`, without rebuilding dependencies.
Its manifest records GCC 13.3.0 and 16,786 installed-file entries. Only the
manifest is in the archive; local review verifies its inputs and recorded CI
verification, rather than rehashing installed libraries. The archive also omits
the release executables; build evidence consists of successful commands and
their matching Cargo completion logs.

- Archive SHA-256: `54848f6a3b70b5755ea01c12a546aec6e6aa4b764e45259a008c996777716ef7`.
- Original `result.json` SHA-256: `d9865f5a030f8485b2988979492040ba39566cc7a208654ca9a7da6e0c667133`.
- Native manifest SHA-256: `0a11f7f24fd1fa043e703afcfc27a89a59bfd630e759858d8fac3f1fc40a93eb`.

The original archive and extracted logs are retained locally. Review used the
supplied artifact without GitHub API polling, a push or workflow dispatch.

## Remaining Acceptance

This adds complete Linux storage/CLI and release-bundle evidence to the earlier
[RPC/setup](phase6-focused-linux-2026-10-04.md),
[BT transfer/security/recovery](phase6-focused-bt-linux-2026-10-04.md),
[interface/feature](phase6-focused-interfaces-linux-2026-10-04.md) and
[sanitizer/parser-fuzz](phase6-focused-sanitizers-linux-2026-10-04.md) slices.
Each record remains bound to its own source commit and selected tests.

The complete current-source platform/MSRV matrix, remaining workspace and
release-platform coverage, and all six performance scenarios remain required.
These Linux results do not establish native behavior on other platforms. The
mixed 1,000-BT-peer and 1,000-HTTP-range workload still has no acceptance
measurements from these runs. The
[Phase 6 gates](../docs/project/implementation-readiness.md#phase-6-local-implementation-and-open-acceptance)
remain open.
