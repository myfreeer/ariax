# Phase 6 Focused BT Linux Validation

The supplied `ci-focused-linux.zip` records a passing native Linux validation
command for `38d07e48dce1f220bb55e88595f4e030a5960cf5` on
`local/removable-storage-mixed-bt-20261003`. All 15 recorded commands passed,
including 70 Python helper tests, 24 Rust harness tests and four native probes.
No selected test failed or was ignored.

The validation command took 285.902 seconds, including compilation; this is not
the elapsed time of the entire workflow. No benchmark executable was built or
run. This evidence completes the selected Linux transfer/security/recovery
slice and leaves full Phase-6 acceptance open.

## Executed Coverage

| Rust Suite Or Filter | Passed | Test Seconds |
| --- | ---: | ---: |
| `ariax-bt-libtorrent-sys --test native` | 2 | 3.81 |
| `ariax-bt --test adapter` | 4 | 0.91 |
| `ariax-engine --lib http_control::phase6_` | 12 | 41.28 |
| `ariax-storage --lib session_store::bt::tests::` | 6 | 0.11 |

The storage count includes the child entry point for the process-crash fixture.
Its parent test verifies four child executions: before and after checkpoint
commit in both WAL and rollback-journal modes. Those executions are not added
to the 24-test total. The narrow engine/storage filters exclude 415/230 other
tests; this was not a full workspace run.

The bridge and adapter tests verify real v1/v2/hybrid transfers, including
multi-piece v2 fixtures, pre-storage approval, bounded/redacted rejection,
retained completion credit, stale live settings, checkpoint failure and resource
release. The engine tests cover torrent and magnet transfers, disjoint selected
files and collision mapping, metadata-only completion, traversal/symlink and
invalid-selection rejection before payload creation, atomic JSON v3 import,
and option validation.

Lifecycle tests pass for checkpointed pause/removal, active shutdown, expired
shutdown retaining a dirty generation, and corrupt-payload rechecks before
resuming. SQLite tests verify that counters, resume data and dirty state remain
atomic across crashes, stale completions reject, metadata bindings stay exact,
and older development formats reject without modifying their artifacts.

| Native Probe | Passed | Test Seconds |
| --- | ---: | ---: |
| `native_bounded_output` | 1 | 0.00 |
| `native_openssl_callbacks` | 1 | 0.00 |
| `native_destination_policy` | 1 | 5.69 |
| `native_private_storage` | 1 | 0.00 |

CTest reports 5.77 seconds total. Its retained detailed log confirms completion
of all 16 tracker/web-seed endpoint cases and the redirect-policy and
allowed/blocked DHT stages. The endpoint cases include DNS resolution,
credential/secret rejection, redirect limits and tracker-discovered peers.
Native private-storage and OpenSSL callback/vector probes also complete.
This ordinary build does not provide sanitizer evidence.

Documentation, workflow syntax, publication-path, whitespace and formatting
checks pass. Rust tests use 1.97.1, `--locked`, `--release`, `--all-features` and
one test thread. The recorded host is
`Linux-6.17.0-1022-azure-x86_64-with-glibc2.39`.

## Artifact And Native Provenance

The [machine-readable record](phase6-focused-bt-linux-2026-10-04.json) retains the
original command/result metadata, named Rust tests, native probe outcomes and
input digests. Review matched the source commit and recomputed all 15 command-log
SHA-256 values. The original archive and extracted logs are retained locally.

CI verified the cached libtorrent 2.1.1, Boost 1.91.0 and OpenSSL 3.6.3
installation in sanitizer mode `none`. Its manifest contains 16,786 installed
file entries. Source, patch and builder digests match the recorded Git commit;
no native dependency rebuild occurred. Only the manifest is included in the
artifact, so local review verifies its inputs and the recorded successful CI
verification, rather than rehashing installed libraries.

- Archive SHA-256: `00bbb019421b163711ad4ed19c8037f152d76b1a4284a6ad9a203d486bbfc4f5`.
- Original `result.json` SHA-256: `01ed36bf7932dd3e0ab454b8be88f6ec983c69c0f98903863acf081ba8d700e2`.
- Native manifest SHA-256: `0a11f7f24fd1fa043e703afcfc27a89a59bfd630e759858d8fac3f1fc40a93eb`.
- CTest detailed-log SHA-256: `3b5a803ab747c2d9f37133fdb02ceacfcfc87d1e4baf3ed5f0a20499eebccb02`.

Review used the supplied artifact without GitHub API polling, another push or
another workflow dispatch.

## Remaining Acceptance

The [first focused slice](phase6-focused-linux-2026-10-04.md) records the earlier
RPC, setup and live-option regressions at `12386f7`; this second slice provides
separate evidence at `38d07e4`. Neither substitutes for the complete
platform/MSRV/feature matrix, remaining interface coverage, sanitizer/fuzz
execution or all six performance scenarios. The mixed 1,000-BT-peer and
1,000-HTTP-range workload still has no acceptance measurements from these runs.
The [Phase 6 gates](../docs/project/implementation-readiness.md#phase-6-local-implementation-and-open-acceptance)
remain open.
