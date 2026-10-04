# Phase 6 Focused Linux Validation

The supplied `ci-focused-linux.zip` records a passing native Linux validation
command for `12386f70096e2a54994412a9d166539f37a80904` on
`local/removable-storage-mixed-bt-20261003`. All 13 recorded commands passed.
The validation command took 228.782 seconds, including Rust compilation; this
is not the elapsed time of the entire workflow.

The run passes 70 Python helper tests and 35 Rust regressions, with no failed or
ignored Rust tests. It verifies the native Linux behavior of the focused fixes.
No benchmark executable was built or run, and this evidence does not close full
Phase-6 acceptance.

## Executed Coverage

| Rust Suite Or Filter | Passed |
| --- | ---: |
| `bt_peer_fixture` | 2 |
| `bt_peer_startup` | 3 |
| `permission_policy` | 3 |
| `rpc_benchmark_setup` | 5 |
| `rpc_benchmark_workload` | 2 |
| `rpc_origin_metrics` | 4 |
| `rpc_stalled_credit` | 4 |
| `rpc_budget::tests::` | 6 |
| `rpc_client::tests::` | 2 |
| `http_rpc::tests::stalled_` | 3 |
| BT live-option update during a peer refresh | 1 |

The socket integration test
`socket_stalls_retain_each_consumers_credit_and_release_on_disconnect` passes
on native Linux. It requires independent-consumer progress and credit release
after disconnect; the earlier WSL1 cleanup failure no longer prevents validating
this behavior on a native host.

`live_option_waits_for_peer_refresh_and_rejects_a_second_pending_change` also
passes. It covers queued and submitted peer reads, rejection of a second update,
native and storage acknowledgements, active restart rejection, and recovery of
the persisted option. The mixed setup fixture runs HTTP and BT together while a
third task remains waiting; separate fixtures enforce peer caps and bounded
handshake startup.

The run also passes documentation, workflow lint, publication-path, whitespace
and Rust formatting checks. Tests use Rust 1.97.1, `--locked`, `--release`,
`--all-features`, and one test thread. The recorded host is
`Linux-6.17.0-1022-azure-x86_64-with-glibc2.39`.

## Artifact And Native Provenance

The [machine-readable record](phase6-focused-linux-2026-10-04.json) retains the
original command/result metadata, named Rust test outcomes, log hashes and native
input digests. Review matched the exact source commit and recomputed the SHA-256
of all 13 command logs.

The archive contains the native installation manifest for libtorrent 2.1.1,
Boost 1.91.0 and OpenSSL 3.6.3, with 16,786 installed-file entries. Its source,
patch and builder digests match the recorded Git commit. CI verified the cached
installation with `scripts/bt_native.py --verify`, using sanitizer mode `none`;
there was no libtorrent/OpenSSL rebuild. The archive contains only the manifest
of that installation. Local artifact review verifies its input digests and the
recorded successful CI verification.

- Archive SHA-256: `5063cc2623a94193f52470af99f691d58d82ad0460d3d7d2ee66de448631201f`.
- Original `result.json` SHA-256: `734c5fb7156b4c60169b40afa32fff73e99b042d8073f2bf517960d761d0531b`.
- Native manifest SHA-256: `0a11f7f24fd1fa043e703afcfc27a89a59bfd630e759858d8fac3f1fc40a93eb`.

The archive snapshot and extracted logs are retained locally with the recorded
SHA-256 digests. Review used the supplied artifact without GitHub API polling,
another push or another CI dispatch.

## Remaining Acceptance

The [temporary workflow](../docs/development/continuous-integration.md#temporary-focused-validation)
deliberately runs a narrow set of native regressions. Full platform, MSRV and
feature coverage, broader native transfer/security/crash/recovery suites,
sanitizer/fuzz execution, and the complete six-scenario performance campaign
remain required. In particular, this run provides no measurements with 1,000 BT
peers alongside 1,000 HTTP ranges. The
[Phase 6 gates](../docs/project/implementation-plan.md#phase-6-bittorrent-full-build)
remain open.
