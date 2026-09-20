# Phase 5 Validation Record

The Phase 5 implementation and all required local validation pass, including
bounded fuzzing and all five native Windows benchmark scenarios. Native Linux
acceptance remains deferred until CI is ready.

Implementation checkpoint: `88d1a8321afb85229695e306f015d9f3bcbd5074`.
The [raw evidence](phase5-windows-gnu-2026-09-15.json) records the implementation
source hashes, benchmark binary, reports, fuzz attempts and validation log hashes.
The implementation citation uses the sanitized publication history. Raw
`sourceCommit` and artifact hashes remain unchanged; `publishedSourceCommit`
and the [history map](publication-history-map.json) connect the two histories.

## Implemented Scope

HTTP(S), FTP/FTPS and SFTP use the shared scheduler, admission, resource,
persistence and shutdown owners. SHA-512, SHA-256, SHA-1 and MD5 content checksums
share immutable verification manifests. Chunk verification waits for every
contributing lease, normally hashes ordered ingress, and uses bounded readback
for recovery or reordering. SHA-1 and MD5 remain compatibility checksums.

Bounded Metalink v3/v4 parsing, selected-file admission and automatic following
share the CLI, RPC and Rust admission path. Atomic expansion records prevent
duplicate children after restart. JSON migration v2 retains verification data
without the original XML; v1 import remains supported. Aria2 text export rejects
tasks whose required verification metadata it cannot preserve.

FTP/FTPS uses owned control/data connections, validated passive/active peers and
exclusive sequential failover. HTTP(S) and SFTP share random-access leases.
SFTP checks trust before credentials, bounds framing and outstanding reads,
and requires an exact current challenge for approval. Protocol feedback uses
credential-free origin keys, with bounded selectors and statistics.

The reviewed SuppaFTP 10.0.1 and russh-sftp 2.3.0 forks retain complete upstream,
patch, license and file inventories. `minimal` enables Metalink; `standard`
adds FTP/FTPS and SFTP; `full` and `compat` inherit that implemented set. The
default build remains the HTTP checkpoint. Required journal records 27–32
extend journal v1 without changing existing record meanings or SQLite schema v2.

## Gate Evidence

All six gates pass locally. The names below identify executable success,
rejection and recovery coverage in the passing workspace suites; additional
platform and measurement evidence follows.

| Gate | Representative Passing Evidence |
| --- | --- |
| `P5-01` Shared foundations | `cpu::tests::reservations_survive_execution_and_completion`, `cancelled_receiver_cannot_release_running_work`, all four protocol feature-bundle checks, and default-feature HTTP regression suites. |
| `P5-02` Verification and recovery | `manifest_two_leases_require_both_commits_and_the_expected_digest`, `manifest_mismatch_invalidates_contributors_before_redownload`, `committed_manifest_spans_without_durability_recover_through_readback`, and digest partition/property tests. |
| `P5-03` Metalink | `phase5_metalink_admission_is_atomic_ordered_and_self_contained`, `xml_paths_algorithms_sources_and_selection_fail_closed`, `phase5_automatic_follow_is_atomic_retains_or_omits_xml_and_completes_empty_children`, and `phase5_process_exit_recovers_trust_and_atomic_expansion_without_duplicate_children`. |
| `P5-04` FTP/FTPS | `ftp_one_stream_checkpoints_resumes_and_pins_pasv_peer`, `ftps_requires_private_data_trust_and_pre_tls_active_peer_authorization`, and `ftp_reply_limits_close_poisoned_channels_and_logs_redact_all_text`. |
| `P5-05` SFTP | `sftp_trust_precedes_authentication_and_bounded_reads_share_http_ranges`, `framing_rejects_before_payload_and_closes_all_outstanding_requests`, `phase5_drained_host_challenge_persists_and_only_exact_approval_clears_it`, and real OpenSSH interoperability on both clients. |
| `P5-06` Integration | `phase5_json_v2_migrates_selected_verification_into_a_new_root_atomically`, `cli_options_use_shared_validation_and_reject_unknown_or_duplicate_flags`, `selectors_obey_feedback_priority_capacity_and_disabled_sources`, origin-feedback tests, and the five native Windows benchmark scenarios. |

## Local Validation

Workspace builds, tests and lints use the repository's Rust 1.97.1 distributions;
the dedicated MSRV check uses 1.88. Windows commands run through MSYS2 with
MinGW64 first; strict Clippy uses the pinned frontend, driver and a separate
target directory. Workspace suites use two test threads and exclude only the
separately exercised 1,000-task regression.

| Check | Result |
| --- | --- |
| Linux under WSL 1 build | `cargo build --locked --workspace` passes. |
| Linux default-feature tests | 375 engine tests pass; the other workspace suites and doctests pass. |
| Linux all-feature tests | 392 engine tests pass; storage 226, runtime 67, core 49 plus 46 scheduler integrations; CLI, config, runtime integrations, vendor contracts and xtask pass. |
| Native Windows-GNU build | `cargo build --locked --workspace` passes. |
| Windows default-feature tests | 373 engine tests pass; the other workspace suites and doctests pass. |
| Windows all-feature tests | 390 engine tests pass; storage 222, runtime 67, core 49 plus 46 scheduler integrations and native security 12; remaining workspace suites pass. Required reparse-point coverage is enabled. |
| Strict Clippy | Both platforms pass `--workspace --all-targets --all-features -- -D warnings`. |
| MSRV 1.88 | Linux `check --locked --workspace --all-targets --all-features` passes. |
| Generated contracts | Verification against aria2 `9e7273583f83e881e3ec067b523ba88724088d2f` passes: 89 reviewed options, 32 journal records, 32 events, 74 semantic actions and 1,184 transition cells. |
| Dependency checks | All four protocol feature bundles, exact SQLite feature closure, verifier rejection cases and complete fork provenance pass. |
| Formatting and documentation | Rustfmt, staged whitespace checks and Markdown heading/TODO/unresolved inspection pass. |

Ignored engine/storage entrypoints are child processes exercised by their crash
and recovery parent tests. The opt-in OpenSSH test is exercised separately on
both clients. The retained Linux 1,000-task log records passing 75.67-second and
75.71-second runs before the final allocation-window and test-fixture fixes;
that unchanged bulk-control path was not rerun after those fixes. The final
native Windows 1,000-task regression passes separately in 83.77 seconds after
compilation ends.

## Regressions And Interoperability

Tests cover atomic Metalink admission and selection, unsafe names and resource
limits, empty files, mixed mirrors, digest mismatch, chunk contributors,
committed-but-unverified recovery, interrupted trust transactions, process exits
during expansion, generation-bound approval and self-contained JSON migration.
Loopback wire tests cover FTP/FTPS endpoint and TLS policy, sequential resume,
dependency logging and framing bounds. SFTP scenarios cover authentication
ordering, rejected trust, bounded reads, malformed peers and reservation refunds.

Final validation found an allocation-window option bug: aria2 projects an
allocating task as `waiting` even after its worker starts. A live rate patch
could persist without updating the worker's bucket. A deterministic regression
reproduced the old four-byte grant after a one-byte limit patch. Allocating
generations now use the active live-update, restart and rejection rules. Tests
cover both lifecycle states, unchanged values after rejection, delayed drain,
exactly one generation promotion and restart recovery.

Linux and native Windows clients pass private-key authentication, SHA-512
chunked offset reads, final attributes and exact reservation refunds against
OpenSSH 10.5p1 with OpenSSL 3.6.3. The fixture creates a private key copy and
empty known-host file. Windows files receive current-user ownership and the
existing exact protected ACL; user SSH files are untouched. The runner passes
fixture variables inside the native environment and terminates the actual
MSYS2 daemon through its PID file. Cleanup checks find no remaining SSH listener.

Other repaired validation failures include safe host-pin recovery in builds
without SFTP, isolation from user `known_hosts`, and FTP feedback previously
being dropped by the random-access source constructor.

## Bounded Measurements

All five optimized native Windows scenarios pass on their first attempt using
the implementation checkpoint above. The host is an Intel Core i7-4700HQ with
eight logical processors, Windows 10 Enterprise LTSC 10.0.17763, Rust 1.97.1
Windows-GNU and LLVM 22.1.6. The engine uses two Tokio workers, the concurrency
profile and `BlockingDiskLane`.

Each transport admits its 1,000 active HTTP ranges through Metalink with a
complete SHA-256 chunk manifest, then completes 20,000 measured calls and 1,000
additional mutation-verification calls. These scenarios measure the shared
control plane during Metalink-admitted HTTP transfers; FTP/FTPS and SFTP have
separate functional/security coverage above.

| Transport | Aggregate p99 (ms) | Worst Operation p99 (ms) | Longest Burst (ms) | Scenario Time (s) | Peak Sampled Working Set (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| HTTP | 17.260 | 34.435 | 414 | 44.650 | 139.09 |
| WebSocket | 17.064 | 28.010 | 417 | 44.910 | 140.52 |
| Content-Length stdio | 17.424 | 35.674 | 420 | 51.655 | 142.63 |
| NDJSON stdio | 17.605 | 38.946 | 419 | 50.184 | 140.48 |

All ordinary-operation p99 values are below 50 ms. No measured burst exceeds
426 calls or 420 ms, cooldowns are 250 ms, and every scenario finishes inside
90 seconds. Renewed active-range barriers, per-status connection checks,
stalled-consumer credit release, resource limits and clean shutdown pass.
Peak RPC/resident reservations are 37.77/194.98 MiB against 64/896 MiB.
The largest transport owner turn is 6.514 ms with at most 12 steps; its
approximately 1 ms scheduling budget is cooperative, not a hard deadline.

The separate administrative scenario completes in 10.298 seconds with zero
active ranges. Its 128-task import/export/save, bulk controls, 128-result purge
and shutdown pass. Concurrent query p99 peaks at 3.790 ms and urgent
acknowledgement at 17.380 ms. Its longest query burst is 402.025 ms; shutdown
acknowledgement/drain take 0.016/50.337 ms. Total operation completion times are
recorded separately from those latency gates.

Compilation and fuzzing finish before benchmark collection. Preflight and
five-second native process samples observe no concurrent compiler. Limits stay
at 1,000 calls or 500 ms per burst, at least 250 ms cooldown, and a 90-second
scenario deadline; incomplete or over-limit runs fail. The
[performance profile](../performance-profiles.md#native-windows-phase-5-evidence)
preserves this campaign separately from the historical P4 reports.

## Bounded Fuzzing

All ten targets pass 512 ASan/coverage executions each, 5,120 total across
83 accepted bursts. The isolated Linux-under-WSL debug binaries use cargo-fuzz
0.13.2, Rust 1.97.1 with `RUSTC_BOOTSTRAP=1`, and instrumented inline counters.
Processes run 16–128 inputs, cap input length at 8 KiB and have at least 250 ms
cooldown. The longest accepted process is 487.167 ms. Counts include corpus
initialization and repeated seeds; this is a smoke campaign, with longer native
CI fuzzing still required by the broader validation matrix.

Targets cover Metalink, verification manifests, HTTP response/request headers,
retry specifications, discard accounting, journal replay, RPC JSON, session
documents and URL rules. Session seeds include self-contained JSON v2
verification and rejected zero chunk length. The complete seed inventory and
per-target binary hashes are recorded in the raw evidence.

Three attempts are retained and excluded: Metalink 128 executions took
1,049.571 ms; RPC 128 took 1,361.025 ms; and RPC 32 took 507.287 ms. All exited
successfully without a sanitizer/parser failure, but exceeded the 500 ms
acceptance limit. Disabling per-function diagnostic printing with
`-print_funcs=0` and reducing batch sizes preserved ASan and coverage while
allowing the remaining execution budgets to pass. No accepted target was
repeated after its budget was complete.

## Deferred Coverage

Native Linux acceptance remains deferred until CI is ready. WSL 1 checks do not
substitute for native kernel/backend or benchmark evidence. The manual native
Linux workflow supports the Metalink benchmark fixture but has not run for
acceptance. HTTP/2, BitTorrent/Phase 6, growing layouts, new disk backends and
the full release/platform matrix remain outside this phase. No tag is created.
