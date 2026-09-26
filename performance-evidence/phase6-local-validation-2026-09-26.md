# Phase 6 Local Validation Record

The local Phase 6 implementation and acceptance harness are complete on top of
`64b17f7d1c8ae7d2722c634d6685deae64045640`. This record describes the local
checkpoint and focused offline checks. It does not establish native acceptance
or close any `P6-01` through `P6-06` gate. No remote, GitHub API, push or CI action
was performed during this continuation.

## Local Scope

The [implementation plan](../docs/project/implementation-plan.md#phase-6-bittorrent-full-build)
maps all six work packages. This checkpoint adds native transfer, endpoint,
recovery and CLI/RPC/Rust parity fixtures; a bounded metadata fuzz target;
separate ASan/UBSan provisioning; and the mixed benchmark with 1,000 BT peers
alongside 1,000 HTTP ranges. Magnet reconstruction retains validated endpoints,
and the native patch installs the IP filter before startup and checks outgoing
DHT packets and HTTP redirects.

Checkpoint tests cover process exit before and after the SQLite commit in WAL
and DELETE journal modes. The engine suite also covers an expired shutdown
deadline after a successful pause checkpoint and restart: shutdown must report
failure while the previous resume blob and dirty generation survive reopening.
The selected-file test waits for metadata approval before connecting its peer,
so a fast transfer cannot finish before the test observes admission.

The final simplification pass makes the shared transfer specifications and
catalog concrete types in `transfer_spec.rs`, removes historical HTTP aliases,
and consolidates the SHA-256-only checksum API and duplicate option fields into
`ContentChecksum`. HTTP keeps one terminal verification path for all four
algorithms, charges its complete read buffer to the CPU budget, and retains
fully durable offline rechecks. Regression sources cover successful verification,
strict mirror eligibility, malformed input and repeated mismatch after recovery.

Configuration dumps now default to the documented JSON envelope and reject the
former `legacy` format. BT task-effective dumps share the sanitized option
query; global statistics aggregate BT download/upload rates and selected-file
progress from the same immutable query root. Tests cover all dump formats,
missing/unknown tasks, restart, invalid arguments and retained query snapshots.
Current subsystem and project documents describe v3-only storage and distinguish
historical evidence from pending Phase-6 CI acceptance.

## Passing Focused Checks

| Check | Result |
| --- | --- |
| `python3 -B -m unittest discover -s scripts -p 'test_ci.py' -v` | 16 tests pass, including complete/missing mixed benchmark evidence and sanitizer/fuzz command orchestration. These use synthetic reports and mocked native commands. |
| `python3 -B -m unittest discover -s scripts -p 'test_bt_native.py' -v` | Six tests pass, including matching sanitizer configure/build/install settings, provenance isolation and patch rejection. No native library is built by these tests. |
| `ariax-bt-metadata` | Six unit tests and two fixture integration tests pass with Rust 1.97.1, `--locked --offline` and two build jobs. All 47 resolved package names, versions and checksums match the repository lockfile. |
| `peer_wire.rs` | Both framing tests pass under standalone Rust 1.97.1 with `--edition=2024 --test -Dwarnings`. |
| Fixture reproduction | `generate.py --check` reproduces the payload and all eight torrent fixtures. |
| Native patch application | The actual `scripts/bt_native.py` patch applier accepts all 12 affected files copied from the pristine pinned libtorrent 2.1.1 source. |
| Workflow syntax | The locally available actionlint 1.7.12 executable passes with external shell/Python lint integrations disabled. |
| Offline dependency-policy checks | The protocol feature verifier self-test, protocol vendor verifier and SQLite feature verifier regression script pass. These do not substitute for resolving the complete feature graph. |
| Formatting and documentation | Pinned rustfmt checks all 40 changed Rust files. `git diff --check` and `python3 -B scripts/check_docs.py` pass across 53 Markdown files. |
| Worktree portability | All 443 current tracked and new files pass the local path audit; ignored local configuration is excluded. No repository Cargo/native build target was created. |

The metadata test used a temporary workspace containing only the metadata crate
and torrent fixtures, with the repository's workspace settings and exact
dependency pins. This avoids resolving unrelated workspace dependencies.
Temporary test workspaces and binaries were removed; no repository Cargo or
native build target was created.

## Unexecuted Acceptance

An isolated offline attempt to run `ariax-storage`'s BT tests stopped during
dependency resolution: the cache lacks the locked `indexmap` 2.14.2 required by
`toml` through the config dev-dependency. No storage compilation or test ran,
and no dependency was downloaded or changed to work around the missing cache.

A focused `ariax-engine` checksum-option regression command also stopped before
compilation because the local crates.io cache lacks the pinned `md-5` package.
It used `--locked --offline`, the repository's Rust 1.97.1 executables and a
temporary target directory, which was removed. The new engine/CLI checksum and
config/statistics regressions therefore remain uncompiled and unexecuted.

The new native engine, bridge, endpoint and CLI tests have not been compiled or
executed at this checkpoint. The process-crash tests, sanitizer/fuzz campaign,
mixed benchmark, full workspace checks, complete feature graph and platform/MSRV
matrix still require recorded execution. Full builds and native validation
remain assigned to CI by the repository guidelines; CI actions remain outside
the currently authorized local scope.

The [historical five-scenario baseline](ci-baseline-2026-09-22.md) applies only to
its recorded source and scenarios. Current acceptance must include all six
scenarios from the same source. The [readiness gates](../docs/project/implementation-readiness.md#phase-6-local-implementation-and-open-acceptance)
remain open, including the separate kernel/backend, custom BT storage and
release-platform requirements. No release tag is warranted by this record.
