# Phase 6 Acceptance And Phase 7 Handoff

Phase 6 BitTorrent implementation and scoped acceptance are complete as of
October 5, 2026. `P6-01` through `P6-06` pass using the retained full functional
CI, native benchmark and local OpenSSH evidence, with an explicit audit of
unchanged production/build inputs. Phase 7 hardening is ready to begin.
Ariax remains unreleased; this decision does not close the separate backend,
compatibility, release-platform or tagging requirements.

## Accepted Evidence

| Evidence | Actual Source | Verified Result |
| --- | --- | --- |
| [Full functional CI](phase6-full-ci-2026-10-05.md) | `a6f9b0d` | Nine archives, 87 command logs; all configured platform/MSRV/feature jobs; native ASan/UBSan and 709,762 bounded Rust-ASan parser executions |
| [Native Linux benchmarks](phase6-benchmarks-rerun-2026-10-05.md) | `9107ecb` | All six complete scenarios; mixed HTTP/BT has 20,000 primary calls, 2,000 verifications, 8.000 ms aggregate p99, 31.311 ms worst operation p99 and a 486 ms maximum burst |
| [Local OpenSSH](phase6-openssh-local-2026-10-05.md) | `624af25` | WSL Linux and native Windows-GNU clients pass authentication, checked offset reads, final attributes and complete fixture/reservation cleanup |

The closure reviews local `b8a12e7`. Changes after the benchmark commit are
documentation/evidence only. Each source record retains its actual commit,
binary and log hashes; no later full-CI execution is asserted. Earlier focused
BT and interface records add detailed test descriptions, while the full matrix
provides the current production-source validation.

## Production And Build Source Equivalence

The [machine-readable closure](phase6-acceptance-2026-10-05.json) compares
`a6f9b0d`, `9107ecb`, `624af25` and `b8a12e7`. It retains the entire compared
inventory, Git modes/blob IDs, content SHA-256 values and every changed path.
All **418 compared files** have identical paths, modes and contents across
those commits. They cover production code, dependencies/lockfiles, native
patches, toolchain/build configuration, workflows, generated inputs and existing
validation sources outside the reviewed benchmark changes.

The audit includes every tracked file except `README.md`, `docs/**`,
`performance-evidence/**` and these three explicitly reviewed files:

- `crates/ariax-engine/benches/rpc_active_profile.rs`
- `crates/ariax-engine/benches/rpc_active_profile/burst_timing.rs`
- `crates/ariax-engine/tests/rpc_burst_timing.rs`

The compared inventory's SHA-256 is
`7b6a905c8cf844b28094b57184285f9a952c1d9ae04356194929c572b6875619`.
Its canonical JSON encoding is specified in the machine record. Exclusions
are explicit; an additional changed production or build-input path would fail
the equality check. Identical source inputs do not assert identical compiled
binary bytes across hosts, paths or builds.

## Separate Benchmark Change Validation

The three code changes add bounded failure diagnostics and their regressions.
The helper is imported only by the benchmark and its test; it is not part of
the downloader's production modules. Primary/verification step attribution
does not change the 400 ms launch cutoff, 500 ms/1,000-call burst cap, 50 ms
p99 gate or workload counts.

Four focused regressions pass with Rust 1.97.1 and warnings denied, covering
the exact burst boundary, primary/verification attribution and untimed delay.
The benchmark and regression target pass a focused default-feature compile
check. Native Linux CI then builds the updated optimized benchmark with all
features and completes all six scenarios. The closure preserves the focused
test log and compile-result hashes alongside the native report references.

This satisfies the [source-equivalence rule](../docs/protocols/libtorrent-integration.md#phase-6-gates):
reuse the full CI evidence for unchanged production/build inputs and validate
the reviewed benchmark changes separately. A different commit SHA caused by
documentation or this validated diagnostic change does not itself require
another full matrix. New production/build changes require validation of their
affected scope. Platform requirements and acceptance limits are unchanged.

## Gate Closure

| Gate | Accepted Behavior And Evidence |
| --- | --- |
| `P6-01` Native integration | Full CI verifies the pinned native builds on Linux, macOS, Windows MSVC/GNU, both MSRV targets, feature graphs and native ASan/UBSan coverage. |
| `P6-02` Safe admission | Native and workspace fixtures cover v1/v2/hybrid torrents, magnets, pre-storage metadata approval, protected roots, destination/redirect filtering and malformed/path/selection rejection. |
| `P6-03` Scheduling and resources | Bridge/adapter and engine tests cover bounded ownership, pressure, cancellation and live options. The complete mixed benchmark verifies active peers/ranges, resource ceilings, retained-credit cleanup and control latency. |
| `P6-04` Persistence | Workspace/native tests cover v3 identity, old-format rejection, tracked checkpoints, alert loss, dirty shutdown/recovery and commit crash points in both SQLite journal modes. |
| `P6-05` Interfaces and options | Full CLI bundles and interface tests cover shared Rust/CLI/RPC behavior, BT-disabled rejection, restart parity, acknowledged live updates and explicit active restart-required rejection. |
| `P6-06` Acceptance | Full functional/sanitizer/fuzz evidence, six complete native measurements and fresh local OpenSSH are combined through the source audit and separate diagnostic validation. |

The [focused BT record](phase6-focused-bt-linux-2026-10-04.md) names transfer,
ownership and checkpoint/crash fixtures. The
[interface record](phase6-focused-interfaces-linux-2026-10-04.md) details feature
rejection and interface parity. Those test surfaces also execute in the later
full matrix. The accepted scope remains the existing protected-root libtorrent
integration; a custom storage backend is a deferred capability.

## Phase 7 Work And Retained Limits

The [hardening work packages](../docs/project/implementation-plan.md#phase-7-hardening)
start with mixed-burst reproducibility, security/FFI review, longer bounded
fuzzing and expanded sanitizer coverage including supported TSan configurations,
then platform stress and release preparation.

The earlier [530.097 ms mixed-burst failure](phase6-benchmarks-2026-10-05.md)
remains a failed run. Its cause is unresolved; the later passing campaign does
not establish repeated-run stability. `P7-01` retains every future attempt and
reports tail latency and failure frequency under unchanged measurement rules.
The four diagnostic regressions cover accounting/attribution, not a production
latency fix.

OpenSSH evidence uses a Windows/MSYS2 server and a WSL 1 Linux client, with
native Windows-GNU client coverage separately recorded. The focused SFTP builds
retain their two existing dead-code warnings per client. These results do not
claim a native Linux OpenSSH server run, new adversarial coverage or a complete
release matrix. Historical machine records keep their provisional acceptance
flags; this closure is the superseding acceptance decision.

The closeout changes only documentation/evidence. No runtime code, acceptance
threshold, workflow or feature setting changes. No new full CI, benchmark,
workflow dispatch, push or tag is performed.
