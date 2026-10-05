# Phase 7 Local Hardening

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-local-hardening-2026-10-05.json)

This campaign starts from `b81b8ea` on `local/phase7-hardening`. It implements
the approved local portion of `P7-01` through `P7-04`, preserving Phase 6's
accepted baseline and the actual platform of each new result. The local campaign
is complete; this record does not approve a release or close all Phase 7
acceptance requirements. No push or CI dispatch is part of this campaign.

## Security Repairs

The [review](../docs/reviews/phase7-local-security-review.md) records four repairs
and the reviewed ownership, authentication, destination, persistence and native
provenance boundaries:

| Finding | Repair | Focused Regression |
| --- | --- | --- |
| `P7-S01` | Reject the 65th distinct tracker before growing the torrent/magnet overlay. | Exact-cap and duplicate success; reject overflow before inspecting a later malformed endpoint. |
| `P7-S02` | Bound recovery enumeration at 65,536 names and 16 MiB of encoded names. | Both count and byte boundaries, with directory contents unchanged on rejection; WSL and Windows adapters. |
| `P7-S03` | Bound project-owned DNS collections at 32 distinct addresses and enforce smaller configured limits before normalization growth. | Order and deduplication, smaller limits, and an iterator proving processing stops at the first excess distinct answer. |
| `P7-S04` | Decode Windows directory records from checked byte slices. | Adjacent records, every truncation of the minimum record, malformed offsets/lengths and visitor rejection; Linux and Windows. |

The owning contracts describe the new bounds. These changes introduce no public
API signature, persistence format, protocol capability or migration. Oversized
recovery directories now fail inspection before unbounded retention. The Windows
repair removes an invalid safety argument; this review does not assert a
previously observed real-world memory crash.

## Focused And Repeated Checks

The focused Linux run recorded 163 invocations: 155 passed and eight failed
while establishing strict private permissions on E: DrvFs. Their failed logs
remain evidence of unavailable local execution. Native Windows exercises these
permission-dependent paths. Its initial driver attempts also retain a Linux-only
target selection error and a parent/child test-count false negative. The
corrected supplement explicitly expects both owner-lock harness results and
covers the portable BT fixture, removable storage, permission policy and runner.

The separate extended run passed **500 WSL invocations and 600 native Windows
invocations**. Each selected case ran 50 times. Coverage includes completion
credit, admission races, cancellation, stalled RPC shutdown, disk shutdown,
forced process crashes and BT checkpoint recovery. Windows additionally runs
strict-permission hot-backup and mixed-import crash matrices unavailable on the
allowed WSL volume. Successful repeated checks do not establish native Linux
latency or hardware power-loss behavior.

All 80 Python helper tests pass. Generated contracts verify the pinned aria2
inventory, 108 reviewed options, 36 RPC methods, six notifications, core state
transitions, storage/journal/session/runtime contracts and 55 pinned destination
policy prefixes. Focused Linux and native Windows Clippy checks pass with
warnings denied.

## Fuzzing And Native Instrumentation

All 11 Rust-ASan parser targets passed both campaigns:

| Campaign | Accepted Executions | Mutation Executions |
| --- | ---: | ---: |
| Short | 5,632 | 4,969 |
| Extended | 112,640 | 98,071 |

Every target completed 512 accepted executions, then 10,240 in the separate
extended pass. Initialization is counted separately. No crash input or sanitizer
report was emitted. The retained runner records deterministic process seeds,
original seed hashes, accumulated corpus hashes, coverage, process limits,
commands, binary identities and all attempts. The budgets and limitations are
specified in the [local workflow](../docs/development/local-hardening.md).

ASan/UBSan and TSan capability probes pass clean controls and detect intentional
heap overflow, signed overflow and a data race. Nonzero results for these
negative controls are expected and identified separately. Native dependencies
are rebuilt with the current helper; stale builder manifests are rejected.
Ordinary native provisioning and all three supported C++ probes pass, followed
by 17 Rust bridge/library checks. Windows Clippy overlapped some ordinary bridge
correctness checks; their elapsed values carry no performance claim.

ASan/UBSan provisioning and all three supported native probes pass, followed by
17 Rust bridge/library checks linked to the instrumented native runtime. TSan
provisioning and the same three native probes also pass without race reports.

The native-only TSan Rust harness records nine failed invocations in the prebuilt
`libtest` completion channel. An empty Rust-only test reproduces the same report,
while a direct `main` passes. A documented direct driver removes only the crate
gate and two test attributes, then calls the unchanged native test bodies on its
main thread. Both bodies pass five repetitions each under strict TSan. An initial
lock-regeneration attempt selected newer cached dependencies and was rejected
before compilation; the corrected driver preserves repository-locked versions.
No report suppression is used, and the failed attempts remain retained.

Native Windows passes all four C++ probes, 43 bridge/adapter/library invocations,
and 24 BT engine invocations, including repeated transfer and shutdown recovery.
The combined ASan/TSan CMake configuration is rejected as required. An archive
audit finds the expected sanitizer references in all nine inspected native and
generated bridge archives. Instrumented C/C++ libraries and bridges do not
imply instrumentation of ordinary Rust code or its standard library.
Strict Unix private-storage and adapter fixtures require another filesystem or
platform; local skips do not replace that acceptance requirement.

## Diagnostics And Release Preparation

All 40 scheduled native Windows transport attempts emit valid complete reports
and pass their raw timing gates. The table retains every scheduled attempt:

| Transport | Attempts | Median Aggregate p99 (ms) | Maximum Aggregate p99 (ms) | Median Worst-Operation p99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| HTTP | 10 | 12.292 | 13.129 | 15.264 |
| WebSocket | 10 | 12.398 | 13.324 | 16.190 |
| Content-Length | 10 | 12.647 | 13.285 | 16.525 |
| NDJSON | 10 | 12.626 | 13.330 | 15.224 |

WebSocket attempt 2 observed `cc.exe`/`cc1.exe` activity and remains marked in
the record. A separate additional WebSocket attempt passes with no compiler
activity observed: aggregate p99 is 12.115 ms and worst-operation p99 is
15.344 ms. This gives 41 retained observations, including ten quiet observations
per transport. No timing outliers were removed from the scheduled sample.

Small native Windows transport runs remain diagnostic-only. They cannot resolve
the retained native Linux mixed-burst failure at 530.097 ms, or establish repeated
stability from the later passing 486 ms maximum. Existing limits and raw failures
remain authoritative; benchmarks remain manual-triggered.

Independent Linux and Windows-GNU `minimal` and `standard` builds through
`release-cli` are byte-for-byte reproducible within each platform. All eight
artifacts also pass `--help` and reject an unknown option.
The runs use locked dependencies, identical
path-remapping arguments, a fixed `SOURCE_DATE_EPOCH`, and the documented Cargo
profile. GNU PE timestamp insertion is disabled for Windows, while preserving
`link-self-contained=no` and the matching MinGW runtime.

Runtime inspection finds Linux imports of libc, libm and libgcc_s, with GLIBC
symbols through 2.34. Both Windows bundles import only Windows system DLLs,
with no separate MinGW DLL dependency. These results describe the locally built
artifacts; they do not establish compatibility on other hosts.

The generated inventory verifies 54 of 207 pinned aria2 handlers. Ariax remains
parallel to aria2; no `aria2c` replacement, migration, public C ABI or release tag
is approved by this work. Native Linux timing/kernel coverage, macOS/MSVC release
validation and hardware power-loss behavior retain their separate requirements.

Release-input inspection records license declarations for all 224 third-party
packages in the default workspace resolution. The 11 repository packages remain
`publish = false`, declare no license, and have no repository license file.
Finding `P7-R01` leaves distribution-license selection and notice packaging open;
the inventory does not assert legal compatibility or change any license.

## Provenance And Retained Attempts

Raw files are retained in the disk-backed campaign directory, represented as
`$P7` in the portable JSON. Repository and cache paths also use named
placeholders. Normalization affects only this checked-in report; its hashes bind
the unchanged raw files, source snapshots, binary identities and command logs.
All temporary files and build outputs remain on E:. Pinned Rust 1.97.1 uses
separate Linux and Windows-GNU installations; native sanitizer configurations
use separate build/install directories. No global toolchain is changed.

The initial stale-native-manifest rejection, unsupported WSL fixtures, corrected
runner attempts and intentional negative probes remain visible. Later successful
attempts do not overwrite them. Linux and Windows process/fixture audits find
no remaining campaign test processes or fixture directories. Eight empty failed
WSL fixture directories were removed and recorded. Successful release build
intermediates and selected native intermediates were removed after retaining
artifacts, source archives, manifests and logs.

The final JSON verifies individual command-log hashes and confirms production
inputs are unchanged since the focused pass. The native helper's diagnostic
wording changed before the second source snapshot and fresh native rebuilds;
the final helper suite passes. No validated production/support input changed
after that second snapshot. Workspace formatting, focused Clippy, documentation,
publication-path and whitespace checks pass.
