# Phase 6 Full Functional CI Validation

Review on October 5, 2026 verifies the supplied full-CI artifacts for
`a6f9b0db93e1af30f24d61aaf73b8cde8e4c6d23` on `main`. All nine archives match
that source commit, and all 87 command-log SHA-256 values and successful exit
codes verify. Preflight and every configured matrix group pass. This establishes
the full functional CI matrix for the current implementation; Phase-6 performance
acceptance remains open.

The user reports the aggregate CI result as green. The supplied artifacts cover
preflight and all eight validation groups; `CI Required` emits no separate
artifact. Review made no GitHub API requests and launched no additional workflow.

## Verified Matrix

| Artifact | Commands | Validation Seconds | Verified Coverage |
| --- | ---: | ---: | --- |
| `ci-preflight` | 16 | 36.964 | Helper tests, docs, formatting, workflow lint, publication, pinned reference, generated contracts and dependency policies |
| `ci-linux` | 11 | 395.727 | Native probes, workspace build, default/all-feature tests, strict Clippy and `release-capi` build |
| `ci-macos` | 10 | 427.787 | Native probes, workspace build, default/all-feature tests and strict Clippy on arm64 |
| `ci-windows-msvc` | 11 | 908.896 | Native probes, workspace build, default/all-feature tests and strict Clippy |
| `ci-windows-gnu` | 11 | 841.960 | Native probes, workspace build, default/all-feature tests and strict Clippy through MINGW64 |
| `ci-msrv-linux` | 4 | 58.080 | Rust 1.88 workspace check with all targets/features |
| `ci-msrv-windows-gnu` | 4 | 55.429 | Rust 1.88 workspace check with all targets/features |
| `ci-feature-bundles` | 11 | 621.995 | Complete CLI tests and `release-cli` builds for all four bundles |
| `ci-bt-safety` | 9 | 185.105 | Native ASan/UBSan probes, bridge/adapter tests and Rust-ASan parser fuzzing |

Durations are each validation command's reported elapsed time, including
compilation, not complete job or workflow times. They must not be added to infer
workflow duration. Non-MSRV jobs use Rust 1.97.1. All commands use the recorded
locked dependency graph where applicable.

The four native platforms each pass `native_bounded_output`,
`native_openssl_callbacks`, `native_destination_policy` and
`native_private_storage`. The sanitizer job passes the same four probes, for
20 native probe executions total. Windows test logs also confirm native ACL
and reparse-point regressions pass. Each platform completes default and
all-feature workspace tests and Clippy with warnings denied.

The CLI feature job passes 29 tests each for `minimal` and `standard`, and
30 each for `full` and `compat`: 118 executions across feature selections.
Every bundle completes its `release-cli` build. No CLI test is ignored or
filtered out in this job.

## Ignored Tests And Fault Fixtures

The six ignored workspace helper entries belong to storage/journal process-crash,
backup/rollback recovery and owner-lock fixtures. Their parent tests execute and
pass; source assertions validate child exit or termination and recovered state.
Some child output is captured by the parent harness. Nested subprocess summaries
are retained in the logs and are not summed into a cross-platform test total.
Intentional panic diagnostics in fault-injection tests do not indicate a failed
test; all recorded Rust test summaries pass.

One additional test, `openssh_public_key_offsets_and_final_attributes_interoperate`,
is ignored in each all-feature workspace run. Its reason requires
`scripts/run-openssh-interop.py` and a private loopback sshd. These artifacts do
not establish a fresh live OpenSSH interoperability result.

Preflight passes all 70 Python helper tests. Each Windows helper run reports
70 tests with two skipped, so 68 pass. The skipped `TemporaryDirectoryTests`
cover Unix temporary-directory aliases and are explicitly excluded on Windows;
they pass in the Linux preflight run.

## Sanitizers And Bounded Fuzzing

The sanitizer job passes four native probes and 14 bridge/adapter Rust tests.
Native C++ and bridge code receive ASan/UBSan instrumentation; the ordinary Rust
test harnesses link that runtime. The parser fuzz executable receives Rust ASan
and coverage instrumentation. No sanitizer or leak report is present.

`bittorrent_metadata` completes 709,762 executions against a 20-second budget,
reporting 21 seconds elapsed, 2,986 coverage counters and 11,142 features.
Seed `2186854086` starts from 11 repository seeds verified byte-for-byte.
Peak reported RSS is 305 Mb, below the 512 limit; inputs are limited to 1 MiB
with a two-second timeout. No crash artifacts are retained.

The fuzzer reports 1,664 final live corpus entries. The archive contains 1,661
corpus files totaling 280,307 bytes; these are distinct recorded counts. The
original corpus and hashed inventory remain with the local archive snapshot.
This is bounded fuzz evidence, not a long-duration campaign.

## Artifact And Native Provenance

The [machine-readable record](phase6-full-ci-2026-10-05.json) retains archive,
result and command-log hashes, command metadata, host/toolchain details, ignored
test reasons, native provenance and fuzz results. All native source, patch and
builder digests match Git at the recorded source commit. Archive path and
integrity checks passed before extraction.

All eight native manifests record libtorrent 2.1.1, Boost 1.91.0 and OpenSSL 3.6.3.
Each job reuses and verifies its matching native installation; no dependency
rebuild is observed. Ordinary installations use sanitizer mode `none`; the
separate Linux installation records `address` and `-fsanitize=address,undefined`.
Compiler identities and installed-file counts are preserved per artifact.

Installed libraries, release executables and detailed CTest endpoint logs are
absent from the archives. Local review verifies manifest inputs and successful
CI verification/build/probe logs; it does not rehash absent binaries or claim
additional detailed endpoint traces. The portable record normalizes macOS and
Windows runner-home paths; original archives/reports and their hashes remain unchanged.

## Remaining Acceptance

The configured platform/MSRV/workspace, CLI feature/release-build and sanitizer
matrix now has passing evidence at one source commit. All six performance
scenarios, including 1,000 BT peers alongside 1,000 HTTP ranges, still require
manual benchmark execution and verified reports against that candidate source.
Fresh live OpenSSH evidence and separately tracked backend/release-platform
requirements are not supplied by this run. Release tagging remains disabled.
The [Phase 6 acceptance status](../docs/project/implementation-readiness.md#phase-6-local-implementation-and-open-acceptance)
remains open.
