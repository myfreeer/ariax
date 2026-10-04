# Phase 6 Focused Sanitizer And Parser Fuzz Linux Validation

The supplied `ci-focused-linux.zip` records a passing native Linux validation
command for `596332b872274b60d385b35824de36d387605dbf` on the temporary branch
`local/removable-storage-mixed-bt-20261003`. All 14 recorded commands passed,
including 70 Python helper tests, 14 Rust tests, four native ASan/UBSan probes
and the bounded BT metadata parser fuzz campaign. No selected test failed or
was ignored, and no sanitizer diagnostics or crash artifacts were found.

The validation command took 187.679 seconds including compilation; this is not
the elapsed time of the entire workflow. It built and ran no benchmark. This
passes the selected sanitizer and parser-fuzz coverage; full Phase-6 acceptance
remains open.

## Executed Coverage

| Rust Suite | Passed | Test Seconds |
| --- | ---: | ---: |
| `ariax-bt` unit tests | 8 | 1.55 |
| `ariax-bt` adapter integration tests | 4 | 2.16 |
| `ariax-bt-libtorrent-sys` native integration tests | 2 | 4.67 |

The tests cover bridge budgets and ownership, safe file mapping and output-root
policy, metadata approval, real v1/v2/hybrid transfers, bounded/redacted
rejection, retained completion credit, stale live settings and shutdown cleanup.
All features are enabled, with no failed, ignored or filtered Rust tests.
Three additional empty harness/doc-test summaries add no tests to the total.

| Native Probe | Passed | Test Seconds |
| --- | ---: | ---: |
| `native_bounded_output` | 1 | 0.02 |
| `native_openssl_callbacks` | 1 | 0.03 |
| `native_destination_policy` | 1 | 6.28 |
| `native_private_storage` | 1 | 0.03 |

CTest reports 6.42 seconds total. Its detailed log confirms all 16 endpoint
cases and the redirect-policy and allowed/blocked DHT stages completed.
Command logs and the detailed CTest log contain no sanitizer, leak or
runtime-error signatures.

Native C++ and bridge code receive ASan/UBSan instrumentation. The ordinary Rust
test harnesses link that runtime but are not compiled with `-Zsanitizer`.
The parser fuzz executable receives Rust ASan and coverage instrumentation.
The campaign configures `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1` and
`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`.

## Bounded Parser Fuzzing

The `bittorrent_metadata` target completed 799,033 executions. Its configured
20-second budget finished in a reported 21 seconds, with seed `1692982595`.
It loaded 19,059 instrumented counters and finished with 2,894 coverage counters
and 10,567 features. Peak reported RSS was 314 Mb, below the configured 512 limit.
Inputs were limited to 1 MiB with a two-second timeout. No crash artifact or
sanitizer diagnostic was found.

All 11 initial seeds match Git byte-for-byte. The archive retains 1,573 corpus
files totaling 246,223 bytes, with a maximum file size of 967 bytes. The fuzzer
reports 1,579 final live corpus entries; these are separate recorded counts.
The original corpus and its hashed inventory are retained locally with the
archive. This bounded campaign does not establish long-duration fuzz coverage.

## Artifact And Native Provenance

The [machine-readable record](phase6-focused-sanitizers-linux-2026-10-04.json)
retains original command metadata, named test outcomes, fuzz statistics, seed
hashes and native provenance. Review matched the source commit, all 14 command-log
SHA-256 values, and native source, patch and builder digests against Git.
Documentation, workflow syntax, publication paths, whitespace and formatting
checks pass. Rust is 1.97.1 and the recorded host is
`Linux-6.17.0-1022-azure-x86_64-with-glibc2.39`.

CI verified the cached libtorrent 2.1.1, Boost 1.91.0 and OpenSSL 3.6.3
installation once with `--sanitizer address --verify`, without rebuilding native
dependencies. The manifest records Clang 18.1.3 and
`-fsanitize=address,undefined`, with 16,786 installed-file entries. Only the
manifest is in the archive; local review verifies its inputs and the recorded
successful CI verification, rather than rehashing installed libraries.

- Archive SHA-256: `1e2ad4e83d9149dcb6ce87f46753cfd7b2ad2a84cf8df6a3003e1e207f1a0fee`.
- Original `result.json` SHA-256: `254e9bfcffeff23e104134a968c80f2f2f6c6ef39cd82c358494c9ecfd8cc2df`.
- Native manifest SHA-256: `96608ed9962ee928b67c4bd608de0f81c76777ce578a51f4d12788d9b1353163`.
- CTest detailed-log SHA-256: `82d38fac806c76b8e1f8a80f94c4045b90064b91bf30d5701832a0b178edf693`.
- Corpus inventory SHA-256: `f23c1d461a492764d396e50c644ee609a006f4fe4782729dd4de63acb2a42d07`.

The original archive and extracted logs are retained locally. Review used the
supplied artifact without GitHub API polling, a push or workflow dispatch.

## Remaining Acceptance

This adds sanitizer and bounded parser-fuzz evidence to the earlier
[RPC/setup slice](phase6-focused-linux-2026-10-04.md),
[BT transfer/security/recovery slice](phase6-focused-bt-linux-2026-10-04.md) and
[interface/feature slice](phase6-focused-interfaces-linux-2026-10-04.md).
Each record remains bound to its own source commit and selected tests.

Complete current-source platform/MSRV and feature-bundle tests and release
builds, remaining workspace coverage and all six performance scenarios remain
required. The mixed 1,000-BT-peer and 1,000-HTTP-range workload still has no
acceptance measurements from these runs. The
[Phase 6 gates](../docs/project/implementation-readiness.md#phase-6-local-implementation-and-open-acceptance)
remain open.
