# Phase 6 Native Linux Benchmark Review

Acceptance update: the [Phase 6 closure](phase6-acceptance-2026-10-05.md)
accepts the milestone using verified production/build source equivalence and
separate validation of benchmark changes. It supersedes the provisional open
acceptance and identical-commit requirement below. The original run outcomes,
failures, source identities and platform limits remain unchanged.

Review on October 5, 2026 verifies the supplied manual benchmark artifact for
`a6f9b0db93e1af30f24d61aaf73b8cde8e4c6d23`, the same source as the passing
[full functional CI matrix](phase6-full-ci-2026-10-05.md). HTTP, WebSocket,
Content-Length stdio, NDJSON and administrative scenarios pass. Mixed HTTP/BT
fails: a measured burst lasts **530.097 ms**, exceeding the **500 ms** limit.
Phase-6 performance acceptance remains open.

The subsequent [manual rerun](phase6-benchmarks-rerun-2026-10-05.md) at
`9107ecb` passes all six complete measurements with unchanged acceptance limits
and added burst diagnostics. This earlier failure remains part of the evidence;
the passing rerun does not establish its cause.

## Verified Results

| Scenario | Wall Seconds | Aggregate p99 Milliseconds | Worst Operation p99 Milliseconds | Maximum Burst Milliseconds | Peak Sampled RSS Bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| HTTP | 25.584 | 8.923 | 12.631 | 410 | 156807168 |
| WebSocket | 25.042 | 8.913 | 12.957 | 407 | 159408128 |
| Content-Length | 52.326 | 9.624 | 13.124 | 412 | 164433920 |
| NDJSON | 30.493 | 8.989 | 14.290 | 405 | 158822400 |

Each transport completes 20,000 primary calls and 1,000 mutation verifications
under 1,000 active Metalink-admitted HTTP ranges. The complete reports pass the
shared validator, including per-operation latency, workload counts, renewed
barriers, memory budgets and stalled-consumer retention and cleanup. The table
uses the reports' integer millisecond burst values; validation also requires
the harness to enforce the exact duration limit.

The administrative scenario passes in 5.610 seconds with no active ranges.
All six operations complete; worst concurrent-query p99 is 0.929 ms, maximum
query burst is 400.829 ms, and maximum urgent acknowledgement is 6.803 ms.
Shutdown acknowledgement is 0.015 ms and drain is 17.960 ms. Sampled RSS
is 27,529,216 bytes. Total bulk-operation durations are separate from query and
acknowledgement limits.

## Mixed HTTP/BT Failure

The mixed process exits with code 1 after 31.554 seconds. Fixture startup,
1,000 active HTTP ranges, peer/payload barriers and stalled-consumer setup
succeed. Progress reaches 9,515 primary calls in 25 completed bursts before
the failure:

```text
active RPC benchmark failed: mixed-bt burst exceeded 500 ms: 530097 us
```

Stdout is empty and no complete mixed report exists. The artifact does not
identify the failing operation, exact sample or burst, verification duration,
or final latency distribution. A slow RPC, paired verification or scheduling
delay cannot be distinguished from these logs. This is a failed acceptance
measurement; the partial progress supplies neither a passing mixed latency
result nor completed cleanup evidence.

## Provenance And Integrity

The [machine-readable record](phase6-benchmarks-2026-10-05.json) preserves the
source manifest, complete five passing reports, all six run records, command
metadata, failed stderr and artifact hashes. Review verifies all 482 source-file
hashes against Git at the recorded commit, all five command-log hashes and
successful command exits, and all twelve scenario stdout/stderr hashes.
Archive integrity and member paths pass review. The five complete reports
pass `scripts/ci.py` validation again locally.

The host is native Linux 6.17 on Azure, x86_64, with four logical CPUs and
Rust 1.97.1 (`8bab26f4f`, LLVM 22.1.6). The optimized harness uses all features.
No compiler processes are observed during any scenario. The overall benchmark
command takes 707.981 seconds including compilation; this is not the workflow
duration or the measured RPC time.

| Object | SHA-256 |
| --- | --- |
| Supplied archive | `443d6199997ce3d6cae62add8294961f970b39416b5bfcc38079bfe5e77d477e` |
| Recorded benchmark binary | `255e2a77a9c3628a24450dd2682afdef1714a27ee1eb1fc00ba4afbc2b344a55` |
| Mixed stderr | `75ea8d01b9eaddc02be3301daf036cd13b93f00e78acc866dd9875b09fd70432` |

All six runs record the same binary hash as the manifest. The executable is
absent from the archive, so local review does not independently rehash it.
Original archives and extracted logs remain in the local evidence snapshot;
the portable record contains no workstation paths.

## Follow-Up And Acceptance Scope

The follow-up harness records bounded burst timing summaries on failure:
primary and verification counts, last and slowest timed steps, their operation
and sample positions, and time outside those steps. It preserves the 400 ms
launch cutoff, 500 ms/1,000-call burst limit, 50 ms p99 gate and full workload.
It changes diagnostic reporting and does not establish a fix for this failure.

Four focused regressions pass using the shared helper compiled directly with
Rust 1.97.1 and `-D warnings`. They cover the exact acceptance boundary,
primary-call and verification attribution, and an overrun outside timed steps.
The default-feature `cargo check --locked --offline -p ariax-engine --bench
rpc_active_profile --test rpc_burst_timing` passes in 49.850 seconds including
dependency checks. Formatting, documentation, publication-path and whitespace
checks pass. Native/all-feature reruns are still needed for the updated harness.
Full builds and measurements remain in
CI; WSL 1 results cannot satisfy native Linux performance acceptance.
Benchmarks remain manual-only. This review launches no workflow or push.

The five passing results remain valid for `a6f9b0d`; later candidate sources
need their own validation. The mixed gate, fresh live OpenSSH evidence and
separate backend/release-platform requirements remain open in the
[acceptance status](../docs/project/implementation-readiness.md#phase-6-local-implementation-and-open-acceptance).
