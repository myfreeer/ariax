# Phase 7 Local Source-Mutation Timing

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-source-timing-2026-10-06.json)

Twenty planned native Windows attempts pass with 200 correlated `changeUri`
measurements. Their time is predominantly inside the production backend future,
which narrows the next investigation beyond transport framing. The retained
100.250 ms failure remains unexplained and failed against the unchanged 50 ms
limit. No production timing repair or release acceptance is claimed.

## Diagnostic Change

This slice starts at `54bd6af` on `local/phase7-hardening`. Setting
`ARIAX_BENCH_SOURCE_TIMING=1` enables a benchmark-only wrapper around source
mutations. It stores at most 125 backend calls in preallocated storage and pairs
them with the single auxiliary client's measured calls. Exact call counts,
ordinals, sample positions and successful backend completions are required.
Missing, extra, unfinished, duplicated or inconsistent records reject diagnostic
validation. The uninstrumented default emits no additional report field.

Each paired record retains monotonic nanosecond durations and Unix-time anchors.
Backend time begins immediately before constructing the production backend
future and ends when that future returns. It includes admission, owner queueing,
persistence, publication and scheduling within that interval. The remaining
round-trip time includes transport, dispatcher work, response encoding/decoding
and scheduling outside that interval. These are measured intervals, not causal
attributions to a single subsystem. Wall-clock adjustments limit anchor
correlation; observation adds overhead.

The wrapper, storage and fixture endpoint are confined to benchmark sources.
No production RPC interface, runtime code, native patch, dependency lockfile or
acceptance threshold changed. Report validation accepts historical reports that
lack the optional field and verifies any new field that is present. It preserves
the distinction between a structurally valid diagnostic and a timing pass.

## Local Verification

Pinned Rust 1.97.1 builds the affected Windows benchmark using its existing
optimized dependency cache. Cargo reports only `rpc_active_profile` rebuilt.
The old benchmark executable is copied and hash-verified before Cargo replaces
its cached output; both original and new binary identities are retained.
No full workspace or native dependency rebuild is performed.

Five new Rust tests pass on WSL Linux and native Windows, covering exact
correlation, nanosecond accounting, the storage cap, incomplete/duplicate
completion, count mismatch, ordering, failure status and invalid durations.
The same test target and imported diagnostic module pass focused Clippy with
warnings denied on both platforms. The Rust-emitted fixture also passes Python
report validation on both platforms.

Three new Python tests pass on both hosts. The existing CI-helper suite passes
37 tests on Linux; Windows passes 35 with two Linux-specific tests skipped.
Those skips are explicit and are not counted as Windows passes. The first
Linux direct-test compile selected release-profile `panic=abort` dependencies,
which the unwind test harness rejected. That failed compile is retained;
selecting the existing unwind dependency cache resolves it without rebuilding
dependencies or weakening tests.

## Fixed Windows Sequence

The plan declares ten rounds, each containing one Content-Length and one
NDJSON attempt, before execution. All 20 complete in 126.48 seconds. Each uses
the existing small diagnostic workload: 16 active HTTP ranges, 1,600 primary
calls, 80 real auxiliary mutations and 80 verification calls. Across the
sequence, 32,000 primary calls include 200 measured source mutations. These
Windows diagnostics do not establish native Linux mixed-BT acceptance.

| Transport | Attempts Passed | Source Calls | Median Round Trip | Largest Round Trip | Backend Time For Largest Call | Outside Backend For Largest Call |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Content-Length | 10 | 100 | 14.117 ms | 18.443 ms | 18.226 ms | 0.217 ms |
| NDJSON | 10 | 100 | 13.960 ms | 17.388 ms | 17.269 ms | 0.120 ms |

The largest outside-backend interval across all source calls is 0.649 ms for
Content-Length and 0.340 ms for NDJSON. Complete reports retain every paired
sample, per-operation latencies, burst diagnostics, resource limits, stalled
consumer credit cleanup and engine shutdown. The 50 ms p99 gate, 400 ms launch
cutoff and 500 ms burst cap are unchanged. No failed attempt is replaced by a
retry. One separate default-mode check also passes and confirms that disabling
the option emits no `sourceMutationTiming` field.

A separate Windows observer samples host CPU/memory and matching processes
nominally every 250 ms, with collection cost and a 512-sample per-attempt cap.
Unrelated compiler activity is observed in all 20 attempts and remains recorded.
Sampling neither filters timing results nor attributes them to host load.
Per-thread scheduling and individual persistence-operation latency remain
unmeasured. No compilation from this slice overlaps its measured sequence.
The Phase 6 pruning below completes before measurements start.

The historical Content-Length failure's original report hash still verifies,
and current validation still classifies it as failed. Passing source-call
intervals in this run cannot retrospectively explain that earlier delay.
Further attribution needs evidence inside the backend path before changing
queueing, persistence or scheduling behavior.

## Phase 6 Pruning And Cleanup

Phase 6 caches and native installations still support current local checks,
and its accepted baseline still needs reproducible evidence. The user-authorized
prune therefore removes only extracted files with byte-for-byte equivalents in
retained ZIP archives: **3,477 files, 31,235,209 logical bytes and 89 empty
directories**. All 29 inspected archives remain unchanged. Unique logs,
failures, source trees, current build caches and toolchains remain available.

The retained `phase6-prune-plan.json` maps each removed path to its archive
member and SHA-256; its hash and the archive hashes are recorded in the portable
evidence. This supports reconstructing earlier extracted paths without treating
the archive's presence alone as proof of equivalent contents.

After process-exit and fixture-cleanup checks, four empty temporary directories
from this slice are removed. Test and benchmark binaries, original failures,
raw reports, host samples, compiler identities and command logs remain retained.
The final integrity pass verifies 72 direct command-log hashes.

Native Linux timing, `io_uring` and strict Unix filesystem coverage, fully
instrumented Rust standard-library/harness TSan, macOS/MSVC, fresh/minimum OS,
the final candidate matrix, hardware power loss and compatibility acceptance
remain separate gates. CI, push, tagging and release remain on hold.
