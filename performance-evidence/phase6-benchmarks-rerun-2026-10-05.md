# Phase 6 Passing Native Linux Benchmark Rerun

Acceptance update: the [Phase 6 closure](phase6-acceptance-2026-10-05.md)
accepts the milestone using verified production/build source equivalence and
separate validation of benchmark changes. It supersedes the provisional open
acceptance and identical-commit requirement below. The original run outcomes,
failures, source identities and platform limits remain unchanged.

Review on October 5, 2026 verifies all six manual benchmark scenarios at
`9107ecb1b82b3e1c720de8bdc24d4a1e8657f774`. HTTP, WebSocket, Content-Length
stdio, NDJSON, administrative and mixed HTTP/BT reports pass the shared
validator. This supplies the first complete passing mixed measurement with
1,000 HTTP ranges and 1,000 BT peers for the current implementation.

The [earlier mixed failure](phase6-benchmarks-2026-10-05.md) at `a6f9b0d`
remains retained. The only subsequent code changes add benchmark diagnostics
and their tests; acceptance limits and production behavior are unchanged.
One passing rerun does not identify the cause of the earlier 530.097 ms burst
or establish repeated-run stability.

## Verified Measurements

| Scenario | Wall Seconds | Aggregate p99 Milliseconds | Worst Operation p99 Milliseconds | Maximum Burst Milliseconds | Peak Sampled RSS Bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| HTTP | 18.856 | 8.602 | 14.823 | 407 | 156557312 |
| WebSocket | 18.857 | 8.607 | 16.969 | 409 | 157450240 |
| Content-Length | 38.874 | 9.251 | 28.127 | 444 | 163778560 |
| NDJSON | 23.765 | 8.822 | 16.363 | 410 | 157818880 |
| Mixed HTTP/BT | 47.331 | 8.000 | 31.311 | 486 | 194818048 |

Each transport completes 20,000 primary calls and 1,000 mutation verifications
under 1,000 active Metalink-admitted HTTP ranges. Mixed HTTP/BT completes
20,000 primary calls and 2,000 mutation verifications. Aggregate and every
operation's p99 remain below 50 ms. The table preserves the reports' integer
millisecond burst values; the harness also checks the exact 500 ms bound.
The 400 ms launch cutoff and 1,000-call burst cap remain unchanged.

The administrative scenario completes all six operations in 5.794 seconds
with no active ranges. Worst concurrent-query p99 is 0.681 ms, maximum query
burst is 383.341 ms, and maximum urgent acknowledgement is 6.642 ms. Shutdown
acknowledgement is 0.025 ms and drain is 17.475 ms. Sampled RSS is 27,312,128
bytes. Total bulk-operation durations remain separate from query and urgent
acknowledgement limits.

## Mixed Workload And Resource Evidence

All 1,000 BT peers remain represented in projections and status queries.
The run completes 1,000 acknowledged live BT option changes, 1,000 auxiliary
HTTP mutations and 44 measured bursts, with 44 renewed BT payload barriers.
BT download progress is 737,280,000 bytes, matching 45 pulses of 1,000
16 KiB blocks including the initial barrier. The busiest burst contains
592 calls. Worst operation p99 is 31.311 ms for `unpause`.

Peak sampled engine RSS is 194,818,048 bytes against the 1 GiB limit;
accounted resident memory peaks at 376,344,996 bytes against 896 MiB, and
RPC reservations peak at 21,636,042 bytes against 64 MiB. Fixture processes
remain excluded from engine memory. Both stalled consumers retain at least
256 KiB above their baselines and release their owners after disconnect.
Cleanup is verified. Shutdown acknowledgement is 0.103 ms, engine drain is
697.049 ms and fixture cleanup is 21.117 ms.

Some individual calls exceed 50 ms: the largest mixed sample is 201.133 ms
for `pause`. The ordinary-call contract gates p99, not each sample's maximum.
The 486 ms maximum burst passes the separate 500 ms gate. These tails remain
visible in the complete reports; they are not removed by filtering.

## Integrity And Source Identity

The [machine-readable record](phase6-benchmarks-rerun-2026-10-05.json) retains
the complete manifest and reports, command/run records, all artifact-file
hashes, comparison with the functional baseline and review limits. Verification
matches all 488 tracked source-file hashes and their exact inventory to Git at
`9107ecb`, all five successful command logs, and all twelve scenario stdout/stderr
logs. Each stdout contains exactly its run record's one complete report.
All six reports pass `scripts/ci.py` validation again locally. Additional review
checks confirm stalled-consumer retention/cleanup and unchanged burst limits.
Archive integrity, duplicate-entry and member-path checks pass.

The runner is native Linux 6.17 on Azure, x86_64, with four logical CPUs.
Rust 1.97.1 (`8bab26f4f`, LLVM 22.1.6) builds the optimized all-feature
benchmark. Cached native dependency verification succeeds. No compiler
processes are observed during any measured scenario. The overall benchmark
command takes 242.698 seconds including compilation; this is not workflow
duration or measured RPC time.

| Object | SHA-256 |
| --- | --- |
| Supplied archive | `cee8a63212b45c87e594f4588b015ce164fe7bdee87b1c324227f6fe55300f68` |
| Recorded benchmark binary | `36c75f7145a3e7f12bdd78f675e53f98b5fa6456367c843a67c4f6a68e885a1f` |

All six runs and the manifest record the same binary hash. The executable is
absent from the archive and cannot be rehashed locally. The original archive
and extracted files are preserved separately from the earlier failed run.

## Acceptance Scope And Next Evidence

The six-scenario native Linux measurement gate passes at `9107ecb`. The
[full functional matrix](phase6-full-ci-2026-10-05.md) remains evidenced at
`a6f9b0d`; automatic CI was explicitly skipped for `9107ecb`. Git comparison
confirms only benchmark diagnostics, their tests and documentation/evidence
changed between those sources. This artifact establishes an all-feature
optimized benchmark build and measurements, not another full functional matrix
or execution of the new regression test target.

Phase-6 acceptance remains open: final native test, sanitizer, fuzz and
benchmark evidence must converge on the same candidate source under `P6-06`.
Fresh live OpenSSH interoperability is the next focused fixture gap; separate
kernel/backend, custom BT storage and release-platform requirements remain
tracked. Repeated mixed measurements can investigate burst stability without
discarding the earlier failure. Benchmarks remain manual-only; this review
launches no workflow or push and changes no runtime code.

The subsequent [local OpenSSH run](phase6-openssh-local-2026-10-05.md) at
`624af25` supplies the fresh live interoperability evidence for both clients.
Only documentation/evidence changed between this benchmark source and that run.
