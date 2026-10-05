# Phase 7 Bounded Local Campaign

This campaign continues `local/phase7-hardening` from
`3968f4c649fa308b2fdc7c421222cd2c2b1377ed` on the existing WSL1 and native
Windows-GNU hosts. The retained [machine-readable record](phase7-local-campaign-2026-10-05.json)
binds source and executable identities, commands, counts, raw reports and
cleanup. It adds local evidence; Phase 7 and release acceptance remain open.

## Budget And Source Identity

The original start was **2026-10-05 14:19:45 UTC**, with a work deadline of
19:49:45 and a hard deadline of 20:19:45. The final thirty minutes remained
reserved for evidence and cleanup. Resuming the interrupted session preserved
these deadlines and completed results. CI and pushes stayed on hold.
Workloads and fixture cleanup finished at **18:15:21 UTC**, after 3 hours,
55 minutes and 36 seconds. Final evidence checks and the local checkpoint
followed within the original budget.

New output uses the authorized F: temporary root because E: lacks headroom.
The campaign verifies 52 retained executable hashes, eleven tool identities,
four native installations and eleven instrumented fuzz binaries before reuse.
The source review compares 388 inputs with the retained build baseline.
Manifest differences are parsed license metadata; the changed benchmark and
timing tests are excluded from that reuse. The Windows benchmark and maintained
direct TSan driver receive focused builds with matching inputs. Native
installations and source caches are reused in place, with two compiler workers
and incremental compilation disabled.
Preflight matching hashes describe the baseline before this evidence and
documentation checkpoint; updated guidance is not asserted to retain those
earlier document hashes.

## Transport Diagnostics

All **410 scheduled attempts** are retained: 100 each for HTTP, WebSocket,
Content-Length and NDJSON, plus ten administrative runs. Transport attempts
use the existing small diagnostic geometry and unchanged timing/resource
limits. They do not establish native Linux mixed-BT acceptance.

**409 attempts pass and one fails its timing gate.** In round 12,
Content-Length `changeUri` takes **100,250 microseconds**, above its 50,000
microsecond limit. That report's aggregate p99 is 13,211 microseconds and its
longest burst is 440 ms. Complete mutation cycles, stalled-consumer credit
cleanup and shutdown checks are retained. No passing rerun replaces the failure.

The first runner stops after nine passing attempts because it incorrectly
classifies unrelated compiler processes as fixture children. The continuation
imports those nine records and resumes at attempt ten. Both runner results and
the correction remain available. The completed continuation is still marked
failed because of the timing result.

There are 1,230 host observations. Collection takes a median 3.93 ms and at most
33.01 ms. Compiler activity appears in 56 attempts; none is observed in the
failed attempt. Its sampled CPU intervals are approximately 30–33%, with ample
available memory. Five-second sampling cannot attribute a 100 ms delay, so
these observations do not establish host overload as the cause.

The largest sampled process RSS across transport reports is 27,004,928 bytes.
First-ten and last-ten median RSS remain close, but each attempt starts fresh;
that comparison cannot establish absence of a persistent-process leak. Process
I/O counters include pipes, network and devices, and are not physical disk-write
measurements.

## Persistent RPC Process

A separate native Windows process completes **18,000 requests in 1,800.12
seconds**, including **2,571 expected unknown-method rejections**. The other
queries check version, methods and empty-session status. All responses are
retained and independently checked for their expected method sequence and IDs.
EOF shutdown, database reopening and final process-exit checks pass.

The run records 61 resource observations. It triggers neither the 1 GiB RSS
limit nor the declared consecutive-growth screen. No compiler is observed in
those samples. The largest sampled working set is 14,323,712 bytes.

| Metric | First Six Samples, Median | Last Six Samples, Median | Maximum |
| --- | ---: | ---: | ---: |
| Private bytes | 7,786,496 | 7,761,920 | 7,823,360 |
| Handles | 114 | 112 | 116 |
| Threads | 16 | 14 | 17 |

This is a persistent empty-session control-plane check. It creates no download
payloads and does not establish multi-hour active-transfer stability. Per-thread
scheduling, disk queue latency, physical disk-write accounting and CPU
frequency/throttling are not collected.

## Functional And Recovery Checks

The groups complete **5,475 scheduled test/probe invocations**.
One standalone crash-child entrypoint returns without configured work, leaving
**5,474 behavioral invocations**. Its execution remains in the raw record;
configured parent crash tests provide the actual crash coverage.

| Group | Executions | Scope |
| --- | ---: | --- |
| Focused WSL / Windows | 101 / 118 | Admission, bounded RPC, policy, parser and filesystem success/rejection paths |
| Repeated WSL / Windows | 2,000 / 2,400 | 200 repetitions per selected lifecycle, credit, cancellation, shutdown and recovery case |
| BT WSL / native-ASan | 45 / 45 | Transfer, mapping, bounded rejection and checkpoint cases |
| BT Windows adapter / engine | 150 / 280 | Private storage, v1/v2/hybrid and magnet transfer, selection, restart and shutdown |
| Windows protocol integration | 66 scheduled; 65 behavioral | HTTP, Metalink, FTP/FTPS, SFTP, verification and recovery |
| Native ordinary / ASan-UBSan / TSan / Windows probes | 30 / 30 / 30 / 40 | Bounded output, callbacks, destination policy and available private storage |
| Additional WSL / Windows portable regressions | 70 / 70 | Configuration parsing, buffering, CPU/handle/rate bounds, profiles, statistics and scheduler snapshots |

Each group validates exact selected counts and fixture/process cleanup.
Windows covers ACL, reparse-point, private-file, hot-backup and mixed-import
crash cases unavailable on the allowed WSL DrvFs volumes. Those Windows results
do not close the strict Unix-permission or kernel/backend gates.

All **137 Python helper tests** pass, along with workspace formatting, pinned
aria2/generated contracts, protocol-vendor and feature-closure checks, SQLite
feature checks and the package catalog. Focused Clippy evidence is reused only
for its reviewed unchanged compiled inputs; this campaign does not run or claim
a new full Clippy/platform matrix.

## Fuzzing And Instrumentation

All eleven retained Rust-ASan targets complete three rounds of 30 batches:
**506,880 accepted executions**, comprising **440,792 mutations** and 66,088
initialization executions. All 7,920 processes satisfy the 500 ms accepted
process bound; none produces a sanitizer or crash report. Each target retains
the 8 KiB input cap, 512 MiB RSS cap, per-process timeout and cooldown. Later
rounds begin from the previous corpus, and initialization is counted separately.

The retained targets cover BT metadata, Metalink, verification manifests, HTTP
response validation, request headers, retry specifications, discard budgets,
journal replay, RPC JSON, session documents and URL rules. Their original seeds,
commands, deterministic seed values and corpus inventories remain traceable.

The maintained direct TSan driver additionally passes **ten native case
executions**. Native ASan/UBSan and TSan instrument C/C++ libraries and the bridge;
ordinary Rust harnesses linking them are not fully instrumented Rust coverage.
The historical native-only Rust harness failures remain retained. Fully
instrumented Rust standard-library and harness coverage is still open.

## Full And Compat Artifact Inspection

Focused Linux `release-cli` builds of `full` and `compat` succeed, along with
CLI startup, invalid-option rejection and import inspection. Both binaries
retain **20 absolute native-dependency path matches**. The native installation
and cached Boost sources appear in those paths. Neither binary is approved for
release or inherits the earlier minimal/standard reproducibility result.

Both Linux binaries import `libstdc++.so.6` in addition to their other runtime
libraries, require glibc **2.38** and GLIBCXX **3.4.30**, and use
`/lib64/ld-linux-x86-64.so.2`. The minimal/standard runtime inventory cannot be
applied to them. Native path remapping and the final supported runtime baseline
remain release work; this campaign does not provision replacement native builds.

Both Windows-GNU bundles also build and pass startup, rejection and import
inspection. Each retains **37 absolute native-dependency path matches** and
directly imports `libstdc++-6.dll`. The inspected installed MinGW dependency
closure adds `libgcc_s_seh-1.dll` and `libwinpthread-1.dll`; DLL hashes and package
provenance are recorded. Existing GCC/runtime-exception and winpthread notices
are present, but a final redistributable archive has not been assembled or
validated. All four full/compat layouts remain planned.

Earlier minimal/standard independent reproducibility, package integrity and
reduced-environment operation evidence is reused. No duplicate rebuild of those
four binaries is needed for this campaign.

## Evidence And Cleanup

The first audit verifies **27,330 log hashes**, 13,665 command records, all
scheduled diagnostic reports and the retained fuzz corpus bytes. It checks
exact commands/counts and keeps the failed diagnostic result. Missing reports,
zero-work helpers, ignored tests and exhausted budgets are not promoted to passes.
The follow-up verifies another **360 logs and 180 commands**, the new artifact
identities, all three runtime DLLs and reconstruction of every removed corpus
member. The persistent run's complete 18,000-response transcript is verified
after process exit.

After that audit, cleanup removes **57,287 duplicate corpus files** totaling
**3,389,929 logical bytes**, plus **7,920 empty artifact directories**. Every
removed corpus member is byte-verified against its retained final-round copy.
Earlier generation inventories and the reconstruction mapping remain available.
All unique/final corpus inputs, logs, failures, native installations, executable
artifacts and useful compilation caches are retained.
Cleanup also removes **490 zero-byte compiler temporary files** from owned
directories after their builds finish. These are separate from reusable target
caches, which remain available.
Finally, the successful RPC fixture's two files, totaling **114,688 bytes**,
are inventoried and removed after database reopening. Across these controlled
deletions, **57,779 files and 3,504,617 logical bytes** are removed. Final checks
find no campaign processes and empty owned temporary directories. These counts
do not estimate physical writes or the payloads already removed by test fixtures.

## Remaining Acceptance

The retained native Linux mixed-BT timing failure still needs attribution and
repeated measurements on a suitable native Linux host. WSL1 continues to load
its existing `libnanosleep` system preload; no timing, `io_uring` or strict Unix
private-permission acceptance is inferred from it.

Multi-hour active-transfer stability, fully instrumented Rust race coverage,
full/compat packaging and reproducibility, fresh/minimum-OS validation,
macOS/MSVC, hardware power loss and the final release matrix remain open.
Existing native platform evidence remains usable only through its matching
source review. No CI-gating redesign, push, tag or release is part of this work.

Remaining CI coverage includes native Linux mixed-BT timing, `io_uring` and
strict Unix permissions; macOS/MSVC platform-specific code and artifacts;
supported fully instrumented Rust race checks; and the final candidate's
MSRV/feature/release matrix. The portable checks recorded here can be reused
only where their source and configuration match that future candidate.
