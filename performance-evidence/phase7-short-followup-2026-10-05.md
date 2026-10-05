# Phase 7 Short Local Follow-Up

[Documentation](../docs/README.md) · [Machine-readable evidence](phase7-short-followup-2026-10-05.json)

This follow-up starts from `4b5b117` on `local/phase7-hardening`. It completes
the short local analysis and reusable native TSan driver work following the
[local campaign](phase7-local-hardening-2026-10-05.md). No new benchmark,
dependency build, soak, push or CI dispatch is part of this follow-up.

## Mixed-Burst Comparison

Both retained archives verify against their recorded SHA-256 identities, as do
the mixed run logs and all 970 source hashes against their respective Git
commits. The sources differ only in benchmark diagnostics, their regressions,
and documentation/evidence; no production or dependency-build change explains
the different outcomes.

| Observation | Failed Run At `a6f9b0d` | Passing Run At `9107ecb` |
| --- | --- | --- |
| Maximum burst | 530.097 ms reported; 30.097 ms above the cap | 486 ms, truncated to integer milliseconds; headroom is greater than 13 ms and at most 14 ms |
| Completion | Last progress: 9,515 primary calls in 25 completed bursts; no complete report | 20,000 primary calls, 2,000 verifications, 44 bursts |
| Operation attribution | Missing: failure predates detailed timing diagnostics | Aggregate operation distributions, without per-burst sample positions |
| Compiler overlap | None observed | None observed |

Progress logs occur every five completed bursts. The failed burst is therefore
between 26 and 30 inclusive, not necessarily burst 26. The synthetic regression
using sample 9517 and burst 26 is a diagnostic-format test, not an observed
trace of this failure.

The 400 ms launch cutoff permits an already-admitted primary call and paired
verification to finish before checking the separate 500 ms cap. The failed
interval extends 130.097 ms beyond that cutoff. This does not prove one call
took that long: request preparation, response checks and scheduling delays can
also contribute. The initial and renewed barriers, post-burst barriers and
250 ms cooldown are outside the measured burst interval.

The passing run identifies useful investigation candidates:

| Mutation | Maximum Primary Round Trip (ms) | p99 (ms) |
| --- | ---: | ---: |
| `pause` | 201.133 | 10.546 |
| `addUri` | 201.000 | 14.026 |
| `changeUri` | 159.952 | 23.175 |
| `remove` | 137.869 | 12.747 |
| `bt.changeOption` | 124.371 | 11.376 |
| `unpause` | 109.072 | 31.311 |
| `removeDownloadResult` | 77.578 | 3.427 |

These are the only operation groups with maxima above 50 ms. Given their sample
counts and the actual per-operation p99 rank, between seven and sixteen primary
calls exceed 50 ms in the passing run. This is compatible with passing p99
gates and leaves mutation acknowledgements and paired verification as useful
places to investigate. It does not identify the operation in the earlier
failure. Peer/status query maxima are much smaller in this passing run;
response size alone is not evidence of the burst cause.

Across all 44 passing bursts, primary round trips consume 17,094,668 us of
17,682,260 us measured burst time. The remaining 587,592 us combines verification
and untimed work; it cannot be labeled scheduler, disk or lock delay. Likewise,
the passing run's 3.045 ms maximum owner turn, 0.189 ms maximum lock wait and
below-limit sampled memory are not measurements of the failed interval.

No production fix or threshold change is justified by these records. The next
native measurement already has failure diagnostics for the last/slowest step,
operation, primary/verification phase, sample position, timed total and other
time. A future failure can distinguish those categories; a timed RPC still
combines client, transport, server and scheduling time. Further attribution
must follow the observed category. The prior failure remains unexplained, and
two separate campaigns do not establish a failure rate or repeated stability.

## Maintained Native TSan Driver

[`scripts/bt_tsan.py`](../scripts/bt_tsan.py) prepares the reviewed direct driver
from the unchanged native integration-test source. It removes only the feature
gate and two test attributes and calls the original bodies on the main thread.
It verifies native provenance, generated source/fixtures, dependency identities,
source inventories and binary hashes before reusing an executable. Missing,
changed, added or removed inputs reject reuse. New builds copy the repository
lock and reject a changed dependency resolution before compilation.

The retained TSan binary executes both cases successfully through the maintained
tooling. A subsequent run from its new self-contained record also passes both
cases. Each invocation uses strict TSan, has its exact case marker verified,
and leaves no fixture directory or owned process group. No Cargo invocation or
rebuild is needed for these four native executions.

The first import attempt rejected the partial second-pass source snapshot
because it omits `.cargo/config.toml`. The first-pass snapshot supplies that
unchanged configuration, while the second snapshot records the later native
helper identity. Both are checked in chronological order. The failed import is
retained with zero test executions; subsequent maintained records embed their
complete relevant source inventory.

Focused helper regressions cover successful reuse, changed/removed inputs,
incomplete provenance, dependency drift, failed builds, unexpected case
inventories, runtime reports, missing markers and fixture cleanup failures.
All 91 Python helper tests pass, including eleven direct-driver regressions.
The fresh-build orchestration is checked with fake command results; generated
driver bytes match the already-built native executable's retained provenance.
Usage and short timeouts are documented in the
[local workflow](../docs/development/local-hardening.md#maintained-direct-tsan-driver).

The original nine native-only TSan `libtest` reports remain retained. This
driver covers native C/C++ and FFI execution, not Rust synchronization or its
standard library. Full Rust TSan and the remaining Phase 7 platform/release
gates stay open.

## Final Verification

Documentation, publication-path and whitespace checks pass. The portable JSON
binds the changed sources, raw attempt logs, artifact comparison and cleanup
audit. All temporary files remain in the E: follow-up directory; existing
native binaries and archives are reused without modifying the global toolchain.
