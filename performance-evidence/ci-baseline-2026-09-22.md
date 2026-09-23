# Remote And Native Linux Baseline

The remote and fail-fast CI prerequisite is complete at
`af5d1930a7021251a7886665695c111419a4770c`.
[CI run 35720518419](https://github.com/myfreeer/ariax/actions/runs/35720518419)
passed on September 22, 2026. The
[retained evidence](ci-baseline-2026-09-22.json) contains job results, original
artifact/log hashes, source hashes, host details and complete benchmark reports.
All command logs and all 377 source hashes were verified on September 23;
every report was revalidated with `scripts/ci.py`. Invocation paths are omitted
from the portable record; measurements and original artifact hashes are unchanged.

## Functional Validation

Preflight, Linux, macOS, Windows MSVC, Windows GNU, both MSRV 1.88 jobs,
and all four feature bundles pass. Native platform jobs run workspace builds,
default/all-feature tests and strict Clippy with Rust 1.97.1. Preflight includes
formatting, workflow syntax, generated contracts, pinned references, source
provenance, feature policies and publication checks. `CI Required` passes.
The release-tag job is intentionally inapplicable to this ordinary branch push.

## Native Linux Measurements

The optimized binary was built before measurement on a native Linux x86_64
runner with two logical CPUs. All five scenarios ran sequentially, without
compiler processes. Each transport completed 20,000 measured calls while its
1,000 Metalink-admitted ranges remained active, with renewed warmup barriers
and per-status range checks. Reports retain every operation's sample count and
latency, mutation evidence, memory accounting and engine-exit shutdown boundary.

| Transport | Measured Calls | Worst Operation p99 | Longest Burst | Peak RSS |
| --- | --- | --- | --- | --- |
| `http` | 20,000 | 14.516 ms | 407 ms | 141.04 MiB |
| `websocket` | 20,000 | 15.190 ms | 409 ms | 143.27 MiB |
| `ndjson` | 20,000 | 15.381 ms | 410 ms | 143.57 MiB |
| `content-length` | 20,000 | 15.231 ms | 411 ms | 143.78 MiB |

Every operation satisfies p99 ≤50 ms. Bursts stay within 1,000 calls and 500 ms,
with at least 250 ms cooldown; every scenario finishes within 90 seconds.
Measured resident, RPC and RSS usage stays within its recorded limit.
The separate 128-task administrative scenario passes all six bulk/import/
export/save/purge operations, concurrent query and urgent-command progress,
later-action precedence and clean shutdown.

## Gate Effect

This evidence closes the deferred P4-11 and Phase 5 native Linux control-plane
measurement gate and permits Phase 6 implementation. Earlier Windows evidence
remains applicable to its recorded commits. Kernel/backend-specific coverage,
Phase 6 acceptance, remaining release-platform requirements and release tagging
remain separate gates.
