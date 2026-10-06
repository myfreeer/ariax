# Phase 7 Local Follow-Up In Progress

[Documentation](../docs/README.md) · [Evidence](phase7-local-four-2026-10-06.json)

The local goal covers Windows mutation attribution, active-transfer stability,
experimental compatibility review, and candidate inventory/advisory preparation.
It is **in progress**. No release, push, CI dispatch, migration or replacement
of aria2 is approved by these results.

## Source-Mutation Stages

The optional `control-diagnostics` engine feature attaches a fixed six-slot
trace to an explicitly instrumented request. It records mailbox admission,
owner dispatch, source-plan preparation, commit start after drain, publication,
and reply delivery. Normal bundles do not enable the feature. No request
parameters, credentials or source URLs enter the trace. The benchmark's existing
125-call cap and all timing thresholds remain unchanged.

Six correlation tests pass on Linux and Windows, together with four Python
source-timing tests and the existing CI-helper tests. Three native Windows
engine tests cover trace ordering, invalid/incomplete traces, successful source
publication and rejected source calls. The initial unit-test compile exposed
one missed argument in an existing test-only call site; that failure is retained
and the corrected harness passes. Only the engine and benchmark were rebuilt
for the measured executable, using the existing dependency cache.

Twenty predeclared attempts pass in 121.86 seconds, with 200 paired calls.
Largest round trips are 17.197 ms for Content-Length and 16.711 ms for NDJSON.
Median preparation-to-commit intervals are 5.200/5.107 ms; median
commit-to-publication intervals are 8.083/7.903 ms respectively. Those intervals
include asynchronous completion polling and scheduling, not disk service time
alone. The retained 100.250 ms failure is still unexplained and still fails the
50 ms gate; passing new measurements cannot replace it.

## Active-Transfer Recovery Finding

A bounded 60-second fixture check exercises 16 active HTTP ranges, periodic
payload pulses, and pause/resume of the real active download. Resource samples
remain bounded in this short check. After terminating the active engine, the
first bootstrap successfully recovers 132 tasks. A subsequent RPC shutdown
returns `OK`, but the next bootstrap fails semantic journal replay with
`GenerationNotDrained` at sequence 90.

The retained active journal contains 16 begun generation-1 leases without abort
records after the process crash, followed by a staged snapshot and a generation-2
`GenerationStarted` for retry readmission. The replay guard correctly rejects
that sequence. This is an actionable recovery finding; no storage repair or
long-soak pass is claimed yet. The original fixture, executable identities,
decoded record inventory, command outputs and failed assessment are preserved.
The planned 30-minute run waits for a repaired, passing fixture check.

## Compatibility And Advisory Review

The compatibility inventory has 108 reviewed registry entries, including 54
of the pinned aria2 reference's 207 option handlers. These are option-handler
counts, not RPC-method coverage. An upstream option missing from this registry
is not automatically unsupported: direct CLI/RPC handling must also be reviewed.
The experimental scope and support/rejection review remain open.

A live OSV query covers all 357 registry packages in the current workspace lock
and retains request/response hashes. Matches require applicability review for
`hickory-resolver`, `rustls`, `russh`, `pageant`, `rsa`, and `rustls-pemfile`.
The rustls entries include duplicate identifiers for the same issue; the PEM
entry is an unmaintained-package advisory. Registry version matches alone do
not establish reachability, and this is not yet the exact release closure.

Published fixes `hickory-resolver` 0.26.2 and `rustls` 0.23.45 declare MSRVs
compatible with 1.88. `russh` 0.63.2 and `pageant` 0.2.3 declare Rust 1.89,
so blindly updating to those releases would violate the repository's MSRV.
Review enabled algorithms, client/server reachability and possible compatible
backports before selecting the SSH remediation. No dependency change has yet
been validated, and current retained packages remain drafts.

Raw evidence is under `/mnt/f/temp/ariax/phase7-local-four-20261006`.
Unique artifacts and failures remain retained. Native Linux timing/kernel
coverage, fully instrumented Rust TSan, other platforms, fresh/minimum OS and
physical power-loss acceptance remain separate gates.
