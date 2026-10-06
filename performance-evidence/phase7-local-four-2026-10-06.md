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
that sequence. The original fixture, executable identities,
decoded record inventory, command outputs and failed assessment are preserved.

The recovery repair now retains abandoned lease IDs for nonterminal tasks,
appends their aborts through the bounded owner queue, and flushes before scheduler
publication. The recovered projection advances only after the exact flush
acknowledgement, so its sequence and clean-shutdown state match fresh replay.
Terminal journals gain no records, and the original invalid journal is not
rewritten or accepted by a weakened replay guard.

Linux and native Windows each pass 19 semantic-journal tests, four native-startup
tests, and one option-scope regression. The new checks cover each abort prefix,
unknown/committed lease rejection, owner failure before the flush, early flush
rejection, and equality between published recovery state and journal replay.
An initial test compile with an incorrect scope enum name is retained separately.
Focused builds use copied caches on F:; the original E: caches remain available.
The first standard CLI build resolved its additional FTP/SFTP dependency graph;
subsequent corrected CLI and benchmark builds rebuilt only affected workspace
targets. No complete workspace or native-library build was run.

The corrected 60-second active fixture passes crash recovery, clean RPC shutdown
and a second recovery. Both bootstraps retain all 132 tasks, and the interrupted
task retains its GID with zero completed bytes: provisional payload was not
promoted to trusted progress. The 30-minute run also completes both recovery
checks, with 60 samples, six pause/resume cycles and 900 payload pulses. However,
accounted resident memory grows by about 1 MiB per pause/resume cycle. Passing
its coarse 32 MiB private-memory growth screen does not establish stability.

The original 120-second dense probe confirms the growth: private memory rises
from 11,419,648 to 115,916,800 bytes between its first and 90-second samples.
Its assessment fails because the runner collected four samples while requiring
six. That runner defect does not invalidate the raw growth observation, and
neither the failed assessment nor its fixture is discarded.

The ingress path dropped pooled buffers on cancelled reads, failed reply
channels and admission backpressure. Ordinary `BufferLease` drop quarantines
unreleased ownership, and pool shutdown permanently retains quarantined storage
and its resident permits. A protocol-only guard now explicitly releases known
`NetworkFill`/`Filled` ownership and holds the originating pool alive until the
release or storage handoff. Unexpected ownership still quarantines; no OS or
storage completion invariant is weakened. HTTP, FTP and SFTP use this guard.

Four lifecycle regressions pass on Linux and Windows: repeated cancellation and
closed replies reuse one allocation, task abort releases its pending buffer,
storage handoff retains ownership, and unexpected disk ownership quarantines.
Windows passes all 43 HTTP multi-range tests and three focused FTP/SFTP tests.
The first Windows driver stopped after requesting a nonexistent SFTP test filter;
a separate direct run uses the actual subsystem/transfer filters and passes.
Linux passes 42 of those 43 HTTP tests; a first-durable-piece timeout in the
remaining recovery test passes three unchanged reruns. The original timeout
remains unexplained and retained. The repaired 180-second dense probe passes 178 pause/resume cycles, 90 pulses,
and both recovery checks. It collects 18 samples at ten-second intervals and
additionally screens accounted resident growth at 2 MiB. First/last three-sample
medians are 13,005,001/13,006,025 accounted resident bytes, 12,300,288/15,110,144
private bytes, 422/418 handles, and 14/11 threads. This removes the observed
per-cycle pooled-buffer retention; the remaining private-memory drift and
longer-term stability are not declared resolved. The probe uses separately
retained executables and unchanged transfer gates. All owned fixture processes
exit, all 132 tasks recover, and interrupted payload remains untrusted.

## Compatibility And Advisory Review

The compatibility inventory has 108 reviewed registry entries, including 54
of the pinned aria2 reference's 207 option handlers. These are option-handler
counts, not RPC-method coverage. An upstream option missing from this registry
is not automatically unsupported: direct CLI/RPC handling must also be reviewed.
The per-download boundary now has an inventory-derived regression: all 152
unreviewed upstream names other than the explicit `pause` flag reject without
task metadata; boolean/string pause and a reviewed split option succeed, while
invalid pause/split values reject. The configuration document distinguishes
current executable support from target category ownership. This does not claim
full parity or that startup flags are unsupported in their separate CLI scope.
Six selected compatibility checks pass on each host, covering advertised RPC
dispatch, typed option bounds, rejection before metadata creation, and real-worker
slow-slot/retry-wait behavior. Full aria2 parity remains outside
this experimental subset; registry labels do not promise unimplemented behavior.

The initial live OSV query covers 357 registry packages. The updated lock has
356; its rescan has no matches for the repaired DNS/TLS dependencies or removed
PEM wrapper. All three Hickory crates are locked at 0.26.2, rustls at 0.23.45,
and rustls-webpki at 0.103.15. A first build exposed Resolver's insufficient
transitive lower bound; aligning Hickory's network/protocol crates resolves it.
The retained failed compile is not counted as validation. Rustls's maintained
`pki_types` PEM API replaces the unmaintained `rustls-pemfile` dependency.
Ten HTTP/TLS tests and seven resolver tests pass on both hosts, including
certificate selection and malformed PEM rejection. All updated crates declare
MSRVs at or below 1.88; that declaration is not a fresh full MSRV build.

Remaining registry matches require these distinctions:

| Dependency/Advisory | Local Applicability Review | Remaining Work |
| --- | --- | --- |
| russh client channel callbacks (`GHSA-47hw-gvq5-r2gm`) | Ariax overrides only host-key checking and KEX diagnostics, not the channel callbacks identified by the advisory. | Confirm transport-level effects and choose a compatible fix/backport; not a blanket clearance. |
| russh MAC `none` / hybrid KEX (`GHSA-p8qx-h547-fjw9`, `GHSA-w3jg-pjxf-73p4`) | Negotiation allowlists exclude MAC `none` and hybrid ML-KEM. Diagnostic `none` denotes AEAD and is not the negotiation allowlist. | Preserve these restrictions and review the security backports. |
| russh server advisories (`GHSA-35g8-35p8-c8fw`, `GHSA-g6xm-f9xp-qq35`, `GHSA-m65r-rprj-r5rg`) | Production uses the SSH client; server implementations are test fixtures. | Track the vulnerable dependency separately from production reachability. |
| Pageant (`GHSA-g4mp-vgx3-xrvm`) | Windows authentication connects to the OpenSSH named pipe; it does not invoke Pageant. | Keep the dependency match visible until upgrade/backport review closes it. |
| RSA Marvin (`RUSTSEC-2023-0071`) | SSH authentication uses signatures; the advisory concerns private-key decryption. No fixed version is published. | Finish private-decryption reachability review. |

Russh 0.63.2 and Pageant 0.2.3 require Rust 1.89. Upstream patches and archive
checksums are retained, but none is silently substituted for the current
MSRV-compatible pins.

OpenSSL's official vulnerability index lists 24 affected-version matches for
pinned 3.6.3, fixed in 3.6.4/3.6.5. These include relative-CRLDP certificate
memory amplification (`CVE-2026-35189`); the QUIC, DTLS, CMP, CMS, signing and
other entries need individual applicability review. The empty GitHub repository
advisory response is not clearance. An OpenSSL upgrade must also reconcile the
existing callback backport and pass its strict/native regressions. No native
source pin or retained installation is relabeled as repaired. The attempted
Boost/libtorrent security-page URLs returned 404; their review remains open.

The exact-source draft inventory records eight normal/build dependency graphs
(minimal/standard/full/compat on Linux and Windows-GNU), 314 distinct packages,
declared licenses/MSRVs, cached archive verification, native source/patch hashes,
and 416 implementation/packaging source hashes. All eight graphs pass the
protocol/provider policy checker. It is a source inventory, not rebuilt release
packages. The historical packages still contain their original dependencies.

Raw evidence is under `/mnt/f/temp/ariax/phase7-local-four-20261006`.
Unique artifacts and failures remain retained. Native Linux timing/kernel
coverage, fully instrumented Rust TSan, other platforms, fresh/minimum OS and
physical power-loss acceptance remain separate gates.
