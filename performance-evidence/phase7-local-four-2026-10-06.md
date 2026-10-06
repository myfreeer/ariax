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

A subsequent predeclared sequence completes 100 alternating attempts in 615.64
seconds using the retained six-stage diagnostic binary, after the long active
soak exits. All 1,000 `changeUri` calls pass: maxima are 20.847 ms for
Content-Length and 23.105 ms for NDJSON. Median preparation-to-commit intervals
are 5.303/5.347 ms, and commit-to-publication intervals are 8.320/8.260 ms.
These intervals still include scheduling and asynchronous completion polling.
The original 100.250 ms source-mutation failure remains unattributed.

The overall sequence **fails**: Content-Length attempt 81 records a 91.107 ms
`addUri` call against the unchanged 50 ms gate. Its worst completed burst is
405.853 ms, within the separate 500 ms burst bound. Maximum owner lock wait and
active-turn duration are 16/586 microseconds; neither measures time between
turns or persistence waits. The source-only stage trace cannot attribute this
new admission delay. The complete audit corrects the interim progress report,
which missed this failure in abbreviated output. All attempts, including the
failure, remain retained in `measurements-extended`, with summary in
`timing-extended-summary.json`.

Two hundred command-log hashes and 100 host-sample hashes verify; correlation,
report/exit agreement and cleanup checks pass. No build is started by this
slice. Unrelated compiler/build activity appears in 43 attempts after the
initial quiet portion; that observation does not establish causation. Passing
source calls do not erase either failed mutation measurement or establish
release acceptance.

The new admission diagnostic uses 13 fixed request-local offsets: mailbox
admission, dispatch, preparation queue/start/finish/observation, finalization
queue/start/finish/observation, scheduler start, publication and reply delivery.
It is opt-in under `control-diagnostics` and records no request parameters.
Its independent 125-call benchmark stream uses `addUri` sample positions rather
than the source stream's positions. A shared correlation checker preserves the
distinct six- and thirteen-stage schemas. Incomplete, misordered and rejected
admissions do not become complete successful samples. Worker elapsed intervals
still include scheduling, so they are not pure CPU or filesystem service time.

Three admission-trace engine tests and three existing source-trace tests pass
on Linux and native Windows. Seven correlation tests pass on each host, along
with five Python timing checks and 37 CI-helper tests. The ordinary Linux
engine build without diagnostics and the Linux benchmark check pass. The native
benchmark is rebuilt into `admission-diagnostics`; older binaries remain retained.
Both historical failures still reject under the updated report validator. The first measured attempt stops on a correlation error: the new checker and
its unit fixture both used sample 119, while the actual eight-phase workload
places `addUri` at sample 159. The end-to-end check exposes the mistake. The
failed executable, source diff, command logs and leftover fixture remain
retained; its final host snapshot shows no live fixture processes. The mapping
is corrected; focused rebuilds and seven correlation tests pass on both hosts.
The corrected `admission-diagnostics-v2` run completes all 100 predeclared
attempts in 632.72 seconds, with valid end-to-end correlations and no failed
gates. All 1,000 source calls and 1,000 admissions pass. Two hundred command
logs and 100 host reports verify, and fixture cleanup completes. Other matching
build processes appear in 85 attempts; no build is started by this slice.
Maximum source latency is 25.892 ms, including a 16.949 ms preparation-to-commit
interval. Maximum admission latency is 31.418 ms, including 20.961 ms between
finalization observation and scheduler start. The latter interval includes
journal installation, asynchronous completion polling and owner progress; it
does not isolate storage service time. Median admission preparation-worker time
is 3.825 ms, finalization-worker time 0.015 ms, installation/owner progress
2.865 ms, and scheduler-publication time 3.003 ms. These are elapsed intervals,
not exclusive CPU measurements. The new trace localizes the longest passing
admission but does not attribute the retained 91.107 ms failure. Neither
historical latency failure is cleared, and no production timing fix is claimed.

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

The retained ingress-repair binaries also pass a predeclared 30-minute dense
run: 1,798 pause/resume cycles, 900 payload pulses and 180 resource samples.
First/last three-sample accounted-memory medians are identical at 13,005,529
bytes. Handles decrease from 421 to 419 and threads from 13 to 11. Private-memory
medians rise from 13,164,544 to 25,423,872 bytes, within the unchanged 32 MiB
growth screen but still an unexplained drift. Read-only `VirtualQueryEx`
snapshots show increasing committed private regions and unchanged mapped/image
commitment; they do not identify the allocator or establish a leak. Each query
took less than 6 ms and inspected region metadata, not process contents.

After the full run, forced termination, recovery of all 132 tasks, clean RPC
shutdown and a second bootstrap pass. The interrupted task retains its GID and
zero trusted completed bytes; all owned processes exit. Raw results and region
snapshots remain in `recovery/active-churn-fixed-1800`. These exact binaries
predate the latest dependency updates and are evidence for the ingress repair,
not a rebuilt candidate or release acceptance. The longer result strengthens
the buffer-lifecycle finding without clearing remaining private-memory drift.

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

The [advisory disposition inventory](phase7-advisory-dispositions-2026-10-06.json)
maps all eight remaining OSV findings to current package versions and hashed
source/test evidence. It distinguishes the client backport, excluded negotiated
configurations, unused server/Pageant paths, and unresolved RSA assurance. It
preserves all six russh findings after vendoring reduces the registry package
count to 355. These are scoped review dispositions, not blanket dependency
clearance or advisory ignores.

Remaining registry matches require these distinctions:

| Dependency/Advisory | Local Applicability Review | Remaining Work |
| --- | --- | --- |
| russh client channel callbacks (`GHSA-47hw-gvq5-r2gm`) | Unknown channel-open failures also enqueue unbounded handle replies. A bounded wire probe reproduced 256 unknown callbacks. The upstream client channel-state checks are now backported to the pinned vendor. | Linux and native Windows pass the unknown-channel/pending-rejection regression and existing 17-scenario SFTP fixture. Other russh advisories remain separately tracked. |
| russh MAC `none` / hybrid KEX (`GHSA-p8qx-h547-fjw9`, `GHSA-w3jg-pjxf-73p4`) | Negotiation allowlists exclude MAC `none` and hybrid ML-KEM. Real offers restricted to either combination reject before authentication/read on Linux and Windows. Diagnostic `none` denotes AEAD and is not the negotiation allowlist. | The upstream crate remains affected; these negotiated configurations are excluded in Ariax. |
| russh server advisories (`GHSA-35g8-35p8-c8fw`, `GHSA-g6xm-f9xp-qq35`, `GHSA-m65r-rprj-r5rg`) | Production uses the SSH client; server implementations are test fixtures. | Track the vulnerable dependency separately from production reachability. |
| Pageant (`GHSA-g4mp-vgx3-xrvm`) | Windows authentication connects to the OpenSSH named pipe; it does not invoke Pageant. | Keep the dependency match visible until upgrade/backport review closes it. |
| RSA Marvin (`RUSTSEC-2023-0071`) | The SSH signing path reaches `rsa_decrypt_and_check` through PKCS#1 signing without a blinding RNG. This shared primitive does not establish a Marvin decryption oracle; no RSA decryption API was found in the production russh/ssh-key path. | No published fixed version; retain the RustSec failure and finish vetted mitigation/applicability review. |

Russh 0.63.2 and Pageant 0.2.3 require Rust 1.89. Upstream patches and archive
checksums are retained, with the client-only channel-state backport documented in `vendor/README.md`; no
MSRV-incompatible version is substituted.

OpenSSL's official vulnerability index lists 24 affected-version matches for
pinned 3.6.3, fixed in 3.6.4/3.6.5. These include relative-CRLDP certificate
memory amplification (`CVE-2026-35189`); the QUIC, DTLS, CMP, CMS, signing and
other entries need individual applicability review. The empty GitHub repository
advisory response is not clearance. An OpenSSL upgrade must also reconcile the
existing callback backport and pass its strict/native regressions. The source pin is now
3.6.5 with independently verified archive SHA-256. The callback port changes
only one hunk offset and sparse-array whitespace context; its changes to the
29 files remain semantically the same. Exact patch application succeeds and
changed source rejects atomically. After generating fresh 3.6.5 headers, 22 C
translation units and the C++ callback fixture pass syntax checks. The first
C++ command omitted the fixture macro; the corrected command matches the CMake
definition and succeeds. Eighteen native-builder Python tests pass with mocked
builds, not native compilation. Linked callback/endpoint/native runtime tests
against new libraries remain required. Retained 3.6.3 installations and package
manifests are preserved and are not relabeled as repaired. The attempted
Boost/libtorrent security-page URLs returned 404. Subsequent dated NVD review
found five historical Rasterbar libtorrent CVEs with listed ranges at or below
1.1.3, outside the pinned 2.1.1. The Boost CPE query returned four historical
entries covering Regex 1.33/1.34, Locale 1.48–1.52, and zlib through Boost before
1.78; pinned Boost 1.91 is outside those described ranges. The keyword query's
yt-grabber result concerns another application. These retained database results
are version-applicability evidence, not proof of absence of vulnerabilities.

The exact-source draft inventory records eight normal/build dependency graphs
(minimal/standard/full/compat on Linux and Windows-GNU), 314 distinct packages,
declared licenses/MSRVs, cached archive verification, native source/patch hashes,
and 505 implementation/packaging/policy source hashes in the refreshed snapshot. All eight graphs pass the
protocol/provider policy checker. It is a source inventory, not rebuilt release
packages. The historical packages still contain their original dependencies.
The latest snapshot is `candidate-inventory-admission-v2/inventory.json`, SHA-256
`5993904e5bce1ac837ab0f7bd6f3d4c3c7d072e72286cf543b9819a05dfb6d62`.
It embeds the complete protocol-vendor provenance. Russh becoming a local path
package does not erase its upstream advisory matches; the OSV snapshot remains
tracked alongside the client-only backport.

The previously missing `deny.toml` now defines an executable workspace policy.
Checksum-verified cargo-deny 0.19.0 runs directly from F: without toolchain or
global installation changes. Its fresh RustSec snapshot is commit
`ef6173cbc5c50ec8166f9a5b28f07834144373ee`. License, banned-dependency and source
checks pass. Duplicate versions warn; unpublished path dependencies have an
explicit wildcard exemption. No advisory is ignored. The first built-in Git
fetch failed; CLI Git fetched the same database successfully. The local config
diff is limited to the database location and fetch implementation, verified
against the committed policy.

The check exposed yanked chacha20 0.10.1, now locked at non-yanked 0.10.2
(MSRV 1.85). The final advisory check has only the RSA error and no yanked
warning. The SFTP fixture passes 17 scenarios on both hosts, including actual
ChaCha20-Poly1305 transfer and the two forbidden-negotiation cases. The initial
Linux run used F:, where an explicit 0600 file creation reported 0777 and host
trust correctly rejected it; the unchanged executable passes with Linux `/tmp`
fixtures, where 0600 is preserved. Both observations and the first failure are
retained. RustSec lacks the additional GitHub-only matches recorded by OSV;
passing one database would not erase the other review obligations.

The SSH channel baseline delivers all 256 forged unknown-channel callbacks and
fails the zero-delivery assertion. The patched Linux and native Windows runs
pass with zero unknown deliveries while preserving a real pending open rejection.
The regression observes callback delivery; the accompanying source check proves
the rejection precedes the unbounded handle reply enqueue. Each host also passes
the existing 17-scenario SFTP fixture. Results are retained in
`recovery/checks-ssh-channel-{baseline-v2-linux,fixed-linux,fixed-windows}.json`.
The initial fixture-path compile error remains retained separately. Applying the
recorded patch to the verified archive reproduces all 83 vendor files exactly.
The refreshed cargo-deny policy passes licenses/bans/sources and reports only
the unresolved RSA advisory, with no advisory ignore or yanked warning.

The RSA upstream review retains PRs 702 (open; decryption blinding) and 680
(open draft; implicit rejection for PKCS#1 v1.5 decryption), plus issue 626.
Neither is a published fix to apply blindly. Upstream distinguishes signing
blinding defense in depth from Marvin's decryption oracle. The RSA policy error
stays visible with no ignore entry pending a supported disposition. Pageant's
`connect_pageant` uses `PageantStream`; Ariax's `connect_named_pipe` uses Tokio's
Windows named-pipe client and does not invoke that shared-memory path.
Research responses are retained under `advisories/remaining-research`.

Raw evidence is under `/mnt/f/temp/ariax/phase7-local-four-20261006`.
Unique artifacts and failures remain retained. Native Linux timing/kernel
coverage, fully instrumented Rust TSan, other platforms, fresh/minimum OS and
physical power-loss acceptance remain separate gates.
