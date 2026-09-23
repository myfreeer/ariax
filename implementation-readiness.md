# Implementation Readiness

Status: overall implementation is underway. The P0 contract blockers recorded
in `final-preimplementation-review.md` are resolved in their normative
documents, the scoped Phase-3B/3C HTTP(S) downloader milestone is checkpointed
at `1099be9`, and the Phase-4 control-plane checkpoint is executable at
`ec415ff`; Phase-4B implementation and native Windows benchmark evidence are
checkpointed at `f9edb5c`. A 2026-09-06 implementation audit found that the Phase-4
checkpoint needed multicall authentication ordering, authenticated pushed
events, complete registry-backed retry admission, active option restart replay,
active source replacement outcome handling, and per-client RPC budget
reservation. Phase 4B implements these six repairs and completes the runtime
option, configuration, session, interface and slow-slot integration gates.
P4-11 control progress at `8fefde2` adds immutable query projection outside
the control owner, one managed runtime, nonblocking mutation/admission continuations, and
bounded bulk progress with later per-task intent taking precedence. The expanded
Windows campaign is tracked separately from the historical status/global-template
report. Native Linux benchmark acceptance passes in the September 22 CI
baseline at `af5d193`; release matrix work remains governed by the exit
criteria below and `implementation-plan.md`.

Phase 5 implementation at `88d1a83` now passes all six local
[shared transfer gates](detailed-protocol-transfers.md#scope-and-checkpoints).
The [September 15 validation record](performance-evidence/phase5-validation-2026-09-15.md)
covers shared protocol transfers, verification, migration and current evidence.

This document is the handoff checklist from architecture design to detailed
module design and implementation.

Repository scaffolding, generated inventories, core/config types, the scheduler
kernel and ordered driver, journal/SQLite persistence, bounded startup repair,
native capability handoff, runtime adapters, stats sampling, and process
bootstrap have executable checkpoints. The completed Phase-3B checkpoint now
connects that foundation to ordinary HTTP(S) URI admission and the real
scheduler: task/source/options persist atomically, recovery rebuilds the source
catalog, the policy client owns DNS/SSRF/Happy-Eyeballs/redirect/proxy/auth/cookie
decisions. DNS cache-miss leaders and followers share the configured total
waiter cap, and dropping all receivers cancels the backend lookup and releases
its capacity. Deterministic virtual-time coverage pins the second Happy
Eyeballs racer to the configured fallback delay, and a changed zero-TTL answer
set is part of direct-transport identity so it cannot reuse the old idle
connection. Supervised multi-mirror range workers commit only validated
non-overlapping leases. Durable-piece restart and terminal evidence are
persisted before public completion. Five bounded JSON-RPC methods run over
loopback HTTP/1.1 and Content-Length stdio and expose packet-independent live
stats. Same-origin strong-ETag endgame duplicate fencing, candidate settlement,
dirty-overlap rollback, and crash-safe replay are now executable. The bounded
SHA-256 `Repr-Digest` profile also verifies probe/range bodies and admits
secondary origins only for exact-range endgame after digest equality is fenced.
The process-owned profile-capacity boundary is also executable for HTTP: profile
resolution, native handle subtraction, shared resident and transport-socket
permits (including proxy routes), selected storage-file descriptor permits,
profile selection in RPC startup, and the local C10k/active-range harness are
covered. HTTP discarded payload is also bounded by a process-owned atomic
process/host/task/attempt guard, including probe, retry/cancel, checksum, and
endgame cleanup paths, with separate RPC diagnostics. The broader evictable
file-handle LRU remains pending; Phase 5 adds the FTP/SFTP consumers. The latest retry
decision is now a bounded `tellStatus` object with exact live cause/status,
attempt credit, wait policy, numeric source/piece/lease correlation, prior
lease disposition, and next action. Restart reconstructs only journaled
error-class/reason/cap/wait evidence and labels it recovered. Hot-backup publication
residue now has descriptor-bound same-file/link-count recovery, no-clobber race
preservation, crash-point, and temporary-unlink fault coverage. A standalone,
workspace-excluded cargo-fuzz package now covers HTTP response/request headers,
retry specifications, discard-budget invariants, and journal replay; the
first-slice interruption matrix now covers one byte before a piece boundary,
the exact boundary, and one byte into the next lease while checking that every
begun lease receives exactly one commit or abort disposition. The remaining
read-admission evidence proves storage/buffer backpressure withholds the next
HTTP body poll while remaining cancellation-responsive. A deterministic
scheduler/storage race now also delivers cancellation after disk completion but
before provisional acknowledgement, proves `CancellationDrained`, and audits a
single cancelled lease with no false durable progress. Multi-range injected
disk rejection records one exact `storage_rejected` abort. A URI-scoped
oversized transport-frame fault now exercises the full range pipeline, charges
the bounded discard hierarchy, records one `oversized_body` abort, publishes an
`InvalidRange`/`DisableSource` diagnostic, and admits no retry or durable byte.
Implemented redirects settle before `BeginLease`; the redirect-policy contract
still proves that an adapter with an open lease must abort it. Scheduler
cancellation covers both a blocked body read and the disk-completion boundary.
Representation restart now flushes an explicit `restarting` journal marker
before `NextAdmission`; staged-prefix recovery preserves that reason, accepts
only the exact task snapshot, appends only the missing generation suffix, and
the promoted prefix exposes no old durable piece. The live control path records
the exact marker/snapshot/`representation_restart` sequence. The required local
protocol/crash matrix is executable, including parent-driven storage-process
kills at every provisional-write/data-sync/journal boundary on Linux and native
Windows-GNU.
The minimal process shutdown path now drives the fixed coordinator through real
runtime admission, bounded HTTP-worker drain, all-journal flush/close, bounded
session-owner join, and clean/dirty session-marker persistence. Cooperative
drain is clean; active synchronous abort and asynchronous timeout are dirty.
The control-plane checkpoint now adds one shared JSON-RPC dispatcher with
aria2 method tokens, notifications, bounded batches and multicall, query/queue/
source/typed-option/config/session controls, unique GID prefixes, loopback
WebSocket, a bounded coalescing event broker, direct CLI controls, and a typed
Rust embedding API. HTTP, WebSocket, stdio request/response, CLI, and the
library facade all reach the same process-owned control plane. Content-Length
and NDJSON stdio use the same bounded pushed event broker. Optional HTTP Basic,
aria2 text-session compatibility, combined transports, EOF policies, event
filters and compatibility modes have executable success/rejection coverage.
This remains a phase checkpoint, not a release/tag: broader RFC 9530
identity, `Content-Digest`, alternate HTTP digest negotiation, broader validators,
growing/chunked transfers, HTTP/2, broader RPC, adaptive profile tuning, and the
complete release matrix remain gates. Optimized
Linux and native Windows-GNU capacity evidence, including mandatory process
residency samples, is recorded in
`performance-profiles.md`; each remaining
adapter, transfer module, and integration still follows its Definition Of Ready
checklist below.

## Start Here

Implementation should begin from these source-of-truth documents:

- `configuration.md` for option metadata, config formats, URL rules, runtime
  update behavior, reload, and dump policy.
- `implementation-plan.md` for phase order and exit criteria.
- `requirements-traceability.md` for acceptance coverage.
- `security-recovery.md` for invariants that cannot be relaxed.

`review-findings-response.md`, `review-findings-round2.md`,
`review-findings-round3.md`, and `final-preimplementation-review.md` are
historical, non-normative audit records. They explain why rules were adopted,
but an implementation follows the focused subsystem documents when wording
differs.

Focused subsystem docs are then used as module-level design inputs.

Detailed first-slice docs:

- `detailed-core.md` for ids, errors, task state, scheduler commands,
  snapshots, cancellation, and persistence hooks.
- `detailed-config.md` for option registry artifacts, flat config parser, URL
  rules, runtime update application, reload, and dump.
- `detailed-storage.md` for safe paths, layouts, offset mapping, storage
  validation, journal format, recovery, and finalization.
- `detailed-runtime.md` for lanes, resource budgets, queue wrappers, buffer
  leases, backpressure, and cancellation.
- `detailed-http-first-slice.md` for sequential HTTP, strong-ETag resume, range
  validation, retry integration, stats, pause, and tests.

## Hard Invariants

These are not optional implementation details:

- No parsed-only implemented options.
- No metadata path bypasses `SafePathBuilder`.
- Persisted progress is bound to the canonical output root and stable file
  identities. Path adjacency, names, mtimes, or a copied control file never
  authorize reuse; relocation/rebind follows the explicit identity-or-digest
  protocol in `detailed-storage.md`.
- No project-owned HTTP/FTP/SFTP/Metalink protocol worker writes directly to a
  final file descriptor. The initial BitTorrent full build is the explicit
  exception: libtorrent may use its own storage internals only inside the
  isolated BT adapter lane described in `libtorrent-integration.md`.
- No cursor-based writes in segmented downloads.
- No range body is durable unless status, length, and placement validation pass.
- Every storage attempt span is identified by `LeaseId` under one protocol
  `TransferAttemptId`; writes remain provisional until validator checks issue
  `CommitLease` for that exact span, and `AbortLease` is recovery-visible. A
  sequential stream advances through piece-aligned checkpoint leases, never one
  all-or-nothing whole-remainder lease.
- Endgame candidates remain uncommitted until every competing write is fenced.
  A dirty/uncertain overlap group rolls all touched pieces back to pending
  metadata and in-memory state; physical bytes may remain only as untrusted
  overwrite targets.
- No `PieceDurable` record trusted after recovery may precede the required data
  flush. Balanced mode may batch the boundary; strict mode performs it per piece.
- No active-task option mutation happens without declared `runtime_update`.
- Restart-driven option changes project to aria2 `waiting` and emit no pause event.
- RPC status values are limited to `active|waiting|paused|error|complete|removed`;
  internal states are mapped, never serialized directly.
- GIDs are exactly 16 lowercase hexadecimal characters on the compatibility wire.
- RPC secret authentication follows aria2's `token:<secret>` positional convention.
- No unbounded transfer queues or unbounded payload allocation.
- Every accounted runtime allocation takes both its named domain permit and a
  global resident-byte permit. Domain caps may sum above the profile limit, but
  simultaneous reservations may not.
- No external WebSocket/stdio subscriber can block the scheduler or grow an
  unbounded event queue.
- No whole HTTP segment, Metalink file, or project-owned payload is buffered in
  a `Vec<u8>` on the normal path. Libtorrent's internal buffers stay behind the
  BT adapter boundary.
- SFTP's externally owned packet buffer plus returned data vector is reserved
  before each offset request and charged to `sftp_ingress_budget` until copied
  into a `BufferLease` and released.
- No blocking disk I/O on network event-loop threads.
- Live disk-backend failover stops admission and drains/cancellation-confirms
  every accepted operation in the old `BackendEpoch` before handles are reopened
  or work is readmitted under a fresh generation.
- No RPC method returns synthetic engine state for implemented behavior.
- No shell execution unless unsafe compatibility is explicitly enabled.
- No backend runtime failure path panics, asserts, or aborts when fallback or a
  typed configuration error is possible.
- Every submitted `BufferLease` has exactly one owner through completion, error,
  cancellation, quarantine, or pool return.
- No retry wait holds transfer buffers or worker threads.
- No slow-slot demotion fires for local disk, CPU, buffer, journal, or
  user-rate-limit backpressure.
- Range/split/resume requests use identity encoding and a known representation
  extent; decoded/wire offsets are never mixed.
- User headers cannot override generated Host, framing, range, encoding,
  validator, integrity, or credential headers.
- RPC request, response, batch, list, per-client work, and serialized-byte
  bounds are enforced before amplification; list queries use immutable
  membership indexes rather than scanning the scheduler in an actor turn.
- FTP passive endpoints and active callbacks are bound to the approved control
  peer by default; any administrator override passes the complete destination
  policy and never trusts a server-advertised endpoint by itself.
- FTP control sockets use the downloader connector plus `connect_with_stream`;
  passive data uses an owned endpoint-validating builder, and active mode needs
  the patched pre-TLS peer predicate/accept loop. Crate default connect/build/
  NAT-workaround paths are forbidden.
- FTP control replies are line/aggregate/count capped before proportional
  allocation; unpatched SuppaFTP 10.0.1 is not an implementation candidate.
- FTP dependency diagnostics never format raw wire commands/replies,
  credentials, paths, FEAT text, or listings; canary tests cover every log level.
- An SFTP host key is accepted only by pin, matching `known_hosts` policy, or an
  explicit challenge-id/fingerprint approval command. Generic task resume never
  approves a key.
- Untrusted remote RPC cannot use proxy-side DNS or arbitrary CONNECT destinations
  without an explicitly trusted, destination-filtering proxy policy.
- Persisted session/control artifacts never contain plaintext authentication,
  proxy, cookie, or signed-request secrets.

## Source-Of-Truth Artifacts To Generate

Phase 0 should generate or maintain:

- option registry,
- aria2 compatibility matrix,
- runtime update matrix,
- aria2-versus-extension runtime compatibility matrix,
- config schema and URL-rule schema,
- RPC method matrix,
- internal-state-to-wire-status matrix,
- complete state × command/error transition matrix,
- build-feature matrix,
- repository artifact/toolchain matrix,
- documented option inventory extracted from the repository's Markdown design
  documents,
- diagnostics field matrix,
- error-code matrix,
- control-journal record/schema/version matrix,
- compact-checkpoint and persisted root-binding/rebind matrices,
- exact SQLite schema, pragma, migration, and backup matrix,
- runtime resource/cap matrix, including metadata cardinality and global
  resident-permit accounting,
- backend-epoch/live-failover and file-handle-budget matrices,
- reserved-header and proxy trust-policy matrices,
- secrets-at-rest persistence matrix.

CI should fail when the generated artifacts disagree with docs, CLI help, RPC
allowlists, config parser, or implementation feature flags.

## First Implementation Slice

The first useful vertical slice spans implementation Phases 1–3 and should be:

1. option registry with flat config parser,
2. `SafePathBuilder` plus persisted root binding,
3. `FileLayout` and `GlobalOffsetMapper`,
4. `BufferPool` and bounded queue wrappers,
5. `ControlJournal` with lease commit/abort, serialized sequence assignment,
   balanced/strict durability ordering, and checkpoint-only `PieceStateChunk`,
6. `SessionStore` with the exact v2 schema, v1 migration, root binding, and
   explicit relocation/rebind entry point,
7. known-length identity HTTP sequential download through `StorageEngine`,
8. strict strong-ETag range resume with provisional writes and explicit
   commit/abort,
9. packet-independent `StatsSampler` connected to HTTP worker counters and RPC
   current/durable speed rendering,
10. JSON-RPC `addUri`, `tellStatus`, `pause`, `remove`, and `getGlobalStat`
    against the real scheduler.

The minimal scheduler and these five RPC methods are pulled forward into Phase 3;
Phase 4 expands the control plane, runtime mutation, transports, and session APIs.
This removes the earlier circular Phase-0/Phase-4 gate. The slice proves the core
contracts before adding growing/chunked layouts, Metalink, FTP/SFTP, BitTorrent,
or HTTP/3.

All ten items now have an executable Phase-3B checkpoint. The local
rate-limit arbiter now has deterministic debt, fairness, scoped-reconfiguration,
cancellation, and 1,000-stream tracking evidence. The local stall-diagnostic,
same-origin endgame, bounded exact-range
cross-origin endgame, digest-only durable-range restart revalidation, and
C10k/active-range harnesses are executable. Deterministic storage-boundary
ENOSPC, permission-denied, short-write, partial-fsync, torn-tail, and
same-inode publication faults now prove that failed writes publish no false
durable piece and leave a replayable journal; same-file residue is removed only
after installed header/linkage replay succeeds. Child-process exit and
parent-driven kill tests cover every data/journal barrier on Linux and native
Windows-GNU. They retain an OS-visible unflushed `PieceDurable` record after a
process kill, while the deterministic Linux power-loss cut removes that tail
and recovers only the prior flushed prefix. Native release and real hardware
poweroff evidence still gate the final Phase-3 claim.

The detailed first-slice docs above are the intended module design input for
this vertical slice.

## Phase 4 Repair Gates

The six defects below were established against `ec415ff` and are repaired.
Linux 1.97.1 and native Windows-GNU workspace tests pass, including production
retry admission, authenticated transports, delayed cancellation, durable
prefixes, exact mirror promotion, source rollback and disconnected callers.
Strict Clippy passes on both platforms; the all-target/all-feature graph also
checks at Linux MSRV 1.88. The instrumented parser smoke evidence is recorded
with the completion gates below.

Reservations cover borrowed result preflight, typed conversion, scheduler
simulations, retained snapshots and pending owner lifetimes. Rejection before
journal creation, timeout retention, unused-plan cleanup and independent
client/owner slots have executable regressions. Accepted add, option, source
and batch-import mutations retain an owner continuation through publication,
including after caller disconnect; failed drains record dirty checkpoints.
Real output placement and piece-geometry application are covered by `P4-07`.
Allocation-contract tests and deterministic writer stalls remain separate from
the real-worker native Windows measurements under `P4-11`.

| Gate | Current Implementation | Required Evidence And Owner |
| --- | --- | --- |
| `P4-01` Multicall authentication | Repaired: member-token envelopes dispatch without an outer token. | HTTP/WebSocket/stdio regression tests pass; invalid/missing tokens never dispatch a member. `invalid_envelopes_and_tokens_never_authorize_events` covers malformed, nested, empty, and over-limit envelopes. [API authentication](apis-and-embedding.md#rpc-authentication). |
| `P4-02` Event authentication | Repaired: connection-local context installs a subscriber after authentication, before execution. | WebSocket two-client/reconnect/shutdown and Content-Length first-call/method-error tests pass; no-secret event tests remain green. Each method still requires its token. [Event delivery](apis-and-embedding.md#event-delivery-and-slow-consumers). |
| `P4-03` Retry admission | Repaired: complete bounded retry registry and exact production-policy preflight before journal creation. Runtime/config scopes are covered by `P4-07`. | `retry_admission_recovers_canonical_options_with_production_policy` and `rejected_admission_has_no_artifacts_and_does_not_fault_the_scheduler` pass with profiles, aliases, modifiers, forbidden keys, and a subsequent valid add/query. [Retry admission](retry-policy.md#registry-and-persistence-boundary). |
| `P4-04` Active option recovery | Replay repaired: accepted snapshots survive drain, generation persistence includes atomic mirror promotion, and recovery restores exact staging and fresh patch identities. Live-only rate changes no longer restart. | Delayed-worker split/output/mixed patches, all durable prefixes, consecutive generations, rejected staging, and live-rate persistence pass. Exact promotion mismatch and injected SQLite rollback tests pass; strict journal replay tests remain green. Real output-path storage application also passes under `P4-07`. [Option application](detailed-config.md#runtime-update-application), [journal rules](detailed-storage.md#control-journal-format). |
| `P4-05` Active source replacement | Repaired: asynchronous replacements quiesce the worker without changing user intent, then commit the complete source set and exact queue state before catalog publication and ordinary readmission. | Delayed-worker tests cover both method shapes, independent queries, pause/remove races, disconnected callers, and restart before/after commit. Retry-wait tests cancel stale timers with retained/released slots. SQLite queue mismatch and injected source-write failure roll back both changes. The synchronous primitive rejects active replacement before mutation. [Source persistence](session-persistence.md), [mutation contract](apis-and-embedding.md#control-mutation-recovery). |
| `P4-06` RPC work accounting | Repaired: profile/client/resident reservations cover parsing, typed input, scheduler/draft copies, native and JSON projections, responses, events, multicall results, transport caches, and pending owner lifetimes. | Transport stalls, independent clients, four outstanding requests, overflow/failure/cancellation, preflight/refunds, deferred mutations, native projections, scheduler rejection before journal creation, timeout retention, unused-plan cleanup, event retention, and 1,000-active-task forecasts have executable regressions. Native optimized real-download/RSS evidence is tracked separately by `P4-11`. [RPC bounds](apis-and-embedding.md#query-and-response-work-bounds), [runtime budgets](detailed-runtime.md#resourcemanager). |

Preserve the existing strict journal replay and uncertain-write failure rules.
An invalid journal from the old checkpoint must not be made valid by ignoring a
duplicate snapshot; any repair of existing damaged artifacts needs its own
explicit recovery design. The bounded journal/parser fuzz targets and native
persistence coverage remain required when implementing these repairs.

## Current Integration Gates

### Phase 4B Completion Evidence

`P4-01` through `P4-10` are implemented and tested. The P4-11 control-progress
implementation adds immutable query roots, two bounded projection slots, one
managed urgent/bulk runtime, nonblocking persistence preludes, filesystem
preparation outside the owner, and resumable bulk controls. The previously
deferred native Linux measurement gate passes with the September 22 CI
baseline; local WSL checks remain distinct from that native evidence.

Deterministic regressions cover queries with the owner locked, stalled filesystem
and SQLite work, import queue revalidation/fencing, snapshot identity and
retention, saturation and refund paths, accepted work after the last backend
handle, and 1,000-task bulk progress with later pause/remove taking precedence.
Coalescing preserves intervening task actions and rejects overflow instead of
reordering a later pause ahead of a resume. The expanded Windows campaign adds
128-row projections, 32-source metadata queries, eight real auxiliary-task
mutations, and separately timed administrative operations. The September 13
report is historical and does not validate the current implementation.

The `P4-09` HTTP Basic portion is executable: startup validates the sensitive
`rpc-secret`/`rpc-user`/`rpc-passwd` CLI and environment settings before bootstrap;
HTTP and WebSocket enforce Basic before body dispatch or event subscription.
Method tokens remain independent, duplicate credentials reject, and cloned
dispatchers share rejection throttling. Linux 1.97.1, MSRV 1.88, native
Windows-GNU workspace tests, strict Linux/native Clippy, and generated contract
checks pass. The updated RPC fuzz target now passes 2,000 instrumented smoke
runs across all three compatibility modes and both token policies, with virtual
time for throttle waits. The remaining `P4-09` interfaces also pass as below.

The `P4-08` source sanitization boundary is executable: live query-bearing HTTP
sources persist and export only credential placeholders, and startup retains
those tasks in the catalog. Source replacement clears only a matching source
requirement after durable acknowledgement and preserves user pause intent.
Storage rejects unsafe source metadata on write and recovery. Tests cover
secret canaries in every persistence artifact, JSON export, rejected replacement,
restart before/after replacement, and a real range transfer through a recovered
safe mirror with non-contiguous source IDs. Linux 1.97.1, MSRV 1.88, native
Windows-GNU workspace tests and strict Linux/native Clippy pass. The Linux
workspace run uses four test threads after a concurrent-build run exceeded an
existing cross-mirror test deadline.

The rest of `P4-08` is executable: strict whole-document JSON/aria2 validation,
atomic SQLite batch admission with exact member confirmation, orphan journal
identity avoidance, private atomic configured exports, periodic saving, and
shutdown drain. The CLI accepts local input/export settings, and the typed Rust
API shares the same import/export/save path. Ariax-only options stay in metadata
comments instead of invalidating ordinary aria2 option lines. Fault and child
process tests cover rollback, committed-batch recovery, and complete old/new
exports at every publication boundary. A native Windows test exposed inherited
ACLs on descriptor-created files; creation now supplies the protected private
descriptor before writing any bytes. Linux 1.97.1 and native Windows-GNU workspace
tests, strict Linux/native Clippy, Linux MSRV 1.88 checking, workspace build,
formatting, and generated contract verification pass. The session parser also
passes 2,000 bounded instrumented fuzz smoke runs.

The P4-11 Linux-under-WSL and native Windows workspace builds, tests and strict all-target,
all-feature Clippy cover the implementation, with the 1,000-task stress case
run separately. The final workspace runs pass 348 engine tests on Linux and
346 on Windows; adding the separately passing stress case gives 349 and 347.
Linux MSRV 1.88 checking, formatting, generated contracts and
the exact rusqlite feature graph also pass. That checkpoint's generated inventories cover
50 reviewed options. Its RPC JSON, session-document and URL-rule fuzz
targets each pass 2,000 AddressSanitizer/coverage smoke runs in four 500-run
bursts with 250 ms cooldowns; counters were loaded. The prior September 13
checkpoint records the broader eight-target smoke campaign. These bounded
runs complement the regression and crash suites and do not replace longer
native CI campaigns.

The expanded native Windows campaign passes all four 20,000-call transport
scenarios and the 128-task administrative scenario. Worst ordinary operation p99
is 37.543 ms; the longest measured burst is 421 ms; sampled working set stays
below 140 MiB. The [validation record](performance-evidence/p4-11-validation-2026-09-14.md)
separates passing evidence and the retained failed preliminary run. The
September 22 CI baseline closes its deferred native Linux measurement gate.

| Gate | Status And Evidence |
| --- | --- |
| `P4-07` Configuration and runtime options | Closed. Registry-driven admission and atomic typed patches preserve live/pending/restart/rejection behavior. `runtime_patch_rejections_are_grouped_value_free_and_atomic`, `versioned_reload_and_url_rules_preserve_precedence_and_reject_atomic_mixed_changes`, and `explicit_layout_changes_download_new_placement_and_geometry_without_clobbering` pass. URL-rule bounds, precedence, redacted flat/JSON/TOML dumps, and rollback are covered. |
| `P4-08` Session compatibility | Closed. Linux/native Windows tests and Linux MSRV checking pass as recorded above. Configured aria2/JSON export, explicit/periodic/shutdown saves, local input and typed Rust operations share bounded sanitized documents. Whole-document validation, atomic batch metadata, disconnect retention and crash tests prevent invalid task prefixes and partial exports. |
| `P4-09` Public interfaces | Closed. CLI, HTTP, WebSocket, both stdio framings and typed Rust queries/mutations share the dispatcher. `every_advertised_method_executes_or_reports_its_disabled_protocol`, combined-transport/EOF integration tests, native parity and credit exhaustion, authenticated filters, diagnostics, and all compatibility modes pass success/rejection coverage. |
| `P4-10` Slow-slot scheduling | Closed. Default `off`, demote/pause, queue ordering, cooldown, user precedence, recovery and concurrent local-pressure guards are covered. `slow_remote_workers_free_slots_and_user_controls_override_cooldown` and `retry_wait_slot_policy_uses_real_worker_deadlines` pass. Automatic retry readmission preserves range deadlines and attempt counts under unchanged snapshots; strict generation rejection remains tested. |
| `P4-11` Performance and platform evidence | Implementation and expanded native Windows campaign pass: immutable queries outside the owner, one managed runtime, nonblocking mutation/admission work, bounded bulk continuation, later-action precedence, and cancellation ownership. Four transports each complete 20,000 measured calls under 1,000 active ranges; worst operation p99 is 37.543 ms and longest burst 421 ms. The separate 128-task administrative scenario also passes. [Reports and limits](performance-profiles.md#native-windows-control-plane-evidence). Native Linux measurement and the full functional CI matrix pass at `af5d193`; native backend and release-packaging gates remain separate. |

The session schema stays at v2 and journal replay remains strict. Native disk
backend additions, hardware poweroff, release packaging, and tagging remain
separate roadmap gates. Record checkpoints and bounded benchmark evidence before
removing generated target outputs; preserve pinned toolchains and archives.

### Cross-Phase Integration

- Keep the executable crash matrix green: deterministic partial-fsync,
  torn-tail, same-inode rotation residue, child-process exit/kill, and the
  Linux durable-prefix power-loss model cover the HTTP/storage boundary. The
  blocking fallback matrix runs natively on Windows-GNU; the deferred Windows
  overlapped backend, real hardware poweroff, and remaining release-platform
  runs remain separate evidence gates.
- Keep the new persisted `HttpRangeIdentity` restart path fail-closed: replay
  validates its SHA-256/length fingerprint before network history, matching
  mirrors are reprobed, and every locally verified durable range is fetched,
  hashed, and discarded from one matching source before pending work is
  released.
- Expand the bounded SHA-256 `Repr-Digest` exact-range profile only after
  `Content-Digest`, coverage metadata/parameters, alternate algorithms, and
  server-advertised whole-entity admission have explicit bounded contracts.
- Expand validator support beyond strong ETag and add HTTP/2 only behind its
  separately bounded stream/pool contract.
- Preserve the Phase-4 control-plane repairs and default loopback boundary:
  route `system.multicall` before outer token parsing while checking
  every inner member, gate WebSocket/stdio event subscriptions on successful
  authentication, and enforce per-client/global RPC budget reservations before
  parsing or serializing additional work.
- Keep every accepted retry option aligned with the registry and persisted-option
  policy before task admission; an accepted task must round-trip its complete
  retry snapshot through recovery.
- Keep active option patches and active source replacement replayable and
  outcome-consistent across cancellation drain, journal, SQLite, catalog, and
  scheduler transitions.
- Keep the executable minimal shutdown path green and carry the same typed
  timeout/dirty outcome into future native disk/CPU and BitTorrent lanes rather
  than bypassing the coordinator.
- Keep the hot-backup publication recovery matrix green across native
  platforms; native release and real poweroff evidence remain separate gates.
- Run real io_uring and native Windows I/O coverage in supporting CI.
- Pass MSRV 1.88 and the full native Linux, Windows, and macOS release matrix
  before any tag; configured workflow jobs alone are not completion evidence.

## Phase 5 Local Completion

`P5-01` through `P5-06` pass locally at `88d1a83`: shared bounded CPU and
transfer ownership, four checksum algorithms, multi-lease verification and
recovery, streamed Metalink v3/v4 admission/following, patched FTP/FTPS and
SFTP, mixed-source scheduling, selectors/statistics and CLI/RPC/Rust parity.
JSON migration v2 retains selected verification without the original XML;
version-1 import remains supported. Journal v1 gains required records 27–32;
existing record meanings and SQLite schema v2 are unchanged.

Default and all-feature workspace suites, builds and strict Clippy pass on
Linux-under-WSL and native Windows-GNU. MSRV 1.88, generated contracts, all four
protocol feature bundles, SQLite closure/rejections and fork inventories pass.
Both clients pass real OpenSSH authentication and bounded offset reads. Named
success/rejection and crash regressions are recorded in the
[validation record](performance-evidence/phase5-validation-2026-09-15.md).

Ten ASan/coverage fuzz targets each pass 512 executions in accepted bursts of
at most 500 ms, with three over-limit attempts retained and excluded. All five
native Windows benchmark scenarios pass: four transports each complete 20,000
measured calls under 1,000 active ranges admitted through Metalink, followed by
the separate 128-task administrative scenario. Worst operation p99 is
38.946 ms and longest transport burst is 420 ms. The
[Phase 5 performance evidence](performance-profiles.md#native-windows-phase-5-evidence)
is separate from the historical P4 reports.

## Deferred But Tracked

The [remote and fail-fast CI prerequisite](continuous-integration.md) passes
at `af5d193`. The [retained baseline](performance-evidence/ci-baseline-2026-09-22.md)
verifies every required CI job, all five native Linux reports, command logs and
source hashes. This closes the P4-11 and Phase 5 native Linux control-plane
measurement gate and permits the complete Phase-6 BitTorrent build.
Phase 6 permits breaking unreleased internal APIs and moving to fresh
schema/JSON v3 stores without new backward-compatibility machinery.
Native kernel/backend coverage and the full release-platform matrix remain
separate gates.

These are intentionally not first-slice blockers, but their registry and
feature-gate status must exist from the start:

- libtorrent full build,
- XML-RPC,
- C ABI,
- HTTP/3/QUIC,
- ECH,
- DoH/DoT,
- native TLS provider,
- custom libtorrent storage backend,
- true kernel zero-copy shortcuts,
- specialized thingbuf/rtrb hot-lane queues and disk-latency adaptive tuning,
- chunked or content-decoded growing-layout downloads,
- compact build without SQLite.

## Resolved Ecosystem Choices And Prototype Gates

`library-choice.md` records the choices made by the 2026-07-25 final review:

- Hyper/hyper-util for HTTP/1.1 and HTTP/2,
- Hickory Resolver for the in-process async DNS backend,
- russh plus a pinned receive-cap patch for russh-sftp 2.3.0,
- russh's exact re-exported ssh-key 0.7.0-rc.11 for one OpenSSH key,
  certificate, and `known_hosts` representation,
- russh defaults disabled with exactly ring+flate2+rsa,
- a pinned control-reply-cap patch for SuppaFTP 10.0.1 with Tokio/rustls for FTP
  and FTPS protocol mechanics,
- the low-level io-uring crate behind the project-owned Linux disk adapter,
- rusqlite with defaults disabled and `bundled+backup+cache+limits` on a
  dedicated session thread, including the first `minimal` build,
- a dedicated project-owned Rayon pool for CPU-heavy work,
- bounded Tokio/crossbeam queues for the baseline,
- Quinn plus h3/h3-quinn only as the feature-gated HTTP/3 experiment.

The remaining prototypes validate an already-defined fallback boundary; they do
not reopen the public architecture:

- Fall from the low-level io-uring backend to the bounded blocking backend if
  secure open, accepted-operation draining, cancellation, or quarantine gates
  fail; correctness contracts remain identical.
- Ship SFTP only with the pinned russh-sftp receive-cap patch (or an upstream
  release that passes the identical tests); unpatched 2.3.0 cannot satisfy the
  allocation bound.
- Ship FTP only with the pinned SuppaFTP control-reply-cap patch (or an upstream
  release that passes the identical tests); unpatched 10.0.1 cannot satisfy the
  parser allocation bound.
- Enable libssh2 only if documented russh interoperability gaps remain after the
  Phase-5 server matrix.
- Introduce thingbuf/rtrb only after a measured lane and producer topology prove
  a benefit over the bounded baseline.
- Ship HTTP/3 only after interoperability, proxy, fallback, flow-control, and
  binary-size gates pass; otherwise its option remains `feature_gated`.
- Add control-files-only or system-SQLite profiles only after their recovery and
  packaging behavior is designed and measured.

The exact locked dependency graph, target matrix, licenses, advisories, and MSRV
are Phase-0 generated artifacts. A prototype failure selects the documented
fallback and does not permit silently changing correctness contracts.

## Definition Of Ready For Coding

A module is ready to implement when it has:

- owned design doc or section,
- explicit inputs and outputs,
- option registry entries if user-configurable,
- state machine states and terminal errors,
- rejection rules,
- persistence/recovery impact,
- tests and fuzz targets,
- diagnostics fields,
- cross-platform behavior,
- feature-gate and build-profile behavior,
- externally visible compatibility behavior and any intentional divergence,
- on-disk schema/version and migration behavior when persistence is touched,
- no unresolved P0 item assigned to it by `final-preimplementation-review.md`.

If any item is missing, add it to the design before coding that module.

## Definition Of Done For A Feature

A feature is done only when:

- implementation and docs agree,
- compatibility matrix status is correct,
- unsupported/partial cases fail explicitly,
- unit/property/fault tests cover normal and rejection paths,
- fuzz target exists for untrusted parsers,
- metrics expose the feature's health and queue pressure where relevant,
- crash/recovery behavior is tested if it touches storage or task state,
- CLI, RPC, config, and library APIs share the same behavior,
- no generated artifact drift is present,
- every option marked `implemented` passes its non-default behavioral-fingerprint
  test; a registry test id without observed behavior is insufficient,
- external API queues have bounded slow-consumer tests,
- release/build changes produce the declared Cargo release artifacts on every
  supported platform.
