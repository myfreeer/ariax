# Implementation Readiness

Status: implementation underway. The P0 contract blockers recorded in
`final-preimplementation-review.md` are resolved in their normative documents,
and module work proceeds under the phase exit criteria below and in
`implementation-plan.md`.

This document is the handoff checklist from architecture design to detailed
module design and implementation.

Repository scaffolding, generated inventories, core/config types, the scheduler
kernel and ordered driver, journal work, SQLite session storage and owner,
persistence-effect composition, bounded pure startup reconciliation planning,
source-derived credential admission, ordered SQLite startup repair, the native
startup handoff/coordinator, and bounded stats sampling now have executable
checkpoints. Concrete descriptor-safe capability/install/appender adapters and
publication-last native startup execution are also executable. The bounded
non-persistence runtime adapters and the process-bootstrap path are now wired
through the experimental CLI. The first end-to-end downloader slice is not
complete, but its fresh HTTP/1.1 known-length path is now executable against an
externally approved numeric peer. It validates the response head before body
polling, uses descriptor-backed strict-durability storage leases, preserves
prior durable pieces across short-body/cancellation aborts, reads back trusted
pieces before resume, and revalidates root/file identities during recovery.
Ordinary DNS/SSRF resolution, TLS/redirect/proxy policy, broader validator
choices, and real scheduler/RPC transfer dispatch remain gates, so this is not
a release/tag-ready checkpoint. Each remaining native adapter, transfer
module, and integration still follows its Definition Of Ready checklist below.

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
9. packet-independent `StatsSampler` (bounded pure/runtime primitive complete;
   protocol producers and RPC rendering remain integration work),
10. JSON-RPC `addUri`, `tellStatus`, `pause`, `remove`, and `getGlobalStat`
    against the real scheduler.

The minimal scheduler and these five RPC methods are pulled forward into Phase 3;
Phase 4 expands the control plane, runtime mutation, transports, and session APIs.
This removes the earlier circular Phase-0/Phase-4 gate. The slice proves the core
contracts before adding growing/chunked layouts, Metalink, FTP/SFTP, BitTorrent,
or HTTP/3.

The detailed first-slice docs above are the intended module design input for
this vertical slice.

## Current Integration Gates

- Connect the tested fresh/resume HTTP runtime adapter to the live scheduler and
  control dispatcher; the pinned-peer CLI remains a bounded harness until that
  process path is complete.
- Implement the policy-owned connector milestone: bounded DNS, special-use/SSRF
  filtering, final-peer binding, then TLS trust, redirect revalidation, and
  proxy/no-proxy destination checks. Do not expose ordinary URI resolution
  before these policies are enforced.
- Integrate live protocol retry classification and backoff with the executable
  persisted retry/slow/no-space recovery paths.
- Drive the bounded orderly-shutdown coordinator across every real lane,
  including dirty-checkpoint persistence when an adapter times out or detaches.
- Close the hot-backup publication residue window with verified same-file
  cleanup or a native atomic no-replace primitive and the required crash/unlink
  fault matrix.
- Run real io_uring and native Windows I/O coverage in supporting CI.
- Pass MSRV 1.88 and the full native Linux, Windows, and macOS release matrix
  before any tag; configured workflow jobs alone are not completion evidence.

## Deferred But Tracked

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
