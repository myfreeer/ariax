# Requirements Traceability

Phase 4B completion is tracked by gates `P4-01` through `P4-11` in
`implementation-readiness.md`. Each gate requires a named regression or
integration test and recorded platform evidence before its status is closed.
The selected real-download RPC p99 target is 50 ms; the existing mock dispatcher
benchmark does not satisfy that requirement.

Status: reviewed contract; the scoped Phase-3B/3C HTTP(S) downloader milestone
is checkpointed at `30b70c5`, and the bounded Phase-4 control-plane checkpoint
is executable at `71acb03`. Phase 4B repairs authentication, retry admission,
active option journal recovery, live-rate changes, and source mutations. Shared
RPC reservations, transport ownership, borrowed result preflight, and typed
input/native projection accounting are implemented. Scheduler simulations and
status drafts reserve before mutation and retain credit through pending driver
work. Complete runtime option application and the remaining completion gates
are still open.
Core scheduling,
bounded persistence/recovery, native storage handoff, runtime ownership,
packet-independent stats, process bootstrap, and the first public Phase-3B
multi-mirror HTTP(S) slice have executable checkpoints. The latter includes
atomic source-aware admission/recovery, DNS/SSRF/Happy-Eyeballs,
redirect/proxy/auth/cookie policy, supervised range/retry workers, and a shared
real-scheduler JSON-RPC/CLI/library control surface. It also includes
method-token authentication, bounded batch/multicall/list work, unique GID
prefixes, typed option/source/config/session controls, loopback WebSocket,
bounded scheduler-observed events, and process-owned pooling/bounded ingress,
hierarchical rate limiting and stall diagnostics, persisted retry waits, and
strong-ETag-bound single-source/strict-fallback recovery with pre-network
durable-piece digest verification. A bounded `tellStatus.retryDiagnostic`
snapshot now exposes exact live trigger/status, remaining total/per-source
attempt credit, wait selection, numeric source/piece/lease correlation,
disposition, and next action without URI text; restart labels its coarser
journaled error-class/reason reconstruction. Persisted user SHA-256 now also provides
strict concurrent-mirror identity, digest-bound restart, bounded descriptor-
based final verification, and terminal digest evidence. Persisted
stale-validator `fail`, fresh `revalidate`, and descriptor-authorized bounded
`restart-if-safe` now cross the worker, scheduler, session schema v2/control
journal, and public option
boundaries without retaining old durable bytes. A flushed `restarting` marker
preserves representation-restart reason authority across the staged
next-admission crash window; recovery reuses only an exact matching snapshot,
and generation promotion invalidates old progress before new admission.
Same-origin strong-ETag endgame
fencing, candidate settlement, dirty-overlap rollback, and crash-safe replay are
implemented. The bounded SHA-256 `Repr-Digest` profile now negotiates strict
probe/range evidence, verifies response bodies, journals accepted digests, and
admits secondary origins only for exact-range endgame. Its persisted
digest/length identity now drives fail-closed restart reprobe and durable-range
network revalidation before pending work is released. Broader RFC 9530/Metalink
identity, `Content-Digest`, alternate checksum algorithms,
Last-Modified/unsafe-override resume, broader protocols/control APIs, adaptive
profile tuning, and the complete release matrix remain incomplete. Optimized
Linux and native Windows-GNU capacity evidence, including mandatory RSS or
working-set samples, is recorded in `performance-profiles.md`.

The Phase-4 audit of `71acb03` identified six control-plane repair gates: multicall
envelopes must be dispatched before outer token parsing while checking every
inner token; pushed WebSocket/stdio events must wait for successful client
authentication; every accepted retry option must be registry- and
persistence-approved; active option patches must replay through one staged
snapshot; active source replacement must not report failure after committing
source rows; and all RPC transports must enforce the documented client and
process budgets. Each gate's owner and required evidence are tracked in
[implementation-readiness.md](implementation-readiness.md#phase-4-repair-gates).
Phase 4B closes `P4-01` through `P4-03` with Linux, MSRV, and native Windows-GNU
regressions. The `P4-04` journal/live-rate repairs pass the same platforms,
including delayed cancellation, durable prefixes, exact mirror promotion, and
consecutive generations. `P4-05` now covers cancellation drain, desired pause
state, remove races, disconnected callers, retry timer cancellation, atomic
source/queue rollback, and restart before and after commit. These regressions
pass on Linux, MSRV, and native Windows-GNU. Shared RPC request/response/event
reservations, bounded request parsing, blocked-writer ownership, and release
paths pass the same workspace test matrix. Borrowed source/session preflight,
bounded result accumulation, typed input forecasts, retained option-patch leases,
and native admission/projection credit also pass that matrix. Scheduler/draft
reservations cover mutation rejection before journal creation, pending-driver
timeouts, unused-plan cleanup, event retention, and resumed admission after
credit release. An allocation-contract test covers 1,000 active scheduler tasks;
real-download latency/RSS evidence remains under `P4-11`. Complete runtime option
application under `P4-07` also remains open.

`P4-08` now covers strict whole-document JSON/aria2 parsing, atomic batch task
metadata with crash recovery, private configured exports, periodic/final saves,
and CLI/Rust/RPC import/export parity. Tests prove invalid suffixes publish no
task prefix, disconnected accepted imports finish, and export failures leave
complete old or new bytes. Linux/native Windows workspace tests and Clippy,
Linux MSRV 1.88, generated contracts, and a short 2,000-run parser fuzz smoke
check pass. Native Windows file creation supplies a protected private ACL;
instrumented fuzz and final performance/platform evidence remain `P4-11` gates.

The implemented HTTP discard hierarchy now has deterministic process/task/host
fault coverage and a standalone bounded fuzz package (`fuzz/`) for HTTP
headers, retry specifications, discard accounting, and journal replay. The
storage boundary also has deterministic ENOSPC, permission-denied, short-write,
partial-fsync, torn-tail, and same-inode publication coverage that leaves no
false durable progress and removes same-file residue only after installed
header/linkage replay succeeds. Scheduler cancellation after disk completion is
fenced before lease commit and drains with exactly one cancelled journal
disposition; multi-range disk rejection terminates its opened lease once with
the `storage_rejected` reason. A bounded URI-scoped oversized frame is discarded
before storage, aborts the opened lease once as `oversized_body`, and disables
the offending source as a non-retriable invalid range. Redirects in the
implemented client settle before lease admission, while the redirect-policy
contract retains the mandatory open-lease abort for future adapters.
Child-process exit and parent-driven kill tests cover every provisional-write,
data-sync, and journal-publication barrier on Linux and native Windows-GNU. An
OS-surviving kill retains a complete visible `PieceDurable` record, while the
Linux durable-prefix power-loss cut removes its unflushed tail and recovers only
the prior barrier. Hot-backup publication residue now has descriptor-bound
same-file/link-count recovery, no-clobber raced-destination preservation, and
crash/unlink fault coverage. Minimal real-process shutdown now closes admission,
boundedly drains HTTP workers, flushes/closes every installed journal, joins the
session owner, and writes a clean marker only after those barriers; failure or
timeout persists dirty recovery evidence. Native release and real poweroff
evidence remain incomplete until supporting CI runs.

This maps the requested properties to design documents.

## Coverage Index

Normative ownership is distributed as follows; topic documents reference the
detailed contract rather than redefining its types:

- architecture/build: `README.md`, `library-choice.md`, `implementation-plan.md`,
  `implementation-readiness.md`,
- core/config/API: `detailed-core.md`, `configuration.md`, `detailed-config.md`,
  `apis-and-embedding.md`,
- runtime/resources: `detailed-runtime.md`, `threading-model.md`,
  `messaging-model.md`, `backpressure.md`, `performance-profiles.md`,
  `event-backends.md`,
- storage/recovery: `detailed-storage.md`, `disk-adapter.md`, `buffer-pool.md`,
  `zero-copy.md`, `session-persistence.md`, `security-recovery.md`,
- transfer protocols: `detailed-http-first-slice.md`, `split-download.md`,
  `retry-policy.md`, `redirect-policy.md`, `rate-limiting.md`,
  `protocol-modernization.md`, `detailed-ftp-sftp.md`, `metalink-chunking.md`,
  `libtorrent-integration.md`,
- scheduling/observability: `download-scheduling.md`, `stats-and-stalls.md`.

`review-findings-response.md`, `review-findings-round2.md`,
`review-findings-round3.md` (which consolidates the round-4 external
verification), and `final-preimplementation-review.md` are review history;
resolved rules must live in one of the normative documents above.

## Performance

Requirement:

- async net/disk I/O,
- multi-CPU worker threads,
- non-blocking control,
- C10k.

Design coverage:

- `README.md`: runtime lanes, protocol pipeline, memory model, observability.
- `disk-adapter.md`: storage engine, backend choices, disk queue, buffer
  ownership.
- `backpressure.md`: adaptive network/disk/CPU/memory feedback loops.
- `threading-model.md`: split event/disk/cpu/libtorrent pools with global
  resource budgets.
- `messaging-model.md`: high-performance bounded queue choices and shared
  pooled-buffer ownership across lanes.
- `performance-profiles.md`: C10k vs throughput tradeoffs and presets.
- `split-download.md`: range leasing, retry, endgame, and fallback semantics.
- `metalink-chunking.md`: Metalink checksum alignment and readback policy.
- `stats-and-stalls.md`: packet-independent speed sampling and stuck socket
  reporting.
- `event-backends.md`: backend probing and fallback.
- `implementation-plan.md`: 10k low-activity socket and 1k active range
  benchmarks.

Acceptance:

- control p99 latency remains under target while downloads are active,
- no blocking disk I/O on network runtime threads,
- no unbounded queues,
- hot queues pass descriptors/leases, not payload copies,
- event-loop lag and queue depth are observable,
- every accounted allocation holds both a named-domain permit and the global
  resident-byte permit; aggregate reservations remain below the profile limit,
- C10k means concurrent low-activity sockets, not 10,000 retained HTTP
  keep-alive entries or per-connection transfer buffers.

The executable HTTP profile resolver and capacity harness now provide direct
acceptance evidence: 10,000 real loopback sockets plus 1,000 simultaneous 64 KiB
HTTP `206` range responses fit the concurrency accounted envelope, while a low
native handle limit produces an explicit C10k rejection. Optimized Linux and
native Windows-GNU runs are recorded with mandatory process-residency samples
at the 10,000-socket and 1,000-live-response barriers. Selected HTTP storage
files and proxy sockets consume the shared file/socket domains. The broader
evictable file-handle LRU, non-HTTP profile consumers, and the remaining
release-platform matrix remain open.

Boundary:

- C10k is a downloader scalability target, not a product promise to provide a
  load-testing tool, traffic generator, or multi-server orchestration system.
- Multi-instance coordination, distributed scheduling, and server-fleet
  management are non-goals.

## Security

Requirement:

- no RCE,
- no leak,
- no use-after-free,
- correct read/write placement,
- poweroff recovery.

Design coverage:

- `security-recovery.md`: safe paths, no-RCE policy, RPC security, range
  rules, control journal, crash scenarios, unsafe/FFI policy.
- `redirect-policy.md`: redirect limits, identity revalidation, and
  cross-origin credential/header handling.
- `protocol-modernization.md`: proxy modes, CONNECT/SOCKS destination policy,
  TLS/DNS behavior, and cookie scope.
- `detailed-http-first-slice.md`: reserved headers, exact response validation,
  provisional lease commit/abort, and body-size limits.
- `detailed-ftp-sftp.md`: FTP/SFTP offset validation and protocol-specific
  concurrency limits, FTP data-endpoint validation, plus the SFTP
  host-key/authentication/algorithm policy resolved by
  `final-preimplementation-review.md`.
- `rate-limiting.md`: ingress-debited payload rate accounting, deterministic
  hierarchical arbiter fairness/debt evidence, live scoped reconfiguration, and
  bounded discard budgets.
- `session-persistence.md`: private file permissions and secrets-at-rest policy.
- `disk-adapter.md`: offset-only writes, storage validation, fsync/rename
  policy.
- `README.md`: storage model and recovery model.

Acceptance:

- shell hooks disabled by default,
- safe path builder is mandatory,
- pure Rust crates forbid unsafe code,
- every write is global-offset validated,
- crash tests pass for all journal states,
- persisted progress is accepted only under a matching root/file identity or
  the explicit per-piece-digest rebind protocol,
- generic resume cannot approve an SFTP host key; approval names the current
  challenge id and displayed fingerprint,
- FTP passive/active data endpoints cannot escape the approved control peer or
  the complete destination policy,
- dependency/source gates reject unpatched SuppaFTP/russh-sftp receive paths;
  FTP control replies and SFTP packet framing fail before over-cap allocation,
- the FTP dependency never formats credentials, raw commands/replies, paths,
  FEAT text, or listings into logs at any enabled level,
- every cookie jar is initialized with the pinned/versioned Mozilla Public
  Suffix List and cookie support fails closed if it is unavailable or invalid,
- the owned cookie wrapper, not cookie_store alone, enforces the defined
  schemeful-site SameSite context and rejects `SameSite=None` without `Secure`,
- proxy-side DNS/CONNECT cannot bypass private-address policy for untrusted RPC,
- redirects re-run destination, identity, cookie, and credential checks,
- user headers cannot override generated framing/range/security headers,
- plaintext secrets do not enter session, journal, export, or temporary files.

## Compactness

Requirement:

- pooled buffers,
- small RAM footprint,
- small binaries.

Design coverage:

- `README.md`: memory model and binary-size rules.
- `disk-adapter.md`: buffer ownership and bounded disk fallback.
- `buffer-pool.md`: lazy/preallocated buffer policy, lifecycle, scaling caps.
- `performance-profiles.md`: named-domain plus global resident permits,
  metadata/cache cardinality, protocol ingress, RPC work, and handle budgets.
- `zero-copy.md`: allowed zero-copy optimizations and forbidden shortcuts.
- `implementation-plan.md`: memory peak gates.

Acceptance:

- buffer pool has hard budgets,
- external SFTP vectors and transform output have explicit ingress/domain
  budgets rather than hiding outside the pool,
- segment payloads are not stored in state,
- feature-gated build profiles exist,
- CI tracks `minimal` binary size.

## Community Reuse

Requirement:

- choose performant, battle-tested libraries such as libuv, Tokio, Asio,
  libtorrent, or libevent.

Design coverage:

- `library-choice.md`: comparison and final choices.
- `README.md`: selected components and build profiles.

Acceptance:

- no custom event loop in the first production version,
- libtorrent used for full BitTorrent build,
- libtorrent isolated from the main control/network event loop,
- platform backends selected by measured capability, not compile-time hope.

## Maintainability

Requirement:

- well documented and tested,
- edge cases, fuzzing, rejections,
- undefined behaviors explicit.

Design coverage:

- `README.md`: testing strategy.
- `configuration.md`: typed option registry and generated docs.
- `security-recovery.md`: rejection rules and unsafe-code policy.
- `implementation-plan.md`: staged exit criteria.
- `implementation-readiness.md`: generated artifacts, hard invariants, and
  definitions of ready/done.
- `detailed-core.md`: complete state transition table, GID codec, scheduler
  commands, snapshots, and persistence hooks.
- `detailed-config.md`: generated option/runtime/error artifacts and behavioral-
  fingerprint enforcement.
- `detailed-storage.md`: journal schema, lease transaction, recovery, and
  finalization contracts.
- `detailed-runtime.md`: queue/ownership/cancellation/shutdown contracts.
- `detailed-http-first-slice.md`: reviewed first-slice HTTP state-machine
  contract plus executable fresh/strong-ETag resume and the public Phase-3B
  known-length multi-mirror path: verified TLS, generated destination policy,
  bounded DNS cache/singleflight with leader/follower waiter admission and
  cancellation cleanup, deterministic Happy Eyeballs fallback timing, DNS-
  answer-set-keyed reconnect revalidation, redirect/proxy/auth/cookies,
  segmented range/retry recovery, process-owned bounded ingress/rate control,
  live stats, worker supervision, pre-network durable-piece digest readback,
  strong-ETag-bound single-source/strict-fallback recovery, user SHA-256-bound
  strict concurrent mirrors and final verification, public stale-validator
  policy, crash-safe representation-generation restart, same-origin strong-ETag
  endgame fencing/rollback, bounded SHA-256 `Repr-Digest` parsing and body
  verification, exact-range cross-origin endgame fencing, journaled response
  digest evidence, persisted digest-only restart identity with local and network
  durable-range revalidation, first-slice checkpoint-boundary interruption
  recovery with exactly-once lease dispositions, pre-read storage-backpressure
  admission, and the Phase-4 shared dispatcher/control/event checkpoint.
  Broader RFC
  9530/Metalink identity, `Content-Digest`, additional checksum algorithms,
  Last-Modified/unsafe-override resume, HTTP/2, growing bodies,
  aria2 text-session compatibility, and the remaining Phase-4 exit
  matrix remain pending.

Legacy HTTP Basic RPC now has startup registry/CLI/environment validation,
HTTP request and WebSocket upgrade gates, independent method tokens, redacted
failures, and shared rejection throttling. Success/rejection and event-isolation
tests pass on Linux, MSRV, and native Windows-GNU; the broader public-interface
matrix remains under `P4-09`.

Acceptance:

- parser and journal fuzz targets exist; the implemented HTTP/journal targets
  are checked in under `fuzz/` and are workspace-excluded from production builds,
- every untrusted parser and variable-length metadata path rejects its hard
  byte/item/depth cap before proportional allocation or work,
- unsupported options fail explicitly,
- docs generated from option metadata,
- every runtime state transition is typed and tested.
- implementation cannot start for a module until its option, state,
  rejection, recovery, tests, diagnostics, and feature-gate behavior are
  defined.
- no option may be marked implemented unless its non-default behavioral test
  observes an effect in the owning subsystem.

## Cross-Platform

Requirement:

- major desktop platforms.

Design coverage:

- `README.md`: Linux, Windows, macOS desktop target.
- `event-backends.md`: Tokio/Mio network readiness by supported platform and
  separately probed disk backend capabilities.
- `README.md`: standalone Cargo workspace artifact strategy and
  native/cross-build rules.

Acceptance:

- CI runs Linux, Windows, and macOS,
- backend probe tests pass on each,
- unsupported backend selection never hard crashes,
- native Linux, Windows, and macOS CI builds the declared release artifacts,
- WSL Linux and native MinGW toolchains are never mixed into one target build.

Current status: local WSL and MinGW checks are development evidence, not the
required native Linux, Windows, and macOS release/tag matrix. Workflow entries
for macOS and MSRV 1.88 are gates, not proof that those gates passed for this
checkpoint.

## aria2-Style Configurability

Requirement:

- configurable like aria2.

Design coverage:

- `configuration.md`: typed option registry, layering, input file, RPC changes,
  compatibility matrix, flat config, optional URL default rules, reload, and
  dump/export policy.
- `configuration.md`, `detailed-config.md`, and
  `implementation-readiness.md`: parsed-only options fail CI.

Acceptance:

- CLI help, config parser, input-file parser, RPC option allowlists, docs, and
  compatibility matrix share one source of truth,
- every option declares both allowed scope and runtime update behavior,
- aria2-style flat config remains the stable compatibility format,
- optional URL rules apply only deterministic per-download defaults and reject
  startup-only or unsafe options,
- reload is explicit, atomic, and limited by each option's runtime update
  behavior,
- dump/export redacts secrets by default and never silently rewrites the user's
  hand-written config,
- `changeOption` and `changeGlobalOption` drive real engine behavior,
- per-download runtime changes are either live, waiting-only, controlled
  restart/new-generation, startup-only rejection, or unsafe-compat rejection,
- waiting/active/stopped queues match aria2 semantics for implemented methods.
- active restart-only options automatically restart, appear as `waiting`, and
  emit no pause event,
- `P4-04`: an acknowledged restart patch retains its identity and staged
  options through delayed cancellation and recovery; only the actual flushed
  generation promotion advances the current SQLite mirror,
- the generated compatibility matrix separates aria2 options from extensions,
- compatibility GIDs are exactly 16 lowercase hexadecimal characters,
- error codes and runtime-update outcomes come from one generated vocabulary.

## Third-Party Integrations

Requirement:

- useful for existing aria2 integrations and embedders.

Design coverage:

- `apis-and-embedding.md`: aria2-compatible RPC, native Rust API, optional C
  ABI, runtime ownership, stdio transport, versioning.

Acceptance:

- implemented RPC methods drive real engine behavior,
- RPC-over-stdio uses the same dispatcher and scheduler as HTTP/WebSocket RPC,
- native API is typed and not just JSON-RPC wrapped in-process,
- C ABI is opaque-handle based and introduced only after Rust API stability,
- CLI/RPC/library share the same scheduler and state model.
- direct method tokens follow one policy on HTTP, WebSocket, and stdio;
  `P4-01` routes `system.multicall` before direct-method authentication and
  checks every inner member without an outer token,
- each WebSocket/stdio client has a bounded queue, documented coalescing, and a
  successful-authentication gate before it receives pushed events, plus a
  snapshot recovery path after dropped status/stat events,
- RPC request, response, batch, list, per-client pending-work, and serialized
  output limits are enforced before dispatch/amplification; list queries clone
  immutable membership indexes instead of blocking scheduler progress. Per-client
  and global `rpc_budget` reservations cover pending response bytes, with at
  most four accepted requests and 8 MiB of request/command state per client.

## Protocol Modernization

Requirement:

- modern HTTP/TLS/DNS behavior without compromising correctness.

Design coverage:

- `protocol-modernization.md`: HTTP keep-alive, HTTP/2, HTTP/3/QUIC, TLS 1.3,
  ECH, DoH/DoT, proxy interaction, and range semantics.

Acceptance:

- HTTP/1.1 keep-alive and TLS 1.3 are baseline,
- OS trust store is the default CA source, with custom CA stores supported,
- HTTP/2 multiplexing is supported when the HTTP stack is mature,
- HTTP/3, ECH, DoH, and DoT are feature-gated with clean fallback,
- range validation and disk placement rules are identical across protocol
  versions,
- range/split/resume uses identity encoding and known representation lengths,
- a growing sequential layout has an explicit maximum/final-extent commit,
- proxy/redirect destination validation occurs at the final connection hop,
- generated framing, range, validator, and credential headers are authoritative.

## Retry And Scheduling

Requirement:

- configurable retry behavior,
- optional slow-slot queue scheduling.

Design coverage:

- `retry-policy.md`: retry classes, status-code sets, `Retry-After`, caps,
  stale connection and stale validator policy.
- `download-scheduling.md`: active/waiting queues, optional slow-slot demotion,
  retry-wait slot policy, and backpressure guardrails.
- `stats-and-stalls.md`: packet-independent stall/slow/backpressure state.

Acceptance:

- retry triggers are explicit and bounded,
- `P4-03`: production admission and recovery accept every canonical retry
  setting; invalid options reject before mutation and subsequent calls work,
- `Retry-After` is respected only for retryable responses and capped,
- stale connection retry is distinct from stale validator restart/failure,
- slow-slot demotion is off by default,
- local disk/CPU/buffer/rate-limit backpressure never triggers remote-slow
  demotion,
- retry waits do not hold transfer buffers.
- retry waits update one bounded, non-secret, lease-correlated task diagnostic
  without requiring another network packet; recovered diagnostics never invent
  an unpersisted HTTP status or transport trigger.
- discarded traffic is separately observable and bounded; HTTP now atomically
  charges finite process/final-origin-host/task/attempt scopes with no refund,
  stops polling/retrying on exhaustion, and exposes consumed/remaining credit;
  other protocol producers remain phase gates. The configured
  limiter's wire-versus-committed accounting is explicit rather than permitting
  an unbounded bypass.

## Session Persistence

Requirement:

- poweroff-safe resume and maintainable session state.

Design coverage:

- `session-persistence.md`: text vs control-file vs SQLite vs KV store
  comparison and hybrid decision.
- `security-recovery.md`: control journal format and crash scenarios.

Acceptance:

- per-task progress survives torn writes,
- global queue and stopped results are transactional,
- `P4-05`: active source replacement drains before mutation, preserves desired
  pause, resumes automatically when eligible, and recovers old/new source sets
  at the commit boundary without returning an ordinary rejection after commit,
- text export/import exists for debugging/migration,
- RocksDB/LMDB are not mandatory dependencies,
- one serialized appender owns sequence assignment, payloads, and segment
  continuity,
- checkpoints encode the durable piece map in bounded, canonical
  `PieceStateChunk` records rather than replaying an unbounded live history,
- the exact SQLite v2 schema, exact v1 migration, pragmas, and fail-closed
  version policy are validated; hot rollback recovery is process-crash tested,
  backup publication has integrity and no-clobber coverage, and journal-install
  transitions have transaction, reopen, pointer-invariant, and stale-command
  coverage; explicit crash-point matrices remain phase gates,
- an existing broad persistence parent, intermediate symlink/reparse component,
  sidecar beside a missing or empty main database, hard-link alias, and
  symlink/non-regular SQLite artifact fail closed; missing directories are
  private at creation,
- hot rollback page-one and committed-WAL preflight rejects a newer committed
  schema before artifact mutation, supports rollback recovery with a damaged
  main header, and rejects legacy page-size-zero rollback journals,
- `${db}.ariax-owner-lock` enforces cooperative single-writer ownership among
  Ariax processes; external raw SQLite writers bypass it and are unsupported,
- task/install/host-challenge/stopped-result reads are count/byte bounded, task
  options are policy-checked on read, and cross-queue moves preserve dense
  ordering in one transaction,
- each stopped result has exactly one retained `Stopped` task row as its queue
  owner; terminal publication and deletion are atomic dense transactions, and
  result deletion removes metadata without deleting downloaded output,
- host-key challenge text is valid UTF-8, the stored SHA-256 fingerprint matches
  the presented key, and the referenced task is paused; the same semantic
  validation runs before v1 migration backup creation so invalid inputs do not
  accumulate backups,
- WAL and DELETE are transactionally page-one write/rollback probed; truncate
  checkpoint behavior and validated, file-synced, no-clobber backup publication
  are tested, including orphan destination-sidecar rejection and
  ASCII-case-insensitive rejection of every `-wal`, `-shm`, or `-journal`
  destination suffix, with Unix parent-directory sync and no equivalent Windows
  directory-entry crash-durability claim; owned temporary backup main/sidecar
  files are cleaned after success and validation failure,
- the current backup primitive does not yet recover a residue created by a
  crash or temporary-unlink failure from destination-link publication until
  removal is durably synced; verified same-file cleanup or a native atomic
  no-replace primitive, with crash-point and unlink-error injection across that
  entire window, is required before production use or tagging,
- journal installation completion/clearing requires the exact
  gid/checkpoint/new-journal token and cannot bypass primary-pointer invariants,
- every task persists and validates its output-root binding; relocation/rebind
  is explicit and identity- or digest-proven,
- provisional range attempts recover as aborted/pending unless a commit record
  is present,
- journal/layout/options precedence is deterministic when SQLite and task state
  disagree,
- authentication/proxy/cookie secrets are omitted or stored through the declared
  secure credential mechanism.

## Event API Selection

Requirement:

- detect and use best available or user-configured event API,
- fallback instead of hard crash.

Design coverage:

- `event-backends.md`: Tokio/Mio network backend contract, legacy option aliases,
  separately selected disk backends, graceful degradation, and no-hard-crash rule.
- `configuration.md`: `event-backend` and aria2-compatible aliases.

Acceptance:

- user-selected unavailable supported backend falls back or cleanly errors
  depending on `event-backend-fallback`,
- fallback reason is logged and exposed via diagnostics,
- live backend failure uses a stop/drain/abort/close/reopen barrier; only a fully
  settled old epoch may readmit work under a fresh backend epoch and task
  generation,
- legacy `event-poll` configurations are accepted through the documented alias,
- no independent raw reactor is promised inside the Tokio runtime.

## Build And Artifact Integration

Requirement:

- ship the Rust implementation as a standalone project without breaking or
  silently replacing the existing aria2 C++ release artifact.

Design coverage:

- `README.md`: standalone `ariax` workspace, pinned aria2 reference checkout,
  packaging, native and cross-toolchain policy.
- `implementation-plan.md`: Phase-0 build baseline and Phase-7 parity decision.
- `library-choice.md`: Tokio/Mio and platform adapter ownership.

Acceptance:

- the workspace fails cleanly when its toolchain/dependencies are missing,
- the aria2 C++ tree is used only as a pinned read-only compatibility reference,
- release CI builds the exact feature profiles on Linux, Windows, and macOS,
- Cargo.lock and dependency-vendoring policy are represented in release tarballs,
- an `aria2c` replacement/alias is not shipped until parity, migration, and
  rollback are explicitly approved.

## Non-Goals

Requirement clarification:

- load-testing and multi-instance/server orchestration are explicitly non-goals.

Design coverage:

- `README.md`: non-goals and internal scalability validation wording.
- `implementation-plan.md`: benchmark gates are implementation validation only.

Acceptance:

- no CLI, RPC, or library API is designed around generating test traffic,
  managing remote test servers, or coordinating multiple downloader instances,
- performance tests may use local harnesses or simulators, but those harnesses
  are not supported product interfaces.
