# Implementation Plan

Status: implementation remains underway for the overall roadmap. The scoped
Phase-3B/3C HTTP(S) downloader milestone is checkpointed at `30b70c5`, and the
Phase-4 control-plane checkpoint is executable at `71acb03`, with the Phase-4B
implementation and native Windows benchmark evidence at `f2f560e`. Phase 0
contracts and the gated Phase-1/2 core, config,
persistence, recovery, runtime, storage, and native capability work have
executable checkpoints. The completed HTTP checkpoint covers
the first public known-length multi-mirror HTTP(S) vertical slice: atomic
task/source/current-option persistence before scheduler publication;
source-aware restart recovery; verified TLS and bounded HTTP/1.1 reuse;
policy-owned DNS cache/TTL/singleflight, generated special-use filtering and
Happy Eyeballs; redirect/proxy/auth/cookie policy; non-overlapping range leases
with bounded persisted retry and durable-piece restart; process-owned transport
pooling, bounded body ingress, hierarchical download-rate arbitration,
lowest-speed/stall diagnostics, packet-independent live stats, real worker
supervision; pre-network descriptor-bound SHA-256 validation of recovered
pieces; exact strong-ETag/resource/length recovery binding for single-source and
strict-fallback range tasks; persisted user SHA-256 final verification and
digest-bound restart; strict concurrent mirrors when that checksum is present;
bounded stale-validator `fail`, fresh `revalidate`, and descriptor-authorized
`restart-if-safe` with crash-safe generation persistence and old-progress
invalidation; and `addUri`, `tellStatus`, `pause`, `remove`, and
`getGlobalStat` over loopback HTTP and Content-Length stdio. The bundled Mozilla
Public Suffix List is version/hash/license pinned and verified by generated
contracts. `tellStatus` now also carries one bounded, lease-correlated retry
decision with exact live cause/status/caps/wait/action fields and a deliberately
coarser recovered journal view.

The checkpoint intentionally does not complete all of Phase 3 or the project
definition of done. Same-origin endgame duplicate ranges with a strong ETag and
the bounded SHA-256 `Repr-Digest` exact-range cross-origin profile are now
executable under the overlap-fence contract. Matching probe digests retain
secondary mirrors as endgame-only peers; they do not authorize ordinary
concurrent pieces or replace the persisted whole-file checksum. `Content-Digest`,
digest parameters/coverage metadata, alternate algorithms, server-advertised
whole-entity admission, Metalink chunk hashes, Last-Modified/unsafe-override
resume, HTTP/2, growing/chunked transfers, the Phase-4 query/bulk progress gates,
native Linux benchmark acceptance, and the full release-platform matrix remain
phase/tag gates. Hot-backup
publication residue is now recovered through
descriptor-bound same-file/link-count validation with no-clobber collision and
crash/unlink fault coverage.

The profile-capacity slice is now executable: profile-derived process/socket/file
and resident limits are resolved, HTTP direct and proxy sockets consume the
shared process/socket permits, selected storage files consume two permits for
their capability and blocking-lane descriptors, and transport, ingress, and
storage buffers share the resident budget. The local C10k/active-range harness
passes under an isolated raised Linux handle limit and rejects insufficient
native capacity explicitly; optimized Linux and native Windows-GNU runs also
exercise the complete socket and active-range phases. Adaptive tuning,
non-HTTP domain wiring, the broader evictable file-handle LRU, and the remaining
release-platform matrix remain phase gates. Linux RSS and native Windows-GNU
working-set sampling are mandatory benchmark checks rather than inferred from
the permit envelope.

The HTTP discard-bound slice is executable: a process-owned ledger atomically
charges process, canonical final-origin host, task, and attempt scopes without
refund; body polling stops when no credit remains; probe, retry/cancel,
checksum, stale queued-chunk, and endgame settlement paths reconcile the same
counter; and RPC status exposes consumed/remaining task credit. FTP/SFTP and
libtorrent discard producers remain later adapter work. The standalone
workspace-excluded `fuzz/` package now provides bounded cargo-fuzz targets for
HTTP response/request headers, retry specifications, discard accounting, and
journal replay. Deterministic storage-boundary ENOSPC, permission-denied, and
  short-write, partial-fsync, torn-tail, and same-inode publication faults now
  leave no false durable piece and replay cleanly; publication residue is
  removed only after installed header/linkage replay succeeds. Child-process
  exit plus parent-driven kill coverage exercises every provisional-write,
  data-sync, and journal-publication barrier on Linux and native Windows-GNU,
  and a deterministic Linux power-loss cut model verifies the durable prefix.
  Real poweroff and the remaining release-platform matrix remain final Phase-3
  gates.

This is a staged plan for building the design without repeating the incomplete
rewrite pattern. The native-startup orchestration boundary, central-journal
filesystem backends, runtime-effect boundary, product bootstrap, and the
scoped Phase-3B/3C HTTP dispatch slice are now executable and checkpointed.
The deferred capabilities, real hardware poweroff evidence, and remaining
release-platform matrix above remain later roadmap gates; they are not part of
this scoped milestone.

## Phase 0: Contracts, Compatibility Inventory, And Build Baseline

- Use `implementation-readiness.md` as the readiness gate for all later module
  work.
- Generate aria2 option inventory from a pinned aria2 source checkout
  (`src/OptionHandlerFactory.cc` and `doc/manual-src/en/aria2c.rst` at a
  recorded commit hash).
- Create the machine-readable option/runtime-compatibility matrix, including
  whether aria2 implements each option, aria2's active-change behavior, this
  design's behavior, and whether any difference is intentional.
- Mark every option as implemented, partial, unsupported, unsafe_compat, or
  feature_gated.
- Assign every option an explicit scope and runtime update behavior:
  `live`, `waiting_only`, `active_restart`, `new_generation`, `startup_only`,
  `unsafe_compat_only`, `bt_live`, or `bt_restart_required`.
- Define URL default rules schema and keep it separate from flat config.
- Create RPC method matrix with source of truth for each response field.
- Create the complete task-state transition and aria2 wire-projection matrices.
- Freeze the v1 journal record layouts, checkpoint `PieceStateChunk` encoding,
  persisted output-root binding/rebind protocol, and exact SQLite v2 schema,
  v1 migration, pragmas, backup, and recovery-precedence matrices.
- Freeze the exact 16-hex GID codec, RPC token convention, error-code vocabulary,
  reserved-header policy, and secrets-at-rest policy.
- Create a new-option inventory from the design docs and fail CI if a
  documented option is missing registry metadata.
- Generate the profile resource/cap matrix: global resident and named-domain
  permits, queue bytes/items, handle/socket/file subcaps, parser/metadata
  cardinality, DNS/cache limits, RPC amplification/work limits, and protocol
  ingress budgets.
- Generate the special-purpose-address classifier from a pinned IANA registry
  snapshot; record source date/hash/license and fail CI when generated policy
  tables drift from the pinned input or security-policy overrides.
- Stand up the standalone Cargo workspace and build the experimental `ariax`
  artifact; record the pinned aria2 reference checkout used for compatibility
  generation.
- Add `rust-toolchain.toml` for Rust 1.97.1/edition 2024, commit `Cargo.lock`,
  declare and test MSRV 1.88, and generate the Linux/macOS/Windows target and
  native-ABI matrix. Windows MSVC is the primary release ABI; Windows GNU is a
  secondary all-MinGW build and never mixes native ABIs.
- Pin the direct choices in `library-choice.md`, record feature unification and
  native dependencies, and run license/advisory/source-policy checks with
  `cargo-deny` (committed `deny.toml` is the normative policy) plus scheduled
  `cargo-audit`. Build release binaries with `cargo-auditable` and produce a
  CycloneDX SBOM (`cargo-cyclonedx`) and dependency tree for each release
  profile.
- Pin the patched russh-sftp and SuppaFTP sources/commits and assert their
  inbound-frame/control-reply cap diffs and provenance in CI; pin/version/hash/
  license the bundled Mozilla Public Suffix List and construct a cookie-jar
  smoke test that rejects public-suffix cookies. Exact prerelease dependencies
  inherited from selected crates are allowlisted individually, never by a
  wildcard policy.
- Assert the SuppaFTP patch never formats raw commands, credentials, paths,
  welcomes/replies, FEAT text, or listings into logs; canary-secret tests capture
  every enabled log level. Do not use its workspace-global `no-log` feature.
- Assert rusqlite's defaults are disabled and its direct feature roots are
  exactly `bundled+backup+cache+limits`; freeze the resolved rusqlite closure as
  `backup+bundled+cache+hashlink+limits+modern_sqlite` and the corresponding
  `libsqlite3-sys` closure. Exercise hot backup and verify every required
  per-connection SQLite limit before persistent-mode tests run.
- Assert russh resolves with defaults disabled and exactly ring+flate2+rsa, with
  no aws-lc/DSA/DES feature; cookie tests install the pinned PSL and exercise the
  owned SameSite filter across redirect and mirror contexts.
- Encode the README artifact/profile/panic matrix as Cargo profiles
  (`release-cli` abort, `release-capi` unwind) and per-artifact CI release
  jobs; assert in CI that no job builds the C-ABI artifact under
  `panic=abort` and that feature unification enables exactly one rustls
  crypto provider.
- Record native Linux and Windows baselines for journal/data sync, queue wakeup,
  buffer ownership, and minimal-binary size. WSL/DrvFS numbers are diagnostic,
  not release baselines.

Exit criteria:

- Every option marked `implemented` has an assigned behavioral-fingerprint test
  id; execution of those tests starts with the owning implementation phase.
- CLI help, docs, and RPC allowlists come from the same registry.
- Design option inventory and registry metadata are in sync.
- The state/wire, GID, auth, error, header, persistence-secret, build-artifact,
  and runtime-compatibility contracts have no unresolved entries.
- The workspace builds the declared Rust targets on Linux, Windows, and macOS or
  fails with an actionable toolchain/dependency error.

## Phase 1: Core Types And Minimal Scheduler

- `SafePathBuilder` and persisted output-root/file-identity binding
- `FileLayout`
- `GlobalOffsetMapper`
- `ControlJournal`, including checkpoint-only `PieceStateChunk`
- Exact SQLite v2 `SessionStore` schema, v1 migration, pragmas, backup, and
  identity-preserving relocation/digest-verified rebind entry points
- `OptionRegistry`
- Flat config parser, config check, and redacted effective-config dump
- `TaskState`, complete transition table, scheduler queue model, and immutable
  status snapshots
- Minimal real scheduler commands required by the first vertical slice
- `BufferPool`
- `StatsSampler`
- Retry policy parser and status-code set parser
- URL rule matcher

Detailed design inputs:

- `detailed-core.md`
- `detailed-config.md`
- `detailed-storage.md`
- `detailed-runtime.md`

Exit criteria:

- Unit and property tests for all pure types.
- Fuzz targets for paths, control journal, options, URL rules, retry status
  sets, and HTTP range parser.
- Journal replay covers sequence gaps, segment rotation, provisional lease abort,
  hash failure, and store-precedence conflicts.
- Journal checkpoint compaction passes its trigger, crash-point, chunking, and
  descriptor-budget tests; replay time after compaction is bounded and
  measured.
- Root relocation/rebind tests prove that adjacency, names, mtimes, and copied
  control files cannot authorize progress; retained pieces require matching
  identities or per-piece digest evidence.
- SQLite schema creation, exact v1-to-v2 migration, raw hot-rollback page-one
  and committed-WAL unsupported-newer-version rejection without artifact
  mutation, supported hot rollback recovery, legacy page-size-zero fail-closed
  behavior, owner-lock contention, private-path/artifact rejection, sidecar
  rejection beside a missing or empty main database, case-insensitive reserved
  backup-suffix rejection, bounded semantic reads, dense cross-queue
  transitions, one-to-one stopped task/result retention and atomic deletion,
  host-key UTF-8/fingerprint/paused-task validation, preflight-before-backup
  rejection of invalid v1 semantics, WAL/DELETE write probes and
  checkpointing, file-synced no-clobber validated backup, and tokenized
  journal-install pointer transaction/reopen, stale-command, and crash-point
  tests pass. Backup crash-point coverage must prove that every crash from
  destination-link publication until temporary-link removal is durably synced
  either leaves a directly usable destination or performs verified recovery
  without deleting a raced destination replacement. The same matrix must inject
  temporary unlink failures after publication. That matrix is now executable
  on the file-backed session store, including exact-link-count rejection of
  unknown aliases and preservation of invalid same-file residue.
- Finalize intent/redo recovery passes every crash-point and collision test.
- Scheduler matrix tests cover every state × semantic action and aria2 wire
  projection. Executor tests cover the implemented command/event subset,
  barriers, stale correlation tokens, atomic rejection, bounded effects, and
  queue serialization; remaining matrix-only internal actions stay explicit.

## Phase 2: Runtime And Backends

- Tokio/Mio network runtime using its supported platform readiness backend; no
  second raw network reactor is built into the same runtime.
- Linux low-level io-uring disk adapter prototype, with the bounded blocking
  backend as the runtime/capability fallback.
- Windows overlapped I/O prototype.
- Bounded blocking disk fallback.
- `DiskBackendKind` enum dispatch and total `DiskWriteOutcome` buffer ownership.
- Bounded submission queues and non-rejecting completion drain topology.
- Backend-epoch live-failover barrier and generation readmission.
- Root/data file-handle LRU with process/profile budgets and identity-checked
  reopen.
- Backend diagnostics RPC.

Exit criteria:

- Forced backend failure tests prove graceful fallback.
- No blocking disk I/O on network runtime threads.
- Every submitted buffer is returned or quarantined exactly once across success,
  error, cancellation, and shutdown.
- A settled live failover reopens only under a new backend epoch/generation;
  cancellation uncertainty faults the affected tasks instead of reusing handles.
- File-handle pressure never evicts in-flight/dirty handles and never leaves a
  balanced durability group without its data/journal barrier.

## Phase 3: HTTP(S) Downloader

- Hyper/hyper-util client integration with downloader-owned connector and a
  separately budgeted body-frame ingress adapter.
- HTTP/1.1 keep-alive with strict connection identity.
- HTTPS with TLS 1.2/1.3, OS trust store defaults, and custom CA support.
- Proxy/no-proxy behavior for HTTP(S), including destination validation at the
  final proxy hop.
- Known-length, identity-encoded sequential download.
- Resume with validator handling.
- Segmented range download with dynamic non-overlapping range leases and strict
  validation.
- Provisional `LeaseId` writes with explicit commit/abort.
- Reserved generated-header enforcement and redirect revalidation.
- Disk queue integration.
- Rate limiting and retry policy.
- Control journal recovery.
- Packet-independent stats and stall detection.
- Minimal JSON-RPC `addUri`, `tellStatus`, `pause`, `remove`, and
  `getGlobalStat` against the Phase-1 scheduler. This is the RPC surface required
  by the first implementation slice; the full control plane remains Phase 4.

Phase-3B checkpoint boundary: the items above are executable for known-length
HTTP(S) with ordinary multi-mirror URI admission, including bounded ingress,
hierarchical rate limiting, lowest-speed diagnostics, and persisted range retry
waits. Single-source and strict-fallback recovery additionally verifies
journaled durable-piece SHA-256 evidence before network access and requires the
exact persisted strong ETag, resource fingerprint, and length before releasing
pending ranges. A persisted user SHA-256 checksum additionally admits strict
concurrent mirrors, permits digest-bound recovery, and gates `TaskComplete` on a
bounded descriptor-based whole-file hash. Stale-validator `fail`, fresh
`revalidate`, and bounded `restart-if-safe` are wired through public option
admission, worker supervision, exact next-admission snapshot persistence, and a
durable `restarting` intent marker whose recovered exact snapshot promotes with
the original representation-restart reason. The new generation invalidates all
old durable pieces before descriptor-bound rewrite. Same-origin strong-ETag
endgame duplicate fencing, candidate settlement,
dirty-overlap rollback, and crash-safe replay are executable. Strict mode also
negotiates and bounds SHA-256 `Repr-Digest`, verifies probe and range bodies,
journals accepted response digests, and permits a secondary origin only for an
exact-span endgame race whose head digest matches the original. A flushed
`HttpRangeIdentity` now binds the settled probe digest and representation length
before leases; restart reprobes matching mirrors and re-fetches every locally
verified durable range for body/digest comparison before pending ranges can be
released. Missing or mismatched identity evidence fails closed. Broader RFC
9530/Metalink identity, additional checksum algorithms, Last-Modified/unsafe-
override resume, adaptive profile tuning, and the native portion of the full
Phase-3 exit criteria below still gate a later phase completion claim.

Detailed design input:

- `detailed-http-first-slice.md`

Exit criteria:

- Fault tests for ignored Range, short/oversized body, lease abort, generated-
  header conflict, redirect/proxy SSRF, disk full, process kill, and poweroff
  simulation. Ignored/short/oversized responses, lease aborts, generated-header
  conflicts, redirect/proxy SSRF, deterministic ENOSPC/permission-denied/
  short-write/partial-fsync faults, torn-tail and same-inode rotation recovery,
  child-process exit plus parent-driven kill barriers, and the Linux power-loss
  cut model are executable. The five storage boundaries run on Linux and native
  Windows-GNU and distinguish an OS-surviving process kill from a simulated
  power-loss cut. Hot-backup publication/link cleanup, raced-destination
  preservation, and unlink-failure recovery are executable too. The deferred
  native overlapped backend, real hardware poweroff evidence, and remaining
  release-platform matrix are not inferred from the blocking fallback model.
- The first-slice interruption matrix cancels one byte before a piece boundary,
  exactly at the boundary, and one byte into the next lease. Recovery proves the
  exact durable prefix and audits the journal so every begun lease has exactly
  one terminal commit or abort disposition.
- Discard fault coverage proves bounded oversized/short-body overrun, atomic
  process/host/task/attempt exhaustion, no refund after abort/rollback, prompt
  source/retry-cycle stop, and separate consumed/remaining diagnostics. The
  HTTP guard, short-body exhaustion path, and URI-scoped oversized-frame fault
  are executable: the latter crosses the real range worker, ingress/rate/discard
  accounting, storage lease, retry policy, stats, and journal; it writes no
  provisional bytes, aborts once as `oversized_body`, and disables the source
  without another attempt. Deterministic process/task/host tests and standalone
  fuzz targets remain in `fuzz/`. The required local protocol/crash fault matrix
  is executable; real hardware poweroff and the remaining release-platform
  matrix still gate the final Phase-3 completion claim.
- 1,000 active HTTP range benchmark with bounded memory. The checked-in harness
  now drives 1,000 real loopback `206` range responses under the shared ingress,
  socket, and resident permits. Optimized Linux and native Windows-GNU runs are
  recorded in `performance-profiles.md`; both require measured process
  residency below the profile target while all 1,000 responses remain live.
- 10,000 concurrent low-activity socket benchmark with the reusable idle HTTP
  pool still capped at its profile limit; measured accounted reservations and
  process RSS must fit the profile target/permit envelope and documented native
  stack/headroom allowance. Linux `/proc` RSS and native Windows-GNU working-set
  readings are executable; the remaining release-platform matrix still gates
  completion.
- DNS tests now cover positive/negative TTL clamps, TTL=0, answer-count limits,
  bounded singleflight plus per-name/total leader-and-follower admission,
  all-waiter cancellation cleanup, and special-use-address filtering. Virtual-
  time coverage starts the second Happy Eyeballs racer at the exact configured
  fallback delay, and a zero-TTL answer-set change opens a newly admitted direct
  connection rather than reusing the prior idle connection. This exit gate is
  executable.
- Rate-arbiter tests now cover bounded overshoot debt and accurate debt
  diagnostics, FIFO refill fairness, explicit scoped limits across default
  reconfiguration, cancellation cleanup, and 1,000 active stream-scope
  tracking. The rate-limit evidence is deterministic under virtual time.
- Storage/buffer backpressure withholds the next `PrepareRead` admission before
  another HTTP body poll, publishes the bounded backpressure diagnostic, and
  remains cancellation-responsive while capacity is unavailable.
- A scheduler cancellation delivered after disk completion but before the
  provisional write acknowledgement aborts the current lease before commit,
  publishes `CancellationDrained`, and recovers with no false durable piece.
  Multi-range disk rejection likewise gives every opened lease exactly one
  `storage_rejected` abort instead of a generic retry disposition.
- Stuck socket speed drops to zero without waiting for another packet.
- Retry waits expose a bounded non-secret diagnostic without waiting for
  another packet: exact live trigger/status, attempt credit, wait policy,
  source/piece/lease correlation, disposition, and next action; restart marks
  journal-reconstructed error-class/reason evidence as recovered.
- The minimal RPC methods drive the real scheduler and expose only aria2's closed
  status/GID/error shapes.
- Minimal process shutdown executes stop-admission, bounded HTTP worker drain,
  all-journal flush/close, bounded session-owner join, and final clean/dirty
  SQLite publication in coordinator order. Active synchronous abort and
  asynchronous worker timeout persist a dirty checkpoint; future native
  disk/CPU and BitTorrent lanes must surface the same typed outcome.

## Phase 4: Control Plane And RPC

Phase 4B implementation: the shared dispatcher, method-token/HTTP Basic authentication,
notifications, bounded batches/multicall/list results, query and queue controls,
unique GID prefixes, typed task/global option changes, source replacement,
versioned config check/reload/dump and URL rules, bounded JSON/aria2 session
export/import and configured saves, filtered events, loopback WebSocket,
Content-Length/NDJSON stdio, combined transports, direct CLI controls, and
typed Rust embedding are executable. Real option restart and slow-slot/retry
scheduling tests pass. Native Windows status/global-template p99 evidence is
recorded; query projection outside the control owner and bounded progress for
other synchronous/bulk mutations remain open under `P4-11`. Native Linux
measurement is deferred until CI is ready, as requested on
September 13, 2026. Release-platform gates remain separate.

- Complete control-plane operations over the Phase-1 scheduler.
- Request/response, batch, list-page, per-client pending-work, and serialized-byte
  caps with immutable membership indexes for list queries.
- Optional slow-slot scheduler policy.
- CLI add/pause/resume/remove/status.
- JSON-RPC and WebSocket events.
- Expand the request-only Content-Length stdio transport with authenticated
  batch/list/event delivery and slow-consumer handling.
- `changeOption` and `changeGlobalOption`.
- Config reload/check/dump commands and RPC diagnostics.
- Session save/load.
- aria2-compatible `--save-session` text export and native JSON export.
- Typed Rust query, control, configuration, session and event APIs.

Exit criteria:

- RPC methods drive real engine state.
- stdio RPC, HTTP RPC, CLI, and library API share the same control dispatcher.
- p99 RPC latency benchmark under active downloads.
- aria2 response-shape compatibility tests for implemented methods.
- CLI/RPC/library use the same scheduler.
- Runtime-update tests cover every aria2 option and documented extension;
  restart-driven changes appear as `waiting` and emit no pause event.
- Config reload/dump tests cover secret redaction, URL-rule rejection of unsafe
  or startup-only options, and atomic failed reload.
- WebSocket and stdio clients have bounded event queues with tested coalescing,
  overflow, snapshot recovery, and disconnect behavior.
- Oversized responses fail as typed complete errors, `system.multicall` obeys
  work/response caps without claiming transactional semantics, and one client
  cannot reserve the process RPC budget.

Phase 4B repairs multicall envelope authentication, connection-local event
authorization, retry-option registry/persistence alignment, active option
replay, and source replacement after cancellation drain. Shared RPC reservations
and transport ownership, borrowed result preflight, and typed input/native
projection accounting are implemented. Scheduler simulations and status drafts
reserve before mutation and retain credit through pending driver work. The
[repair gates](implementation-readiness.md#phase-4-repair-gates) record regression
evidence. Runtime option application and the interface/scheduling completion
requirements below are implemented and tested.

### Phase 4B Completion

Phase 4B completes the implementation and regression gates `P4-01` through
`P4-10` in `implementation-readiness.md`:
authentication and connection-local events; shared RPC accounting; replayable
option/source changes; registry/configuration completion; sanitized session
compatibility; CLI/transport/Rust parity; and opt-in slow-slot scheduling.
Performance and platform evidence belong to `P4-11`. Its measured portion passes
natively on Windows-GNU for all four transports: 20,000 calls each, 1,000 active
HTTP ranges, worst p99 0.542 ms against 50 ms, and bounded memory under stalled
consumers.
Measured bursts last at most 343 ms. These status/global-template measurements
do not close query projection outside the control owner or bounded progress for
other synchronous/bulk mutations. Those implementation/evidence requirements
remain open. Native Linux acceptance remains deferred until CI is ready; the
manual native Linux workflow preserves the same measurement gates. Do not claim
all of `P4-11`, deferred platform evidence or the complete release matrix passed.

## Phase 5: Metalink, FTP, SFTP

- Metalink parser with safe XML settings and chunk checksums.
- Metalink checksum-aligned verification with bounded ordered reassembly and
  readback fallback.
- Patched-SuppaFTP adapter with sequential resume per source by default;
  concurrency is across distinct mirrors rather than overlapping REST-to-EOF
  streams.
- FTP EPSV/PASV and active-mode endpoint validation tied to the approved control
  peer, with administrator overrides re-running the complete destination policy.
- russh/patched-russh-sftp adapter with project-owned bounded offset-request pipelining,
  host-key verification, known-hosts policy, authentication ordering, algorithm
  policy, timeout/rekey handling, and secret redaction.
- Mirror selection and server stats.
- Optional HTTP/2 enablement if the selected HTTP stack and tests are mature
  enough.

Exit criteria:

- Multi-source Metalink downloads stream to disk without whole-file buffering.
- Protocol-specific options have behavioral tests.
- Metalink chunk hash path avoids disk readback in normal operation.
- FTP tests prove lease-end connection close/accounting and no unbounded discarded
  tail traffic.
- FTP tests reject passive bounce/SSRF endpoints and active callbacks from any
  unapproved peer.
- FTP integration tests assert control sockets enter only through
  `connect_with_stream`, passive data sockets only through the owned builder,
  and active mode uses the patched pre-TLS peer predicate/accept loop; default
  connect/builder/NAT-workaround paths and proxied active mode are unreachable.
- FTP parser/source tests prove a line/aggregate/count overflow is rejected
  before proportional allocation, closes the connection, and includes FEAT
  continuation handling.
- FTP dependency-log tests cover USER/PASS/ACCT, SITE/custom commands, paths,
  greeting/reply/FEAT text, and listings with canaries at every log level.
- SFTP tests prove generic resume cannot approve a host key, challenge approval
  is exact and stale-safe, and packet-buffer-plus-returned-vector memory stays
  inside `sftp_ingress_budget` across cancellation and malformed replies.
- SFTP source and fault tests prove over-cap framing allocates no payload, a
  malformed frame closes rather than resynchronizes, and all outstanding raw
  requests receive exactly one terminal result.

## Phase 6: BitTorrent Full Build

- libtorrent adapter.
- Safe path integration.
- Queue and status integration.
- Torrent/magnet add through CLI/RPC.
- Select-file and index-out mapping.
- Seeding and stopped result persistence.
- Bounded command/event bridge outside the main event loop.
- BT-specific live setting updates through the command channel.
- Shutdown barrier that waits for requested resume data before the global session
  checkpoint and BT teardown.

Exit criteria:

- BT claims are backed by end-to-end tests.
- Minimal builds reject BT options clearly.
- Shutdown timeout/failure produces an explicit dirty checkpoint and recovery test.
- Command/event/blob queues enforce their exact item/byte caps, alert pressure
  follows the defined loss/coalescing policy, resume blobs obey size/cadence
  limits, and the shutdown barrier honors its timeout without unbounded memory.

## Phase 7: Hardening

- Security review.
- Fuzzing budget in CI.
- ASAN/UBSAN/TSAN for FFI builds.
- Internal stress/scalability validation on Linux, Windows, macOS.
- Documentation generated from metadata.
- Decide, from recorded parity results, whether `ariax` remains parallel or may
  provide the `aria2c` compatibility artifact; document migration and rollback.
- Optional C ABI design/prototype after Rust API stabilization.
- HTTP/3/ECH/DoH/DoT remain feature-gated unless maturity gates pass.
- No supported load-testing product surface or multi-instance orchestration
  surface is added; benchmark harnesses are validation-only.

Exit criteria:

- No known parsed-only features.
- No unbounded queues.
- No whole-segment buffering.
- Recovery tests pass across forced kill points.
- Native Linux, Windows, and macOS release artifacts are built through the
  documented Cargo release path with reproducible dependencies.
