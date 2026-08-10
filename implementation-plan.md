# Implementation Plan

Status: implementation is underway. Phase 0 contract generation and the gated
Phase 1 core/config/storage work have executable checkpoints. This includes the
deterministic bounded scheduler kernel, journal v1 framing/replay and typed
payloads, SQLite session schema v2, exact v1 preflight and transactional
migration, dense queue/slow metadata transitions, owner locking, WAL/DELETE
probes, journal-install tokens, retained stopped-result transactions, and
the validated no-clobber backup primitive. An ordered runtime driver, immutable
applied snapshots, bounded blocking-disk fallback, dedicated session owner, concrete
persistence-effect sink, host-key write/approval transactions, persisted-delay
recovery, pure cross-store startup planning, bounded source-derived credential
admission, ordered SQLite startup repair, and packet-independent stats sampling
are also executable. Descriptor-safe Unix/Windows root and central-journal
capabilities, install/appender recovery, owner-thread appender construction, and
publication-last native startup are executable. Bounded non-persistence effect
adapters and the product process-bootstrap path are also executable. A narrow
fresh HTTP/1.1 known-length transfer now runs through a descriptor-backed
`StorageEngine`, bounded pooled buffers and positional disk writes, strict
piece durability, replay recovery, cancellation/short-body aborts, and an
explicit pinned-peer CLI control. The first recoverable strong-ETag range
resume milestone is also executable: safe validator persistence/replay,
no-truncate descriptor reopen/readback, strict `206`/`Content-Range`/`If-Range`
validation, continued leases, and bounded runtime lifecycle effects are covered
by focused tests and the CLI. The direct plaintext destination connector is
also executable: ordinary authority validation, bounded system resolution,
canonical numeric admission, generated pinned-IANA special-use filtering,
all-answer rejection, final-peer binding, and fresh/resume runtime-effect
integration are covered by focused tests and generated contracts. TLS,
redirect/proxy policy, DNS cache/TTL/singleflight and Happy Eyeballs,
weak/digest-only resume, segmented scheduling, live scheduler/RPC dispatch,
hot-backup crash-residue reconciliation, remaining backend work, and the
native-platform/MSRV release CI gates remain phase and tag gates. These
checkpoints do not yet satisfy the complete first
vertical slice or the project definition of done.

This is a staged plan for building the design without repeating the incomplete
rewrite pattern. The native-startup orchestration boundary, central-journal
filesystem backends, runtime-effect boundary, and product bootstrap are now
executable; the remaining HTTP policy/dispatch slice and native CI evidence
remain phase gates.

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
  temporary unlink failures after publication.
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

Detailed design input:

- `detailed-http-first-slice.md`

Exit criteria:

- Fault tests for ignored Range, short/oversized body, lease abort, generated-
  header conflict, redirect/proxy SSRF, disk full, process kill, and poweroff
  simulation.
- 1,000 active HTTP range benchmark with bounded memory.
- 10,000 concurrent low-activity socket benchmark with the reusable idle HTTP
  pool still capped at its profile limit; measured accounted reservations and
  process RSS must fit the profile target/permit envelope and documented native
  stack/headroom allowance.
- DNS tests cover positive/negative TTL clamps, TTL=0, answer-count limits,
  bounded singleflight/waiters, cancellation, two-racer Happy Eyeballs timing,
  reconnect revalidation, and special-use-address filtering.
- Stuck socket speed drops to zero without waiting for another packet.
- The minimal RPC methods drive the real scheduler and expose only aria2's closed
  status/GID/error shapes.

## Phase 4: Control Plane And RPC

- Complete control-plane operations over the Phase-1 scheduler.
- Request/response, batch, list-page, per-client pending-work, and serialized-byte
  caps with immutable membership indexes for list queries.
- Optional slow-slot scheduler policy.
- CLI add/pause/resume/remove/status.
- JSON-RPC and WebSocket events.
- JSON-RPC over stdio transport.
- `changeOption` and `changeGlobalOption`.
- Config reload/check/dump commands and RPC diagnostics.
- Session save/load.
- aria2-compatible `--save-session` text export and native JSON export.
- Native Rust API skeleton.

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
