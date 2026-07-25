# Implementation Plan

Status: draft.

This is a staged plan for building the design without repeating the incomplete
rewrite pattern.

## Phase 0: Contracts, Compatibility Inventory, And Build Baseline

- Use `implementation-readiness.md` as the readiness gate for all later module
  work.
- Generate aria2 option inventory from `src/OptionHandlerFactory.cc` and
  `doc/manual-src/en/aria2c.rst`.
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
- Freeze the exact 16-hex GID codec, RPC token convention, error-code vocabulary,
  reserved-header policy, and secrets-at-rest policy.
- Create a new-option inventory from the design docs and fail CI if a
  documented option is missing registry metadata.
- Add the optional Cargo/autotools bridge and build the experimental `ariax`
  artifact without replacing `src/aria2c`.
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
- `./configure --enable-ariax=yes` either builds the declared Rust target or
  fails with an actionable toolchain/dependency error.

## Phase 1: Core Types And Minimal Scheduler

- `SafePathBuilder`
- `FileLayout`
- `GlobalOffsetMapper`
- `ControlJournal`
- `SessionStore` schema and migration shell
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
- Scheduler model tests cover every state × command/error transition and aria2
  wire projection.

## Phase 2: Runtime And Backends

- Tokio/Mio network runtime using its supported platform readiness backend; no
  second raw network reactor is built into the same runtime.
- Linux io_uring disk prototype with fallback.
- Windows overlapped I/O prototype.
- Bounded blocking disk fallback.
- `DiskBackendKind` enum dispatch and total `DiskWriteOutcome` buffer ownership.
- Bounded submission queues and non-rejecting completion drain topology.
- Backend diagnostics RPC.

Exit criteria:

- Forced backend failure tests prove graceful fallback.
- No blocking disk I/O on network runtime threads.
- Every submitted buffer is returned or quarantined exactly once across success,
  error, cancellation, and shutdown.

## Phase 3: HTTP(S) Downloader

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
- Stuck socket speed drops to zero without waiting for another packet.
- The minimal RPC methods drive the real scheduler and expose only aria2's closed
  status/GID/error shapes.

## Phase 4: Control Plane And RPC

- Complete control-plane operations over the Phase-1 scheduler.
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

## Phase 5: Metalink, FTP, SFTP

- Metalink parser with safe XML settings and chunk checksums.
- Metalink checksum-aligned verification with bounded ordered reassembly and
  readback fallback.
- FTP adapter with sequential resume per source by default; concurrency is across
  distinct mirrors rather than overlapping REST-to-EOF streams.
- SFTP adapter.
- Mirror selection and server stats.
- Optional HTTP/2 enablement if the selected HTTP stack and tests are mature
  enough.

Exit criteria:

- Multi-source Metalink downloads stream to disk without whole-file buffering.
- Protocol-specific options have behavioral tests.
- Metalink chunk hash path avoids disk readback in normal operation.
- FTP tests prove lease-end connection close/accounting and no unbounded discarded
  tail traffic.

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
  documented autotools/Cargo path with reproducible dependencies.
