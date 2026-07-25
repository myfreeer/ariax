# Requirements Traceability

Status: draft.

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
`review-findings-round3.md`, and `review-findings-round4.md` are review history;
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
- `implementation-plan.md`: 10k idle socket and 1k active range benchmarks.

Acceptance:

- control p99 latency remains under target while downloads are active,
- no blocking disk I/O on network runtime threads,
- no unbounded queues,
- hot queues pass descriptors/leases, not payload copies,
- event-loop lag and queue depth are observable.

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
- `detailed-ftp-sftp.md`: FTP/SFTP authentication, offset validation, and
  protocol-specific concurrency limits.
- `rate-limiting.md`: bounded accounting for committed and discarded traffic.
- `session-persistence.md`: private file permissions and secrets-at-rest policy.
- `disk-adapter.md`: offset-only writes, storage validation, fsync/rename
  policy.
- `README.md`: storage model and recovery model.

Acceptance:

- shell hooks disabled by default,
- safe path builder is mandatory,
- pure Rust crates forbid unsafe code,
- every write is global-offset validated,
- crash tests pass for all journal states.
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
- `zero-copy.md`: allowed zero-copy optimizations and forbidden shortcuts.
- `implementation-plan.md`: memory peak gates.

Acceptance:

- buffer pool has hard budgets,
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
- `detailed-http-first-slice.md`: executable first-slice HTTP state machine.

Acceptance:

- parser and journal fuzz targets exist,
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
- `README.md`: Cargo/autotools artifact strategy and native/cross-build rules.

Acceptance:

- CI runs Linux, Windows, and macOS,
- backend probe tests pass on each,
- unsupported backend selection never hard crashes,
- native Linux, Windows, and macOS CI builds the declared release artifacts,
- WSL Linux and native MinGW toolchains are never mixed into one target build.

## aria2-Style Configurability

Requirement:

- configurable like aria2.

Design coverage:

- `configuration.md`: typed option registry, layering, input file, RPC changes,
  compatibility matrix, flat config, optional URL default rules, reload, and
  dump/export policy.
- `review-findings-response.md`: parsed-only options fail CI.

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
- aria2 token authentication, including `system.multicall`, follows the same
  dispatcher policy on HTTP/WebSocket,
- each WebSocket/stdio client has a bounded queue, documented coalescing, and a
  snapshot recovery path after dropped status/stat events.

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
- `Retry-After` is respected only for retryable responses and capped,
- stale connection retry is distinct from stale validator restart/failure,
- slow-slot demotion is off by default,
- local disk/CPU/buffer/rate-limit backpressure never triggers remote-slow
  demotion,
- retry waits do not hold transfer buffers.
- discarded traffic is separately observable and bounded; the configured
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
- text export/import exists for debugging/migration,
- RocksDB/LMDB are not mandatory dependencies,
- one serialized appender owns sequence assignment, payloads, and segment
  continuity,
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
- legacy `event-poll` configurations are accepted through the documented alias,
- no independent raw reactor is promised inside the Tokio runtime.

## Build And Artifact Integration

Requirement:

- integrate the Rust design into this autotools repository without silently
  replacing or breaking the existing C++ release artifact.

Design coverage:

- `README.md`: parallel `ariax` artifact, Cargo workspace, configure/make bridge,
  packaging, native and cross-toolchain policy.
- `implementation-plan.md`: Phase-0 build baseline and Phase-7 parity decision.
- `library-choice.md`: Tokio/Mio and platform adapter ownership.

Acceptance:

- `--enable-ariax=yes` fails cleanly when its toolchain/dependencies are missing,
- default C++ builds remain intact during Rust bring-up,
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
