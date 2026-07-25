# Downloader Design

Status: draft design, intended to be refined before implementation.

This design is for a new downloader that keeps the mature aria2 user model
while fixing the major safety, scalability, and completeness problems found in
the earlier `aria2_rust` prototype's review findings (summarized in
`review-findings-response.md`).

## Goals

- Handle C10k network concurrency without blocking control operations.
- Use async network and disk I/O where the platform can provide it, and use
  bounded worker pools where it cannot.
- Keep memory bounded with pooled buffers and streaming pipelines.
- Preserve aria2-style configurability, including input-file scoped options,
  RPC option changes, runtime limits, queue control, and session persistence.
- Make every state transition explicit: range validation, disk placement,
  checksum validation, pause/resume, retry, failure, and recovery.
- Avoid RCE, path traversal, secret leaks, data corruption, use-after-free, and
  unbounded allocation.
- Support Linux, Windows, and macOS as first-class desktop platforms.

## Non-Goals

- Exact source compatibility with aria2 internals.
- Shell hook compatibility by default. Shell execution is an explicit unsafe
  compatibility feature because "no RCE" is a hard requirement.
- A hand-written BitTorrent stack in the first production version. BitTorrent is
  too large and security-sensitive to rewrite casually.
- Claiming aria2 option compatibility for options that are only parsed.
- A load-testing product, traffic generator, benchmark orchestration service, or
  synthetic multi-server test harness. The project needs internal stress tests
  to prove downloader behavior, but it does not ship as a load-testing tool.
- Multi-instance or multi-server orchestration. The core is a single-process
  downloader engine; cluster scheduling, daemon fleet management, distributed
  queues, and deployment automation are external concerns.

## Language And Library Choice

The core should be written in Rust.

Reasons:

- Memory safety directly addresses use-after-free and many leak classes.
- Async Rust has battle-tested network and scheduling infrastructure.
- The option, state-machine, recovery, and protocol parsers benefit from strict
  types and fuzzable pure functions.
- Unsafe code can be forbidden in most crates and isolated in small platform or
  FFI adapters.

See `library-choice.md` for the runtime/library comparison and why this design
chooses Tokio/Mio plus platform disk adapters over libuv, standalone Asio, or
libevent for the Rust core.

See `disk-adapter.md` for the explicit build-vs-library decision for disk I/O.
The storage engine is project-owned because it defines correctness and recovery;
low-level OS mechanisms are reused behind narrow backends.

See `backpressure.md` for how network reads adapt to disk, CPU, and memory
pressure on HDDs, SSDs, NVMe devices, and slower filesystems.

See `threading-model.md` for the split-pool worker model. Event loops, disk I/O,
CPU hashing, and libtorrent are separate lanes by default and coordinated by a
global resource manager.

See `buffer-pool.md` for allocation policy, lifecycle, copy targets, and C10k
scaling behavior for transfer buffers.

See `performance-profiles.md` for the single user-facing `--profile` setting:
`auto`, `concurrency`, `throughput`, `latency`, and `compact`. It coordinates
workers, buffers, disk queues, backpressure, and scheduler behavior.

See `messaging-model.md` for the bounded queue choices between worker lanes.
Hot paths use shared pooled buffers and descriptor queues, not copied payloads
or one generic async channel everywhere.

See `split-download.md` for dynamic non-overlapping range leases, lease/span
retry, endgame duplication, and sequential fallback behavior.

See `retry-policy.md` for configurable retry triggers, status-code retry sets,
`Retry-After` handling, bounded waits, and stale connection/validator behavior.

See `redirect-policy.md` for redirect depth/loop limits, validator revalidation
across redirects, and cross-origin credential stripping.

See `rate-limiting.md` for the token-bucket hierarchy, fairness, precedence over
read-backpressure, and reconciliation with libtorrent's internal limiter.

See `detailed-ftp-sftp.md` for FTP sequential-resume and SFTP offset rules,
`SIZE`/`MDTM` validators, ASCII-mode prohibition, and FTPS/SFTP transport rules.

See `download-scheduling.md` for active/waiting queue policy and optional
slow-slot demotion, where slow tasks can free `max-concurrent-downloads` slots
for queued downloads.

See `metalink-chunking.md` for how Metalink checksum chunks interact with
dynamic range leases and when disk readback is allowed.

See `stats-and-stalls.md` for monotonic time-based speed sampling and explicit
stalled/backpressured/rate-limited states, so displayed speeds do not freeze
when sockets stop producing packets.

See `apis-and-embedding.md` for aria2-compatible RPC, native Rust embedding,
and the optional staged C ABI.

See `configuration.md` for the typed option registry, aria2-style flat config,
optional URL default rules, runtime option mutation, reload, and config dump
policy.

See `protocol-modernization.md` for HTTP/1.1 keep-alive, HTTP/2 multiplexing,
HTTP/3/QUIC, TLS 1.3, ECH, and DoH/DoT policy.

See `session-persistence.md` for the hybrid SQLite plus per-task control
journal decision.

See `implementation-readiness.md` for the handoff checklist, hard invariants,
generated artifacts, first implementation slice, and prototype-only decisions.
The detailed first-slice module contracts live in `detailed-core.md`,
`detailed-config.md`, `detailed-storage.md`, `detailed-runtime.md`, and
`detailed-http-first-slice.md`.

Selected community components:

- Tokio and Mio for the default async executor and cross-platform network
  readiness: Linux epoll, macOS/BSD kqueue, Windows IOCP.
- A platform I/O adapter for disk:
  - Linux: optional io_uring backend after runtime probing succeeds.
  - Windows: IOCP/overlapped file I/O backend.
  - macOS/BSD: kqueue-ready network plus bounded disk worker pool, because
    portable non-blocking file I/O is limited.
  - Fallback: bounded blocking disk pool with strict queue limits.
- hyper or a similarly maintained HTTP stack for HTTP/1.1 and optional HTTP/2.
  The downloader owns range validation and placement, not the HTTP client.
- rustls for TLS by default, with optional native-tls platform integration.
- The first slice uses bounded Tokio channels for async lanes and bounded
  crossbeam channels for blocking workers. Specialized thingbuf/rtrb queues are
  introduced only after a measured lane-specific bottleneck and a producer-count
  proof; no queue family is linked without an owned topology.
- libtorrent-rasterbar for BitTorrent in the "full" build. It is mature,
  scalable, supports DHT/PEX/magnet/web seeds, and already handles many BT edge
  cases. The integration must still sanitize paths and translate state through
  the downloader scheduler.
- libssh2 or russh for SFTP, selected behind a feature flag after prototype
  benchmarks. SFTP must not block network reactor threads.

Build profiles:

- `minimal`: HTTP(S), Metalink, JSON-RPC, no BitTorrent, rustls only.
- `standard`: HTTP(S), FTP, SFTP, Metalink, JSON-RPC/WebSocket.
- `full`: standard plus BitTorrent through libtorrent.
- `compat`: full plus explicitly unsafe compatibility features such as shell
  hooks, disabled by default and rejected over unauthenticated or remote RPC.

Binary-size rules:

- features are opt-in, not linked by default,
- CLI, RPC, BitTorrent, SFTP, XML-RPC, WebSocket, and native TLS are separate
  features,
- release builds use LTO, one codegen unit, panic abort, and stripped symbols
  in packaged binaries,
- generated tables replace duplicated option/help strings where practical,
- `minimal` is the size baseline and CI tracks binary-size regressions.

## Repository And Build Integration

The Rust implementation lives in its own standalone repository, separate from
the aria2 C++ tree. This design set moves with it. The aria2 checkout is a
pinned compatibility reference, not a build host, and the existing C++
`aria2c` binary is untouched by this project.

Repository layout and artifacts:

```text
Cargo.toml                   workspace root
crates/ariax-*               engine and adapter crates
bin/ariax                    experimental CLI/RPC binary
```

Build rules:

- Cargo is the release entry point; no autotools bridge is required.
- Phase-0 compatibility inventories (option list, manual text, RPC shapes) are
  generated from a pinned aria2 source checkout recorded in the repository
  (commit hash plus generation script), so the compatibility matrix is
  reproducible without building aria2.
- Release CI builds the exact `minimal`, `standard`, `full`, and `compat`
  artifacts rather than testing an unrelated developer feature set.
- Release tarballs include `Cargo.lock` and a reproducible vendored-crate bundle
  or an explicitly documented online-build policy. Cross builds pass toolchain,
  linker, and native dependency paths explicitly; a WSL Linux rustc is never
  mixed with the native MinGW Rust target.

The Rust binary may provide an `aria2c` compatibility name only after the
Phase-7 parity decision records RPC/config/session behavior, migration, and
rollback. Until then it is named `ariax` and packaged as experimental.

## High-Level Architecture

```text
CLI / config / RPC / lib API
          |
          v
ControlPlane  <---->  StatusSnapshotStore
          |
          v
RequestScheduler
  active queue, waiting queue, stopped results
  global/per-host/per-download budgets
          |
          v
DownloadTask
  protocol workers, segment scheduler, retry policy
          |
          +----> ProtocolAdapter: HTTP, FTP, SFTP, Metalink, BitTorrent
          |
          +----> StorageEngine
                  safe path builder
                  file layout mapper
                  async disk queue
                  control journal
                  checksum verifier
```

The control plane and scheduler must never wait on a network transfer, file
allocation, checksum pass, or RPC request body. All long work is represented by
tracked tasks with cancellation tokens and bounded queues.

## Runtime Model

The process has these runtime lanes:

- `control`: small current-thread runtime for RPC parsing, CLI commands, signal
  handling, config validation, and scheduler commands.
- `network`: multi-thread Tokio runtime sized from `--net-workers` or CPU count.
- `disk`: backend-specific async completion loop or bounded blocking pool.
- `cpu`: bounded work-stealing pool for hashing, decompression, bencode/XML
  parsing, and metadata validation.
- `bt`: libtorrent session thread(s) in the full build, isolated behind an event
  bridge.

The `bt` lane is not part of the main control or HTTP network event loop. The
main scheduler communicates with libtorrent through bounded command/event
channels and normalized snapshots.

Every lane reports event-loop lag and queue depth. The scheduler uses those
signals for backpressure.

The default production profile uses split pools. A compact shared-pool profile
is allowed for small builds, but it cannot claim C10k performance and still must
keep blocking disk work off event-loop threads.

## Request Lifecycle

Request groups follow aria2's proven split between active, reserved, and
stopped state, similar to aria2's `src/RequestGroupMan.cc`.

States:

- `Accepted`: options parsed and validated, no file/network side effects.
- `Waiting`: queued in reserved order, visible to `tellWaiting`.
- `Allocating`: storage layout and file allocation in progress.
- `Active`: one or more protocol workers are running.
- `Paused`: durable control file saved, can return to waiting.
- `PausedSlow`: optional scheduler state for slow-slot demotion. It is enabled
  only by policy and is distinct from user pause.
- `PausedRestarting`: internal active-option restart quiescence. It is rendered
  as aria2 `waiting` and never emits a pause event.
- `RetryWait`: waiting for retry policy timer. It may or may not consume an
  active slot depending on configured scheduling policy.
- `Verifying`: checksum or piece verification in progress.
- `Seeding`: BitTorrent only.
- `Complete`: data durable and final rename done.
- `Error`: terminal error stored in stopped results.
- `Removed`: user removed task; partial files follow configured policy.
- `StoppedResult`: retained terminal result, queried separately from the live
  task set.

This is a high-level lifecycle summary. `detailed-core.md` is normative for the
complete state × command/error matrix and the closed aria2 wire-status mapping.

Admission control:

- `max-concurrent-downloads` limits active groups.
- `split` limits segments per group.
- `max-connection-per-server` limits connections per host per group.
- Global socket budget limits total open network connections.
- File descriptor budget limits open files and libtorrent handles.
- Disk memory budget limits in-flight write buffers.
- RPC control budget limits large request bodies and long status scans.

Changing an active option either applies live or triggers a controlled restart.
The option registry defines this per option. Parsed-only behavior is forbidden.
Every option also declares its allowed scope and runtime update behavior so
aria2-compatible `changeOption`/`changeGlobalOption` calls cannot silently
mutate the wrong generation.

## Protocol Pipeline

For HTTP(S), FTP, and SFTP:

```text
SegmentScheduler -> ProtocolWorker -> BufferPool -> DiskWriteQueue
       ^                  |                 |              |
       |                  v                 v              v
  retry state       response verifier   hash update   durable ack
```

Rules:

- Range requests must receive `206 Partial Content`.
- `Content-Range` must match requested start, end, and total when total is
  known.
- Range, split, and resume operate only on identity-encoded representations with
  a known extent. Wire offsets and decoded-file offsets are never conflated.
- A server returning `200 OK` to a range request is not accepted as a segment.
  The task either falls back to a single sequential download from offset 0 or
  fails according to `always-resume` and retry policy.
- Short and oversized bodies are rejected. Oversized bodies are not truncated
  silently.
- Sequential resume requires a validator: ETag, Last-Modified, checksum, or
  explicit user override. If a resumed request gets `200 OK`, existing partial
  data is not overwritten unless the scheduler switches to a full restart.
- Decompression filters operate before final placement only when the entity
  length semantics are unambiguous. A fresh sequential unknown-length response
  uses the explicitly selected growing-layout path with a configured maximum and
  commits its final extent before completion. Ranged encoded responses are
  rejected.
- Split downloads use a bounded pool of range workers. Each worker leases a
  non-overlapping chunk, writes it provisionally through storage, commits the
  lease only after exact response validation, then asks for another chunk.
  Overlapping duplicate requests are allowed only in small endgame mode and only
  when the shared-identity/hash gate permits content-aware arbitration.
- Generated framing, range, identity, and credential headers are authoritative.
  Conflicting user headers are rejected before a request is sent and are
  revalidated after every redirect.

For Metalink:

- XML is parsed with entity expansion disabled.
- Metalink paths and names pass through the same safe path builder as torrents.
- Chunk checksums are verified streaming; whole files are never buffered.
- Mirror ranking reuses server stats and feedback selector behavior.

For BitTorrent:

- The first production version uses libtorrent in the full build.
- The downloader owns user-visible queueing, option validation, safe output
  paths, RPC state, and final result persistence.
- The libtorrent adapter owns peer swarm mechanics and emits normalized events:
  piece finished, metadata received, state changed, error, stats snapshot.
- If libtorrent is not available, BT options remain visible in the compatibility
  matrix as unsupported in that build and fail with a precise error.

## Storage Model

Storage is global-offset based. Each download has a validated `FileLayout`:

- `root`: canonical output root.
- `files`: sorted file entries with relative safe paths, length, selected flag,
  and global offset range.
- `piece_length`: fixed for the task.
- `total_length`: exact when known.

All disk writes are `WriteBlock { task_id, generation, lease_id, global_offset,
len, buffer_lease, expected_piece }`. Range-attempt writes are provisional until
`CommitLease`; `AbortLease` makes them non-durable and eligible for overwrite.
The storage engine maps global offsets to one or more file offsets. It rejects
writes that:

- are outside the layout,
- cross an unselected file except for a shared piece edge that requires it,
- do not match the current task generation,
- overlap a completed durable piece unless the piece is being explicitly
  revalidated.

Completion is not "bytes received". Completion means:

1. the owning lease/attempt passed status, exact-length, validator, and placement
   checks and was committed,
2. bytes were written at the intended offsets,
3. piece or chunk hash is valid when a hash is available,
4. data was flushed according to the selected durability mode before the control
   journal claimed it durable,
5. the control journal marks the piece durable,
6. final file metadata is synced according to durability mode.

## Recovery Model

Poweroff recovery is handled with a journaled control file, not with in-memory
progress guesses.

Control file contents:

- magic, version, endian marker,
- task id and stable gid,
- protocol and metadata hashes,
- file layout hash,
- piece length, total length, selected files,
- validators: ETag, Last-Modified, content digest, torrent info hash, Metalink
  hashes,
- durable piece bitset,
- in-flight generation counter,
- upload/session counters for BitTorrent,
- last clean shutdown marker.

Write protocol:

- Data writes complete as provisional lease blocks.
- Exact response validation commits the lease; failure appends/records an abort
  and returns the span to pending.
- Before any trusted `PieceDurable` record, the corresponding data is flushed in
  balanced/strict mode. Fast-mode records remain provisional on recovery.
- One serialized journal appender assigns sequence numbers and appends the
  commit/abort/durability records.
- The journal is synced at the mode's checkpoint boundary.
- Parent directory is fsynced on platforms that require it.

On startup:

- Invalid or torn journal records are ignored.
- In-flight pieces are reset to pending.
- Completed pieces are trusted only if the control file, layout hash, and
  validators match. Otherwise they are rechecked or redownloaded.
- Existing target files without a matching control file are never truncated by
  default. The user must set `allow-overwrite=true` or choose a new name.

Durability modes:

- `fast`: periodic control saves and final fsync. Per-piece progress is
  provisional after a crash and is re-read/revalidated or redownloaded; the
  journal never makes unflushed data trusted.
- `balanced`: default. At each completed-piece group or time boundary, flush the
  affected data files, append the group's `PieceDurable` records, and `sync_all`
  the journal. Work since the last boundary may be redownloaded, but committed
  records never point at unflushed data.
- `strict`: for every completed piece, flush its data before `PieceDurable` and
  `sync_all` the journal before publishing durability. Its extra cost is the
  per-piece data/control ordering and frequency, not a portable assumption that
  `sync_all` is always slower than `sync_data`.

## Memory Model

Memory is budgeted, not accidental.

- One global transfer buffer pool with size classes: 16 KiB, 64 KiB, 256 KiB,
  1 MiB.
- The pool is lazy/on-demand within hard caps by default, with optional
  preallocation for registered I/O or benchmark profiles.
- Protocol workers read payload bytes into pool buffers; storage writes from
  those same buffers; hashers borrow immutable slices.
- Buffers return after disk ack, hash use, and journal state no longer need
  them.
- `disk-cache` and buffer-pool bytes are hard caps, not hints.
- Hashers consume borrowed slices and incremental state when possible.
- RPC status uses snapshots and atomics, not deep locks over live tasks.
- No segment, Metalink file, or project-owned payload is buffered as a whole
  unless an explicit size-capped metadata path requires it. Libtorrent's
  internal payload buffers stay behind the BT adapter boundary.

Default target envelope:

- 10,000 idle connections: bounded primarily by socket/task overhead, no per
  connection megabyte allocations.
- 1,000 active range streams: at most one or two borrowed buffers each, subject
  to disk backpressure.
- Metadata size limits are configurable and enforced before allocation.

Zero-copy is allowed as an optimization only when it does not weaken placement,
hashing, recovery, throttling, or cancellation semantics. See
`zero-copy.md`.

## Security Posture

- Pure Rust crates use `#![forbid(unsafe_code)]`.
- Unsafe code is allowed only in `platform-io` and `bt-libtorrent` adapter
  crates, with written invariants and sanitizer coverage.
- Metadata-derived paths never use raw `join`.
- RPC binds to localhost by default.
- RPC secret/token is required when listening on non-loopback unless
  `--rpc-insecure-listen=true` is explicitly set.
- Secrets are redacted from logs, status, panic hooks, and tracing spans.
- Session databases, journals, exports, and temporary files are private to the
  user (0600 or equivalent ACL). Authentication/proxy/cookie secrets are omitted
  from ordinary persistence and must be supplied again or resolved from an OS
  keyring; plaintext secret persistence is forbidden.
- Shell hooks are disabled by default and unavailable through normal RPC option
  changes.
- Redirects, proxies, and DNS resolution obey SSRF guardrails when RPC is
  remotely exposed.
- Untrusted remote RPC cannot use proxy-side DNS or arbitrary CONNECT targets
  unless the proxy is explicitly trusted and destination-filtered.
- User headers cannot override generated Host, framing, range, encoding,
  validator, integrity, or credential headers.
- XML external entity expansion is disabled.
- Archive extraction is out of scope; the downloader writes exactly requested
  files.

## Observability

Expose:

- selected event backend and fallback reason,
- event-loop lag p50/p95/p99,
- active sockets, open files, disk queue depth,
- current, smoothed, and average speeds sampled independently of packet arrival,
- stalled/backpressured/rate-limited connection counts,
- buffer pool use by size class,
- per-task retry causes,
- per-host active connections and recent error rate,
- RPC latency and lock wait time,
- journal save latency and last successful recovery checkpoint.

Metric cardinality is bounded so observability does not itself break C10k:

- per-host and per-task series are capped and aggregated (top-N by activity,
  with a spillover "other" bucket), not one unbounded label set per host/task,
- always-on exported metrics are bounded gauges/counters; unbounded per-entity
  detail (per-lease diagnostics, per-task retry-cause breakdowns) is served
  on demand through status queries, not continuously exported,
- the cardinality caps are themselves configurable and surfaced in diagnostics.

This is required for C10k claims. The benchmark suite must fail if p99 control
latency exceeds the configured target while transfers are active.

## Testing Strategy

Required before production claims:

- Unit tests for option parsing, path building, range validation, layout
  mapping, control journal decoding, and scheduler state transitions.
- Property tests for path sanitization, global offset mapping, URI parsing,
  Content-Range parsing, bencode, Metalink, and input-file parsing.
- Fuzz targets for HTTP headers, bencode, Metalink XML, RPC JSON/XML, control
  file recovery, and path components.
- Fault injection for short reads, oversized responses, server ignores Range,
  disk full, permission denied, lease abort, hash failure after write, partial
  fsync, torn/rotated control journals, and process kill during every recovery
  state.
- Internal scalability validation:
  - 10,000 idle HTTP sockets.
  - 1,000 active range streams writing to disk.
  - 10 GiB allocation while polling RPC at 100 QPS.
  - 1,000 BitTorrent peer simulators in full build.
  - Memory peak checks by split, piece length, and disk-cache settings.
- Compatibility tests against aria2 RPC response shapes for implemented
  methods and options, including restart-as-`waiting`, token authentication,
  exact 16-hex GIDs, error codes, and documented runtime-update divergences.
- Security tests for proxy-resolved SSRF, redirect-to-private targets, reserved
  header overrides, secret persistence, and Windows/Unicode path edge cases.

These stress and scalability checks are CI or lab validation, not a shipped
load-testing or multi-instance orchestration feature.

## Existing Code Lessons

From aria2:

- Keep active/reserved/stopped queues and queue position operations.
- Keep server stats and feedback/adaptive URI selection.
- Keep control files and never truncate partial files without a matching
  resume story.
- Keep a write cache, but make its memory limit hard and backpressured.
- Keep event backend configurability, but remove assert/crash behavior when a
  backend cannot initialize.

From the `aria2_rust` prototype review (`review-findings-response.md`):

- Do not accept metadata paths without safe path construction.
- Do not use sequential `await` in the engine dispatch loop.
- Do not store whole segments or files in `Vec<u8>`.
- Do not advertise RPC/options before they drive real engine behavior.
- Do not mark progress complete before bytes are written and validated at the
  correct offsets.
