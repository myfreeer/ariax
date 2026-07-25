# Response To aria2_rust Review Findings

Status: draft.

This document maps the earlier `aria2_rust` prototype's review findings
(`REVIEW_FINDINGS.md` in that repository) to design decisions.

## Safety And Security

Metadata-derived path traversal:

- Fixed by mandatory `SafePathBuilder`.
- Applies to torrent, Metalink, Content-Disposition, `out`, `index-out`, RPC
  metadata upload, and session restore.
- Raw `PathBuf::join` on metadata is forbidden by lint and review rule.

HTTP resume truncates partial file:

- Fixed by storage generations and explicit open modes.
- Resume opens existing files without truncate.
- Full restart is a separate generation with explicit overwrite policy.
- Existing files without matching control data are protected by default.

Segmented HTTP accepts invalid range responses:

- Fixed by strict `206` and `Content-Range` validation.
- `200 OK` to a range request triggers controlled single-download fallback or
  failure, never offset write.
- Exact body length is required before durable commit.

Engine failure, timeout, and retry handling ineffective:

- Fixed by supervised tasks, join sets, cancellation tokens, and typed terminal
  states.
- Timeouts wrap real protocol futures, not empty futures.
- Failed commands either retry through policy or emit terminal result and leave
  active state.

CLI RPC options not wired:

- Option registry cannot mark `enable-rpc` implemented unless startup creates
  a listener and binds it into the live engine.
- RPC methods enqueue real scheduler commands and read live task snapshots.

## Performance And Scalability

Engine scheduling serial:

- Fixed by non-blocking scheduler and tracked worker tasks.
- Admission control limits concurrency without serializing active transfers.
- Control plane has a separate lane from network/disk work.

HTTP split capped and serialized:

- `split` and `max-connection-per-server` are bounded by config and global
  budgets, not hard-coded low limits.
- Range workers run concurrently through semaphores and per-host budgets.
- Workers lease dynamic non-overlapping chunks; retry is per chunk/mirror/error
  class.

Segment downloads buffer whole ranges:

- Fixed by streaming buffers from protocol workers to disk queue.
- Segment state keeps offsets, hashes, retry counters, and length, not payload.
- The buffer pool forbids arbitrary payload `Vec<u8>` segment storage in
  protocol code.

BitTorrent peer handling sequential:

- Avoided initially by using libtorrent for BT in full builds.
- If a native BT stack is later written, it must use per-peer tasks, bounded
  request pipelines, and a shared piece/block scheduler.

Disk writes and preallocation stall scheduling:

- Fixed by disk backend lane and allocation tasks.
- File allocation reports progress and never blocks network or control runtime.
- Disk queue backpressures protocol workers before memory grows.

RPC lock-heavy/in-memory only:

- Fixed by live scheduler integration and snapshot store.
- Hot counters are atomics or sharded stats.
- Status requests do not hold global locks while formatting responses.

Benchmarks do not prove 1k scalability:

- New benchmark gates include 10k idle sockets, 1k active streams, RPC latency
  under I/O, memory ceilings, event-loop lag, and BT peer simulators.
- Stats tests verify speeds update on clock ticks and stuck sockets do not
  freeze displayed throughput.

## Implementation Completeness

RPC methods do not drive real engine:

- RPC engine is a facade over `ControlPlane`.
- `addUri`, `addTorrent`, and `addMetalink` create real request groups and
  enter the scheduler.
- Status comes from live task snapshots and stopped results.

RPC status/options/results synthetic:

- Synthetic fixed speeds and timestamps are forbidden.
- `getGlobalStat`, `getSessionInfo`, `removeDownloadResult`, and related
  methods read or mutate real engine state.
- Compatibility tests compare implemented response shapes against aria2.

Queueing and `max-concurrent-downloads` missing:

- Active, waiting, and stopped queues are core scheduler data structures.
- `changePosition`, `tellWaiting`, pause, resume, and admission are defined
  against those queues.

Option surface much smaller than aria2:

- A compatibility matrix is required.
- Options are `implemented`, `partial`, `unsupported`, `unsafe_compat`, or
  `feature_gated`.
- Parsed-only options fail CI.

BitTorrent claims exceed integration:

- Full build uses libtorrent until native support is genuinely complete.
- Feature claims are tied to adapter tests and libtorrent capabilities.
- Minimal builds reject BT options clearly.
- The libtorrent adapter runs outside the main event loop behind bounded
  command/event channels.

CLI control operations placeholder:

- CLI, RPC, and library APIs share the same control commands.
- No separate placeholder coordinator state is allowed.

## Additional Requirements From This Design Session

Configurable like aria2:

- Covered by `design/configuration.md`.
- Requires generated manual/help/RPC allowlists from typed option metadata.

Best available event API with fallback:

- Covered by `design/event-backends.md`.
- Tokio/Mio is the only network reactor; its platform selector is diagnostic,
  not an independently selected project reactor.
- Runtime probing selects the independent disk backend (`io_uring`, IOCP, or a
  bounded blocking pool). Legacy `event-poll` values are compatibility aliases
  and never instantiate raw epoll/kqueue/poll/select fallback code.
