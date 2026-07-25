# Thread Messaging And Queues

Status: draft.

Decision: keep Rust as the implementation language, but do not use one generic
channel everywhere. The downloader uses bounded, lane-specific queues. Hot data
paths pass buffer descriptors and ownership tokens, not payload bytes.

Safe Rust does not prevent shared-memory performance. It prevents accidental
aliasing and lifetime bugs. Shared memory is used through pooled buffers,
atomics, and audited queue crates with safe public APIs.

## Core Rule

Queues carry small messages:

```rust
struct DiskWriteMsg {
    task: TaskId,
    generation: Generation,
    lease: LeaseId,
    global_offset: u64,
    len: usize,
    buffer: BufferLease,
    piece: PieceId,
    completion: CompletionPermit,
}
```

They do not carry `Vec<u8>` payloads or copied byte ranges. Payload memory lives
in the global `BufferPool`; messages transfer ownership of a lease or refer to
an already-pinned slot.

## Queue Classes

### Control Async Queues

Use `tokio::sync::mpsc`, `oneshot`, `watch`, or `broadcast` for:

- CLI/RPC commands,
- scheduler commands,
- cancellation,
- config changes,
- low-rate notifications.

These are not the hot transfer path. Tokio's bounded MPSC has built-in
backpressure and integrates cleanly with async tasks, but it supports one
receiver and should not become the universal worker-pool queue.

None of the selected queue crates is priority-capable, so control-plane priority
is achieved structurally rather than by ordering within one channel. The
scheduler input is three separately bounded sources:

- `control_urgent` (externally produced pause/remove/cancel/position commands),
- `control_bulk` (adds and long status scans),
- internal completion lanes (`CompletionDrain` outcomes and journal/durability
  acknowledgements), which are permit-reserved and are never enqueued by
  external RPC/CLI/API producers.

Externally produced urgent commands have bounded admission: the transport layer
coalesces duplicate per-task requests whose semantics allow it (a second
`pause` of the same gid merges; `remove` supersedes a queued `pause`; `force`
upgrades a queued non-force twin) and rejects the remainder with a typed
busy error when the urgent queue is full. Because at most one coalesced
urgent command per `(gid, kind-class)` plus a small unkeyed reserve is
admitted, external producers cannot occupy the reserved internal lanes, and
internal completions never wait behind an external burst. Admission
backpressure for task creation applies only to `control_bulk`. Drain fairness
is bounded-burst, not absolute priority; see `detailed-runtime.md` Queue
Topology.

Shutdown is not an ordinary queued command: it is delivered through an
out-of-band `watch`/cancellation signal that every lane observes even when all
bounded queues are full. The `Shutdown` RPC/CLI verb acknowledges through the
normal reply path but triggers the signal directly.

### Hot Async MPSC Lanes

The first slice uses bounded `tokio::sync::mpsc` behind `HotMpscLane`. After a
measured bottleneck, `thingbuf::mpsc` may replace it for high-rate bounded MPSC
paths that need async waiting without per-message allocation:

- protocol workers -> disk scheduler,
- protocol workers -> hash scheduler when hashing is not inline,
- many async producers -> one lane owner.

`thingbuf` preallocates its ring at construction. This is acceptable because
capacity is based on active streams and queue budgets, not on the maximum idle
socket count. It should not be used with huge mostly-empty bounds.

### SPSC Ring Lanes

After the first-slice baseline, use a wait-free bounded SPSC ring such as `rtrb`
only when topology is proven to be exactly one producer and one consumer:

- one network worker shard -> one disk submit shard,
- metrics sampler shard -> aggregator shard,
- narrow libtorrent alert forwarding after callback coalescing.

SPSC rings avoid MPMC contention and are the preferred hot-path shape when the
pipeline can be sharded. A blocking disk pool has multiple completion producers
and therefore cannot feed one SPSC ring.

### Blocking Worker Queues

Use `crossbeam-channel::bounded` for OS-thread worker pools that may block or
select over multiple queues:

- bounded blocking disk fallback workers,
- maintenance jobs,
- shutdown coordination.

The normal split-profile CPU/hash executor is the project-owned Rayon pool.
Project code places a bounded admission/byte-budget wrapper in front of Rayon,
then lets Rayon use its own work-stealing queues; it does not build a second
crossbeam worker pool around Rayon. The compact minimum-thread profile may use
one crossbeam-backed shared disk/CPU worker as the explicit exception described
in `threading-model.md`.

Crossbeam is mature, MPMC, supports bounded channels, cloneable receivers, and
blocking/time-limited operations. It must not be used with blocking `send` or
`recv` on async reactor threads.

### Completion Drains

Disk/CPU/journal completion delivery is not an admission queue. Once a backend
operation has been accepted, its outcome and any carried `BufferLease` must
always reach its consumer without competing with externally produced commands.

`CompletionPermit` is the mechanism:

- a move-only permit is reserved from the drain's capacity at submission
  admission, before the operation is handed to the OS/backend/pool,
- if the backend rejects the submission before acceptance, the submitter
  consumes the permit by returning it (and the buffer) immediately,
- if the backend accepts, permit and buffer travel with the operation and the
  permit is consumed by exactly one outcome send, which cannot fail for
  capacity reasons because the capacity was pre-reserved,
- a permit is not `Clone` and has no second consumption path; drop without
  consumption is a leak bug surfaced by debug assertions and drain accounting,
- cancellation does not destroy the permit: a cancelled accepted operation still
  drains to exactly one outcome (or cancel-confirmation), and stale-generation
  outcomes are discarded by the consumer without losing buffer ownership,
- closing a drain first stops new permit reservations, then drains outstanding
  permits to outcomes or quarantine before the receiver is dropped.

The first slice uses one bounded Tokio MPSC completion drain per lane pair. A
measured backend may later use one reserved SPSC lane per worker merged by a
single drainer. Bound disk submission bytes and operations so completion
delivery never fails or blocks the reactor indefinitely.

## Default Topology

```text
control/RPC -> Tokio bounded mpsc -> scheduler

network worker shard -> bounded HotMpscLane -> disk submit lane
disk workers/backend -> CompletionDrain<DiskWriteOutcome> -> storage/journal ack

network/hash submitters -> bounded HotMpscLane (Tokio first slice) -> bounded Rayon admission
Rayon CPU/hash workers -> reserved CompletionDrain (or measured SPSC shard) -> scheduler

libtorrent session thread -> bounded nonblocking bridge -> scheduler snapshots
```

The first slice keeps bounded Tokio lanes. Where measurements later justify it,
prefer sharding into SPSC lanes over one contended MPMC queue. If sharding creates
idle memory or complexity without measured benefit, keep the bounded MPSC lane.

## Backpressure Contract

Every hot queue has:

- item capacity,
- byte budget for carried buffers,
- nonblocking `try_send` path,
- async or blocking wait path appropriate to the lane,
- close/cancel behavior,
- queue depth metrics,
- p99 enqueue wait metrics.

Protocol workers reserve queue credit before reading body bytes when possible.
If queue credit is unavailable, they stop socket reads instead of filling
buffers that cannot be submitted to disk.

Full queues are not fatal by themselves. They are backpressure.

Completion drains are the exception to ordinary queue-full handling: capacity is
reserved before submission, and an already-created completion is never rejected.
`CompletionPermit` is a move-only reservation acquired with submission
admission and consumed by exactly one outcome. Rejection before backend
acceptance returns the permit and buffer immediately; backend acceptance
transfers both to the completion path.

## External Client Event Queues

Each WebSocket or stdio event subscriber owns a separate bounded queue. External
writers never run on or block the scheduler/storage actors.

Event classes:

- JSON-RPC replies, terminal errors, durability failures, and shutdown notices
  are lossless; if they cannot be queued within the client deadline, disconnect
  the client with a deterministic slow-consumer error.
- Replaceable status/stat/progress notifications coalesce by `(gid, event kind)`;
  only the newest snapshot is retained.
- Non-terminal informational events may be dropped after a per-client counter is
  incremented. The next delivered notification includes the loss count.

After coalescing/drop or reconnect, the client uses the normal snapshot query to
recover current state. Queue capacity, coalesced count, dropped count, and
disconnect reason are exposed in diagnostics. One slow client cannot consume
another client's queue budget.

## Shared Memory Model

Allowed:

- move-only `BufferLease` ownership transfer,
- immutable shared metadata through `Arc`,
- atomics for hot counters,
- read-only snapshots for RPC/status,
- audited unsafe internals inside queue crates,
- pinned or registered buffers for io_uring/IOCP.

Disallowed:

- `Arc<Mutex<Vec<u8>>>` transfer payloads,
- mutable shared byte buffers across lanes,
- unbounded transfer queues,
- blocking channel operations on async event-loop threads,
- holding a *filled* buffer while waiting for unrelated queue capacity (an
  empty lease reserved after storage credit is acquired is allowed; see the read
  backpressure flow in `detailed-runtime.md`, which reserves storage credit
  before the buffer lease),
- using queue order as a substitute for storage offset validation.

## Why Rust Still Wins

A C or C++ core can use shared memory queues directly, but it also reopens
use-after-free, aliasing, callback lifetime, and cancellation bugs in the
highest-risk parts of the downloader. Rust can still use shared-memory queues:
the unsafe code lives inside small, audited crates and platform adapters, while
the downloader's transfer ownership remains typed.

The performance target is not "copy Rust values between threads". It is:

- keep payload bytes in pooled buffers,
- move descriptors cheaply,
- use SPSC rings where topology allows,
- use bounded MPSC/MPMC only where the topology requires it,
- measure queue latency and allocation behavior in CI.

## Selection Matrix

```text
control async command      tokio::sync::mpsc bounded (split urgent/bulk lanes)
one-shot reply             tokio::sync::oneshot
state watch                tokio::sync::watch or snapshot atomics
shutdown/cancel signal     out-of-band watch/cancellation token, never a queued slot
hot async MPSC             Tokio bounded first; thingbuf only after benchmark
fixed SPSC hot lane        rtrb only after a proven one-producer/one-consumer benchmark
blocking MPMC workers      crossbeam-channel bounded
disk/cpu/journal completions bounded CompletionDrain with reserved CompletionPermit
external client events     per-client bounded/coalescing queue
metrics samples            atomics + periodic snapshot, not per-byte messages
```

For the first slice, the `hot async MPSC` and any candidate SPSC lane use bounded
Tokio channels behind the wrappers. The named specialized crates are post-baseline
options, not unconditional dependencies.

## Implementation Rules

- Define project-owned wrapper types: `ControlQueue`, `HotMpscLane`,
  `SpscLane`, `CompletionDrain`, `ClientEventQueue`, and
  `BlockingWorkerQueue`.
- Keep queue choices out of user-facing compatibility options.
- Expose selected queue classes and queue depths through diagnostics.
- Use loom or model tests for queue wrapper shutdown/cancellation behavior.
- Keep direct dependency use out of protocol/storage code so queue choices can
  be benchmarked and swapped without touching correctness logic.
- Avoid dynamic dispatch in hot loops; wrappers can be generic or selected at
  construction outside the loop.

## Benchmark Gates

Before a queue class is accepted for a hot lane, measure:

- steady-state messages/sec for descriptor-sized messages,
- p50/p95/p99 enqueue and dequeue latency,
- allocation count after warmup,
- behavior under full-queue backpressure,
- producer/consumer imbalance,
- one producer/consumer and many producer scenarios,
- shutdown/cancel latency,
- cache-line contention under CPU pinning where available.

Correctness gates:

- no payload copies in queues,
- queue full does not leak buffers,
- cancellation returns or quarantines leases,
- late messages from old task generations are rejected by storage.
- N disk workers never share one SPSC completion ring,
- every accepted disk/CPU/journal submission delivers exactly one outcome
  (exactly-once permit consumption, including under cancellation and close),
- a rejected submission returns its permit and buffer synchronously,
- shutdown completes with every ordinary bounded queue full, via the
  out-of-band signal,
- a full urgent queue coalesces duplicate per-task commands and rejects the
  rest with a typed busy error instead of blocking internal completions,
- sustained urgent traffic cannot starve `control_bulk` (bounded-burst drain),
- receiver close during in-flight completions neither leaks a buffer nor
  double-consumes a permit,
- a stalled WebSocket/stdio client neither grows memory nor stalls internal lanes,
- coalesced clients can recover through a current-state snapshot.

## References

- Tokio bounded MPSC: https://docs.rs/tokio/latest/tokio/sync/mpsc/fn.channel.html
- Crossbeam channels: https://docs.rs/crossbeam/latest/crossbeam/channel/
- Thingbuf: https://docs.rs/thingbuf/latest/thingbuf/
- rtrb: https://docs.rs/rtrb/
