# Detailed Runtime, Queues, And Buffer Design

Status: detailed draft for the first implementation slice.

This document defines the concrete runtime lanes, bounded queues, buffer leases,
and cancellation behavior used by HTTP and storage in the first slice.

## Runtime Lanes

First-slice lanes:

```text
control
  single owner of scheduler state and command application

network
  Tokio runtime for HTTP sockets, timers, TLS, protocol workers

disk
  DiskBackend submit/completion lane or bounded blocking workers

cpu
  hash/checksum and expensive parser workers

stats
  periodic sampler, may run on control timer but does not block scheduler
```

The first slice does not need a BT lane, but the `ResourceManager` reserves a
future lane kind so diagnostics and max-thread accounting do not change shape.

## ResourceManager

```rust
pub struct ResourceManager {
    pub max_threads: usize,
    pub socket_budget: Budget,
    pub file_budget: Budget,
    pub buffer_budget: ByteBudget,
    pub quarantine_budget: ByteBudget,
    pub disk_queue_budget: QueueBudget,
    pub cpu_queue_budget: QueueBudget,
    pub rpc_budget: QueueBudget,
}
```

Budget rules:

- budget acquisition is explicit,
- failed acquisition returns backpressure, not allocation growth,
- control/journal priority reserves cannot be consumed by bulk writes; this is
  enforced structurally by the split control queue (see Queue Topology), not by a
  priority-ordered channel,
- `quarantine_budget` bounds buffers held for cancellation-uncertain I/O and
  counts against the pool total (see Buffer Pool),
- `disk-cache` bytes are retained pooled buffers counted against `buffer_budget`,
  not a separate allocator (see Buffer Pool),
- all budgets are visible in diagnostics.

## Queue Wrappers

Project-owned wrappers hide crate choices:

```rust
pub struct ControlQueue<T>;
pub struct HotMpscLane<T>;
pub struct SpscLane<T>;
pub struct CompletionDrain<T>;
pub struct BlockingWorkerQueue<T>;
```

Required operations:

```rust
try_send(T) -> Result<(), Full<T>>
send_async(T, CancelToken) -> Result<(), SendError<T>>
try_recv() -> Option<T>
close(CloseReason)
len() -> usize
capacity() -> usize
metrics() -> QueueMetrics
```

Rules:

- async reactor tasks never block on a blocking queue,
- blocking workers never use unbounded queues,
- full hot queues trigger backpressure and stop network reads,
- completion drains never reject a completion after the OS has accepted the
  operation; submissions are bounded before I/O is issued,
- close returns queued `BufferLease`s or sends them to quarantine,
- queue metrics include p95/p99 wait where measurable.

## Queue Topology

First-slice topology:

```text
RPC/CLI/API urgent -> ControlQueue<UrgentCommand> --\
                                                     >-- scheduler
RPC/CLI/API bulk   -> ControlQueue<BulkCommand>   --/

HTTP worker -> HotMpscLane<StorageCommand::WriteBlock> -> storage
storage -> DiskBackendKind -> CompletionDrain<DiskWriteOutcome> -> storage ack

storage/hash request -> HotMpscLane<HashJob> -> cpu pool
cpu pool -> ControlQueue<VerificationEvent> -> scheduler/storage

stats timer -> snapshot store
```

The control plane uses two bounded channels rather than a priority-ordered
primitive (none of the selected queue crates is priority-capable):

- `control_urgent` carries pause, remove, cancel, shutdown, and journal-critical
  commands. Its capacity is a reserve that bulk commands cannot consume.
- `control_bulk` carries `addUri`/`addTorrent` and long status-scan commands.
  Admission backpressure (`backpressure.md` "reject new RPC adds") applies only
  to `control_bulk`.

The control loop drains `control_urgent` first using a biased `tokio::select!`
(poll urgent, then bulk), so `pause`/`remove` enqueue and are serviced
immediately even when a burst of queued adds fills `control_bulk`. This is the
mechanism behind the "control/journal priority" requirement in
`threading-model.md` and the "pause/remove enqueue immediately" target in
`backpressure.md`.

If implementation starts with Tokio bounded channels for some hot lanes, it
must keep the wrapper API and benchmark replacement with `thingbuf`/SPSC before
C10k claims.

`CompletionDrain` is either one MPSC/MPMC queue or one SPSC queue per disk worker
merged by a single drainer. A blocking pool with N workers never points all N at
one SPSC ring. Capacity/backpressure lives on disk submission; once an operation
is accepted, its completion has reserved drain capacity and cannot be dropped or
rejected because a bounded ring is full.

## Buffer Pool

```rust
pub struct BufferPool {
    classes: [SizeClassPool; N],
    total_budget: ByteBudget,
    quarantine: Quarantine,
}

pub struct Quarantine {
    budget: ByteBudget,       // counts against total_budget
    timeout: Duration,        // bounded wait for completion/cancel-confirm
    held: Vec<BufferLease>,
}

pub struct BufferLease {
    id: BufferId,
    class: SizeClass,
    ptr: BufferStorage,
    len: usize,
    capacity: usize,
    state: BufferState,
    owner: OwnerTag,
    task: Option<TaskId>,
    generation: Option<Generation>,
}
```

Allowed mutable states:

```text
Reserved
NetworkFill
TransformOwned
```

Immutable states:

```text
Filled
Validating
DiskQueued
DiskInFlight
HashBorrowed
JournalPending
DiskDone
Releasable
Free
```

State transitions are checked in debug builds and return typed internal errors
in release builds.

## Quarantine And Reclamation

A lease enters quarantine when a cancellation leaves OS ownership uncertain (an
in-flight io_uring/IOCP write whose completion has not yet been observed). Without
bounds, an unhealthy backend that never reports completion would hold leases
forever, so quarantine is budgeted and reclaimed:

- Quarantined bytes count against `quarantine_budget` and the pool total.
- On cancellation the backend issues an explicit kernel cancel
  (`IORING_OP_ASYNC_CANCEL` / `CancelIoEx`) and reclaims the lease only after a
  completion or a cancel-confirmation.
- If neither arrives within `quarantine_timeout`, the lease's memory is retired
  from the pool (leak-accounted, not returned to a size class) and a replacement
  may be allocated only within the pool total, so memory stays bounded.
- When `quarantine_budget` is exhausted, the backend transitions to `Faulted` and
  stops issuing new I/O (fall back per `event-backends.md`) rather than growing
  memory. The transition is surfaced in metrics.

## Buffer Class Selection

Default classes:

```text
16 KiB
64 KiB
256 KiB
1 MiB
```

Selection inputs:

- task profile,
- current speed,
- disk pressure,
- active stream count,
- range lease size,
- checksum boundary,
- memory pressure.

Idle connections hold no payload buffer. Slow active connections should use
small buffers unless throughput and disk pressure justify growth.

## Read Backpressure Flow

Before an HTTP worker reads body bytes:

1. task generation is active,
2. range lease is valid,
3. cancellation token is not cancelled,
4. storage queue credit is available or reserved,
5. buffer lease is reserved,
6. finite discard-guard credit is available for the attempt,
7. the non-debiting wire-pacing signal from `rate-limiting.md` is not
   exhausted when a user rate limit is configured on the worker's path.

The sequence is check-all-or-back-off, not accumulate-and-hold: if any step
cannot be satisfied, the worker releases anything it tentatively reserved,
removes or keeps read interest per protocol state, publishes a backpressure
reason if sustained, and re-runs the sequence when woken. Nothing scarce is held
across a wait.

Ordering rationale:

- Queue credit is reserved (step 4) before the buffer lease (step 5) so a worker
  never holds a pool buffer while blocked on downstream storage capacity, per the
  `messaging-model.md` shared-memory rule.
- The user rate limiter is a `CommitLease` gate plus a non-debiting read-pacing
  signal, not a token-debiting pre-read admission test. Once exact response
  framing and identity validation succeed, a validated provisional lease waits
  for user tokens with only bounded lease metadata; its buffer has already been
  returned after the provisional disk write. A failed or aborted lease consumes
  no user-rate tokens. Step 6 is instead the separate finite discard guard that
  prevents rejected bodies from becoming an unbounded raw-network bypass, and
  step 7 keeps the wire near the configured rate without debiting tokens for
  uncommitted bytes. See `rate-limiting.md`.

## Cancellation

Cancellation token sources:

- user pause/remove,
- active restart option,
- stale validator restart,
- shutdown,
- retry timeout/hang cancellation,
- backend fatal error.

Cancellation rules:

- network workers stop issuing new reads,
- filled buffers already submitted to disk complete or are rejected by
  generation,
- leases not submitted return to pool,
- retry timers are cancelled without waking disk/cpu lanes,
- cancellation ack is sent to scheduler.

## Graceful Shutdown Ordering

Process shutdown (SIGTERM, RPC `shutdown`, or CLI exit) must quiesce lanes in a
fixed order so recovery state is durable before exit. A normal shutdown must not
lose the recovery guarantee that power-loss recovery provides.

Order:

1. control/network: stop new commands, reads, and lease admission,
2. bt quiesce: stop new BT work and request resume data (full build),
3. disk/cpu: drain in-flight writes and verification, committing or aborting
   each provisional lease,
4. bt checkpoint: await requested resume data and stage it in the task/session
   checkpoint; timeout marks the checkpoint dirty,
5. journal: flush and `sync_all` the serialized journal through its last trusted
   durability/abort record,
6. session: persist queue/state plus the staged BT resume data,
7. bt stop: destroy the libtorrent session only after persistence completes,
8. exit.

Rules:

- The journal fsync (step 5) must complete before the process exits; a shutdown
  that closes the disk/journal lane before the last durable-piece record is
  fsynced is a defect.
- Lanes are closed drain-then-close, never close-then-drain, so a lane blocked on
  a full bounded channel cannot deadlock the control lane that is shutting it
  down. The control lane closes the producer side, lets the consumer drain, then
  joins.
- `shutdown` is an urgent control command (see Queue Topology) so it is serviced
  ahead of queued adds.
- A bounded graceful-timeout applies to each step. If an in-flight fsync exceeds
  the timeout, shutdown records the incomplete checkpoint and exits; recovery
  then treats the last un-fsynced pieces as pending rather than durable.
- BT resume-data timeout never permits a clean checkpoint that omits the requested
  state. The session record is explicitly dirty, and recovery applies the
  libtorrent fallback/recheck policy.

## Disk Backend Contract

This is the normative runtime dispatch definition. `event-backends.md` and
`disk-adapter.md` describe capability/probing and coalescing policy over it and
must not restate a divergent signature. Native `async fn` trait methods are not
dyn-compatible, and boxed erased futures add one allocation per call, so runtime
selection uses enum dispatch. The buffer type is always the move-only
`BufferLease`; there is no separate `Buffer` type.

```rust
pub enum DiskBackendKind {
    IoUring(IoUringBackend),
    Iocp(IocpBackend),
    Blocking(BlockingPoolBackend),
}

pub struct DiskWriteOutcome {
    pub lease: BufferLease,
    pub result: Result<DiskCompletion, DiskErrorKind>,
}

impl DiskBackendKind {
    pub fn name(&self) -> &'static str;
    pub fn capabilities(&self) -> DiskCapabilities;

    pub async fn open(&self, req: OpenRequest) -> Result<FileHandle>;
    pub async fn allocate(&self, file: FileHandle, off: u64, len: u64,
        mode: AllocationMode) -> Result<()>;
    pub async fn write_at(&self, file: FileHandle, off: u64,
        lease: BufferLease) -> DiskWriteOutcome;
    pub async fn read_at(&self, file: FileHandle, off: u64, len: usize)
        -> Result<BufferLease>;
    pub async fn sync_data(&self, file: FileHandle) -> Result<()>;
    pub async fn sync_all(&self, file: FileHandle) -> Result<()>;
    pub async fn rename(&self, from: SafePath, to: SafePath) -> Result<()>;
    pub async fn close(&self, file: FileHandle) -> Result<()>;
}
```

Contract notes:

- The trait is offset-based; no protocol worker ever receives a mutable file
  cursor.
- `sync_data` maps to `fdatasync` (data only); `sync_all` maps to `fsync` (data
  plus metadata). The durability mode chooses which to call, so the backend does
  not take an fsync-mode enum.
- `allocate` is required by the preallocation modes in `disk-adapter.md`;
  `rename` is required by temp-to-final finalization in `detailed-storage.md`.
- `name`/`capabilities` back the disk probe/fallback logic in
  `event-backends.md`;
  the rest of the engine depends on `capabilities`, not concrete system APIs.

`DiskWriteOutcome` always returns the same `BufferLease`, on success or error.
There is no lease-less write error. The storage engine either advances it to
`DiskDone`, returns it to the pool after hashing/journal use, or quarantines it
when OS ownership is cancellation-uncertain. Short writes are errors with the
lease present; retry never aliases or reconstructs ownership through a side
channel.

## Metrics

Expose:

- queue depth/capacity per lane,
- enqueue wait per lane,
- buffer pool allocated/free/in-flight/quarantined bytes,
- buffer wait time,
- disk queue bytes and ops,
- event-loop lag,
- cancellation latency,
- stale generation message count,
- quarantined bytes and quarantine-reclaim (timeout) count,
- control_urgent vs control_bulk queue depth and enqueue wait,
- backend fallback reason.

## Tests

Required tests:

- full hot queue stops HTTP reads,
- cancellation returns unsubmitted buffers,
- disk completion after cancellation is generation-rejected,
- N blocking workers cannot feed one SPSC completion ring,
- completion delivery cannot fail after submission capacity was granted,
- queue close returns leases,
- buffer invalid transition fails,
- buffer leak moves to quarantine,
- quarantine timeout retires an unconfirmed lease and stays within
  `quarantine_budget`; exhausting the budget faults the backend rather than
  growing memory,
- `pause`/`remove` on `control_urgent` are serviced ahead of a full
  `control_bulk` add burst,
- graceful shutdown flushes and fsyncs the journal before closing the disk lane
  (a kill after the last durable record still recovers it),
- graceful BT shutdown waits for resume data before the session checkpoint and
  reports a dirty checkpoint on timeout,
- success, short write, backend error, cancellation, and shutdown each return or
  quarantine exactly one lease,
- blocking worker queue cannot be used from reactor context in tests,
- metrics update under enqueue/dequeue/backpressure,
- slow disk drives read backpressure before memory cap is exceeded.
