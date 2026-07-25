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

session
  one bounded SQLite owner thread in persistent builds

stats
  periodic sampler, may run on control timer but does not block scheduler
```

The first slice does not need a BT lane, but the `ResourceManager` reserves a
future lane kind so diagnostics and max-thread accounting do not change shape.
The session lane is present because the default hybrid store is part of the
first slice; memory-only tests omit it under the reduced thread minimum defined
by `threading-model.md`.

## ResourceManager

```rust
pub struct ResourceManager {
    pub max_threads: usize,
    pub resident_budget: ByteBudget,
    pub socket_budget: Budget,
    pub file_budget: Budget,
    pub buffer_budget: ByteBudget,
    pub http_ingress_budget: ByteBudget,
    pub sftp_ingress_budget: ByteBudget,
    pub piece_metadata_budget: ByteBudget,
    pub task_metadata_budget: ByteBudget,
    pub transform_budget: ByteBudget,
    pub journal_state_budget: ByteBudget,
    pub sqlite_cache_budget: ByteBudget,
    pub metadata_cache_budget: ByteBudget,
    pub cpu_scratch_budget: ByteBudget,
    pub quarantine_budget: ByteBudget,
    pub disk_queue_budget: QueueBudget,
    pub cpu_queue_budget: QueueBudget,
    pub rpc_budget: QueueBudget,
}
```

Budget rules:

- budget acquisition is explicit,
- every resident allocation acquires both its domain permit and one disjoint
  byte charge from `resident_budget`; domain maxima may intentionally sum above
  the resident target for workload flexibility, but simultaneous reservations
  cannot. Releasing either permit without the other is an accounting defect,
- failed acquisition returns backpressure, not allocation growth,
- control/journal priority reserves cannot be consumed by bulk writes; this is
  enforced structurally by the split control queue (see Queue Topology), not by a
  priority-ordered channel,
- `quarantine_budget` bounds buffers held for cancellation-uncertain I/O and
  counts against the pool total (see Buffer Pool),
- `disk-cache` bytes are retained pooled buffers counted against `buffer_budget`,
  not a separate allocator (see Buffer Pool),
- Hyper/TLS-owned response frames count against `http_ingress_budget` until
  copied/split into a `BufferLease` and released; they are never hidden inside
  the transfer-pool number,
- russh-sftp-owned `SSH_FXP_DATA` vectors count against
  `sftp_ingress_budget` from request admission until their bytes are copied into
  a `BufferLease` and the vector is released. Outstanding offset requests
  reserve the configured packet-buffer cap plus requested data length first, so
  the decoder's temporary double allocation cannot create unbudgeted ingress,
- HTTP/1 connection admission reserves its resolved Hyper max read-buffer,
  response-header allowance, and dynamic TLS record-buffer allowance against
  `http_ingress_budget`. HTTP/2 admission reserves a conservative bound of
  `min(connection_window, active_streams * stream_window)` plus one configured
  max frame and header allowance per active stream and dynamic TLS allowance.
  Fixed connection/TLS/socket/runtime objects are measured and charged once as
  connection overhead in the resident-memory/admission equation, not again to
  ingress. The scheduler admits a connection/stream only when both disjoint
  reservations fit,
- HTTP/2 adaptive receive windows are feature-gated in the first slice because
  their growth is not bounded by the fixed reservation formula. Fixed window
  mode is the default and the only implemented bounded-memory mode,
- piece-state admission reserves packed durable/verified maps and bounded sparse
  active-piece metadata against `piece_metadata_budget`; no task creates one
  heap object per possible piece,
- variable file-layout, URI/source, option, retry-history, and task diagnostic
  structures reserve `task_metadata_budget`; the fixed per-task shell is
  separately accounted in the resident equation,
- relocatable decompression/decoding output uses the separate
  `transform_budget`, which is zero while growing/transformed output is
  feature-gated. A feature cannot enable `TransformBuffer` allocation without a
  nonzero bounded profile entry and resident-memory term,
- appender buffers/indexes, SQLite page cache, DNS/cookie/server-stat caches,
  CPU-private scratch, and serialized/pending RPC responses use their named
  budgets rather than disappearing into a generic overhead estimate.
  `rpc_budget` has both item and byte limits; bytes are charged from the first
  serializer chunk until the transport releases them,
- the accounted resident limit keeps a fixed headroom fraction for allocator
  fragmentation and measured framework overhead. A periodic RSS observer may
  stop new admission sooner, but it never authorizes allocation beyond permits,
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
- queue metrics include p95/p99 wait where measurable; per-message metric work
  on hot lanes is limited to relaxed atomic counters and coarse timestamp
  sampling (e.g. every Nth message), not a per-message histogram insert.

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

The control plane uses two bounded external channels plus reserved internal
lanes rather than a priority-ordered primitive (none of the selected queue
crates is priority-capable):

- `control_urgent` carries externally produced pause, remove, cancel, and
  position commands. Its capacity is a reserve that bulk commands cannot
  consume; duplicate per-task commands are coalesced at admission and overflow
  is rejected with a typed busy error.
- `control_bulk` carries `addUri`/`addTorrent` and long status-scan commands.
  Admission backpressure (`backpressure.md` "reject new RPC adds") applies only
  to `control_bulk`.
- Internal completion/journal acknowledgement lanes are permit-reserved
  (`CompletionPermit`) and carry only outcomes for already accepted work.
  External producers cannot enqueue into them, so journal-critical progress
  never competes with RPC traffic.
- Shutdown/global cancellation is an out-of-band watch signal observed by every
  lane, not a queued command slot.

The scheduler drains external urgent work in bounded bursts (up to
`urgent_burst`, default 32 commands) and then services at least one
`control_bulk` command when one is queued, so `pause`/`remove` are serviced
promptly even during an add burst while sustained urgent traffic cannot starve
adds and status scans. Internal completion lanes are polled independently of
this fairness quota: hard completion progress (disk outcomes, journal
durability acks) does not wait behind either external queue. This is the
mechanism behind the "control/journal priority" requirement in
`threading-model.md` and the "pause/remove enqueue immediately" target in
`backpressure.md`.

If implementation starts with Tokio bounded channels for some hot lanes, it
must keep the wrapper API and benchmark replacement with `thingbuf`/SPSC before
C10k claims.

The first slice uses one bounded Tokio MPSC `CompletionDrain` with a move-only
`CompletionPermit` reserved before each disk/CPU/journal operation is accepted.
The permit travels with the operation and sends exactly one outcome without
another capacity race; rejection before acceptance returns permit and buffer
synchronously; cancellation drains the accepted operation to an outcome or
cancel-confirmation rather than destroying the permit. A later measured backend
may use one SPSC queue per disk worker merged by a single drainer; a blocking
pool with N workers never points all N at one SPSC ring. Capacity/backpressure
lives on disk submission, so an accepted completion cannot be dropped or
rejected because a bounded ring is full. `messaging-model.md` owns the full
permit lifecycle (reserve/reject/consume/close).

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
5. an empty buffer lease or bounded HTTP ingress slot is reserved,
6. finite discard-guard credit is available for the attempt,
7. a bounded `RatePermit` is available for the protocol read/body poll.

The sequence is check-all-or-back-off, not accumulate-and-hold: if any step
cannot be satisfied, the worker releases anything it tentatively reserved,
removes or keeps read interest per protocol state, publishes a backpressure
reason if sustained, and re-runs the sequence when woken. Nothing scarce is held
across a wait.

Ordering rationale:

- Queue credit is reserved (step 4) before the buffer lease (step 5) so a worker
  never holds a pool buffer while blocked on downstream storage capacity, per the
  `messaging-model.md` shared-memory rule.
- The user rate limiter is a token-debiting read gate. Raw FTP reads are sized
  by the permit; an SFTP offset request is sent only after reserving its bounded
  response quantum and charges the returned data on acceptance. Hyper body
  polling requires the permit first; a yielded frame is charged immediately and
  any bounded one-frame/window overshoot becomes token debt before another poll.
- Once bytes are accepted from the protocol read, storage writes them without a
  second rate-limit wait. Unused permit bytes are returned, while bytes later
  aborted/discarded are not refunded. Step 6 is the additional finite discard
  guard that bounds cumulative waste independently of bandwidth. See
  `rate-limiting.md`.

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
- `shutdown` is delivered through the out-of-band watch/cancellation signal
  (see Queue Topology), so it takes effect even when every ordinary bounded
  queue is full; the RPC/CLI verb only acknowledges through the normal reply
  path.
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
    pub backend_epoch: BackendEpoch,
    pub lease: BufferLease,
    pub result: Result<DiskCompletion, DiskErrorKind>,
}

impl DiskBackendKind {
    pub fn name(&self) -> &'static str;
    pub fn capabilities(&self) -> DiskCapabilities;

    pub async fn open_safe(&self, req: SafeOpenRequest) -> Result<FileHandle>;
    pub async fn allocate(&self, file: FileHandle, off: u64, len: u64,
        mode: AllocationMode) -> Result<()>;
    pub async fn write_at(&self, file: FileHandle, off: u64,
        lease: BufferLease) -> DiskWriteOutcome;
    pub async fn read_at(&self, file: FileHandle, off: u64, len: usize)
        -> Result<BufferLease>;
    pub async fn sync_data(&self, file: FileHandle) -> Result<()>;
    pub async fn sync_all(&self, file: FileHandle) -> Result<()>;
    pub async fn rename_safe(&self, req: SafeRenameRequest) -> Result<()>;
    pub async fn close(&self, file: FileHandle) -> Result<()>;
}
```

Contract notes:

- The trait is offset-based; no protocol worker ever receives a mutable file
  cursor.
- `SafeOpenRequest`/`SafeRenameRequest` contain the retained canonical-root
  capability plus validated relative components from `SafePathBuilder`. A
  display/serialized absolute `PathBuf` is never reopened as authority. Secure
  descendant resolution is part of every backend, including the blocking
  fallback; an unavailable strong primitive is a typed capability failure.
- `FileHandle` is bound to one `BackendEpoch`; it cannot be submitted to or
  reinterpreted by another backend. Every outcome repeats the epoch. Live
  failover follows the stop/drain/abort/close/reopen barrier in
  `disk-adapter.md`, and a stale-epoch result may release ownership but never
  mutate storage state.
- `sync_data` maps to `fdatasync` (data only); `sync_all` maps to `fsync` (data
  plus metadata). The durability mode chooses which to call, so the backend does
  not take an fsync-mode enum.
- `allocate` is required by the preallocation modes in `disk-adapter.md`;
  `rename_safe` is required by temp-to-final finalization in
  `detailed-storage.md`.
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
- a sustained saturating urgent stream still lets queued bulk commands make
  progress (bounded-burst fairness),
- duplicate urgent commands for one gid coalesce; urgent overflow returns a
  typed busy error and never blocks an internal completion lane,
- shutdown initiates and completes while every ordinary queue is full,
- disk/CPU/journal completions are delivered exactly once per accepted
  submission under load, cancellation, and lane close,
- graceful shutdown flushes and fsyncs the journal before closing the disk lane
  (a kill after the last durable record still recovers it),
- graceful BT shutdown waits for resume data before the session checkpoint and
  reports a dirty checkpoint on timeout,
- success, short write, backend error, cancellation, and shutdown each return or
  quarantine exactly one lease,
- blocking worker queue cannot be used from reactor context in tests,
- metrics update under enqueue/dequeue/backpressure,
- slow disk drives read backpressure before memory cap is exceeded.
