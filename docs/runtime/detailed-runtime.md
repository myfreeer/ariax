# Detailed Runtime, Queues, And Buffer Design

[Documentation](../README.md)

Phase-5 parsing and hashing use a private CPU executor with job, resident-byte
and completion reservations acquired before submission. Reservations follow
accepted work through completion even when its caller cancels. The executor
never uses the global Rayon pool; the minimum-thread topology shares a bounded
executor for disk and CPU work. No unbounded completion queue or detached
per-metadata thread is part of the Phase-5 admission path.

Status: first-slice implementation in progress. The poll-driven scheduler
driver, immutable applied status roots, dual byte permits, stable `BufferLease`
state machine, bounded lazy pool/quarantine, item+byte queue credits, reserved
completion delivery, the bounded blocking write lane, and the session-owner
persistence composition sink are implemented. The bounded move-only shutdown
coordinator, packet-independent bounded stats sampler, bounded scheduler runtime
effect adapter, and publication-last process bootstrap are also executable.
Tokio HTTP/RPC lanes, HTTP counter producers and shared profile/RPC reservations
are integrated. Immutable query projection outside the owner, the managed
urgent/bulk runtime, and bounded mutation continuations are implemented under
`P4-11`. Validation and native Windows evidence are recorded in
[implementation-readiness.md](../project/implementation-readiness.md) and [performance-profiles.md](performance-profiles.md). Native kernel
cancellation and the full runtime topology remain roadmap work; native Linux
benchmark acceptance stays deferred until CI is ready.

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
by [threading-model.md](threading-model.md).

## Scheduler Driver And Applied Status

`SchedulerDriver<S: SchedulerEffectSink>` is the sole mutable owner of
`RequestScheduler`. It accepts one external command or event only while idle,
then advances through the resulting effects through an explicit `poll` method.
`DispatchedEffect` pairs an immutable `TransitionEffect` with a nonzero
`EffectDispatchId` assigned from a process-unique allocator. The allocator
never wraps or reuses an exhausted id, so a completion routed from a separately
constructed driver cannot alias the current effect even when both drivers
otherwise have matching task state.

Rules:

- only one scheduler outcome and one sink effect are active at a time,
- `PublishSnapshot` is applied locally and is never sent to an adapter,
- a sink `Full` result leaves the same dispatch id and effect pending for an
  exact retry; `Closed` or `Failed` permanently faults the driver,
- every accepted sink effect returns exactly one `EffectCompletion`; completions
  must arrive in dispatch order and may contain only an acknowledgement event
  valid for that effect identity,
- a completion carrying another driver's dispatch id is rejected before it can
  advance the current effect or staged publication root,
- acknowledgements produced by an effect are deferred until the complete
  producing effect vector has been applied, preventing scheduler re-entry in
  the middle of an ordered batch,
- acknowledgement-derived outcomes run before a new external input and share
  the same staged publication root,
- a batch chain publishes at most one new immutable status root, and publishes
  nothing if dispatch faults,
- callers never receive mutable access to the sink. A sink may expose one typed
  preparation value through `SchedulerEffectSinkPrepare`; the driver accepts
  that value only at the same unfaulted idle boundary as an external scheduler
  input, so preparation cannot invoke or race the ordered dispatch/completion
  path,
- driver faults are sticky and reject further mutation.

The HTTP control owner reserves scheduler simulation and status-draft memory
before accepting a mutating command. The forecast includes existing task
state, a prior root retained by a bulk command, draft validation indexes,
queue/effect copies, and new-task allowance;
it shares the client's 8 MiB request/command ceiling with parsed input.
Exhaustion rejects before journal or session mutation. Command-copy credit
retires when synchronous work finishes, while a still-pending driver chain
retains both input and copy leases across timeout or caller cancellation.
Deferred option/source data keeps its separate input lease after a driver
chain becomes idle. A later owner progress turn reserves fresh scratch before
consuming a runtime event or preparing readmission, so a budget rejection
cannot lose an already-popped event. Faulted chains retain credit until teardown.
At an idle command boundary, the control owner discards unused persistence and
option-application preparations before releasing input credit. These are plans
that were never dispatched, including the unused tail after a rejected or failed
command. Discard uses the driver's typed preparation boundary and is rejected
while an effect chain is active; it cannot cancel an accepted owner write.

Read-only control queries project the last published immutable status root;
they do not first drive pending persistence or build an event-difference map.
Ordinary owner progress publishes task events. This keeps queries available
when command scratch is exhausted, without exposing planned scheduler state.

An accepted public mutation retains a bounded owner continuation when its
scheduler chain outlives the initiating call. The continuation keeps typed
input, scheduler scratch, and the catalog publication data until the next owner
progress turn completes the durable chain. Its reply uses a separate bounded
channel; a disconnected caller cannot cancel the accepted write. The owner
registry is drained before normal engine teardown.

Elapsed wait time does not turn an accepted mutation into a `Busy` rejection,
remove an admitted task's catalog entry, skip option publication, or abandon a
source replacement. `Busy` remains a pre-admission resource or ownership
rejection. Synchronous owner calls retain their blocking wrapper, while the
progress lane completes deferred continuations without exposing planned state.
Shutdown and genuine owner failure retain their separate dirty-checkpoint and
uncertain-write rules.
Shutdown stops new readmission, drains accepted continuation work within the
existing shutdown allowance, and preserves its permits through owner teardown.
If that drain cannot finish, shutdown records a dirty checkpoint; it cannot
report a clean session merely because the worker lane eventually stopped.

`SchedulerDriver` exclusively owns its non-cloneable `StatusSnapshotStore`.
Callers receive cloneable `StatusSnapshotReader` handles, never a writer lineage
that can be attached to another driver. Each load returns one immutable
`Arc<StatusSnapshotRoot>` containing the revision, snapshots paired with their
exact `TaskId`, and dense orders for `Waiting`, `Demoted`, `Paused`, `Active`,
and `Stopped`. Readers release the store lock immediately after cloning the root
and never hold scheduler or adapter locks while formatting. Revision zero is the
empty root; each material committed replacement increments the revision exactly
once. Publication rejects reused process-local `TaskId` values and requires each
task's only queue membership to match its snapshot state. Retry-wait membership
is selected by `retry_wait_holds_slot`; `PausedRestarting` may remain `Active`
while cancellation drains or be `Waiting` after slot release. Terminal-pending
states remain unpublishable and `StoppedResult` belongs only to `Stopped`.

Startup passes `SchedulerRestorePlan` to `SchedulerDriver::begin_restore` before
publishing the driver or its snapshot handle to other lanes. The driver stages
all five recovered queue orders, drains every dispatcher-sized restore batch,
and publishes exactly one complete root only after all recovered timer and
no-space-probe effects have been accepted. Restore acknowledgements are
forbidden. A full or temporarily backpressured sink retries the exact
dispatch; a closed, failed, or forged sink
completion faults startup and leaves the store at empty revision zero. Restore
is rejected against a nonempty snapshot store, and a plan whose private binding
does not exactly match the supplied scheduler or whose batch cursor is no longer
pristine is rejected before any effect is offered.

The host-level persistence composition sink splits this contract without
weakening it. `PublishSnapshot` remains driver-local. Persistence effects are
matched against a bounded catalog entry containing the complete, exact
`TransitionEffect`; all other effects are delegated to one downstream
`SchedulerEffectSink`. A catalog entry is consumed only after its first owner
command is accepted. A full session-owner queue retains the owned command and
the exact dispatch for retry.

The downstream runtime adapter has separate bounded request, event, timer, and
option-plan capacities. Allocation and cancellation effects become move-only
worker requests whose only completion builders preserve the exact task id, GID,
and generation. Retry and slow-readmission timers remain process-local
monotonic entries and emit their correlated scheduler event only at or after the
deadline. No-space probes retain their deadline; startup automatic probes also
consume the one move-only native target authorized by reconciliation. Worker
events use a bounded return queue, and a full adapter leaves the scheduler's
exact offered effect backpressured rather than growing memory.

Option application requires a bounded plan matching the complete
`ApplyOptionPatch` effect. The composition sink exposes persistence and runtime
preparations through one typed idle-boundary enum, so neither catalog can be
mutated during an active driver chain. Missing, duplicate, or mismatched
authority becomes an unrepresentable driver fault rather than an invented
acknowledgement.

`bootstrap_process` is the only publication-producing recovered startup path.
It starts the session owner, replays SQLite-authoritative primary journals in
canonical GID/task-id order, derives credential blockers, applies ordered
SQLite repairs, performs descriptor-safe native root/install/appender recovery,
constructs the composed sink, and drains `SchedulerRestorePlan`. It returns a
`BootstrappedEngine` and snapshot reader only after the driver is idle and every
startup probe target is consumed. Any failure closes the owner best-effort;
normal shutdown closes runtime admission, closes all flushed journals on the
owner thread, and then performs the bounded owner join.

Each persistence plan is validated before catalog admission. It contains only
the command sequence permitted for that effect kind, binds every GID,
generation, patch/challenge/resolution/deletion token, queue state/order, and
terminal status visible in both layers, and is capped in logical steps and
catalog entries. Journal durability steps expand to exactly one append followed
by one flush through the returned sequence. The sink advances at most one
session-owner command at a time and validates every typed result before issuing
the acknowledgement required by the scheduler. The current composition
represents only one failure: `StageOptionPatch` produces
`OptionPatchPersistenceFailed` for a definite first-append rejection before any
append evidence. Once mutation may have occurred, including an accepted
host-key-resolution or stopped-result-deletion command, or for an owner
disconnect/timeout, unexpected result, later flush failure, or SQLite mirror
failure, the sink returns `UnrepresentableFailure` and faults the driver for
recovery. A missing/mismatched plan is likewise driver-fatal.

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

`RpcBudgets` belongs to `HttpProcessResources`: it uses the resolved profile's
RPC item/byte limits and the same resident budget as HTTP ingress and storage.
Cloned dispatchers and listeners retain this instance. A connection owns one
`RpcClientBudget`; its four request slots count a reader-held body, queued or
executing commands, and a response awaiting transport release. The client's
combined allocation ceiling is the smaller of 32 MiB and three quarters of the
process RPC byte limit, so a single client cannot reserve the whole domain.
The separate request/command ceiling remains 8 MiB.

Request reservations precede body allocation and include parser scratch.
Bounded Serde visitors reserve conservative node, container, and string storage
before constructing owned values; a short JSON array of tiny values must not
expand beyond its reservation. Deferred mutations retain their request lease
after the original caller disconnects, until the accepted command retires.

Response admission precedes backend result materialization. Result builders
use a bounded workspace and serialization acquires additional chunk credit
before growing its output. A response owner carries its request slot and byte
permits into `Bytes`; clones and slices keep credit until the last transport
reference is released. Stdio retains that owner through `write_all` and
`flush`, including errors or cancellation. Event delivery uses the same
allocation domain and does not create an uncharged parallel output queue.
Framework receive/write caches remain charged for as long as the connection
retains their capacity; draining their logical contents does not refund that
capacity. A cancelled stdio transport aborts its reader task so queued and
reader-held request leases cannot outlive the transport accidentally.
Event queue entries consume process RPC item credit as well as client, process,
and resident bytes. Queued events may use at most half the process item limit,
leaving command/reply capacity for polling or removing subscriptions even when
their event queues are full. Coalescing, informational loss, and reliable-event
overflow retain their documented behavior when shared credit is exhausted.

Owned JSON result construction first measures a borrowed `Serialize` view with
a bounded counting serializer. This pass accounts for values, map keys,
container capacity hints, strings, and formatting scratch before
`serde_json::to_value` can allocate the result. List builders retain one bounded
row scratch area and charge accumulated rows against the same result allowance.
They iterate immutable queue slices without first copying every GID. Source and
session views borrow URI text; constructing an intermediate URI JSON tree before
the counting pass is not permitted. An oversized event poll leaves the
unreturned event queued. Result overflow remains a complete typed error.

Typed command preparation reserves a conservative peak before copying input:
twelve bytes per input string byte and 1,024 bytes per value/key cover URI
canonicalization/deduplication, typed fields, persisted rows, and ordered-owner
copies. The forecast includes current source text for `changeUri`, whose input
can be tiny while the replacement set is large, and a bounded fixed task/option
shell. Option-only changes share already-validated immutable sources instead
of re-parsing and copying them. A queued generation patch retains the request
lease through cancellation and durable promotion just as a source mutation
does. Reservations beyond the combined 8 MiB request/command ceiling reject
before journal or SQLite mutation. Config and session parsers remain subject
to their own structured limits in addition to this command reservation.
Sequential batch members release each completed command's temporary lease;
their request body and parsed batch remain charged until the batch retires.

Phase 4B implements these request, serializer, event, and transport ownership
reservations using the resolved profile and shared resident budget. Regression
tests cover parser expansion, reader backpressure, deferred commands, retained
body frames, response overflow, writer failure/cancellation, and controlled
HTTP/WebSocket stalls with another client served concurrently. Borrowed result
preflight, bounded row accumulation, typed input preparation, and native-call
projection reservations pass the same Linux, MSRV, and Windows-GNU workspace
matrix. Scheduler simulations, status drafts, and pending owner lifetimes now
use pre-admitted reservations, with failure/refund, timeout, unused-plan cleanup,
and event-retention regressions. The forecast fits 1,000 active scheduler tasks;
this is an allocation-contract test, not a real-download benchmark. The P4-11
implementation below extends these guarantees to immutable query generations,
managed control admission, filesystem preparation, and bulk continuations.
The historical Windows status/global-template report does not validate these
changes; current validation and the expanded campaign are tracked separately
in [implementation-readiness.md](../project/implementation-readiness.md) and [performance-profiles.md](performance-profiles.md). Native Linux
benchmark acceptance stays deferred until CI is ready.

### P4-11 Control Progress Contract

Production native and RPC entry points share one managed control runtime. Read
calls capture an immutable query root through a pointer-only publication lock;
projection, prefix lookup, configuration checking, and export rendering do not
acquire the mutable control owner. The root binds applied membership and status
to matching task identities, source/options metadata, and configuration
generation. Retained generations and projection work remain budgeted. Live
authenticated URI queries and persistence-safe exports keep their separate
source views. Publication precedes successful mutation replies and events.

Urgent and bulk admission use the existing profile capacities and bounded-burst
fairness below. Accepted command state and reply fan-out are bounded, including
coalesced callers. Internal completions have reserved progress and shutdown is
out of band. An owner turn performs at most 32 nonblocking progress steps or
approximately 1 ms before yielding; this is a cooperative scheduling budget,
not permission for one step to block on I/O or process an entire batch. Ordinary
bulk continuations advance at most one target per turn. Filesystem preparation
has one bounded execution slot; query/configuration projection has two separate
slots so a filesystem stall cannot consume query execution capacity.
Bulk continuations and ordinary owner progress alternate first access to that
turn budget. A scheduler step that consumes the remaining cooperative time
cannot indefinitely prevent an already accepted bulk member from starting.
Productive turns yield cooperatively and resume without a timer delay. Idle
turns and unready I/O completions use the wake signal or short polling backoff;
backpressure must not create a busy loop.

Admission preparation captures the configuration and persistence policy, validates
the complete input before creating journals, and runs outside the owner. Existing
tasks continue to progress during that work. Publication revalidates queue
positions against current state and stages one member per turn under the atomic
import fence. Journal installation, drained-pause records, and in-place option
mirrors use owned nonblocking session submissions and completion polling. Their
input, scratch, and reply ownership survive caller cancellation; an uncertain
accepted write faults the control owner for recovery before public success.

Bulk controls capture task identities once. Admission sequence numbers order
conflicting actions: a later accepted per-task pause, resume, or remove
supersedes unfinished earlier bulk work for that task. Already accepted durable
effects finish before the newer action is applied; completed members are not
rolled back. Duplicate pause coalesces, remove supersedes queued pause, and
force upgrades a queued non-force twin. Coalescing retains bounded ownership
for every caller and cannot move a later action ahead of an intervening control
for the same task. Overflow returns Busy. Atomic session import retains its
documented mutation/admission fence while allowing queries.

Deterministic tests cover 1,000-task progress, later per-task precedence,
coalescing barriers, queue saturation, stalled filesystem/SQLite work,
cancellation ownership, and import fencing. A manually driven owner uses the
same productive-turn yield and idle-turn backoff as the managed runtime;
unconditional sleeps must not multiply native timer granularity by the number
of control steps. The unoptimized 1,000-task fixture checks a bounded interval
without forward progress and an overall watchdog; native optimized benchmarks
own the elapsed-time acceptance targets. The explicit synchronous facade
and startup repair may drive blocking work; production callers use the managed
runtime. Native Linux benchmark acceptance remains deferred until CI is ready.

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

The control plane uses two bounded external mailboxes plus reserved internal
lanes rather than a priority-ordered primitive (none of the selected queue
crates is priority-capable):

- `control_urgent` carries per-task pause, resume, remove, result-removal, and
  position commands. Its capacity is a reserve that bulk commands cannot
  consume; duplicate per-task commands are coalesced at admission and overflow
  is rejected with a typed busy error.
- `control_bulk` carries adds, configuration/source changes, and bulk controls.
  Admission backpressure ([backpressure.md](backpressure.md) "reject new RPC adds") applies only
  to `control_bulk`.
- Read queries capture an immutable root and use separate bounded projection
  jobs. Large status scans do not occupy either mutable mailbox or the owner.
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
adds and bulk continuations. Internal completion lanes are polled independently of
this fairness quota: hard completion progress (disk outcomes, journal
durability acks) does not wait behind either external queue. This is the
mechanism behind the "control/journal priority" requirement in
[threading-model.md](threading-model.md) and the "pause/remove enqueue immediately" target in
[backpressure.md](backpressure.md).

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
rejected because a bounded ring is full. [messaging-model.md](messaging-model.md) owns the full
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
  stops issuing new I/O (fall back per [event-backends.md](event-backends.md)) rather than growing
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
  [messaging-model.md](messaging-model.md) shared-memory rule.
- The user rate limiter is a token-debiting read gate. Raw FTP reads are sized
  by the permit; an SFTP offset request is sent only after reserving its bounded
  response quantum and charges the returned data on acceptance. Hyper body
  polling requires the permit first; a yielded frame is charged immediately and
  any bounded one-frame/window overshoot becomes token debt before another poll.
- Once bytes are accepted from the protocol read, storage writes them without a
  second rate-limit wait. Unused permit bytes are returned, while bytes later
  aborted/discarded are not refunded. Step 6 is the additional finite discard
  guard that bounds cumulative waste independently of bandwidth. See
  [rate-limiting.md](rate-limiting.md).

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

`ShutdownCoordinator` implements this ordering as a bounded poll-driven batch.
`begin` is an out-of-band operation and therefore does not reserve capacity in
an ordinary work queue. Minimal builds execute stop-admission, disk/CPU drain,
journal flush, and session persistence; full builds insert BT quiesce,
checkpoint, and final stop at their fixed positions above. Each coordinator
mints one process-unique nonzero authority id, and every sequence-tagged
`ShutdownTicket` is privately bound to that exact authority. A ticket from a
separately constructed coordinator is stale even when its step, sequence,
deadline, and dirty bit otherwise match. Stale or out-of-order completion is
rejected, and authority/sequence exhaustion is a typed error. Each step has a
validated nonzero timeout. The coordinator is move-only: it cannot be cloned or
copied into a second authority that could accept the same ticket and duplicate
a shutdown barrier.

Failure or timeout marks the checkpoint dirty but advances through the
remaining cleanup steps, including journal flush and session persistence. The
session-persistence ticket exposes that dirty state so the store cannot record
a clean checkpoint after an earlier failure. The final report is clean only if
every selected step completed successfully; it retains bounded bitsets and the
first failure rather than allocating an unbounded error list.

The executable minimal process path binds these tickets to real owners.
`StopAdmission` closes the process runtime out of band; `DrainDiskCpu` awaits
the HTTP supervisor, whose worker futures retain their network and storage
completion authority and whose bounded abort result is reported as a timeout;
`FlushJournal` submits descriptor-owner `FlushAllJournals` followed by
`CloseAllFlushedJournals` under the same deadline; and `PersistSession` applies
the bounded session-owner join before reopening SQLite under a fresh owner lock.
Startup writes `clean_shutdown=false` before publishing the process. Shutdown
sets it true only after every earlier step and the owner join succeeded; any
failed or timed-out step leaves or restores it false. Synchronous embedding may
report an attached active worker lane dirty rather than claiming that an
immediate abort was a graceful drain.

## Disk Backend Contract

This is the normative runtime dispatch definition. [event-backends.md](event-backends.md) and
[disk-adapter.md](../storage/disk-adapter.md) describe capability/probing and coalescing policy over it and
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
  [disk-adapter.md](../storage/disk-adapter.md), and a stale-epoch result may release ownership but never
  mutate storage state.
- `sync_data` maps to `fdatasync` (data only); `sync_all` maps to `fsync` (data
  plus metadata). The durability mode chooses which to call, so the backend does
  not take an fsync-mode enum.
- `allocate` is required by the preallocation modes in [disk-adapter.md](../storage/disk-adapter.md);
  `rename_safe` is required by temp-to-final finalization in
  [detailed-storage.md](../storage/detailed-storage.md).
- `name`/`capabilities` back the disk probe/fallback logic in
  [event-backends.md](event-backends.md);
  the rest of the engine depends on `capabilities`, not concrete system APIs.

`DiskWriteOutcome` always returns the same `BufferLease`, on success or error.
There is no lease-less write error. The storage engine either advances it to
`DiskDone`, returns it to the pool after hashing/journal use, or quarantines it
when OS ownership is cancellation-uncertain. Short writes are errors with the
lease present; retry never aliases or reconstructs ownership through a side
channel.

### Bounded Blocking Write Lane

`BlockingDiskLane` is the first portable `BlockingPoolBackend` primitive. It
owns a fixed set of named OS workers and a bounded FIFO. Constructing the lane
is the only operation that creates workers; one disk submission never creates
an OS thread. The lane accepts only opaque, already-safe `BlockingFileHandle`
capabilities. It has no path-taking operation and therefore cannot turn a
display or persisted path back into authority.

Admission is fail-fast and atomic:

1. require the submission's non-cloneable cancellation registration,
2. reject reactor-context calls in module tests,
3. validate the operation id, backend epoch, handle binding, exact lease
   length, and checked `offset + length` against the handle's authorized span,
4. acquire the hard accepted-byte charge and one reserved completion slot,
5. under the queue lock, recheck shutdown and item capacity, transition the
   move-only lease to `DiskQueued`, and enqueue it.

`BlockingDiskCancelHandle` is the cloneable user cancellation handle.
`BlockingDiskCancellationRegistration` is minted with it as one pair, is not
cloneable, and moves into exactly one `BlockingDiskSubmission`. Every rejection
returns that same submission with the exact lease and unchanged cancellation
state, so transient backpressure can retry it. Once accepted, the registration
moves into the worker-owned item and cannot be reused by another operation.
This prevents two operation ids from sharing one cancellation state and
manufacturing a false cancellation.

Failure before step 5 returns the unchanged lease to the submitter. Once step 5
succeeds, exactly one worker owns the lease. A worker either confirms queued
cancellation and returns it as `Releasable`, or transitions it through
`DiskInFlight` to `DiskDone` and emits one typed success, short-write, backend,
or worker-panic result. The accepted-byte charge travels with the completion,
so it is not released until the consumer takes or drops that exact outcome.
Each accepted operation reserves its `CompletionPermit` before enqueue; worker
completion is consequently non-rejecting even when submission admission and
the receiver are closing.

Normal shutdown takes an explicit timeout, clamped to a 300-second hard
maximum. It first closes submission and completion admission, prevents workers
from claiming another queued item, and returns each still-queued lease in a
`WorkerAborted` outcome. Running calls may finish until the bounded deadline.
Workers report exit through a fixed-capacity nonblocking channel; shutdown joins
only workers that both reported exit and are observed finished. At the deadline
it detaches every remaining join handle without manufacturing an outcome for a
lease still owned by that worker.

`BlockingDiskShutdown` reports joined panics, detached workers, the in-flight
count sampled at timeout, whether the completion drain closed, and every
outcome already available. Any detached or in-flight worker makes
`completion_closed` false. The caller must treat the affected backend epoch and
files as write-uncertain: do not reopen, fail over, finalize, or continue using
them in this process. Graceful shutdown records a dirty checkpoint and proceeds
to process exit; recovery treats any uncommitted provisional write as pending.
If a detached call later returns before exit, its now-unclaimed outcome follows
the lease quarantine-on-drop fail-safe. The lane never fake-returns that lease.

`Drop` is nonblocking: it performs the same stop-admission and queued-abort
steps, joins only workers already reported and finished, and immediately
detaches the rest. This zero-wait policy prevents a destructor from defeating
the coordinator's explicit shutdown deadline. A post-close submission is
rejected with its unchanged lease.

Construction rejects zero or implementation-defined oversized worker, item,
and completion capacities before allocation. All queue, identity-set, and
worker-handle reservations are fallible and map allocation failure to a typed
start error rather than panicking on capacity overflow.

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
