# Detailed Core Design

Status: detailed draft for the first implementation slice.

This document defines the core types and state machines shared by config,
scheduler, storage, HTTP, RPC, and session persistence.

## Crate Boundary

Suggested crates for the first slice:

```text
ariax-core
  ids, errors, option snapshots, task state, scheduler commands

ariax-config
  option registry, flat config parser, URL rules, runtime update matrix

ariax-storage
  SafePathBuilder, FileLayout, GlobalOffsetMapper, ControlJournal,
  StorageEngine facade

ariax-runtime
  worker lanes, queue wrappers, cancellation, backend probes

ariax-http
  HTTP protocol adapter for sequential/range downloads

ariax-rpc
  JSON-RPC dispatcher and transports
```

The first implementation may keep these as modules in one crate, but the APIs
should respect these ownership boundaries.

## IDs

```rust
pub struct Gid(NonZeroU64);
pub struct TaskId(NonZeroU64);
pub struct Generation(u64);
pub struct LeaseId(NonZeroU64);
pub struct OverlapGroupId(NonZeroU64);
pub struct PieceId(u64);
pub struct FileId(u32);
pub struct UriId(u32);
pub struct BufferId(NonZeroU64);
```

Rules:

- `Gid` is the user/RPC-visible stable id.
- Its aria2-compatible wire form is exactly 16 lowercase hexadecimal digits,
  including leading zeroes (for example `00000000000000af`).  `0` is never a
  valid GID.
- A supplied `--gid` must contain exactly 16 hexadecimal digits; input case is
  accepted but all output is normalized to lowercase.  A supplied value that
  collides with a live task or retained stopped result fails with `GidCollision`;
  the engine never silently substitutes a generated GID.
- RPC task lookup accepts a unique 1--16 digit hexadecimal prefix for aria2
  compatibility.  Empty, invalid, zero, ambiguous, and absent prefixes fail
  deterministically as `InvalidGid`, `GidAmbiguous`, or `GidNotFound`.
- Every RPC response, event, session record, and exported task reference uses
  the full 16-digit form.  A persisted GID survives restart and is never
  regenerated during recovery.
- `TaskId` is internal and never reused during one process.
- `UriId` identifies one source URI/mirror within a task's resolved source
  list; retry, lease, and server-stat records reference sources by `UriId`.
- `Generation` starts at `0` and increments on restart, option generation
  change, stale validator restart, or recovery resume that invalidates workers.
- `OverlapGroupId` exists only within one task generation and identifies the
  endgame attempts allowed to touch the same provisional verification range.
- Storage, protocol, retry, journal, and RPC command paths carry `Generation`.
- Late messages with old generations are ignored or reported as stale; they
  never mutate durable state.

## Error Model

All subsystems return typed errors:

```rust
pub enum ErrorKind {
    Config,
    UnsupportedOption,
    OptionPatchRejected,
    InvalidGid,
    GidAmbiguous,
    GidNotFound,
    GidCollision,
    InvalidPath,
    PathEscape,
    Network,
    Timeout,
    Retryable,
    InvalidRange,
    StaleValidator,
    ChecksumMismatch,
    Disk,
    NoSpace,
    Permission,
    JournalCorrupt,
    DirtyCheckpoint,
    NeedsCredentials,
    SlowConsumer,
    BackendUnavailable,
    Cancelled,
    InternalInvariant,
}
```

`OptionPatchRejected` is the only public option-change failure vocabulary.
Its stable reason is one of `invalid_value`, `unsupported`,
`not_runtime_mutable`, `requires_new_generation`,
`requires_explicit_bt_restart`, or `unsafe_compat_required`; it is used
consistently by CLI, RPC, and the native API.  A change that is permitted as
`active_restart` is not an error: it is accepted, recorded as pending, and
creates a new generation through the transition table below.
`UnsupportedOption` may exist as an internal parser/registry diagnostic, but a
CLI, RPC, or native option mutation always renders it as
`OptionPatchRejected/unsupported`.

Every error has:

- stable code,
- safe user message,
- optional internal diagnostic,
- retry class when relevant,
- redaction policy.

Panics are allowed only for programmer errors in tests/debug assertions. Runtime
input, backend capability failures, and remote protocol failures return errors.

## Option Snapshots

Task options are immutable per generation:

```rust
pub struct TaskOptions {
    pub generation: Generation,
    pub values: Arc<OptionValues>,
    pub digest: OptionDigest,
}

pub struct TaskOptionState {
    pub initial: Arc<OptionValues>,
    pub current: TaskOptions,
    pub pending: Option<Arc<OptionValues>>,
    pub live: LiveOptionOverrides,
}
```

Rules:

- Protocol workers receive `TaskOptions` at start.
- `live` contains only atomics or lock-free handles for values marked `live`.
- `active_restart` records `pending`, quiesces the old generation, then creates
  a new `Generation` with the pending values.  Its internal
  `PausedRestarting` interval maps to wire status `waiting`, and must not emit a
  user pause event or hook.
- `waiting_only` updates `pending` for active tasks and `current` for waiting
  tasks.
- Option digest is persisted in the session store for diagnostics and recovery.

## Task State Machine

Task state is controlled only by `RequestScheduler`.

`TaskState` is an internal state.  It is not an RPC enum.  The complete state
set is `Accepted`, `Waiting`, `Allocating`, `Active`, `RetryWait`, `Paused`,
`PausedSlow`, `PausedRestarting`, `Verifying`, `Seeding`, `Complete`, `Error`,
`Removed`, and `StoppedResult`.

State transition record:

```rust
pub struct StateTransition {
    pub task: TaskId,
    pub gid: Gid,
    pub generation: Generation,
    pub from: TaskState,
    pub to: TaskState,
    pub reason: StateReason,
    pub at: MonotonicInstant,
}
```

Rules:

- All transitions publish a snapshot update.
- Terminal states persist a stopped result before user-visible completion.
- `PausedSlow` is only produced by `download-scheduling.md` policy.
- `RetryWait` does not imply active slot ownership; slot behavior is controlled
  by `retry-wait-consumes-slot`.
- `Removed` cancels workers and follows configured partial-file policy.

### Transition Table

This table is normative.  An omitted command is an idempotent no-op when its
requested postcondition already holds and otherwise fails with a typed conflict
without side effects.  `abort` means cancel the generation and wait until every
provisional storage lease has either committed or been acknowledged by
`AbortLease`; committed durable pieces remain valid.

| From | Command or result | To | Required effect |
| --- | --- | --- | --- |
| `Accepted` | validation succeeds | `Waiting` | persist task/options; no lease exists |
| `Accepted` | validation fails | `Error` | persist safe error, then `StoppedResult` |
| `Accepted` | pause | `Paused` | persist desired pause; validation work is cancelled |
| `Accepted` | remove | `Removed` | discard unstarted task, then persist result |
| `Waiting` | scheduler admission | `Allocating` | reserve slot; construct layout/worker plan |
| `Waiting` | terminal planning/recovery error | `Error` | persist error/result; no lease exists |
| `Waiting`, `RetryWait`, `Paused`, `PausedSlow` | accepted non-live option patch | unchanged | update current or pending snapshot according to its runtime class |
| `Waiting` / `RetryWait` | pause | `Paused` | cancel timers; persist desired pause |
| `Waiting` / `RetryWait` | remove | `Removed` | cancel timers; persist result |
| `Allocating` | allocation succeeds | `Active` | start current generation only |
| `Allocating` | retryable allocation failure | `RetryWait` | release slot if policy requires; persist retry state |
| `Allocating` | terminal allocation failure | `Error` | release slot; persist error/result |
| `Allocating` | pause or remove | `Paused` or `Removed` | cancel allocation; no lease may escape |
| `Allocating` | active-restart option patch | `PausedRestarting` | cancel allocation, record pending options, then requeue after quiescence |
| `Active` | retryable lease/connection failure | `RetryWait` | abort affected provisional leases; persist retry state |
| `Active` | all required data received | `Verifying` | stop new work; retain committed pieces |
| `Active` | BT payload complete with seeding enabled | `Seeding` | hand off only through the BT adapter |
| `Active` | active-restart option patch | `PausedRestarting` | abort old-generation provisional leases; apply pending only after quiescence |
| `Active` | pause, slow-slot demotion, or remove | `Paused`, `PausedSlow`, or `Removed` | abort/drain leases, release slot, checkpoint according to reason |
| `Active` | terminal protocol, disk, or policy error | `Error` | abort provisional leases, release slot, persist error/result |
| `RetryWait` | timer/admission succeeds | `Allocating` | use current generation; stale timer events are ignored |
| `RetryWait` | retry budget exhausted or terminal retry error | `Error` | cancel timer, release slot, persist error/result |
| `Paused` / `PausedSlow` | resume | `Waiting` | clear user/slow pause; await normal admission |
| `Paused` / `PausedSlow` | remove | `Removed` | apply partial-file policy; persist result |
| `PausedRestarting` | quiescence and option application succeed | `Waiting` | increment generation, clear pause request, emit restart diagnostic only |
| `PausedRestarting` | user pause | `Paused` | cancel the automatic requeue but retain the accepted pending options |
| `PausedRestarting` | application/checkpoint failure | `Error` | persist error/result |
| `PausedRestarting` | remove | `Removed` | cancel pending restart and persist result |
| `Verifying` | verification succeeds | `Complete` | persist verified completion before visibility |
| `Verifying` | active-restart option patch | `PausedRestarting` | cancel verification safely and requeue after pending options apply |
| `Verifying` | recoverable piece/hash failure | `Waiting` | invalidate affected pieces, abort any provisional lease, requeue work |
| `Verifying` | terminal checksum/policy failure | `Error` | persist error/result |
| `Verifying` | pause or remove | `Paused` or `Removed` | cancel verification safely; committed pieces remain valid |
| `Seeding` | stop condition reached | `Complete` | obtain/persist BT checkpoint before terminal result |
| `Seeding` | pause or remove | `Paused` or `Removed` | use the BT shutdown barrier and persist resume data or dirty checkpoint |
| `Seeding` | BT error | `Error` | preserve last safe resume data; persist error/result |
| `Complete` / `Error` / `Removed` | terminal persistence succeeds | `StoppedResult` | retain the final aria2 stopped-result status |
| `StoppedResult` | remove stopped result | absent | delete only the retained result; do not synthesize a live task |
| any nonterminal state | orderly shutdown | recovery state | cancel/abort as applicable, run the BT barrier where applicable, persist a checkpoint, then recover as `Waiting` unless desired pause or journal terminal state says otherwise |

Orderly shutdown applies the equivalent of `abort` to every nonterminal active
generation, uses the BT shutdown barrier for BT tasks, and records recovery as
`Waiting` unless SQLite says the desired state was paused or a journal terminal
marker vetoes reactivation.  A journal `TaskPaused` checkpoint alone is not the
desired-pause authority.

### aria2 Wire Projection

Only this closed set may appear in aria2-compatible `status` fields:

| Internal state | `status` | Visibility rule |
| --- | --- | --- |
| `Accepted`, `Waiting`, `Allocating` | `waiting` | transitional states are observable only after command acknowledgement |
| `Active`, `Verifying`, `Seeding` | `active` | verification/seeding reason is extension-only |
| `RetryWait` with a retained slot | `active` | `retryWait` is extension-only |
| `RetryWait` without a retained slot | `waiting` | selected by `retry-wait-consumes-slot` policy |
| `Paused`, `PausedSlow` | `paused` | slow reason is extension-only |
| `PausedRestarting` | `waiting` | no pause event/hook; restart reason is extension-only |
| non-terminal `NeedsCredentials` condition | `paused` or `waiting` per SQLite desired state | credential requirement is extension-only; the task cannot issue a new lease until credentials arrive |
| `Error` | `error` | include aria2-compatible error code/message |
| `Complete` | `complete` | visible only after completion persistence |
| `Removed` | `removed` | visible until its stopped result is deleted |
| `StoppedResult` | stored terminal status | exposed through stopped-result queries, not as a new live status |

## Scheduler Commands

All CLI/RPC/library operations enter the scheduler through commands:

```rust
pub enum SchedulerCommand {
    AddUri(AddUri),
    Pause { gid: Gid, force: bool },
    Resume { gid: Gid },
    Remove { gid: Gid, force: bool },
    ChangeOption { gid: Gid, patch: OptionPatch },
    ChangeGlobalOption { patch: OptionPatch },
    ChangePosition { gid: Gid, position: QueuePosition },
    Query(QueryRequest),
    Shutdown(ShutdownMode),
}
```

Command rules:

- command parsing does not mutate task state,
- validation happens before side effects,
- every accepted command has an idempotency key or deterministic conflict
  behavior where useful,
- long work returns immediately after scheduling tracked tasks,
- response data comes from snapshots or explicit command acks.

## Snapshots

Hot status reads use immutable snapshots:

```rust
pub struct TaskSnapshot {
    pub gid: Gid,
    pub state: TaskState,
    pub wire_status: Aria2Status,
    pub generation: Generation,
    pub total_length: Option<u64>,
    pub completed_length: u64,
    pub durable_length: u64,
    pub current_speed: u64,
    pub avg_speed: u64,
    pub active_leases: u32,
    pub retry_wait_until: Option<MonotonicInstant>,
    pub last_progress_at: Option<MonotonicInstant>,
    pub error: Option<PublicError>,
}
```

Rules:

- snapshots are replaced atomically,
- RPC formatting never holds scheduler or storage locks,
- extension fields carry backend, stall, slot, retry, and buffer diagnostics,
- aria2-compatible fields are rendered from the same snapshot.
- `wire_status` is produced only by the projection table above; RPC formatters
  must never stringify `TaskState` directly.

## Cancellation

Each task generation has a cancellation token:

```rust
pub struct TaskRuntime {
    pub generation: Generation,
    pub cancel: CancellationToken,
    pub workers: JoinSet<WorkerResult>,
}
```

Rules:

- pausing/removing cancels the generation token,
- workers check cancellation at read, queue reservation, disk submit, and retry
  wait boundaries,
- disk completions from cancelled generations are acknowledged but not made
  durable,
- cancellation never drops a `BufferLease` without returning or quarantining
  it.

## Persistence Hooks

The scheduler emits persistence events:

```rust
TaskCreated
OptionsSnapshot
GenerationStarted
StateChanged
RetryStateChanged
StoppedResult
```

These are scheduler-level control events, not the on-disk journal record enum
(defined normatively in `detailed-storage.md`).  The scheduler never assigns
journal sequence numbers and never appends journal records directly.  The
storage-owned journal appender is the sole owner of `LeaseGranted`,
`LeaseCommitted`, `LeaseAborted`, and `PieceDurable`; a scheduler can only react
to its acknowledged durability outcome.  The persistence coordinator maps
`StateChanged` to control records such as `TaskPaused`/`TaskComplete`/`TaskError`,
`RetryStateChanged` to `RetryState`, and `StoppedResult` to the SQLite
stopped-result row.  The two lists intentionally need not match name-for-name.

On recovery, the control journal is authoritative for durable layout and
terminal/durable state, while SQLite is authoritative for queue membership,
position, and desired pause state.  If secrets were omitted from persisted
configuration, recovery retains durable pieces and enters internal
`NeedsCredentials` rather than discarding work or exposing a secret.

Persistence is asynchronous but ordered per task. A task cannot publish
`Complete` until the required completion persistence has succeeded.

## Tests

Required first-slice tests:

- generation rejects stale worker messages,
- every command either mutates through scheduler or fails without side effects,
- `ChangeOption` behavior matches option `runtime_update`,
- GID codec round-trips full values and rejects zero, collisions, and ambiguous
  prefixes while normalizing response output,
- every state/command/error row above is model-tested, including cancel/remove
  during allocation, retry wait, verification, and seeding,
- active-restart changes `split`, `max-connection-per-server`, and
  `min-split-size` through `waiting` without a pause event,
- pause/remove race with disk completion preserves durable state,
- snapshots update without holding scheduler locks,
- stopped result persists before visible terminal state,
- cancellation returns or quarantines buffers.
