# Detailed Core Design

Status: reviewed first-slice implementation contract. The exhaustive core
transition-contract checkpoint is implemented; scheduler execution remains
pending.

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
pub struct TransferAttemptId(NonZeroU64);
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
- `Generation` starts at `0`. The only live rollover point is admission into
  `Allocating`: admission increments the generation whenever any earlier
  generation of this task started a worker. Restart, stale-validator recovery,
  task-level retry, demotion, manual resume, host-key approval, and accepted
  restart-class option changes all stage work for that next admission; they do
  not increment independently. The increment happens only after the previous
  generation's cancellation drain has completed and is persisted through
  `GenerationStarted` before a new worker can start. Thus one logical restart
  advances exactly once, and an old-generation completion can never become
  current because the task was automatically or manually resumed. Span-level
  lease retry inside one `Active` generation keeps that generation and takes a
  fresh `LeaseId`.
- `TransferAttemptId` identifies one protocol response/data stream. A range
  attempt normally owns one storage lease; a sequential response/FTP data stream
  can advance through many storage leases without opening another connection.
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
    HostKeyApprovalRequired,
    StaleChallenge,
    Disk,
    NoSpace,
    Permission,
    JournalCorrupt,
    DirtyCheckpoint,
    NeedsCredentials,
    SlowConsumer,
    ResponseTooLarge,
    ResourceLimit,
    BackendUnavailable,
    Cancelled,
    InternalInvariant,
}
```

The declaration order is also the stable 1-based version-1 journal number:
`Config=1` through `InternalInvariant=29`. Zero and values above 29 are
rejected. `generated/error_codes.json` is produced from the executable enum and
is the machine-readable source for persistence and API adapters.

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
- `active_restart` records `pending` and quiesces the old generation. After
  quiescence it stages the pending values for normal admission; that admission
  creates the next `Generation` exactly once under the rule above. Its internal
  `PausedRestarting` interval maps to wire status `waiting`, and must not emit a
  user pause event or hook.
- `waiting_only` updates `pending` for active tasks and `current` for waiting
  tasks.
- Option digest is persisted in the session store for diagnostics and recovery.

## Task State Machine

Task state is controlled only by `RequestScheduler`.

The current checkpoint implements the closed command/event/action vocabularies,
the exhaustive state × semantic-action contract, wire projection, and generated
`state_wire.json` artifact. It does not yet implement `RequestScheduler`
command/event execution, construct and order the required `TransitionEffect`
values, validate pending barriers or generation/timer/readmission/probe tokens,
or enforce `MAX_SCHEDULER_EFFECTS`. Those behaviors and their success/rejection
tests remain required before the minimal scheduler is complete.

`TaskState` is an internal state.  It is not an RPC enum.  The complete state
set is `Accepted`, `Waiting`, `WaitingSlow`, `Allocating`, `Active`,
`RetryWait`, `Paused`, `PausedSlow`, `PausedHostKey`, `PausedRestarting`,
`Verifying`, `Seeding`, `Complete`, `Error`, `Removed`, and `StoppedResult`.

Recoverable admission blockers are orthogonal conditions, not additional task
states and not overloaded user-pause state:

```rust
pub struct TaskConditions {
    pub needs_credentials: Option<CredentialRequirement>,
    pub no_space: Option<NoSpaceCondition>,
}

pub struct NoSpaceCondition {
    pub path: RedactedPath,
    pub retry_at: Option<MonotonicInstant>,
}
```

`TaskConditions` is scheduler-owned, bounded, and included in extension
snapshots. A condition prevents `Waiting -> Allocating` until its explicit
clear rule succeeds. It does not overwrite SQLite's desired-pause authority:
user pause can coexist with either condition, clearing a condition never
implicitly clears user pause, and recovery reconstructs the task as `Paused`
or `Waiting` from that desired state before applying the admission gate.

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

- Every externally publishable transition replaces the immutable snapshot.
  `Complete`, `Error`, and `Removed` are internal terminal-pending states: they
  emit terminal persistence first and do not publish a snapshot until the
  matching acknowledgement moves the task to `StoppedResult`.
- Terminal states persist a stopped result before user-visible completion.
- `PausedSlow` is produced only by the `download-scheduling.md` slow-slot
  `pause` policy. It is user-visible pause semantics: no automatic readmission.
- `WaitingSlow` is produced only by the slow-slot `demote` policy. It is
  scheduler-internal queue demotion: the task is automatically readmitted by
  policy and is never presented as paused.
- `PausedHostKey` is produced only before SFTP authentication/data transfer when
  an otherwise acceptable server key requires explicit user approval. No
  credential is sent before this state is resolved.
- `RetryWait` does not imply active slot ownership; slot behavior is controlled
  by `retry-wait-consumes-slot`.
- `needs_credentials` clears only after an accepted credential-bearing option
  patch or replacement source satisfies the recorded requirement. A generic
  `Resume` does not manufacture or approve credentials.
- `no_space` is set only for a mid-transfer ENOSPC/quota result. Explicit
  `Resume` first records and persists unpaused intent, then issues an identified
  allocation/write readiness probe without admitting work. A later `Pause`
  records paused intent and wins over that outstanding probe. Probe completion
  consults the current desired-pause authority, clears the condition only on
  success, and otherwise retains `NoSpace` without losing durable progress.
- `Removed` cancels workers and follows configured partial-file policy.

### Span Run States And Lease-Level Retry

A retryable failure of one lease among several does not change the task state.
The unit of transfer work inside an `Active` task is the planned span:

```rust
pub enum PlannedSpanState {
    Pending,
    Leased(LeaseId),
    RetryWait { until: MonotonicInstant, attempt: u32, error: ErrorClass },
    Done,
}
```

Rules:

- A span whose attempt fails retryably completes `AbortLease`, releases its
  buffers, and enters span-level `RetryWait` with its own timer. Other spans
  keep transferring; the task remains `Active`.
- A span whose timer fires returns to `Pending` and is leased through normal
  worker admission in the current generation; no task-state transition occurs.
- The task transitions to task-level `RetryWait` only when no span is `Leased`
  or `Pending` and at least one span is in span-level `RetryWait` — including
  the sequential single-span case, where lease retry and task retry coincide.
  Its deadline is the minimum span deadline.
- A span whose retry budget is exhausted follows failure policy: fail the task,
  or mark the span terminally failed and fail the task when no eligible source
  remains.
- Span retry state is visible as extension diagnostics (`retry_wait_leases`,
  per-span deadlines); it is never serialized as a task status.

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
| `Waiting` with any admission condition | scheduler admission | `Waiting` | admit no worker and reserve no slot; publish the blocking condition |
| `Waiting` | terminal planning/recovery error | `Error` | persist error/result; no lease exists |
| `Waiting`, `WaitingSlow`, `RetryWait`, `Paused`, `PausedSlow`, `PausedHostKey` | accepted non-live option patch | unchanged | update current or pending snapshot according to its runtime class |
| `Waiting` / `Paused` with `needs_credentials` | accepted satisfying credential/source update | unchanged | clear only `needs_credentials`; preserve desired pause and any other condition |
| `Waiting` with `no_space` | explicit resume or auto-retry probe succeeds | `Waiting` | clear only `no_space`; await normal admission under a fresh generation |
| `Waiting` with `no_space` | explicit resume or auto-retry probe fails | `Waiting` | retain the condition/durable state and report/update the next retry deadline; wire status remains `paused` |
| `Paused` with `no_space` | explicit resume requests a probe | `Paused` | persist `desired_paused=false`, issue a fresh identified probe, and admit no work while the result is pending |
| `Paused` with `no_space` and current `desired_paused=false` | probe succeeds | `Waiting` | clear only `no_space`; await normal admission |
| `Paused` with `no_space` and current `desired_paused=false` | probe fails | `Waiting` | retain `no_space`; wire status remains `paused` even though user-pause intent is clear |
| `Paused` with `no_space` and current `desired_paused=true` | explicit-resume or auto-retry probe completes | `Paused` | update or clear only `no_space`; preserve the later/repeated user pause and never admit |
| `Paused`, `PausedSlow`, or `PausedHostKey` | pause | unchanged | persist desired user pause; invalidate or supersede any earlier resume/probe intent |
| `Waiting` / `WaitingSlow` / `RetryWait` | pause | `Paused` | cancel timers; persist desired pause |
| `Waiting` / `WaitingSlow` / `RetryWait` | remove | `Removed` | cancel timers; persist result |
| `Allocating` | allocation succeeds | `Active` | start the admitted generation only; the admission-time generation increment rule above applies |
| `Allocating` | retryable allocation failure | `RetryWait` | release slot if policy requires; persist retry state |
| `Allocating` | unknown otherwise-acceptable SFTP host key | `PausedHostKey` | release slot; publish/persist challenge; send no credentials |
| `Allocating` | terminal allocation failure | `Error` | release slot; persist error/result |
| `Allocating` | pause or remove | `Paused` or `Removed` | cancel allocation; no lease may escape |
| `Allocating` | active-restart option patch | `PausedRestarting` | cancel allocation, record pending options, then requeue after quiescence |
| `Active` | retryable failure of one lease while other work is runnable or in flight | `Active` | abort only that lease; enter span-level retry wait; no task transition |
| `Active` | retryable failure with no span leased or pending | `RetryWait` | abort affected provisional leases; persist retry state with the minimum span deadline |
| `Active` | all required data received | `Verifying` | stop new work; retain committed pieces |
| `Active` | BT payload complete with seeding enabled | `Seeding` | hand off only through the BT adapter |
| `Active` | active-restart option patch | `PausedRestarting` | abort old-generation provisional leases; apply pending only after quiescence |
| `Active` | slow-slot `demote` policy fires | `WaitingSlow` | abort/drain leases via `abort`, release slot, record readmission deadline |
| `Active` | pause, slow-slot `pause` policy, or remove | `Paused`, `PausedSlow`, or `Removed` | abort/drain leases, release slot, checkpoint according to reason |
| `Active` | mid-transfer ENOSPC or quota result | `Waiting` + `no_space` | abort/drain provisional leases, release slot, preserve durable pieces, persist the condition; wire status is `paused` |
| `Active` | terminal protocol, disk, or policy error | `Error` | abort provisional leases, release slot, persist error/result |
| `RetryWait` | timer/admission succeeds | `Allocating` | the readmission generation rule applies; stale timer events are ignored |
| `RetryWait` | retry budget exhausted or terminal retry error | `Error` | cancel timer, release slot, persist error/result |
| `WaitingSlow` | readmission policy fires | `Allocating` | the readmission generation rule applies; honor `slow-slot-readmit-*` |
| `WaitingSlow` | resume | `Waiting` | user resume overrides the demotion cooldown; normal admission follows |
| `WaitingSlow` | pause | `Paused` | record user pause; cancel readmission timer |
| `WaitingSlow` | remove | `Removed` | apply partial-file policy; persist result |
| `Paused` / `PausedSlow` | resume | `Waiting` | clear user/slow pause; preserve any admission condition and apply its condition-specific resume rule before admission |
| `Paused` / `PausedSlow` | remove | `Removed` | apply partial-file policy; persist result |
| `PausedHostKey` | generic resume/unpause | `PausedHostKey` | reject with `HostKeyApprovalRequired`; generic queue automation must not grant trust |
| `PausedHostKey` | explicit matching host-key approval | `Waiting` | require the current challenge id and displayed SHA-256 fingerprint, pin the exact challenged key for this task, and requeue; stale/mismatched approval fails without sending credentials |
| `PausedHostKey` | explicit matching host-key option | `Waiting` | replace the challenge with the configured pin and requeue under the same rule |
| `PausedHostKey` | remove/CLI stop | `Removed` | reject the challenge before authentication and persist the stop reason |
| `PausedRestarting` | quiescence and option staging succeed | `Waiting` | stage pending values for the next admission, clear restart quiescence, emit restart diagnostic only; admission performs the sole increment |
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
| `WaitingSlow` | `waiting` | demotion reason and readmission deadline are extension-only; never `paused` |
| `Active`, `Verifying`, `Seeding` | `active` | verification/seeding reason is extension-only |
| `Active` with spans in span-level retry wait | `active` | per-lease retry diagnostics are extension-only |
| `RetryWait` with a retained slot | `active` | `retryWait` is extension-only |
| `RetryWait` without a retained slot | `waiting` | selected by `retry-wait-consumes-slot` policy |
| `Paused`, `PausedSlow`, `PausedHostKey` | `paused` | slow/host-key reason and challenge are extension-only |
| `PausedRestarting` | `waiting` | no pause event/hook; restart reason is extension-only |
| `needs_credentials` condition | `paused` or `waiting` per SQLite desired state | credential requirement is extension-only; the task cannot issue a new lease until the requirement is satisfied |
| `no_space` condition | `paused` | disk-space reason/retry deadline is extension-only; this does not set or clear SQLite's desired user pause |
| `Error` | `error` | internal projection only; no snapshot is published before retained-result persistence |
| `Complete` | `complete` | internal projection only; no snapshot is published before retained-result persistence |
| `Removed` | `removed` | internal projection only; no snapshot is published before retained-result persistence |
| `StoppedResult` | stored terminal status | exposed through stopped-result queries, not as a new live status |

## Scheduler Commands

All CLI/RPC/library operations enter the scheduler through commands:

```rust
pub enum SchedulerCommand {
    AddUri(AddUri),
    Pause { gid: Gid, force: bool },
    Resume { gid: Gid },
    ApproveHostKey {
        gid: Gid,
        challenge: HostKeyChallengeId,
        fingerprint_sha256: HostKeyFingerprint,
    },
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
    pub generation: Generation,
    pub total_length: Option<u64>,
    pub completed_length: u64,
    pub durable_length: u64,
    pub current_speed: u64,
    pub average_speed: u64,
    pub active_leases: u32,
    pub retry_wait_leases: u32,
    pub retry_wait_until: Option<MonotonicInstant>,
    pub last_progress_at: Option<MonotonicInstant>,
    pub conditions: TaskConditionsSnapshot,
    pub desired_paused: bool,
    pub retry_wait_holds_slot: bool,
    pub stopped_status: Option<Aria2Status>,
    pub host_key_challenge: Option<HostKeyChallenge>,
    pub error: Option<PublicError>,
    pub terminal_persisted: bool,
}
```

`HostKeyChallenge` exposes a challenge id, canonical host/port, key algorithm,
and SHA-256 fingerprint. The scheduler retains the exact presented public key so
`ApproveHostKey` can pin it; neither the challenge nor the pin is secret.

Rules:

- snapshots are replaced atomically,
- RPC formatting never holds scheduler or storage locks,
- extension fields carry backend, stall, slot, retry, and buffer diagnostics,
- aria2-compatible fields are rendered from the same snapshot,
- `TaskSnapshot::wire_status()` derives status from `state`, conditions,
  desired-pause authority, retry-slot ownership, and retained terminal status;
  RPC formatters must never stringify `TaskState` directly,
- `stopped_status` is present only for `StoppedResult`, and
  `terminal_persisted` is true exactly for that retained result,
- terminal-pending snapshots are rejected even after the persistence
  acknowledgement; the acknowledgement and transition to `StoppedResult` form
  one publication boundary.

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
configuration, recovery retains durable pieces, sets the scheduler-owned
`needs_credentials` condition, and reconstructs `Paused` or `Waiting` from the
SQLite desired state rather than discarding work or exposing a secret.

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
- one retrying lease among active leases keeps the task `Active` and aria2
  `active`; the last active lease failing retryably moves the task to
  `RetryWait` with the minimum span deadline,
- a span readmitted from span-level retry uses the current generation and a
  fresh `LeaseId`; task readmission from `RetryWait`/`WaitingSlow` increments
  the generation only after old-generation cancellation completes,
- active restart, stale-validator restart, pause/resume, and recovery each
  produce exactly one `GenerationStarted` at the subsequent admission, never
  one increment while staging plus another while admitting,
- `needs_credentials` blocks admission until a satisfying source/credential
  update and composes with desired user pause,
- generic resume cannot approve `PausedHostKey`; explicit approval requires the
  current challenge id and fingerprint and rejects a raced/new challenge,
- mid-transfer ENOSPC sets `no_space`, preserves durable pieces, projects
  `paused`, and clears only after a successful explicit/timed readiness probe,
- a pause or re-pause racing an explicit no-space probe remains authoritative;
  the later probe completion may update `no_space` but cannot requeue the task,
- `WaitingSlow` demotion/readmission projects `waiting`, honors
  `slow-slot-readmit-*`, and accepts user pause/resume/remove during the
  cooldown; `PausedSlow` projects `paused` and never readmits automatically,
- active-restart changes `split`, `max-connection-per-server`, and
  `min-split-size` through `waiting` without a pause event,
- pause/remove race with disk completion preserves durable state,
- snapshots update without holding scheduler locks,
- stopped result persists before visible terminal state,
- cancellation returns or quarantines buffers.
