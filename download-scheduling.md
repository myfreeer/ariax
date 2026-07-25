# Download Scheduling

Status: reviewed pre-implementation contract. Implementation pending.

The scheduler owns active, waiting, paused, and stopped task queues. It should
preserve aria2-style `max-concurrent-downloads` semantics by default, while
optionally supporting modern package-manager style slot freeing: a slow task
can be paused or demoted so the next queued task can start.

This feature is optional because some users prefer strict queue order and
stable active sets over maximizing useful slots.

## Goals

- Keep default queue behavior close to aria2.
- Let users opt into freeing active slots from slow or stalled tasks.
- Coordinate with retry policy, especially `retry-on=lowest-speed`, `hang`,
  `timeout`, and stale connection retry.
- Do not confuse remote slowness with local disk, CPU, memory, or user
  rate-limit backpressure.
- Preserve fairness and avoid endless churn between slow tasks.

## Core Queues

```text
waiting queue      tasks not currently admitted
active set         tasks consuming max-concurrent-downloads slots
demoted queue      `WaitingSlow` tasks awaiting automatic readmission
paused-slow queue  `PausedSlow` tasks parked by the explicit `pause` policy
retry-wait queue   tasks or leases waiting for retry timers
stopped results    terminal success/error/removed records
```

`max-concurrent-downloads` counts active tasks. By default, a task in
`RetryWait` still belongs to its active task unless policy explicitly frees the
slot. This preserves aria2-like behavior. A retryable failure of one lease
while other leases are transferring is span-level retry inside `Active`
(`detailed-core.md`) and involves no queue movement at all.

## Slow Slot Freeing

Option:

```text
--slow-slot-policy=off|demote|pause
```

`off`:

- default aria2-compatible behavior,
- slow active tasks keep their active slot unless they terminally fail or the
  user pauses them.

`demote`:

- active task leaves the active set and enters internal `WaitingSlow`,
- durable state is saved,
- in-flight leases are cancelled or drained by generation rules,
- next waiting task may start,
- the demoted task is automatically readmitted by the readmission policy below,
- it projects to aria2 `waiting`, never `paused`; no pause event or hook fires.

`pause`:

- active task enters `PausedSlow`, exposed as a user-visible paused state with a
  slow-slot reason,
- it projects to aria2 `paused`,
- there is no automatic readmission: the user (or automation) must resume it,
- useful for users who want explicit queue control.

Suggested default:

```text
--slow-slot-policy=off
```

When enabled without explicit overrides, resolved defaults are:

```text
slow-slot-grace-period=60
slow-slot-min-active-time=30
slow-slot-max-demotions=3
slow-slot-readmit-after=60
slow-slot-readmit-policy=original-position
```

If `slow-slot-speed-limit` is not explicit, a nonzero
`lowest-speed-limit` is reused; otherwise it resolves to 64 KiB/s when the
policy is enabled. After `slow-slot-max-demotions` is reached, the task is no
longer automatically demoted in that generation: it keeps its slot until normal
retry/terminal/user action. An explicit user restart resets the count; an
automatic readmission does not.

The feature should be easy to enable, but not surprising.

## Slow Classification

A task can be considered remotely slow only when all are true:

- task has active network leases or recently had them,
- current task speed is below `slow-slot-speed-limit` for
  `slow-slot-grace-period`,
- low speed is not caused by local disk, CPU, buffer, journal, or configured
  rate limit backpressure,
- task is not in user-requested pause, verification, allocation, seeding-only,
  or finalization state,
- at least one waiting task can use the freed slot.

Options:

```text
--slow-slot-speed-limit=SPEED
--slow-slot-grace-period=SEC
--slow-slot-min-active-time=SEC
--slow-slot-max-demotions=N
--slow-slot-readmit-after=SEC
--slow-slot-readmit-policy=front|original-position|back
```

The resolved threshold above exists only when the user explicitly enables
`slow-slot-policy`; with the default `off`, it causes no classification work.

## Interaction With Retry Policy

Slow slot freeing and retry are related but distinct:

- retry acts on a failed lease/span, connection, mirror, or task generation,
- slow slot freeing acts on task admission and queue slots.

When `retry-on=lowest-speed` or `retry-on=hang` is enabled:

1. The lease/connection can be cancelled and retried according to
   `retry-policy.md`.
2. If the task remains below the slow-slot threshold after retry action, and
   `slow-slot-policy` is not `off`, the task may be demoted or paused.
3. Retry timers for the task do not consume transfer buffers and may optionally
   not consume an active slot.

Option:

```text
--retry-wait-consumes-slot=true|false|auto
```

`true`:

- aria2-like behavior; retry-wait tasks keep their active slot.

`false`:

- retry-wait tasks leave the active set and allow waiting tasks to start.

`auto`:

- keep slot for short waits,
- release slot for waits longer than `slow-slot-readmit-after` or when all
  active slots are retry-wait/stalled.

Defaults:

```text
--retry-wait-consumes-slot=true
```

The scheduler must avoid double punishment. A single slow connection should not
both consume many retry attempts and repeatedly demote the whole task without
cooldown.

## Backpressure Guardrails

Do not demote a task for slowness when the primary cause is local pressure:

- disk queue saturated,
- buffer pool exhausted,
- CPU hash queue saturated,
- global or per-task rate limit active,
- journal/fsync backlog,
- event-loop lag causing delayed reads.

In those cases the task remains in its normal scheduler `TaskState`; its
connection/stall diagnostic is `Backpressured` or `RateLimited`, not
`RemoteSlow`, and slow-slot policy does not fire. These diagnostics are
extension fields, not additional aria2 wire statuses.

## Readmission

Demoted (`WaitingSlow`) tasks are not lost. The scheduler records:

- original queue position,
- demotion reason,
- demotion count,
- last progress time,
- retry state,
- next eligible readmission time.

Readmission applies only to `WaitingSlow`; `PausedSlow` waits for an explicit
resume. It is allowed when:

- the cooldown expired,
- the task still has pending work,
- retry policy permits another attempt,
- queue policy allows its position,
- global budgets are available.

Readmission must apply the `detailed-core.md` readmission generation rule: the
old generation's cancellation drain completes first, then admission increments
the generation, so late completions from old workers cannot write into the new
state.

## Fairness

Rules:

- cap demotions per task with `slow-slot-max-demotions`,
- do not repeatedly start the same failing mirror ahead of other waiting tasks,
- preserve user priority and `changePosition` semantics,
- do not demote tasks in strict user-selected order modes unless the user
  explicitly enables slow-slot policy,
- expose demotion events through RPC/WebSocket diagnostics.

## Status

Add diagnostic fields in extended RPC/status:

```text
slotState: active|waiting|retryWait|waitingSlow|pausedSlow|pausedUser|stopped
slotReason: none|remoteSlow|retryWait|stalled|user|backpressure
slowSince
demotionCount
readmitAfter
retryWaitConsumesSlot
```

aria2-compatible fields keep their normal shape. Extended fields are hidden
unless requested by compatibility mode.

## Tests

Required tests:

- default `off` policy preserves active queue behavior,
- slow remote task frees a slot only when policy is enabled,
- `demote` produces `WaitingSlow`, projects aria2 `waiting`, emits no pause
  event, and readmits automatically after the cooldown,
- `pause` produces `PausedSlow`, projects aria2 `paused`, and never readmits
  automatically,
- user pause/resume/remove during the `WaitingSlow` cooldown wins over the
  automatic readmission timer,
- disk backpressure does not trigger slow-slot demotion,
- rate-limited task does not trigger slow-slot demotion,
- `retry-on=lowest-speed` cancels/retries the connection and can demote the
  task only after policy conditions are met,
- retry wait slot consumption follows `true`, `false`, and `auto`,
- demoted task readmits without stale worker writes,
- `changePosition`, pause, resume, remove, and stopped results remain
  consistent.
