# Session Persistence

[Documentation](../README.md)

Status: first-slice implementation in progress. The control-journal v1 framing,
segment linkage, bounded replay, torn-tail valid-prefix rules, and all 32
record payload codecs are executable. Phase 5 adds bounded manifests,
protocol validators, whole-file verification, trust decisions and metadata
completion without changing existing record numbers or framing.
Policy-gated typed state
reconstruction, exact generation/layout/lease/finalization validation, and
whole-checkpoint hash validation are also executable. File-backed typed append,
flush acknowledgement, tail/content reopen validation after portable named-file
preflight, and durable linked rotation are executable. That portable boundary
does not establish native namespace authority. The native capability adapters
now provide descriptor-bound root identity revalidation; checkpoint compaction
writing remains pending. The SQLite v3
synchronous primitive creates and validates the exact strict schema, preserves
dense queues across atomic queue/pause/slow-metadata transitions, and enforces
bounded semantic reads and
tokenized journal installs. Phase 6 requires a fresh v3 development store;
older formats are rejected unchanged, without migrations or deletion. Atomic stopped-result retention/deletion, bounded stopped-result and
task-source reads, exact supplied-order queue transitions, no-space updates,
and challenge-bound host-key persistence are executable. A dedicated bounded
session-owner thread exclusively owns the synchronous store and every installed
control-journal appender, and provides bounded admission, reserved per-command
completion delivery, typed store/journal failures, retry-safe owned-command
rejection, and out-of-band shutdown. A bounded engine composition sink validates
exact scheduler-effect plans and advances their SQLite/journal commands one at
a time through that owner. Owner startup and shutdown waits have explicit,
validated hard-capped timeouts; a timeout detaches rather than fake-dropping
thread-owned persistence state and is reported as recovery uncertainty.
The startup session-repair executor now advances the reconciler's exact queue
normalizations, terminal retentions, and journal-authority cache/root repairs
one command at a time through the same bounded owner. Queue-full admission
retains the exact owned command for retry, and an unexpected completion or any
accepted persistence failure makes startup fail closed before publication. The
native filesystem/install/appender executor and publication-last restored
driver are wired through process bootstrap. The checkpoint state writer remains
pending. Phase 4B also implements sanitized atomic JSON/aria2 session import and
export, configured destinations, periodic saves and shutdown save/drain.

Decision: use a hybrid persistence model:

- append-only per-task control journals for crash-critical download progress,
- a small SQLite session database for global queue/state/index metadata,
- optional text export/import for compatibility and debugging.

Do not use only JSON/TOML for active recovery. Do not use RocksDB/LMDB as a
mandatory dependency.

This intentionally differs from the earlier `aria2_rust` prototype's JSON
`.aria2` session files. JSON is acceptable for export/import, but active crash
recovery needs a torn-write-detectable journal.

## Requirements

Session persistence must support:

- poweroff recovery,
- many active downloads,
- queue order,
- active/waiting/stopped state,
- per-task options,
- URI/mirror lists,
- file layout and selected files,
- partial piece/range progress,
- Metalink/torrent metadata references,
- retry state,
- RPC-visible stopped results,
- atomic updates,
- bounded write amplification,
- cross-platform packaging.

## Options Considered

## Text Only: JSON/TOML/YAML

Pros:

- human-readable,
- easy import/export,
- simple debugging,
- no database dependency.

Cons:

- expensive to rewrite for frequent progress updates,
- fragile for power loss unless carefully journaled,
- poor for many tasks and stopped results,
- awkward binary bitsets/resume data,
- easy to corrupt with partial writes,
- hard to update atomically at high frequency.

Decision: use only for import/export/debug snapshots, not primary active
recovery.

## aria2-Style Per-File `.aria2` State

Pros:

- proven concept,
- recovery is local to each output,
- easy to move partial downloads with their control files,
- avoids a single central database as the only source of truth,
- good crash isolation.

Cons:

- global queue/session metadata needs another file,
- scanning many directories can be slow,
- per-file state format must evolve carefully,
- harder to query stopped results or global stats.

Decision: keep this idea, but modernize it as a versioned append-only control
journal per task/download. Use a new suffix by default, such as `.ariax`, to
avoid implying byte-level compatibility with aria2's `.aria2` binary control
format.

## SQLite

Pros:

- mature, ubiquitous, small dependency,
- transactional,
- cross-platform,
- good for queue/index/stopped results/options,
- easy to inspect with tools,
- supports WAL,
- simpler packaging than RocksDB.

Cons:

- not ideal for very high-frequency per-piece writes if abused,
- needs schema migration discipline,
- central DB corruption must be mitigated with backups/checks.

Decision: use SQLite for global session metadata and indexes, not for every
hot data-piece transition.

## LMDB/RocksDB/Other KV Store

Pros:

- high write throughput,
- good key/value model,
- can store bitsets and binary state directly.

Cons:

- larger or more complex dependency,
- harder Windows/macOS packaging,
- more tuning required,
- less user-inspectable,
- overkill for downloader metadata,
- RocksDB in particular is large for compact binaries.

Decision: not mandatory. Consider only as optional enterprise/large-scale
backend if real benchmarks show SQLite plus journals is insufficient.

## Recommended Layout

```text
session.db                  SQLite global metadata
tasks/
  <gid>.ctrl                binary control-journal segment 0
  <gid>.ctrl.00000001       optional rotated segment 1
  <gid>.meta                optional metadata blob or pointer
exports/
  session-export.json       optional user-created export
```

For aria2-like output locality, a task may also place a companion control file
next to the output:

```text
file.iso
file.iso.ariax              companion segment 0
file.iso.ariax.00000001     optional rotated segment 1
```

The companion basename is deterministic and reserved by the safe-path layer:

- single-file layout: `<final-output-name><control-file-suffix>`, even while
  payload bytes are still in a temporary output,
- multi-file layout: `<canonical-root>/.ariax-<16-hex-gid>.ctrl` (rotated
  suffixes follow the same numbering rule).

The builder checks the reserved companion name against metadata/output
collisions before creating payload files. Control artifacts are opened relative
to the same retained root capability with no-follow/reparse rejection; a
symlinked companion is never followed. A user-selected suffix that collides
with the final payload name fails configuration/layout creation rather than
silently moving the journal elsewhere.

The central SQLite DB maps `gid` to the base path of the control-journal segment
set. Segment headers and continuity are normative in [detailed-storage.md](detailed-storage.md).
SQLite paths use the same tagged `PlatformPath` byte encoding as the journal,
not lossy UTF-8 strings.

A task always has exactly one primary segment set and one appender. With
`central` or `beside-output`, that location is primary. With `both`, the
beside-output set is primary and the central copy is a checkpoint replica made
only after the primary has flushed; it is never appended independently. SQLite
stores both paths and the replica's last copied sequence. If the primary is
missing, recovery may promote the replica after validating its complete linked
prefix. Divergent records at the same journal id/sequence are corruption and
must not be merged or selected by timestamp.

## SQLite Responsibilities

SQLite stores:

- session id,
- task gid,
- queue position,
- task state: waiting, demoted, active, paused, stopped,
- scheduler admission conditions that must survive restart (`no_space` plus its
  redacted target/retry parameters; no credential requirement or secret is
  stored, and startup composition must derive one explicit non-secret
  credential-admission record per restored task),
- root output directory,
- safe relative paths or layout hash,
- persistence-safe URI/mirror metadata or a redacted source placeholder,
- mutable per-task options and a mirror of the sanitized generation snapshot,
- stopped results,
- aggregate counters,
- control journal path,
- metadata blob references,
- timestamps,
- compatibility/version info.

SQLite does not store high-frequency per-piece durability transitions in the
normal path.

### SQLite Schema Version 3

The current contract is generated as `generated/session_v3.json`. An empty
version-0 file is initialized in `BEGIN IMMEDIATE`. Nonempty unversioned stores
and every nonzero version other than 3 are rejected unchanged with a typed
unsupported-format error. There are no historical schema readers, migrations,
downgrade paths, or automatic deletion of development stores.

For an existing database, startup performs a streaming, fixed-buffer raw
preflight before opening SQLite. A hot rollback journal contributes its last
valid page-one before-image, then valid committed WAL frames are applied in commit order;
uncommitted frames and torn or invalid tails cannot authorize a version. This
also permits SQLite to recover a supported hot rollback transaction when the
main page-one header is damaged. A legacy rollback-journal header whose encoded
page size is zero fails closed. An unsupported committed version is rejected before
permission changes, owner-lock creation, SQLite open, or journal-mode changes,
leaving the database and sidecars untouched.

After this first preflight, Ariax exclusively locks the private regular file
`${db}.ariax-owner-lock` and repeats version inspection under that lock. The
lock is held for the `SessionStore` lifetime and provides cooperative
single-writer ownership among Ariax processes only. A program that opens the
SQLite database directly does not honor this lock; concurrent raw SQLite
writers are unsupported. A supported database is opened read/write so SQLite
can complete rollback recovery, then the exact versioned schema, integrity,
foreign keys, queue density, decoded task/stopped/host-key/install records, and
install-pointer relation are validated before connection journal policy is
changed.

`PRAGMA user_version=3` is the authoritative current schema version. Version-3 tables
are `STRICT`, enable foreign keys, and use closed integer enums generated from
the same state/error matrices as the API. A `u64` that may exceed SQLite's
signed integer range is stored as an exactly 8-byte little-endian BLOB; hashes,
ids, and platform paths have exact length/codec checks before binding.

| Table | Version-3 Columns and key |
| --- | --- |
| `session` | `session_id BLOB(16) PRIMARY KEY`, `created_ms INTEGER`, `updated_ms INTEGER`, `clean_shutdown INTEGER` |
| `task` | `gid TEXT PRIMARY KEY`, `session_id BLOB(16)`, `task_kind INTEGER` (transfer or BT), `queue_state INTEGER`, `queue_position INTEGER`, `desired_paused INTEGER`, nullable `slow_original_position INTEGER`, `slow_demotion_count INTEGER`, nullable `slow_retry_scheduled_at_ms INTEGER`, nullable `slow_retry_delay_ms BLOB(8)`, journal/path/hash/no-space fields, `created_ms INTEGER`, `updated_ms INTEGER`; foreign key to `session` |
| `task_option` | `gid TEXT`, `scope INTEGER`, `key TEXT`, `canonical_value BLOB`, primary key `(gid, scope, key)`; secret-class registry keys are rejected before SQL |
| `task_source` | `gid TEXT`, `uri_id INTEGER`, nullable `persistence_safe_uri TEXT`, `redacted_fingerprint BLOB(32)`, `needs_credentials INTEGER`, `priority INTEGER`, primary key `(gid, uri_id)` |
| `host_key_challenge` | `gid TEXT PRIMARY KEY`, `challenge_id BLOB(16)`, `canonical_host TEXT`, `port INTEGER`, `algorithm TEXT`, `presented_public_key BLOB`, `fingerprint_sha256 BLOB(32)`, `created_ms INTEGER`; public key/challenge caps come from [detailed-ftp-sftp.md](../protocols/detailed-ftp-sftp.md) |
| `stopped_result` | `gid TEXT PRIMARY KEY`, `terminal_status INTEGER`, `error_code INTEGER`, `safe_message TEXT`, nullable `total_length BLOB(8)`, nullable `layout_hash BLOB(32)`, `completed_ms INTEGER`; RPC output is rendered from these canonical fields, not stored arbitrary JSON |
| `journal_install` | `gid TEXT PRIMARY KEY`, `checkpoint_id BLOB(16)`, `old_journal_id BLOB(16)`, `old_path BLOB`, `new_journal_id BLOB(16)`, `new_path BLOB`, `source_last_sequence BLOB(8)`, `phase INTEGER`, `created_ms INTEGER` |
| `bt_metadata` | `gid TEXT PRIMARY KEY`, generation, v1/v2 identities, root identity, bounded metainfo/magnet/info data, validated stable file mapping, and downloaded/uploaded/seeding counters; foreign key to a BT `task` |
| `bt_resume` | `gid TEXT PRIMARY KEY`, `resume_blob BLOB`, `dirty INTEGER`, tracked checkpoint identity, `saved_ms INTEGER`; baseline default maximum 16 MiB, hard maximum 64 MiB |

`session.clean_shutdown` is a publication marker, not an optimistic process
state. Every startup transaction sets it to false before scheduler/runtime
publication, including a restart of a previously clean database. Minimal
graceful shutdown stops admission, drains workers, flushes and closes all task
journals, and joins the session owner before reopening the database under a new
owner lock to write the final marker. It writes true only when every ordered
barrier succeeded; a failed/timed-out worker, journal, or owner barrier leaves
false. Recovery therefore never interprets a marker left true by a currently
running process or a timed-out graceful shutdown.

A canonical `Complete` stopped result has no error payload and carries both
`total_length` and `layout_hash`. A canonical `Error` carries its required
public error kind/message and no completion fields. A canonical `Removed`
carries neither error nor completion fields. Writes, normal reads, and v3 reopen all reject any other status/payload tuple.

`gid` is validated as exactly 16 lowercase hexadecimal characters by the
application codec before SQL. `queue_position` is indexed with `queue_state`;
temporary duplicate positions are allowed only inside the one reorder
transaction, whose final state is dense and deterministic. All child tables
except `stopped_result` cascade on task deletion. A stopped-result retention
transaction keeps the `task` row as the authoritative `Stopped` queue owner and
inserts the paired canonical result only after the journal terminal record is
valid. Result deletion removes both rows and densifies the remaining stopped
order atomically; downloaded output is not deleted.

The synchronous primitive caps a store at 100,000 tasks, task materialization
at 64 MiB, pending-install materialization at 16 MiB, and each option map at
4,096 entries/4 MiB. Task, stopped-result, host-key-challenge, and install reads
stream rows against count and byte budgets, charge owned-record overhead as
well as variable payload bytes, and decode every persisted value.
Task-option reads also reapply the current
`PersistedOptionPolicy`; direct database tampering cannot turn a formerly or
newly forbidden key into an accepted option.

Ordinary task upserts cannot change queue membership/position or the primary
journal id/path. Queue changes use one `BEGIN IMMEDIATE` transaction. The
scheduler-facing operation supplies the complete final order for every queue
whose membership or position changes; the transaction rejects missing,
duplicate, unexpected, or non-dense membership, applies those orders through a
temporary collision-free position range, changes desired pause state and slow
metadata, and verifies that all supplied orders exactly match the committed
rows. A convenience single-task transition may derive the same orders only for
storage-local callers and tests.

Demoted rows require bounded original-position metadata and a nonzero global
demotion count; readmission clears the cooldown decision without resetting that
count. Primary journal changes are allowed only through the install protocol
below.

`NoSpaceCondition.retry_at` is live monotonic state and is never serialized as
an instant. SQLite stores the wall scheduling decision
(`no_space_scheduled_at_ms`, `no_space_delay_ms`) and recovery applies the same
bounded-conservative clock rule as `RetryState`; absent/expired scheduling data
causes one immediate readiness probe, not automatic admission.
The condition may be set or refreshed while SQLite still records `Active`
during cancellation, while it records `Demoted` for a slow task, or after the
task reaches `Waiting`/`Paused`. Only `Stopped` rejects a new condition; clearing
stale condition columns remains permitted so repair and terminal cleanup can
converge.

The host-key challenge table allows a paused challenge to survive process
restart without persisting a credential. Approval still requires the exact
challenge id and fingerprint, persists the resulting task pin through the
option snapshot, and reconnects/rechecks that pin before any authentication.
Deleting/replacing the current challenge makes an old approval stale. Startup
and v3 reopen require valid UTF-8 canonical host and algorithm text, a
nonzero valid port, exact size caps, a SHA-256 fingerprint matching the stored
presented key, and a referenced task in the `Paused` queue.
An ordinary queue transition cannot move that task out of `Paused` while the
challenge remains. A terminal-retention transaction instead removes the
challenge in the same transaction that moves the task to `Stopped` and inserts
its result, so a crash cannot publish a non-paused challenge or a partial
terminal cleanup.

The storage-owned canonical pin entry is
`sftp-host-key-sha256=<64-lowercase-hex-digits>`. Challenge resolution verifies
that the replacement option snapshot contains that exact entry derived from
the approved fingerprint, verifies the retained public-key bytes again, and
replaces the snapshot plus deletes the challenge in one transaction.

Task-source replacement is atomic per task. It accepts a bounded,
duplicate-free URI-id set and stores either a registry-approved
persistence-safe URI or a redacted fingerprint placeholder. Reads are ordered
by priority then URI id and enforce a 4,096-entry/4 MiB per-task limit before
returning owned records. Startup semantic validation repeats that per-task byte
limit while streaming all source rows under the additional global 64 MiB
task-materialization budget.

The required active control path validates sources and scheduler conflicts
before beginning a bounded internal quiescence operation. It preserves the
user's desired pause state and services cancellation completions before source
mutation; it must not hold the control owner waiting for an event that owner
needs to process. A per-task pending operation prevents overlapping source or
option mutations while explicit pause/remove remains authoritative.

Once cancellation drains, source replacement commits the complete set and any
required queue transition in one transaction through the session owner, then
publishes the prepared catalog replacement. Durable queue/desired-state evidence must retain
automatic readmission for a task that was running; an internal pause must not
persist as a user pause. Restart before commit sees the old sources, while
restart after commit sees the new sources and the latest desired state. Both
paths still apply normal HTTP identity and durable-piece validation.

`ReplaceTaskSourcesAndQueue` is the owner operation for active replacements.
It validates the bounded source set, applies the scheduler's exact queue orders,
replaces all source rows, and verifies dense queues within one
`BEGIN IMMEDIATE` transaction. Queue mismatch or a source-write failure rolls
back both changes. The source mutation's reply is retained separately from the
control lock until this transaction completes; dropping the client reply does
not abandon accepted cancellation or persistence work.

Definite pre-commit rejection leaves the old sources authoritative. A confirmed
commit returns the documented replacement success even when normal admission
is delayed; later worker failures use task status. An uncertain accepted store
operation faults the driver for recovery, without claiming either rollback or
success. The `P4-05` implementation uses scheduler-owned begin/commit commands
and this atomic owner transaction. Delayed cancellation, explicit pause/remove,
disconnected callers, interrupted quiescence, and restart after commit have
regression coverage, as do exact-queue rejection and injected source-write
rollback.

A redacted placeholder (`persistence_safe_uri IS NULL`) must set
`needs_credentials`; otherwise startup would have neither a runnable source nor
an explicit admission blocker. The dedicated owner materializes exactly one
bounded source set for every startup task, including an empty set, under the
same per-task and global budgets. This lets composition distinguish “no stored
sources” from a missing or partial source read.

The synchronous SQLite connection and installed `ControlJournalAppender`
instances never leave their dedicated session-owner thread once spawned.
Callers submit owned commands to a queue whose configured capacity is nonzero
and cannot exceed the fixed 64-request implementation cap. Every accepted
command carries its own reserved one-result completion slot, so a full shared
response queue cannot deadlock the owner; dropping a waiter only drops that
result. Command execution returns a typed persistence error that retains the
concrete `SessionStoreError` or GID-associated `JournalAppenderError`, and separately
identifies a missing journal, a duplicate install, or an install whose embedded
task GID differs from the requested map key. Admission, shutdown, unavailable,
thread-spawn, and panic failures remain owner-level errors rather than being
collapsed into persistence failures.

`SessionHandle::try_submit_owned` returns a rejected command together with its
typed admission error. This is the nonblocking adapter path: a `QueueFull`
result preserves the exact command for retry instead of requiring the caller to
reconstruct persistence payloads. The compatibility `try_submit` API retains
its original error-only surface for callers that do not need ownership back.

Scheduler persistence is described by a bounded engine plan catalog rather
than inferred from an effect kind. Each entry owns the complete exact
`TransitionEffect` plus its missing store/journal payloads, accepts only the
semantically allowed command pattern for that effect, and validates all shared
identity and state fields before admission. Journal steps append once and then
flush through the returned sequence; multi-command plans never have more than
one owner request accepted at a time. Typed command results are checked before
the catalog entry can complete, and only correlated scheduler acknowledgements
are emitted.

Failure acknowledgement is phase-sensitive. `StageOptionPatch` may return the
correlated `OptionPatchPersistenceFailed` event only when its first journal
append returns a definite typed persistence error before any append evidence.
That represented set is limited to a missing installed appender or a
payload/record-construction rejection; journal I/O, a previously faulted appender, and
store/owner failures are treated as uncertain even on the first command.
An owner disconnect, timeout, or unexpected command result is not that proof,
even when it occurs while waiting for the first append result.
Once `JournalAppended` exists, the journal authority may already contain the
staged snapshot; a later flush failure, owner disconnect/timeout, unexpected
result, or SQLite-mirror failure is unrepresentable and faults the scheduler
driver for recovery instead of clearing the patch in memory. The same
fail-closed rule applies to all terminal-plan failures. It also applies to any
accepted host-key-resolution or stopped-result-deletion command failure: an
owner disconnect can lose a successful acknowledgement, and a typed SQLite
error can be returned from an ambiguous commit. Their correlated failure events
remain available only to an adapter that can prove rejection before mutation;
the session-owner composition does not manufacture that proof.

An appender is installed by moving it through a command with the exact expected
GID. The owner validates the appender header before checking and inserting the
single per-GID map entry; appenders carried by accepted install commands that
fail identity or duplicate validation are therefore also dropped on the owner
thread. Journal commands append one owned `JournalPayload` at an exact
generation and return the assigned sequence, flush through an exact requested
sequence and return the actual durable high-water mark, or close and remove one
appender only after `close_flushed` succeeds. A close rejected for unflushed
records leaves the appender installed so the caller can flush and retry. The
all-journals close command preflights every appender for health and an equal
appended/flushed high-water mark before closing any of them, then closes and
drops the complete map on the owner thread.

Shutdown is an out-of-band signal observed independently of normal admission.
It closes admission first, rejects commands that were not accepted, drains
commands already accepted, then drops all remaining appenders, the store, and
its owner lock on the same thread. `shutdown_with_timeout` waits only for its
validated nonzero duration, capped by the fixed five-minute owner maximum. A
joined result proves that thread-owned values were dropped on the owner. A
timeout drops only the join handle and reports `DetachedUncertain`; it does not
mark the owner closed, release its lock, manufacture accepted-command results,
or claim pending journal/SQLite work durable. The shutdown coordinator records
that result as a dirty recovery checkpoint and proceeds toward process exit.

An accepted command's completion does not imply that the owner has dequeued
the following command. Queue-pressure tests synchronize with entry into that
next command before expecting a newly available admission slot. A dropped
completion receiver must not block delivery of later accepted work.

Orderly shutdown first submits the all-journals close command; the out-of-band
join still drops appenders after an unclean or failed close, but does not claim
their unflushed records durable. Dropping the last handle is zero-wait: it closes
the sender by normal ownership destruction, unparks the owner, and detaches any
remaining join handle. Reserved completions do not keep admission alive, but
commands accepted before the last handle was dropped are still drained and
deliver their results. The same owner-thread destruction rule still applies if
the blocked operation later returns.

Startup waits for database open and bounded semantic reads only for
the configured startup timeout under the same hard cap. On timeout it closes
admission, detaches the unpublished owner, and returns `StartupTimedOut`; the
database/appenders remain owned by that thread until it actually exits. Startup
failure is returned before a usable handle is published, and an owner-thread
panic/exit closes all handles with a typed owner-unavailable error. Both
configured waits are validated before thread creation or database mutation, and
an invalid explicit shutdown wait is rejected before admission is changed.

Schema admission is fail-closed. The raw hot-rollback and committed-WAL
version preflight runs before permission changes, owner-lock creation or SQLite
open. Only the current v3 schema and its exact semantics may become active.
Backups retain the current format and are not a conversion mechanism.

### BitTorrent Metadata And Checkpoints

Transfer tasks require their existing journal fields. BT tasks require those
fields to be NULL and have a matching `bt_metadata` row. A metadata transaction
binds the complete identity, protected-root identity, stable mapping and file
selection before the native initialization hold can be released. Late metadata
must match any persisted mapping exactly. No fake transfer journal is created.

Admission and recovery use the same bounded pure metadata parser. Storage
rechecks info hashes, metainfo and magnet identities, file shapes, portable paths,
and the absence of endpoint credentials. A pending magnet has an identity and
protected root but no files; its first metadata commit fixes the complete mapping.
Subsequent metadata commits cannot change that binding. Checkpoint tokens include
both generation and monotonically increasing request number. A stale completion
cannot clear a dirty flag or replace a newer safe blob.

Resume checkpoints use one tracked protocol for periodic saves (60 seconds by
default), pause, remove and shutdown. The native disk release barrier completes
before resume data is serialized; its result is independent of the lossy alert
queue. The session owner commits the bounded resume blob before a clean boundary
is acknowledged. The default limit is 16 MiB (maximum 64 MiB) and default timeout
30 seconds (maximum 300 seconds). Failure, timeout or oversized output preserves
the previous safe blob and sets `DirtyCheckpoint`. A caller disconnect does not
cancel native ownership or the persistence completion.

A running task is marked dirty before it resumes native I/O. Restarts and JSON
imports revalidate identities, root protection and stable mappings, and recheck
payload progress; an opaque resume blob never authorizes paths, destinations,
credentials or unchecked completed bytes.

## Control Journal Responsibilities

Per-task journal stores:

- layout committed,
- lease begun/committed/aborted and provisional piece/span writes,
- piece/span verified,
- piece/hash failure resets,
- piece durable,
- generation changes,
- pause/remove markers,
- checksum validator state when needed,
- torrent/metalink identity hashes,
- recovery markers.

This journal is append-only and may rotate into linked immutable-numbered
segments. One serialized per-task appender assigns the global sequence and
performs every append/flush. Rotation headers, hash links, replay continuity,
checkpoint compaction (including the SQLite `installing`/`installed` pointer
protocol and old-set retirement), finalize intent/done, and the journal
descriptor budget are defined only in [detailed-storage.md](detailed-storage.md). SQLite stores the
installed journal id/path plus any pending installation intent; compaction
never merges divergent copies. Beginning an install returns a
`JournalInstallToken` bound to gid, checkpoint id, and new journal id. Completion
requires that token, re-decodes the persisted intent, and rechecks that the task
still points to the old set before changing the pointer and phase atomically.
Startup validates that `installing` points to the old set and `installed` to the
new set; clearing an installed row requires the same identity token and installed
phase. Rejecting an invalid `installing` candidate uses a separate tokenized
abort transaction that rechecks the old pointer before deleting the exact
intent. Stale complete, abort, and clear commands and ordinary task upserts
cannot replace the primary pointer or act on a newer install.

Native startup sends prepared descriptor-backed journal sets through the
bounded owner queue. The owner performs final torn-tail repair and constructs
the `ControlJournalAppender`; no live appender is returned to or retained by
the engine startup coordinator. Queue-full rejection returns the exact owned
command, including its open handles, for retry. Scheduler restoration occurs
only after every selected journal set is installed on the owner thread.

## Authority And Reconciliation

The stores have deliberately different authorities:

| Field class | Authority | Duplicate-copy rule |
| --- | --- | --- |
| generation, immutable layout/file map/hash, piece length | control journal | overwrite any SQLite cache from the valid journal prefix |
| begun/committed/aborted leases, written/verified/durable pieces, validators, retry checkpoint | control journal | SQLite must not promote or merge progress |
| task-local terminal markers (`TaskComplete`, `TaskError`, `TaskRemoved`) and final digest/layout | control journal | terminal marker is a safety veto; required before SQLite publishes the corresponding result |
| queue membership/order, session id, global desired state, cross-task scheduling | SQLite | journal recovery does not invent queue position |
| recoverable scheduler admission conditions | SQLite plus bounded source-derived recovery input | `no_space` is restored and re-probed before admission; ordinary startup composition derives an explicit `needs_credentials` requirement or `None` from the owner's exact source sets without persisting a secret |
| stopped-result index and retention metadata | SQLite, gated by journal completion | recreate from `TaskComplete` when missing; never use it to manufacture completion |
| mutable non-layout task options and persistence-safe URI/mirror inputs | SQLite | restored after journal generation state is fixed |
| generation-scoped options affecting layout, validators, verification, or durability | control-journal `OptionsSnapshot` | SQLite stores only a searchable mirror and snapshot hash |

Recovery first establishes the valid journal prefix, then applies SQLite-owned
queue/session fields. A disagreement is reconciled in the single direction in
the table; values are never field-by-field merged based on timestamps. SQLite
is updated transactionally after replay when one of its cached journal-owned
fields differs.

`TaskPaused` is a task-local checkpoint marker, not queue authority; SQLite's
desired state decides whether a recovered non-terminal task remains paused or is
eligible to run. Conversely, SQLite queue membership cannot reactivate a
journal generation already marked complete, errored, or removed; that requires
the normal new-generation/new-task operation.

## Atomicity Model

Task progress:

1. append/accept `LeaseStarted` and write blocks provisionally,
2. after exact response-length and validator checks, append `LeaseCommitted` or
   `LeaseAborted`,
3. validate/checksum the complete piece as required,
4. complete the durability mode's data-file barrier,
5. append `PieceDurable` through the single task appender,
6. complete the journal barrier, then publish durable progress.

No journal descriptor flush can substitute for step 4. In `balanced`, steps
4–6 are grouped; in `strict`, they complete per piece. In `fast`, ordinary
progress stops at provisional written/verified records and only finalization
performs the promotion barrier. The exact ordering and primitives are normative
in [detailed-storage.md](detailed-storage.md).

Global queue mutation:

1. if a mutation requires a future generation, append/flush its sanitized
   `OptionsSnapshot(scope=NextAdmission)` before acknowledging it; after the old
   generation drains, admission appends/flushes the one `GenerationStarted`
   record that references and promotes that snapshot. Automatic retry or
   representation readmission performs the same ordering inside one bounded
   persistence effect: append/flush the unchanged next-admission snapshot, then
   append/flush `GenerationStarted`; a generation record without its staged
   snapshot is invalid. An active option patch uses one patch identity and may
   have only one staged snapshot; replay must not treat a flushed staged record
   as permission to append a duplicate. Promotion consumes the exact matching
   patch snapshot once, and an identity/hash mismatch fails closed before
   publication,
2. one SQLite `BEGIN IMMEDIATE` transaction updates its queue/desired-state
   fields and journal snapshot hash; a cross-queue move shifts both queues and
   validates their final dense positions before commit,
3. snapshots are published.

If step 2 fails, journal-owned generation state remains authoritative and the
SQLite mirror is repaired on recovery. If a queue-only mutation has no task
generation effect, it is a normal SQLite transaction and does not append a
redundant journal record.

Option-patch promotion uses one owner transaction to compare the complete
`NextAdmission` option map with the generation's accepted snapshot, replace
`CurrentGeneration`, and delete `NextAdmission`. A mismatch rejects without
changing either map. The engine submits it only after the matching generation
record is flushed and acknowledges generation persistence only after the owner
confirms the transaction. Recovery can reconstruct a missing staged mirror from
the journal before finishing promotion; an unrelated current-only option update
must not be replaced by an older journal snapshot.

Completion:

1. all selected task pieces durable,
2. final file rename/fsync,
3. control journal marks complete,
4. SQLite stopped result transaction,
5. optional removal of companion control file.

The terminal state is user-visible only after both the task journal has a valid
`TaskComplete` record and SQLite has the stopped result. If the process crashes
after `TaskComplete` but before the SQLite transaction, startup treats the
journal as authoritative for file completion and recreates the missing stopped
result. If SQLite says stopped but the journal lacks `TaskComplete`, startup
must verify/finalize from the journal state before publishing completion.

## Recovery

Startup:

- require a dedicated private persistence directory; reject intermediate
  symlink/reparse components, non-regular database/sidecar artifacts, and
  orphan `-wal`, `-shm`, or `-journal` files when the main database is missing
  or empty,
- inspect committed `user_version` from a hot rollback page-one before-image,
  the raw main header, and committed WAL frames before SQLite open; reject a
  newer version unchanged and reject legacy page-size-zero rollback journals,
- acquire `${db}.ariax-owner-lock`, repeat version inspection under the
  cooperative Ariax-only lock, then open a supported version so SQLite can
  complete hot rollback-journal recovery,
- validate the exact schema, integrity, foreign keys, decoded bounded records,
  dense queues, and journal-install pointer relation,
- read task list and journal paths,
- scan companion control files if configured,
- replay each task journal to last valid committed record,
- rebuild and verify its root binding before any payload descendant is opened;
  a mismatch enters the explicit import/rebind path rather than using adjacency,
- take generation/layout/progress from the journal and reconcile cached copies
  to SQLite in the one allowed direction,
- reset begun, aborted, uncommitted, and non-durable spans to pending,
- revalidate fast-mode hints or suspicious durable pieces as required,
- rebuild scheduler queues.

If SQLite is missing but companion control files exist:

- offer/import recovery mode,
- reconstruct tasks from control journals and metadata where possible.

If SQLite is present but corrupt (failed integrity check or open error):

- do not treat a corrupt DB as authoritative,
- move it aside (timestamped backup) rather than deleting,
- rebuild the index from companion control journals exactly as in the
  missing-DB path, since the per-task journals hold the crash-critical state,
- if control journals are also unavailable, fall back to needing-revalidation
  per task rather than silently discarding progress.

If a task journal is missing but SQLite says task was active:

- do not infer progress from file length or allocation,
- mark the task needing full revalidation or error depending on available
  output files and content digests.

### Companion Import And Output-Root Rebinding

The explicit recovery surface is:

```text
ariax session import-control CONTROL_PATH --output-root=ROOT \
  --rebind=identity|verify
```

The native API exposes the same typed operation; remote RPC does not expose it
unless a startup administrator policy explicitly allows local filesystem
imports. `identity` is the default and succeeds only when the stable root and
selected-file identities match, allowing a path-only move/rename. `verify`
permits a different root identity but follows [detailed-storage.md](detailed-storage.md): it rebuilds
all safe paths and retains only pieces whose recorded per-piece content digest
passes readback. Everything else returns to pending.

Scanning a companion file never authorizes a rebind by adjacency or mtime. The
journal's gid/id/linkage and root binding are validated first, the target root
must satisfy allowed-root and safe-open policy, and a live task with the same gid
must be quiesced or rejected as a collision. Import installs the journal/index
transaction only after the new generation and root binding are flushed. A crash
before that point leaves the old installed task authoritative; a crash after it
repairs/creates the SQLite row from the journal.

## Secrets At Rest

Decision: active persistence and plaintext exports omit secrets. The first
implementation does not encrypt credentials into SQLite, control journals,
metadata blobs, backups, temporary files, or session exports.

Secrets include RPC credentials, proxy/user passwords, URI userinfo, cookies,
`Authorization` and signature headers, private-key passphrases, bearer tokens,
and option values marked secret by the registry. A URI with userinfo or an
unclassified query string is treated as sensitive because signed URLs commonly
carry credentials in query parameters. Automatic persistence stores a redacted
origin/path fingerprint and source placeholder instead of such a URI. A scheme
or individual query field may be persisted only when the option/protocol
registry explicitly classifies it as non-secret.

The HTTP source catalog distinguishes an available in-memory request URI from
its optional persistence-safe URI. A live signed URL remains usable by the
current worker, while its persisted source has no URI and retains only a
fingerprint of the origin/path and `needs_credentials=true`. Recovery retains
these placeholders in the catalog so status, option queries, and source
replacement remain available. Workers skip unavailable sources, including when
a safe mirror lets the task continue. A committed caller-supplied replacement
clears only the matching credential requirement, after the source transaction
acknowledges; explicit pause intent remains authoritative.
Source replacement can satisfy a source-URI or origin-HTTP requirement; it
cannot clear an unrelated proxy or private-key credential requirement.

Source metadata validation rejects a purported safe URI containing userinfo,
a query, or a fragment at both write and recovery boundaries. Existing unsafe
metadata is rejected without echoing its URI or silently rewriting the stored
record. Phase 6 uses session schema v3.

Consequences:

- `OptionsSnapshot` is a canonical sanitized map; secret entries are absent,
  not replaced with reversible encodings.
- Validator records store a one-way canonical fingerprint needed for comparison,
  never raw cookies, credentials, or signed headers.
- A recovered nonterminal task whose bounded source set has no runnable
  persistence-safe source but does contain a credential-marked source enters a
  scheduler-owned `needs_credentials` condition and remains paused/waiting until
  the caller supplies credentials or a replacement URI. Existing durable pieces
  are retained. The first credential-marked source in canonical priority/URI-id
  order supplies the non-secret requirement identity; a runnable safe mirror
  prevents a task-wide blocker.
- Plain JSON and aria2-format exports follow the same omission rules and mark
  entries that require credentials. There is no `include-secrets` plaintext
  switch.
- A future encrypted credential store may place opaque key identifiers in
  SQLite, but requires a separate reviewed design for OS-keyring/user-key
  encryption, key rotation, locked-key recovery, and export encryption. It does
  not weaken this default.

The SQLite database and backups require a dedicated private parent directory.
Every existing path component must be a real directory rather than a Unix
symlink or Windows reparse point. If the exact parent already exists with
group/other access on Unix or inherited or foreign allow entries on Windows,
startup rejects it without changing the directory. A missing owned directory
chain is created one component at a time with mode `0700` on Unix or a protected
Windows ACL granting only the current user, SYSTEM, and Administrators. This
avoids silently applying `chmod` or a new ACL to `/tmp`, a project directory, or
another caller-owned broad parent. Windows creation and verification use the
native `ariax-windows-security` adapter with no shell or PowerShell subprocess.

Database, WAL, SHM, rollback-journal, `${db}.ariax-owner-lock`,
temporary-backup, and published-backup files are private regular files (`0600`
on Unix and the corresponding protected Windows ACL). Hard-linked persistence
artifacts are rejected so path-derived owner locks cannot be bypassed by opening
the same file through another name. Existing
supported-version files inside an accepted private directory may be tightened
before use, but symlinks and non-regular artifacts are rejected rather than
followed or replaced. When the main database is absent, existing SQLite
sidecars are rejected as orphans rather than adopted. The same rule applies to
an empty main file, so SQLite cannot silently initialize it while discarding an
untrusted WAL, SHM, or rollback journal. A newer schema is rejected before any
such file-permission change. Journal, companion, metadata, and export artifacts
follow their owning safe-creation rules.

Atomic replacement never leaves a broad-permission temporary file. Rotated
segments and backups retain the source ACL/mode. Deletion is best-effort and is
not claimed as secure erasure on copy-on-write, journaled, flash, or cloud-backed
filesystems; omission is therefore the primary protection. Tests scan the raw
database, WAL/SHM, journals, metadata, temporary/backup, companion, and export
files for seeded secret values and verify recovery's `needs_credentials` path.

## Text Export

Phase 4B exports unfinished work through the configured local destination for
`aria2.saveSession` and periodic saving, or as bounded data for namespaced JSON
RPC export. Both formats use the persistence-safe source view, never live raw
URI strings. Credential placeholders remain explicit and cannot become runnable
URIs through export/import. Remote callers cannot supply filesystem paths.

JSON export/import uses only self-contained format version 3, with explicit
transfer/BT task kinds. Transfer verification metadata preserves the selected
Metalink child's original file index, exact length, chunk geometry, supported
checksums, source priorities and sanitized sources. BT members include bounded
metainfo or magnet identity, validated metadata and mapping, supported options,
and the last safe resume data. They contain no credentials or trusted local
root authority. Imports bind the selected new root and recheck payload progress.
Versions 1 and 2 are rejected without adaptation. Expanded metadata parents are
omitted while their independently exportable children remain in the file.
Aria2 text export rejects atomically if BT or verification metadata cannot be
represented safely.

Every JSON task carries `kind: "transfer"` or `kind: "bittorrent"`. The latter
has a `bittorrent` object with `identity`, base64 `metainfo`, an optional
`magnet`, the complete `files` mapping, and base64 `resumeData`. Imported roots
are selected by the receiving engine; root identities and exported lifecycle
counters are never accepted as recovery authority. A mixed batch commits all
transfer and BT rows in one transaction before scheduler publication. Its
subsequent acknowledgements compare each exact committed member.

Import parses and validates the entire bounded document and reserves admission
capacity before publishing tasks. One bounded filesystem job captures the
configuration and policy, validates every member, then prepares journals outside
the mutable owner. Existing tasks and urgent controls can progress during this
preparation. Publication enters the import fence and revalidates queue positions
against the current scheduler one member per turn. New task journals are prepared before one
session-owner transaction installs the batch metadata; scheduler publication
follows that transaction. Invalid input publishes no task prefix. Uncertain
accepted persistence faults the driver for recovery. JSON import retains its
paused-by-default behavior; aria2 input-file import honors validated per-task
pause options. Existing progress is never inferred from an exported text file.

One import admits at most 1,000 tasks and 8 MiB of owned batch metadata, also
subject to the shared RPC and scheduler limits. Its first task-persistence
effect installs every task, source set, and current option map in one SQLite
transaction. Subsequent member effects compare the exact committed metadata
before scheduler publication. The owner retains the complete import and its
reservations while these effects drain. Journal installation uses nonblocking
session submissions and completion polling; final batch-plan construction runs
outside the owner. The publication fence allows immutable queries but no interleaved
queue mutations or worker admissions. A disconnect does not cancel an accepted
batch. A crash before the transaction publishes no imported metadata; a crash
after commit recovers the complete batch from SQLite and the prepared journals.
Prepared journal directories without committed task rows remain unreferenced.
New HTTP task identities skip existing journal destinations after restart, so
an interrupted preparation cannot overwrite an orphan or prevent later adds.
Any mismatch or uncertain accepted write faults the driver instead of retrying
the import or acknowledging a partial result.

Provide:

```text
ariax session export --format=json
ariax session import session-export.json
```

Export includes queue/sanitized options/persistence-safe URIs but not necessarily
hot progress bitmaps unless requested. Sensitive sources are represented by a
needs-credentials placeholder. It is for migration and debugging, not the
primary crash recovery path.

JSON source entries include stable source identity, optional sanitized URI,
priority, and the credential-required marker. The legacy `uris` projection
contains only persistence-safe strings. These are migration hints, and a
credential placeholder is never converted into a runnable URL. Debug formatting
of source objects also omits sensitive live URI text.
JSON files reject duplicate object fields and unknown migration fields. The
aria2 text form includes a bounded `# ariax-task ` JSON comment per task so
credential placeholders survive ariax reimport; ordinary aria2 readers ignore
the comment. Ariax-only options remain in that comment; indented lines contain
only options advertised by the pinned aria2 registry. When safe URI/option lines
follow it, ariax requires them to match that projection of the comment exactly.
A task containing only unavailable sources is comment-only.

Configured exports bind an existing absolute parent directory through the native
directory capability. A bounded dedicated writer creates a private temporary
file there, writes and syncs the complete document, atomically renames it over
the configured regular file, and syncs the parent where supported. No remote
method accepts a destination path. Before rename, failure leaves the old export
intact; after rename, a reported sync failure leaves a complete new document and
does not roll back to a partial file. Only one export writer may run per engine,
with its request and byte reservations retained until completion. Periodic and
shutdown saves use the same operation as `aria2.saveSession`.
Shutdown waits within its existing drain deadline for any pending save and one
final snapshot; a writer failure or timeout prevents a clean shutdown report.
The export path must be outside the managed control directory and database.
Local input files use a held directory and a no-follow regular-file descriptor,
with the same document limit as RPC input. Reading or parsing failure publishes
no task. The experimental CLI accepts `--input-file=FILE` and
`--input-file-format=aria2|json` before an RPC command; the default is `aria2`.
`EngineBuilder::input_file` uses the same admission path before workers start.
`EngineBuilder::session_export` configures the local writer, and the typed
`Engine` exposes explicit import, export, and save operations.

## aria2 `--save-session` Compatibility

aria2 `--save-session` writes a text input-file-like list of unfinished
downloads and options. This design should support an equivalent compatibility
export:

```text
--save-session=FILE
--save-session-format=aria2|json
```

Rules:

- `aria2` format is for compatibility with existing tooling.
- It records enough non-secret URI/options state to re-add unfinished downloads;
  authenticated sources require credentials to be supplied after import.
- It is not the crash-critical progress journal.
- Importing aria2 session text is supported through the input-file parser and
  option matrix.

The control journal remains the authoritative source for partial byte/piece
state.

## Why Not One Big SQLite For Everything

SQLite can handle many writes, but per-piece hot progress is better isolated:

- a torn or corrupted task journal affects one task,
- companion control files support identity-checked moves and explicit
  digest-verified rebinding of partial downloads,
- strict durability can fsync small task journals without locking global queue
  metadata,
- large bitsets and per-piece records do not bloat the global DB.

The global DB remains small and query-friendly.

## Configuration

```text
--session-store=hybrid|sqlite|control-files|memory
--session-db=PATH
--control-file-dir=PATH
--control-file-location=central|beside-output|both
--control-file-suffix=.ariax
--save-session=PATH
--save-session-format=aria2|json
--save-session-interval=SEC
--auto-save-interval=SEC
--durability=fast|balanced|strict
```

Defaults:

```text
--session-store=hybrid
--control-file-location=beside-output
```

The first production implementation accepts `hybrid`; `memory` is accepted only
for tests or explicit no-resume mode. `sqlite` and `control-files` are reserved
feature-gated values, not parsed-only modes: the baseline registry reports and
rejects them as unsupported because neither a SQLite hot-piece schema nor a
control-file-only global queue/index/recovery protocol is defined. They become
implemented only after those separate designs, migration rules, and fault tests
exist. No baseline code silently aliases either value to `hybrid`.

## Schema And Format

SQLite:

- the exact version-3 tables and unsupported-format rejection rules are defined under
  SQLite Responsibilities above,
- WAL mode by default when supported; WAL and DELETE are each verified with a
  `BEGIN IMMEDIATE` transaction that writes page-one `user_version` and rolls
  back. A failed WAL selection or probe falls back to DELETE and requires the
  same probe rather than assuming the pragma string proves the filesystem works,
- `synchronous=FULL`, `foreign_keys=ON`, a 5-second busy timeout, and new
  databases use 4096-byte pages,
- `cache_size` is set as a negative KiB value from the selected
  `sqlite_cache_budget`; baseline `mmap_size=0` prevents an uncharged mapped
  page cache,
- the rusqlite build enables `bundled`, `backup`, `cache`, and `limits` with
  defaults disabled. Every connection sets `SQLITE_LIMIT_LENGTH=80 MiB`,
  `SQLITE_LIMIT_SQL_LENGTH=1 MiB`, `SQLITE_LIMIT_COLUMN=64`,
  `SQLITE_LIMIT_EXPR_DEPTH=100`, `SQLITE_LIMIT_COMPOUND_SELECT=16`,
  `SQLITE_LIMIT_FUNCTION_ARG=32`, `SQLITE_LIMIT_ATTACHED=0`,
  `SQLITE_LIMIT_LIKE_PATTERN_LENGTH=64 KiB`,
  `SQLITE_LIMIT_VARIABLE_NUMBER=256`, `SQLITE_LIMIT_TRIGGER_DEPTH=16`, and
  `SQLITE_LIMIT_WORKER_THREADS=0`; a platform SQLite that cannot apply a
  required limit fails persistent-mode startup rather than silently widening it,
- the bundled SQLite compile uses
  `-DSQLITE_MAX_LIKE_PATTERN_LENGTH=65536`; without that repository-scoped
  hard ceiling SQLite clamps the required runtime limit to 50,000 and startup
  correctly fails closed,
- WAL auto-checkpoint is 1000 pages. The executable truncate-checkpoint primitive
  reports a busy checkpoint and is a no-op in DELETE mode; clean-shutdown and
  size-trigger invocation of that primitive remain pending integration work,
- the hot-backup primitive writes a private temporary database, validates its
  integrity, exact schema, and persisted semantics, then `sync_all`s that file
  and publishes it with a no-clobber hard link. It never overwrites an existing
  destination, deletes a raced destination replacement, or accepts a
  destination filename ending in `-wal`, `-shm`, or `-journal` under
  ASCII-insensitive comparison;
  pre-existing destination sidecars are rejected rather than adopted. The
  temporary connection is normalized to DELETE journal mode, and owned
  temporary `-wal`, `-shm`, and `-journal` sidecars plus the temporary main file
  are removed on both success and validation failure. Unix also syncs the
  parent directory around temporary-link cleanup; Windows does not
  currently claim crash-durable directory-entry publication. A crash or
  temporary-unlink failure at any point from destination-link publication until
  removal is durably synced can leave or resurrect two names for one inode;
  normal unique-link validation rejects that residue. The backup primitive now
  recovers only a verified generated same-file alias using descriptor identity
  and link-count checks. Collision, invalid-residue, crash-point and unlink-error
  regressions cover the publication window. Periodic backup scheduling, bounded
  generation retention and the full native release matrix remain pending.

Control journal:

The on-disk journal format is defined normatively in [detailed-storage.md](detailed-storage.md)
(segment header/linkage, record framing, payload layouts, CRC coverage, and
record-type enum). This document does not restate the field list. The layout
hash lives in `LayoutCommitted` and `TaskComplete`, not in every record; each
record carries `generation` and a globally continuous per-task `sequence` per
the normative spec.

Both formats must be documented and fuzz-tested. Tests also cover segment
rotation at every boundary, missing/reordered segments, bad previous-segment
hashes, SQLite/journal snapshot disagreement, permission/ACL creation, and
absence of seeded secrets from every persistence artifact.
