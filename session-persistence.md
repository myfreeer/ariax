# Session Persistence

Status: first-slice implementation in progress. The control-journal v1 framing,
segment linkage, bounded replay, torn-tail valid-prefix rules, and 18 scalar
record payload codecs are executable, as are the six bounded collection/path
payloads that complete all 24 v1 record types. Policy-gated typed state
reconstruction, exact generation/layout/lease/finalization validation, and
whole-checkpoint hash validation are also executable. Native root identity
revalidation, durable file appending, checkpoint compaction writing, and the
SQLite session store remain pending.

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
set. Segment headers and continuity are normative in `detailed-storage.md`.
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
- task state: waiting, active, paused, stopped,
- scheduler admission conditions that must survive restart (`no_space` plus its
  redacted target/retry parameters; `needs_credentials` is recomputed from the
  restored redacted option/source set),
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

### SQLite Schema Version 1

`PRAGMA user_version=1` is the authoritative schema version. Version-1 tables
are `STRICT`, enable foreign keys, and use closed integer enums generated from
the same state/error matrices as the API. A `u64` that may exceed SQLite's
signed integer range is stored as an exactly 8-byte little-endian BLOB; hashes,
ids, and platform paths have exact length/codec checks before binding.

| Table | Version-1 columns and key |
| --- | --- |
| `session` | `session_id BLOB(16) PRIMARY KEY`, `created_ms INTEGER`, `updated_ms INTEGER`, `clean_shutdown INTEGER` |
| `task` | `gid TEXT PRIMARY KEY`, `session_id BLOB(16)`, `queue_state INTEGER`, `queue_position INTEGER`, `desired_paused INTEGER`, `primary_journal_id BLOB(16)`, `primary_journal_path BLOB`, nullable `replica_journal_path BLOB`, nullable `replica_sequence BLOB(8)`, `root_display BLOB`, nullable `cached_layout_hash BLOB(32)`, nullable `cached_root_binding_hash BLOB(32)`, `cached_snapshot_hash BLOB(32)`, nullable `no_space_target BLOB`, nullable `no_space_scheduled_at_ms INTEGER`, nullable `no_space_delay_ms BLOB(8)`, `created_ms INTEGER`, `updated_ms INTEGER`; foreign key to `session` |
| `task_option` | `gid TEXT`, `scope INTEGER`, `key TEXT`, `canonical_value BLOB`, primary key `(gid, scope, key)`; secret-class registry keys are rejected before SQL |
| `task_source` | `gid TEXT`, `uri_id INTEGER`, nullable `persistence_safe_uri TEXT`, `redacted_fingerprint BLOB(32)`, `needs_credentials INTEGER`, `priority INTEGER`, primary key `(gid, uri_id)` |
| `host_key_challenge` | `gid TEXT PRIMARY KEY`, `challenge_id BLOB(16)`, `canonical_host TEXT`, `port INTEGER`, `algorithm TEXT`, `presented_public_key BLOB`, `fingerprint_sha256 BLOB(32)`, `created_ms INTEGER`; public key/challenge caps come from `detailed-ftp-sftp.md` |
| `stopped_result` | `gid TEXT PRIMARY KEY`, `terminal_status INTEGER`, `error_code INTEGER`, `safe_message TEXT`, nullable `total_length BLOB(8)`, nullable `layout_hash BLOB(32)`, `completed_ms INTEGER`; RPC output is rendered from these canonical fields, not stored arbitrary JSON |
| `journal_install` | `gid TEXT PRIMARY KEY`, `checkpoint_id BLOB(16)`, `old_journal_id BLOB(16)`, `old_path BLOB`, `new_journal_id BLOB(16)`, `new_path BLOB`, `source_last_sequence BLOB(8)`, `phase INTEGER`, `created_ms INTEGER` |
| `bt_resume` | `gid TEXT PRIMARY KEY`, `resume_blob BLOB`, `dirty INTEGER`, `saved_ms INTEGER`; baseline default maximum 16 MiB, hard maximum 64 MiB |

`gid` is validated as exactly 16 lowercase hexadecimal characters by the
application codec before SQL. `queue_position` is indexed with `queue_state`;
temporary duplicate positions are allowed only inside the one reorder
transaction, whose final state is dense and deterministic. All child tables
except `stopped_result` cascade on live-task deletion. A stopped-result
retention transaction inserts the independent canonical result and removes the
live `task` row only after the journal terminal record is valid.

`NoSpaceCondition.retry_at` is live monotonic state and is never serialized as
an instant. SQLite stores the wall scheduling decision
(`no_space_scheduled_at_ms`, `no_space_delay_ms`) and recovery applies the same
bounded-conservative clock rule as `RetryState`; absent/expired scheduling data
causes one immediate readiness probe, not automatic admission.

The host-key challenge table allows a paused challenge to survive process
restart without persisting a credential. Approval still requires the exact
challenge id and fingerprint, persists the resulting task pin through the
option snapshot, and reconnects/rechecks that pin before any authentication.
Deleting/replacing the current challenge makes an old approval stale.

Schema migration rules are fail-closed:

- migration runs on the dedicated session thread inside `BEGIN IMMEDIATE` and
  takes a private timestamped backup before any non-additive change,
- a binary that sees a newer `user_version` leaves the database and journals
  untouched and exits persistent mode with a typed version error,
- a future binary keeps version-1 journal readers; after successful replay it
  may write a newer checkpoint set and retires version-1 segments only through
  the normal install protocol,
- downgrade is export/import only; no older binary rewrites a newer database.

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
descriptor budget are defined only in `detailed-storage.md`. SQLite stores the
installed journal id/path plus any pending installation intent; compaction
never merges divergent copies.

## Authority And Reconciliation

The stores have deliberately different authorities:

| Field class | Authority | Duplicate-copy rule |
| --- | --- | --- |
| generation, immutable layout/file map/hash, piece length | control journal | overwrite any SQLite cache from the valid journal prefix |
| begun/committed/aborted leases, written/verified/durable pieces, validators, retry checkpoint | control journal | SQLite must not promote or merge progress |
| task-local terminal markers (`TaskComplete`, `TaskError`, `TaskRemoved`) and final digest/layout | control journal | terminal marker is a safety veto; required before SQLite publishes the corresponding result |
| queue membership/order, session id, global desired state, cross-task scheduling | SQLite | journal recovery does not invent queue position |
| recoverable scheduler admission conditions | SQLite or deterministic recovery derivation | `no_space` is restored and re-probed before admission; `needs_credentials` is derived without persisting a secret |
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
in `detailed-storage.md`.

Global queue mutation:

1. if a mutation requires a future generation, append/flush its sanitized
   `OptionsSnapshot(scope=NextAdmission)` before acknowledging it; after the old
   generation drains, admission appends/flushes the one `GenerationStarted`
   record that references and promotes that snapshot,
2. SQLite transaction updates its queue/desired-state fields and the journal
   snapshot hash,
3. snapshots are published.

If step 2 fails, journal-owned generation state remains authoritative and the
SQLite mirror is repaired on recovery. If a queue-only mutation has no task
generation effect, it is a normal SQLite transaction and does not append a
redundant journal record.

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

- open SQLite,
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
permits a different root identity but follows `detailed-storage.md`: it rebuilds
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

Consequences:

- `OptionsSnapshot` is a canonical sanitized map; secret entries are absent,
  not replaced with reversible encodings.
- Validator records store a one-way canonical fingerprint needed for comparison,
  never raw cookies, credentials, or signed headers.
- A recovered task that cannot reconstruct an authenticated source enters an
  scheduler-owned `needs_credentials` condition and remains paused/waiting until the
  caller supplies credentials or a replacement URI. Existing durable pieces are
  retained.
- Plain JSON and aria2-format exports follow the same omission rules and mark
  entries that require credentials. There is no `include-secrets` plaintext
  switch.
- A future encrypted credential store may place opaque key identifiers in
  SQLite, but requires a separate reviewed design for OS-keyring/user-key
  encryption, key rotation, locked-key recovery, and export encryption. It does
  not weaken this default.

Persistence directories are created mode `0700` on Unix and files, including
SQLite WAL/SHM files, journal segments, temporary replacements, and backups, are
created mode `0600`. On Windows, their ACL grants the current user and required
system principals only and disables inherited broad access. Existing artifacts
with broader access are tightened before use; if that cannot be done, persistent
mode fails closed with an actionable error instead of writing sensitive task
metadata insecurely. User-requested export destinations receive the same secure
creation policy.

Atomic replacement never leaves a broad-permission temporary file. Rotated
segments and backups retain the source ACL/mode. Deletion is best-effort and is
not claimed as secure erasure on copy-on-write, journaled, flash, or cloud-backed
filesystems; omission is therefore the primary protection. Tests scan the raw
database, WAL/SHM, journals, metadata, temporary/backup, companion, and export
files for seeded secret values and verify recovery's `needs_credentials` path.

## Text Export

Provide:

```text
ariax session export --format=json
ariax session import session-export.json
```

Export includes queue/sanitized options/persistence-safe URIs but not necessarily
hot progress bitmaps unless requested. Sensitive sources are represented by a
needs-credentials placeholder. It is for migration and debugging, not the
primary crash recovery path.

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

- the exact version-1 tables and migration rules are defined under SQLite
  Responsibilities above,
- WAL mode by default when supported; on filesystems where WAL's shared-memory
  requirement is unreliable (network filesystems and some FUSE/overlay mounts),
  detect the failure and fall back to rollback-journal mode with one startup
  warning rather than risking a corrupt WAL,
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
- WAL auto-checkpoint is 1000 pages, with a truncate checkpoint at clean
  shutdown and when WAL bytes exceed 64 MiB; checkpoint failure is diagnostic
  and never discards the WAL,
- periodic private backup policy uses the SQLite backup API on the dedicated
  session thread and retains a bounded two generations by default.

Control journal:

The on-disk journal format is defined normatively in `detailed-storage.md`
(segment header/linkage, record framing, payload layouts, CRC coverage, and
record-type enum). This document does not restate the field list. The layout
hash lives in `LayoutCommitted` and `TaskComplete`, not in every record; each
record carries `generation` and a globally continuous per-task `sequence` per
the normative spec.

Both formats must be documented and fuzz-tested. Tests also cover segment
rotation at every boundary, missing/reordered segments, bad previous-segment
hashes, SQLite/journal snapshot disagreement, permission/ACL creation, and
absence of seeded secrets from every persistence artifact.
