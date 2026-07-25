# Detailed Storage And Journal Design

Status: detailed draft for the first implementation slice.

This document defines `SafePathBuilder`, `FileLayout`, `GlobalOffsetMapper`,
`StorageEngine`, and `ControlJournal` contracts for HTTP sequential/range
downloads. It is compatible with later Metalink and BitTorrent work but does
not implement those protocols yet.

## Module Ownership

```text
SafePathBuilder      path validation and creation policy
FileLayout           immutable task file map
GlobalOffsetMapper   global offset -> file spans
StorageEngine        validates writes, submits disk I/O, tracks piece state
ControlJournal       durable recovery records
```

Protocol adapters only submit global offsets and buffers. They never own file
cursors or final file descriptors.

## SafePathBuilder

Input:

```rust
pub struct SafePathInput {
    pub output_root: PathBuf,
    pub dir_option: Option<PathBuf>,
    pub out_option: Option<String>,
    pub metadata_components: Vec<String>,
    pub allowed_roots: Vec<PathBuf>,
}
```

Canonical API and output:

```rust
pub struct SafePathBuilder;

impl SafePathBuilder {
    pub fn build(input: SafePathInput) -> Result<SafePathOutput, PathError>;
}

pub struct SafePathOutput {
    pub root: CanonicalRoot,
    pub relative: SafeRelativePath,
    pub full: PathBuf,
}

pub enum PathError {
    InvalidRoot,
    OutsideAllowedRoot,
    InvalidComponent,
    UnsafeExistingPath,
    CreateFailed,
}

// Compatibility name for prose that predates the explicit API signature.
pub type SafePath = SafePathOutput;
```

Algorithm:

1. canonicalize existing output root,
2. verify root is under `allowed-output-root` if configured,
3. validate each path component,
4. reject absolute paths, prefixes, separators, NUL/control, `.` and `..`,
5. apply Windows reserved-name and trailing-dot/space checks,
6. create parent directories stepwise with no-follow checks where available,
7. verify final parent remains under root before opening.

`build` is the only API that converts output options or metadata components into
a filesystem target. No raw `PathBuf::join` on metadata is allowed outside this
builder. `SafePathOutput` contains only a path that has passed these checks; it
does not grant a protocol adapter permission to reopen arbitrary parent paths.

## FileLayout

For first-slice HTTP single-file downloads:

```rust
pub struct FileLayout {
    pub task: TaskId,
    pub generation: Generation,
    pub root: CanonicalRoot,
    pub files: Vec<FileEntry>,
    pub total_length: Option<u64>,
    pub piece_length: u64,
    pub layout_hash: LayoutHash,
}

pub struct FileEntry {
    pub id: FileId,
    pub safe_path: SafePathOutput,
    pub length: u64,
    pub global_start: u64,
    pub global_end: u64,
    pub selected: bool,
}
```

Rules:

- first slice has one selected file starting at global offset `0`,
- layout is immutable per generation,
- unknown total length is allowed only before range/split planning,
- once total length is known, layout hash changes require a new generation,
- zero-length file completion still persists terminal state.

## GlobalOffsetMapper

Input:

```rust
pub struct GlobalSpan {
    pub offset: u64,
    pub len: usize,
}
```

Output:

```rust
pub struct FileSpan {
    pub file: FileId,
    pub file_offset: u64,
    pub len: usize,
}
```

Rules:

- reject overflow in `offset + len`,
- reject spans outside known layout,
- reject unselected file spans,
- split cross-file spans only in later multi-file phases,
- never infer placement from current file cursor.

## Piece Model

First slice uses fixed durability pieces for journal progress even when no
checksum is available:

```rust
pub struct PieceState {
    pub id: PieceId,
    pub start: u64,
    pub end: u64,
    pub written: RangeSet,
    pub provisional: BTreeMap<LeaseId, RangeSet>,
    pub status: PieceStatus,
    pub verified: VerificationState,
}

pub enum PieceStatus {
    Pending,
    InFlight,
    Written,
    Verified,
    Durable,
}
```

Piece length default is registry-controlled and may derive from
`min-split-size`, total length, and durability profile. Exact default can be
finalized during implementation, but it must be persisted in the journal.

No piece is durable until:

- all byte spans belong to committed leases,
- disk write acknowledgements match exact offsets and lengths,
- checksum/validator requirements for the piece are satisfied,
- the data-before-journal durability barrier for the selected mode completes,
- the serialized journal appender commits and flushes `PieceDurable`.

Logical file length, preallocation state, and readable zero-filled holes are not
evidence that any byte span was received. Only committed lease spans and
durable-piece records contribute to completion.

## StorageEngine API

```rust
pub enum StorageCommand {
    OpenLayout(FileLayout),
    BeginLease(LeaseWritePlan),
    WriteBlock(WriteBlock),
    CommitLease(LeaseCommit),
    AbortLease {
        task: TaskId,
        generation: Generation,
        lease: LeaseId,
        reason: LeaseAbortReason,
    },
    MarkVerified { piece: PieceId, generation: Generation },
    FsyncPieceGroup { up_to: PieceId },
    Finalize,
    CancelGeneration { generation: Generation },
}

pub struct WriteBlock {
    pub task: TaskId,
    pub generation: Generation,
    pub lease: LeaseId,
    pub global_offset: u64,
    pub expected_len: usize,
    pub buffer: BufferLease,
    pub piece: PieceId,
}

pub struct LeaseWritePlan {
    pub task: TaskId,
    pub generation: Generation,
    pub lease: LeaseId,
    pub span: GlobalSpan,
    pub validator: ValidatorFingerprint,
}

pub struct LeaseCommit {
    pub task: TaskId,
    pub generation: Generation,
    pub lease: LeaseId,
    pub received_len: u64,
    pub validator: ValidatorFingerprint,
    pub digest: Option<ValidatedDigest>,
}

pub enum WriteAck {
    ProvisionalAccepted { lease: LeaseId, span: GlobalSpan },
    LeaseCommitted { lease: LeaseId, span: GlobalSpan },
    LeaseAborted { lease: LeaseId },
    PieceDurable { piece: PieceId, sequence: u64 },
    Rejected { lease: Option<LeaseId>, error: WriteReject },
}
```

Every range or sequential response attempt has a unique `LeaseId` within its
task generation. `BeginLease` freezes the expected span and validator before
body bytes are accepted. A `WriteBlock` may change the physical output file,
but its span remains provisional and is indexed under that lease. The protocol
validator may issue `CommitLease` only after response framing proves the exact
body length and all required validator/digest checks pass. `StorageEngine`
rechecks the commit against the frozen plan and complete disk acknowledgements.

`AbortLease` removes all provisional visibility for the attempt. Bytes already
written may remain physically present, but they do not enter `written`, are not
replayed as progress, and may be overwritten in the same generation. A crash
has the same effect on every begun but uncommitted lease. Overlapping/endgame
attempts are arbitrated atomically by `CommitLease`: the first eligible commit
wins the span; all losing attempts are aborted before any durable-piece event.

Validation order:

1. task, generation, and active `LeaseId` match,
2. write span is contained in the lease's frozen span,
3. buffer length equals `expected_len`,
4. global span does not overflow,
5. span maps inside layout,
6. target piece matches span,
7. duplicate/overlap policy allows a provisional write,
8. task is not cancelled/removed,
9. disk queue budget is available.

Rejected writes return or quarantine the buffer. The canonical `BufferLease`
type and its state machine are defined in `detailed-runtime.md`; storage never
creates a second payload-buffer representation.

## Hash Failure And Retry

A required checksum is evaluated before `PieceDurable`. On mismatch:

1. abort every uncommitted lease touching the verification piece,
2. append `PieceFailed` through the journal appender,
3. clear the piece's committed `written` ranges, verification state, and any
   provisional state,
4. return the whole verification range to `Pending`, and
5. retry it in place in the same generation, subject to the
   `HashMismatch` retry cap and mirror-selection policy.

No `PieceDurable` is appended for the failed bytes, even if they were already
flushed by a strict or balanced data barrier. They are safe to overwrite because
the journal has not committed them as durable. A mismatch discovered while
revalidating a previously durable piece starts a new recovery generation before
`PieceFailed` clears that piece; replay therefore never rewrites the meaning of
an older generation in place.

## Disk Open Modes

```rust
CreateNew
OpenExistingNoTruncate
OpenForRestartTruncate
OpenTempForFinalRename
```

Rules:

- resume uses `OpenExistingNoTruncate`,
- full restart requires explicit new generation and overwrite/restart policy,
- existing unrelated files are protected by default,
- `remove-control-file` plus `allow-overwrite=true` can force fresh start only
  through explicit restart/truncate mode.

`OpenForRestartTruncate` and allocation mode `trunc` change logical file length;
they do not establish written or durable ranges. Whether the filesystem backs
the resulting unwritten area with sparse extents, zero-filled allocation, or
eager physical blocks is platform/backend-specific and must not affect recovery.

## Control Journal Format

This is the normative journal spec. `security-recovery.md` and
`session-persistence.md` describe recovery reconciliation and the surrounding
session store over this format and must not restate a divergent field list.

All multi-byte fields are fixed little-endian. The `endianness` byte is a
sanity assertion checked after read, not a switch that reinterprets earlier
fields; a reader never has to know byte order before parsing `version`.

Each journal consists of one or more immutable-numbered segments. All segments
for a task share `journal_id`; `sequence` is global across the segment set.

Segment header:

```text
magic: [u8; 4] = "ARXJ"
version: u16 = 1
endianness: u8 = 1  (assertion only; format is fixed little-endian)
flags: u8 = 0       (version 1 rejects unknown bits)
task_gid: u64
journal_id: [u8; 16]
segment_index: u32
first_sequence: u64
starting_generation: u64
previous_segment_last_sequence: u64  (zero for segment 0)
previous_segment_hash: [u8; 32]       (zero for segment 0)
created_at_unix_ms: u64
header_crc: u32     (covers all header bytes before it)
```

All `*_crc` fields are CRC-32C (Castagnoli); the polynomial choice is part of
the version-1 format.

Record:

```text
record_magic: [u8; 4] = "ARXR"
record_len: u32     (payload length; must be <= 16 MiB)
record_type: u16
record_flags: u16 = 0  (version 1 rejects unknown bits)
generation: u64
sequence: u64
payload: [u8]
record_crc: u32     (covers record framing + payload: record_magic..payload)
commit: [u8; 4] = "CMIT"  (written after record_crc)
```

Torn-write rules:

- `record_crc` covers the full framing (`record_magic`, `record_len`,
  `record_type`, `record_flags`, `generation`, `sequence`) plus `payload`, so a
  torn or garbage framing field is detected, not just a corrupt payload.
- `record_len` is bounds-checked against the version-1 16 MiB maximum before
  any payload read, so a corrupt length cannot drive an over-read.
- `commit` is written only after all preceding bytes of that record have been
  written successfully. It is a torn-record delimiter, not by itself a flush
  guarantee. The durability-mode barrier determines when the appender may
  report that the record is persistent.

### Payload Encoding

Payloads use the following versioned primitives:

```text
Id               u64
Span             offset:u64, len:u64
Hash32           [u8; 32]
OptionalId       present:u8, value:u64 when present
OptionalU64      present:u8, value:u64 when present
Bytes            len:u32, data:[u8; len]
Digest           algorithm:Bytes, value:Bytes
OptionalDigest   present:u8, Digest when present
OptionMap        count:u32, repeated key:Bytes/value:Bytes sorted by key
FileLayoutEntry  file_id:Id, global_start:u64, global_end:u64, length:u64,
                 selected:u8, safe_relative_path:Bytes
```

Strings are UTF-8. Boolean and enum values are `u8` unless a field says
otherwise. Decoders reject invalid tags, duplicate `OptionMap` keys, unsorted
keys, non-canonical lengths, invalid UTF-8, and trailing payload bytes.
`Hash32` is SHA-256 over domain-separated canonical bytes; the domain tag is
fixed by each field (`layout`, `options`, `validator`, or `segment`) so hashes
from different namespaces cannot be substituted.

Record types (first slice; the number is the version-1 `record_type` value):

```text
1   TaskCreated
2   OptionsSnapshot
3   LayoutCommitted
4   GenerationStarted   (advances the generation; the only record that does)
5   LeaseStarted        (begins a provisional response attempt)
6   PieceStarted        (associates a piece span with that attempt)
7   PieceWritten
8   LeaseCommitted
9   LeaseAborted
10  PieceVerified
11  PieceFailed
12  PieceDurable
13  RetryState
14  TaskPaused
15  TaskComplete
16  TaskError
17  TaskRemoved
18  CleanShutdown
```

The payload of every first-version record is normative:

| Record | Payload fields, in order |
| --- | --- |
| `TaskCreated` | `durability:u8`, `creator_version:u16` |
| `OptionsSnapshot` | `snapshot_hash:Hash32`, `options:OptionMap`; secret-valued entries are forbidden |
| `LayoutCommitted` | `layout_hash:Hash32`, `total_length:OptionalU64`, `piece_length:u64`, `file_count:u32`, repeated `FileLayoutEntry` |
| `GenerationStarted` | `previous_generation:u64`, `reason:u8` |
| `LeaseStarted` | `lease_id:Id`, `span:Span`, `validator_fingerprint:Hash32` |
| `PieceStarted` | `lease_id:Id`, `piece_id:Id`, `piece_span:Span` |
| `PieceWritten` | `lease_id:Id`, `piece_id:Id`, `written_span:Span` |
| `LeaseCommitted` | `lease_id:Id`, `span:Span`, `validator_fingerprint:Hash32`, `response_digest:OptionalDigest` |
| `LeaseAborted` | `lease_id:Id`, `reason:u8` |
| `PieceVerified` | `piece_id:Id`, `piece_span:Span`, `contributors_hash:Hash32`, `digest:Digest` |
| `PieceFailed` | `lease_id:OptionalId`, `piece_id:Id`, `piece_span:Span`, `error_class:u8`, `attempt:u32` |
| `PieceDurable` | `piece_id:Id`, `piece_span:Span`, `contributors_hash:Hash32`, `validator_set_fingerprint:Hash32`, `digest:OptionalDigest`, `data_barrier:u8` |
| `RetryState` | `scope:u8`, `scope_id:Id`, `attempt:u32`, `next_retry_unix_ms:u64`, `error_class:u8` |
| `TaskPaused` | `reason:u8` |
| `TaskComplete` | `layout_hash:Hash32`, `final_length:u64`, `final_digest:OptionalDigest`, `completed_at_unix_ms:u64` |
| `TaskError` | `error_class:u8`, `retriable:u8`, `diagnostic_id:u64` |
| `TaskRemoved` | `reason:u8` |
| `CleanShutdown` | `checkpoint_sequence:u64`, `shutdown_at_unix_ms:u64` |

`validator_fingerprint` is a fixed hash of the canonical validator tuple, not a
raw cookie, credential, or header block. `contributors_hash` covers the sorted
committed `(LeaseId, Span, validator_fingerprint)` tuples that supplied a piece;
`validator_set_fingerprint` covers their canonical distinct validator set. Thus
a verification piece assembled from multiple leases does not pretend to have a
single source lease. `OptionsSnapshot` contains only the sanitized,
generation-scoped options needed to reproduce layout, verification, and
recovery decisions. Sensitive values are never legal journal payloads.

Only `GenerationStarted` advances the generation. All other records use the
current generation. A `LeaseStarted` without `LeaseCommitted`, and any
`PieceStarted`/`PieceWritten` without a matching durable result, is provisional
and resets to pending during recovery. `LeaseCommitted` makes a span logically
accepted, but only `PieceDurable` makes a complete piece durable across restart.

### Appender Ownership And Rotation

Each task has exactly one serialized `ControlJournalAppender`. It alone assigns
the next gap-free `sequence`, encodes records, rotates segments, and performs
journal flushes. Storage/disk/hash completions submit typed facts to this
appender, and the scheduler-side persistence coordinator submits control facts
(`TaskCreated`, `OptionsSnapshot`, `GenerationStarted`, `RetryState`,
`TaskPaused`, `TaskComplete`, `TaskError`, `TaskRemoved`, `CleanShutdown`)
through the same appender; the scheduler and protocol adapters never append
records themselves.
If an append fails after a sequence is assigned, the appender faults the task
and emits no later sequence until recovery repairs or starts a new segment.
Sequence numbering begins at 1; segment 0 therefore has `first_sequence = 1`.

The appender may batch facts, but it reports two distinct acknowledgements:
`Appended(sequence)` means the record bytes are in the active segment, while
`Flushed(sequence)` means the required journal barrier has completed. A
`PieceDurable` `WriteAck` is emitted only from `Flushed`, never merely from
`Appended`. An explicit `flush(up_to_sequence)` is awaitable for strict pieces,
balanced group checkpoints, finalization, pause/remove checkpoints, and clean
shutdown.

`BeginLease`, provisional disk completions, `CommitLease`, and `AbortLease`
produce `LeaseStarted`, `PieceWritten`, `LeaseCommitted`, and `LeaseAborted`
facts through this same appender. `LeaseCommitted` acknowledgement requires the
record to be accepted as `Appended`, but not flushed; if a crash loses that tail,
recovery safely treats the physical bytes as pending. A journal append failure
rejects/faults the transaction rather than allowing an unrecorded commit to win
endgame arbitration.

Rotation happens only after a flushed record boundary. The appender syncs and
closes the old segment, hashes its valid bytes, creates the next segment under a
temporary name with `segment_index + 1`, `first_sequence = last_sequence + 1`,
the current generation, and the previous segment's last-sequence/hash link,
then syncs and atomically installs the new segment, syncs its parent directory
where supported, and only then appends to it. Old segments remain immutable and
are not removed by rotation. Journal compaction and segment retirement are
outside the first format version.

Recovery orders segments by `segment_index` and verifies task id, journal id,
header CRC, previous-segment link, `first_sequence`, and global sequence
continuity. A missing segment, bad link, or record gap ends the valid global
prefix; newer segments are ignored. Sequence numbering never restarts at a
rotation boundary.

Replay stops at the first invalid magic, oversized length, bad CRC, missing
commit sentinel, or global sequence gap. The valid prefix is authoritative.

## Write And Journal Ordering

Balanced durability:

```text
write_at -> disk outcome -> provisional range
CommitLease -> update committed written range -> required hash/verify
collect complete verified pieces until interval/group boundary
sync_data every data file touched by the group
append PieceDurable records through the single appender
sync_all active journal segment once for the group
emit PieceDurable acks for the flushed records
```

Strict durability:

```text
write_at -> disk outcome -> CommitLease -> verify
sync_all data file(s) for the piece
append PieceDurable -> sync_all journal -> emit PieceDurable ack
```

Fast durability:

```text
write_at -> disk outcome -> CommitLease -> optional verify
append/batch PieceWritten, LeaseCommitted, and PieceVerified only
keep piece operationally complete but provisional across restart
```

The hard ordering invariant is: **no `PieceDurable` record may be appended until
the described data has completed at least the mode's required data flush**.
Journal flushing cannot substitute for flushing a different data file.

Balanced and strict differ by flush frequency and metadata guarantees, not by a
portable claim that one syscall is always faster. Balanced batches verified
pieces, uses `sync_data` for the touched data files, and uses `sync_all` for the
append-growing journal segment. Strict performs per-piece `sync_all` on both
data and journal. These primitives may have similar or very different costs on
different filesystems; Phase 0 benchmarks select group sizes, not correctness
ordering.

Fast mode never writes `PieceDurable` during ordinary progress. After a crash,
its written/verified records are hints: checksummed pieces require readback and
rehash, and pieces without a content checksum return to pending. At successful
finalization, fast mode performs a data flush, appends `PieceDurable` for every
piece that passes final verification, flushes the journal, and only then appends
and flushes `TaskComplete`.

## Recovery

Startup:

1. read SQLite queue membership and locate the task's segment set,
2. validate and replay the journal's global valid prefix,
3. take generation, layout, validator snapshot, lease/piece progress, and task
   completion from that prefix,
4. use SQLite only for its authoritative queue/cross-task fields and reconcile
   any duplicated journal-owned fields from journal to SQLite,
5. reset begun/aborted/uncommitted leases and every non-durable piece to pending,
6. re-read and rehash fast-mode hints where a checksum can prove them; otherwise
   redownload them,
7. verify that each journal-durable span is readable and inside the recorded
   layout,
8. if persistence omitted an authenticated source or required credential, retain
   all verified/durable pieces but create the non-terminal task in
   `NeedsCredentials`; it cannot issue a new lease until the caller supplies a
   replacement source or credentials,
9. otherwise create the scheduler task in a new generation, subject to
   SQLite's desired pause/queue state. A journal terminal marker still vetoes
   reactivation regardless of that SQLite state.

The journal is authoritative for whether output bytes may count as progress:
bytes absent from the journal cannot become complete because file length or
allocation happens to cover them. A journal record also cannot manufacture
missing bytes; if the output is absent, shorter than a durable span, unreadable,
or fails a required digest, recovery downgrades/fails that piece according to
the corruption policy and records the reset in the new generation. Unknown
bytes are never counted complete by logical length, physical allocation, or
zero-filled reads alone.

## Finalization

Completion flow:

```text
all pieces durable
  -> final full checksum if configured
  -> fsync data according to durability
  -> rename temp to final if temp naming is active
  -> fsync parent directory where supported
  -> append TaskComplete through ControlJournalAppender
  -> flush journal through TaskComplete
  -> persist stopped result
  -> publish terminal Complete snapshot
  -> optionally remove companion control file
```

`TaskComplete` records task-local file completion. User-visible completion
requires both `TaskComplete` and the global stopped result transaction. Crash
recovery may recreate a missing stopped result from a valid `TaskComplete`, but
it must not trust a stopped result without a matching journal completion or
fresh finalization verification.

## Tests

Required tests:

- path traversal corpus across Unix/Windows forms,
- `${out}` and Content-Disposition path sanitization,
- global offset overflow rejection,
- stale generation write rejection,
- duplicate write policy,
- exact-length commit, short/oversized abort, stale validator abort, and
  first-eligible endgame `CommitLease` arbitration,
- crash after provisional write and after `LeaseCommitted` but before
  `PieceDurable` resets the affected range to pending,
- hash mismatch appends `PieceFailed`, clears the whole verification range, and
  permits same-generation overwrite,
- resume open does not truncate,
- `200 OK` restart path cannot write at nonzero offset,
- torn journal tail replay,
- corrupted journal CRC replay,
- duplicate/gapped sequence rejection and out-of-order disk completions through
  the single sequence-assigning appender,
- segment rotation, missing/reordered segment, and previous-segment hash-link
  rejection,
- crash after disk write before journal,
- crash after balanced data sync but before journal sync,
- crash after journal bytes but before the required data barrier is never
  constructible through the appender API,
- crash after journal before final rename,
- disk-full write rejection returns/quarantines buffer,
- `trunc` logical length and zero-filled reads never create completed ranges,
- completion requires stopped result persistence.
