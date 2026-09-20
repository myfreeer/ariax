# Detailed Storage And Journal Design

Phase 5 adds immutable verification manifests before any dependent lease.
Required record numbers 27/28 carry a canonical manifest and bounded
continuations; 29 binds FTP/SFTP source validators; 30 records successful
whole-file verification. Existing record numbers and v1 framing retain their
meanings. Manifests bind length, verification geometry and every required
digest. A missing, incomplete or mismatched manifest cannot authorize recovery
or network writes. Contributors cover the complete chunk without overlap;
`PieceFailed` invalidates its contributor set as well as verification evidence.
Implementation and acceptance are tracked in `detailed-protocol-transfers.md`.

Required records 31/32 carry `HostKeyState` and `MetadataComplete`. A trust
decision must match an earlier pending challenge in the drained generation;
a generic pause cannot authorize a pin. Metadata completion binds the original
option snapshot, document digest and ordered children, and requires the
recorded layout when XML is retained. Invalid transitions stop semantic replay
at the preceding valid record.

Managed HTTP workers keep the journal appender on the native session owner
throughout a transfer. Storage appends and flushes use its bounded typed command
queue, so an active option snapshot and piece evidence share one serialized
sequence and one fault latch. The worker retains a command handle, not a second
file writer. Control mutations stage their complete snapshot while network I/O
is active; acknowledgement follows journal flush and the exact SQLite mirror
update. Cancellation drains storage before generation promotion. Completion
leaves the flushed appender installed for scheduler persistence and shutdown.
Standalone storage and pinned transfers retain their local single writer.
An uncertain accepted append is never retried.

Status: first-slice implementation in progress. Portable NFC path validation,
persisted root/file identity binding, immutable layout hashing, and global
offset mapping are implemented. Journal v1 segment/record framing, CRC-32C and
commit validation, linked rotation, bounded replay, and valid-prefix recovery
are executable. Exact typed payload codecs cover all 32 v1 records, including
bounded option maps, chunked layouts, finalization paths, checkpoint
`PieceStateChunk` bitmaps/evidence runs, and the bounded `HttpStrongValidator`
and `HttpRangeIdentity` payloads. Policy-gated cross-record recovery now
verifies snapshot/contributor/state hashes, generation promotion, reassembled
layout/root hashes, lease/piece evidence, finalization pairs, and whole compact
checkpoints. The native-startup handoff and ordering contract, exact Unix and
Windows native-identity codecs, descriptor-safe root/journal capabilities, and
identity-revalidated journal recovery are now executable. General multi-file
data-file execution and compaction writing remain pending. The file-backed
serialized appender now provides gap-free typed append, explicit flush
acknowledgement, latched failure, descriptor-relative tail/content-validated
final-segment reopen after exact named-file preflight, and flushed-boundary
rotation without whole-segment buffering. The single-file HTTP path reopens an
identity-bound descriptor without truncation, reads back trusted durable pieces
before resume, and preserves a bounded strong ETag plus resource/length binding
for strict range continuation.

The SQLite side of checkpoint installation now has transactional
`installing`/`installed` pointer primitives with identity tokens, old-pointer
revalidation, and startup pointer/phase validation. Its executable boundary
also includes private path/artifact enforcement, cooperative Ariax-only
single-writer locking, hot rollback page-one plus committed-WAL version
preflight, journal-mode write probes, and validated file-synced no-clobber
backups with descriptor-bound publication-residue recovery, no-clobber race
preservation, and crash/unlink fault coverage. Session schema v2 adds the
demoted queue, independent slow-demotion
count, and bounded slow retry decision; exact v1 stores migrate transactionally
after semantic preflight and a private no-clobber backup. Retained stopped
results now use a one-to-one stopped-task/result transaction, and deletion
atomically removes both metadata rows while densifying the stopped queue. The
dedicated session owner, challenge-bound host-key operations, and bounded pure
cross-store reconciliation planning are executable. The ordered SQLite startup
repair stage is also executable through the bounded owner. The engine now exposes
a native-startup backend contract and coordinator that consumes the post-repair
handoff, resolves journal-install intents before appenders, and publishes the
restored scheduler last. Its concrete central-journal Unix and Windows adapters
now acquire roots, revalidate selected file identities, resolve install
candidates, prepare appenders without mutation, and move final tail repair and
appender construction onto the session owner. A first concrete single-file
`StorageEngine` now consumes descriptor-backed files, validates piece-aligned
leases and contiguous blocks, submits bounded pooled buffers to the positional
disk lane, hashes returned immutable buffers, orders strict data flushes before
`PieceDurable`, and flushes terminal journal state. HTTP recovery revalidates
descriptor identity and reads back durable pieces before resuming. Bounded
cancellation-fenced overlap groups and explicit path/geometry restart mutations
are executable. General multi-file writes, balanced/fast durability grouping,
checkpoint state writing and native release-matrix evidence remain pending.

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
    pub root: Arc<CanonicalRoot>,
    pub relative: SafeRelativePath,
    pub display_full: PathBuf,
}

pub struct CanonicalRoot {
    pub display: PathBuf,
    pub identity: RootIdentity,
    capability: RootDirectoryCapability,
}

pub enum PathError {
    InvalidRoot,
    OutsideAllowedRoot,
    InvalidComponent,
    UnsafeExistingPath,
    SafeOpenUnavailable,
    CreateFailed,
}

// Compatibility name for prose that predates the explicit API signature.
pub type SafePath = SafePathOutput;
```

Algorithm:

1. open and canonicalize the existing output root as a directory capability,
   recording its stable platform identity,
2. verify root is under `allowed-output-root` if configured,
3. validate each path component,
4. reject absolute paths, prefixes, separators, NUL/control, `.` and `..`,
5. apply Windows reserved-name and trailing-dot/space checks,
6. create/open parent directories stepwise relative to the retained root/parent
   capabilities with no-follow/reparse-point rejection,
7. return only the capability-rooted relative target; the disk backend opens or
   renames the final component relative to that capability and verifies the
   opened identity before any write.

`build` is the only API that converts output options or metadata components into
a filesystem target. No raw `PathBuf::join` on metadata is allowed outside this
builder. `display_full` is diagnostics/UI data only and is never filesystem
authority. Protocol/storage code must pass `root` plus `relative` to
`DiskBackend::open_safe`/`rename_safe`; reopening `display_full` would reintroduce
a check/use race and is forbidden.

`RootDirectoryCapability` is process-local and is not serialized. The journal
persists the root identity/display path and safe relative components. Recovery
reopens the configured root, verifies its recorded identity and allowed-root
policy, and reconstructs the capability before any descendant is accessed. A
supported production backend that cannot provide race-resistant no-follow
opening fails with `SafeOpenUnavailable`; it does not silently downgrade to a
check-then-open path for an untrusted metadata-derived target.

Native identities use the exact `NativeIdentityV1` byte codec. Byte zero is
version `1`; byte one is the platform tag (`1` for Unix, `2` for Windows).
The Unix payload is `st_dev:u64le || st_ino:u64le`. The Windows payload is
`volume_serial:u64le || FILE_ID_128`. Unix identities are therefore exactly
18 bytes and Windows identities exactly 26 bytes. Unknown versions, wrong
lengths, and identities from another platform fail closed. `RootIdentity` and
`FileIdentity` use the same codec; object kind, link count, and containment are
verified separately from the opened descriptor or handle.
Regular-file link counts are widened to `u64` at the native boundary; Unix
`nlink_t` widths differ across Linux and macOS. Missing entries and non-regular
objects are rejected before their counts can authorize publication recovery.

On Windows, an absolute capability open first acquires only the drive-volume or
UNC-share anchor with `CreateFileW`, because applying `OBJ_DONT_REPARSE` to a
DOS absolute name would reject the object-manager drive mapping itself. Every
filesystem component below that anchor is then opened relative to the retained
parent with `NtCreateFile`, `OBJ_DONT_REPARSE`, and
`FILE_OPEN_REPARSE_POINT`; the returned handle is rejected if its attributes
identify a reparse point. The final retained handle, not the mutable display
path or ancestor namespace, is authority after traversal.

Central control journals have a distinct retained `JournalDirectoryCapability`
for the configured control-file directory; an output-root capability is never
treated as authority for a central journal path. Startup acquires the central
capability and every task root before it changes an install row or removes a
candidate. Journal recovery produces a move-only `PreparedJournalSet` holding
the securely opened directory/segment objects, exact relative segment names,
opened identities, bounded replay result, and any permitted final-tail repair.
The session-owner thread consumes that value and constructs the live appender.
The appender retains the directory capability so descriptor-budget reopen,
rotation, synchronization, and retirement never reopen an ambient absolute
path.

## FileLayout

For first-slice HTTP single-file downloads:

```rust
pub struct FileLayout {
    pub task: TaskId,
    pub generation: Generation,
    pub root: Arc<CanonicalRoot>,
    pub files: Vec<FileEntry>,
    pub total_length: Option<u64>,
    pub piece_length: u64,
    pub layout_hash: LayoutHash,
}

pub struct FileEntry {
    pub id: FileId,
    pub safe_path: SafePathOutput,
    pub identity: Option<FileIdentity>,
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
- every selected existing/created target is opened through its safe capability
  before `LayoutCommitted`; its platform file identity is recorded in the
  binding. An unselected/nonexistent entry carries no identity until a later
  generation selects and opens it,
- one task has at most 262,144 layout entries and at most 64 MiB of canonical
  encoded layout data. Variable layout/source/option metadata is charged to
  `task_metadata_budget`; exceeding either the per-task cap or the global
  reservation fails with `ResourceLimit` before a worker starts,
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
checksum is available. The resident representation is compact and sparse:

```rust
pub struct PieceBook {
    pub piece_length: u64,
    pub piece_count: u64,
    pub durable: PackedBitmap,
    pub verified: PackedBitmap,
    pub active: BTreeMap<PieceId, ActivePieceState>,
}

pub struct ActivePieceState {
    pub written: RangeSet,
    pub provisional: BTreeMap<LeaseId, RangeSet>,
    pub verification: VerificationState,
}

pub enum PieceStatus {
    Pending,
    InFlight,
    Written,
    Verified,
    Durable,
}
```

`Pending` is implicit, and `InFlight`/`Written` are derived from the sparse
`active` entry. There is no heap allocation, `BTreeMap`, or full `PieceState`
for every piece. Admission reserves the packed bitmap bytes plus a bounded
sparse-active allowance from the process `piece_metadata_budget`; the number of
active entries is bounded by leased spans and hash-reorder limits. If the exact
layout cannot fit its metadata reservation, admission fails with a typed
resource-limit error rather than allocating outside the resident-memory gate.

For HTTP/FTP without metadata-defined verification pieces, the exact default
`piece-length` is **1 MiB**. An explicit valid `piece-length` overrides it.
Metalink piece hashes define the verification/durable piece length and ignore
the generic option; BitTorrent uses its metadata-owned piece length inside the
BT adapter. `min-split-size` and adaptive profile choices size network leases,
not durability pieces. The resolved piece length and count are persisted in the
journal and cannot change inside one generation.

On recovery, a configured piece length that differs from the journal fails by
default (`allow-piece-length-change=false`). With the explicit compatibility
option enabled, recovery starts a new generation and retains only spans that can
be re-established as complete under the new boundaries by existing content
checks or bounded readback; every other affected span returns to pending. It
never reinterprets old bitmap indexes as new pieces.

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
    pub transfer_attempt: TransferAttemptId,
    pub lease: LeaseId,
    pub span: GlobalSpan,
    pub validator: ValidatorFingerprint,
    pub overlap_group: Option<OverlapGroupId>,
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
    LeaseCommitPending { lease: LeaseId, group: OverlapGroupId },
    LeaseCommitted { lease: LeaseId, span: GlobalSpan },
    LeaseAborted { lease: LeaseId },
    SpanRolledBack { group: OverlapGroupId, span: GlobalSpan },
    PieceDurable { piece: PieceId, sequence: u64 },
    Rejected { lease: Option<LeaseId>, error: WriteReject },
}
```

Every protocol response/data stream has a unique `TransferAttemptId` within its
task generation. A range response normally owns one `LeaseId`. A sequential
HTTP/FTP response advances through a series of piece-aligned `LeaseId`s while
the same transport stream remains open; rotating a storage lease never creates
a new request or buffers a whole piece. `BeginLease` freezes each storage span
and validator before bytes for that span are accepted.

`ValidatorFingerprint` is the `Hash32` over the canonical validator tuple
defined under Payload Encoding; `ValidatedDigest` is a `Digest` whose value the
protocol validator has already checked against the received body. Reason enums
(`LeaseAbortReason` and similar) are closed sets finalized with
`error_codes.json` in Phase 0; each variant maps to one `u8` journal `reason`
value. A `WriteBlock` may change the physical output file, but its span remains
provisional and is indexed under that lease. A range lease commits after exact
response framing/validator checks. A non-final sequential checkpoint lease may
commit after its exact span is written under the already validated response
head; the final lease additionally requires exact response EOF/framing. A later
whole-representation digest failure invalidates the affected verification state
through the normal hash-failure/new-generation rules. `StorageEngine` rechecks
each commit against the frozen plan and complete disk acknowledgements.

`AbortLease` removes all provisional visibility for the attempt. Bytes already
written may remain physically present, but they do not enter trusted progress,
are not replayed as downloaded, and may be overwritten in the same generation.
A crash has the same effect on every begun but uncommitted lease.

An endgame overlap group uses conservative metadata rollback rather than
physical byte rollback:

1. The first exact-length, validator/digest-valid `CommitLease` becomes the
   in-memory commit candidate and receives `LeaseCommitPending`; no
   `LeaseCommitted` record or acknowledgement exists yet.
2. Storage freezes the group, rejects new writes, cancels every competing lease,
   and drains or cancellation-confirms all already accepted disk operations.
3. If no non-candidate write completed and none remains cancellation-uncertain,
   storage appends the loser aborts and then the candidate `LeaseCommitted`; the
   span may proceed toward verification/durability.
4. If any non-candidate write completed, any member failed validation after
   writing, or cancellation remains uncertain, storage aborts every group member
   including the candidate. It clears in-memory written/verified state for every
   touched verification piece, returns those pieces to `Pending`, and emits
   `SpanRolledBack` only after the group is fenced.
5. Rollback never restores or zeroes physical bytes. They are treated like
   preallocated/undefined file contents and the next ordinary lease overwrites
   the pending pieces before they can become durable.

This deliberately gives up otherwise valid progress when overlapping write
ownership is ambiguous. It keeps the baseline simple: no scratch file, undo log,
or attempt-sized retained buffer is required. Endgame never touches a previously
durable piece, and no `PieceDurable` event is allowed before its overlap group
has settled.

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

For the executable HTTP resume path, recovery retains the descriptor-bound
selected file and verifies its exact length before admitting the appender.
`RootFileCapability::read_exact_at` performs bounded positional readback through
that retained descriptor; every recovered durable piece must match its persisted
SHA-256 evidence before a network request is sent. A recovered strong ETag is
usable only when its resource fingerprint and settled total length match the
replayed layout. The validator record is ordered after layout and before any
network lease, and a duplicate or mismatched lease fingerprint fails replay.
When strict HTTP admission instead relies on the bounded shared-range SHA-256
profile, one `HttpRangeIdentity` binds the settled probe digest and exact total
length before any lease. Replay recomputes its domain-separated fingerprint,
rejects coexistence with `HttpStrongValidator`, and requires each recovered
lease to use that fingerprint. Network recovery then reprobes matching sources
and re-fetches every locally verified durable range before releasing pending
work, as specified by `detailed-http-first-slice.md`.

## Control Journal Format

This is the normative journal spec. `security-recovery.md` and
`session-persistence.md` describe recovery reconciliation and the surrounding
session store over this format and must not restate a divergent field list.

All multi-byte fields are fixed little-endian. The `endianness` byte is a
sanity assertion checked after read, not a switch that reinterprets earlier
fields; a reader never has to know byte order before parsing `version`.

Each journal consists of one or more immutable-numbered segments. All segments
of one set share `journal_id`; `sequence` is global across the segment set. A
task has exactly one installed set — a compaction candidate carries a fresh
`journal_id` until the Checkpoint Compaction install protocol makes it the
installed set.

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
PlatformPath     platform:u8, len:u32, native canonical path bytes
OptionalId       present:u8, value:u64 when present
OptionalU64      present:u8, value:u64 when present
Bytes            len:u32, data:[u8; len]
Digest           algorithm:Bytes, value:Bytes
OptionalDigest   present:u8, Digest when present
OptionMap        count:u32, repeated key:Bytes/value:Bytes sorted by key
FileLayoutEntry  file_id:Id, global_start:u64, global_end:u64, length:u64,
                 selected:u8, safe_relative_path:Bytes, file_identity:Bytes
DurableEvidenceRun first_piece_delta:u32, piece_count:u32,
                 validator_set_fingerprint:Hash32, digest_algorithm:Bytes,
                 digest_value_len:u16, digest_values:Bytes
```

The original version-1 enum tags below are 1-based and closed; zero and unlisted
values are invalid. The executable mapping is generated in
`generated/journal_v1.json`. The new `HostKeyState.decision` field explicitly
uses `0=pending`, `1=approved`, and `2=rejected`:

- `durability`: `1=fast`, `2=balanced`, `3=strict`;
- `OptionsSnapshot.scope`: `1=current_generation`, `2=next_admission`;
- `GenerationStarted.reason`: `1=option_patch`, `2=retry_readmission`,
  `3=representation_restart`, `4=backend_failover`, `5=root_rebind`,
  `6=recovery_repair`, `7=explicit_restart`;
- `LeaseAborted.reason`: `1=cancelled`, `2=redirect`, `3=short_body`,
  `4=oversized_body`, `5=invalid_range`, `6=stale_validator`,
  `7=digest_mismatch`, `8=storage_rejected`, `9=overlap_lost`,
  `10=overlap_uncertain`, `11=retry`, `12=generation_drain`;
- `PieceDurable.data_barrier`: `1=balanced_group`, `2=strict_piece`,
  `3=fast_finalization`, `4=recovery_readback`;
- `RetryState.scope`: `1=task`, `2=uri`, `3=span`, `4=piece`;
- `RetryState.retry_reason`: `1=backoff`, `2=retry_after`,
  `3=policy_clamp`;
- `TaskPaused.reason`: `1=user`, `2=no_space`, `3=slow_slot`,
  `4=host_key_approval`, `5=restarting`, `6=recovery_hold`;
- `TaskRemoved.reason`: `1=user`, `2=session_cleanup`, `3=replaced`.

Every `error_class` uses the stable 1-based `ErrorKind` number in
`generated/error_codes.json`; it is not an unrelated record-local enum.

Strings are UTF-8. `PlatformPath` is the explicit exception: Unix stores raw
path bytes and Windows stores canonical UTF-16LE code units, tagged by platform;
it is display/recovery-location data and is never reopened without the safe-root
binding check. Boolean and enum values are `u8` unless a field says
otherwise. Decoders reject invalid tags, duplicate `OptionMap` keys, unsorted
keys, non-canonical lengths, invalid UTF-8, and trailing payload bytes.
`Hash32` is SHA-256 over domain-separated canonical bytes; the domain tag is
fixed by each field (`layout`, `options`, `validator`, or `segment`) so hashes
from different namespaces cannot be substituted.

The executable v1 semantic domains are `ariax/options-snapshot/v1\0`,
`ariax/contributors/v1\0`, `ariax/validator-set/v1\0`,
`ariax/rebind-validator-set/v1\0`, and `ariax/checkpoint-state/v1\0`.
Option hashes cover the sorted map; contributor hashes cover the sorted
`(LeaseId, Span, validator_fingerprint)` tuples; validator-set hashes cover the
sorted distinct validator fingerprints. Different-identity rebind evidence
covers the prior/new root-binding hashes and exact piece digest.

Version-1 field caps are checked before allocation: `PlatformPath` and a safe
relative path are at most 64 KiB each, a platform identity is at most 256
bytes, a digest algorithm name is at most 32 bytes, and a digest value is at
most 64 bytes. `OptionMap` has at most 4096 entries, 256 bytes per key, 64 KiB
per value, and 4 MiB total canonical bytes. A layout has at most 262,144 files
and 64 MiB of canonical entries across its chunks. These are in addition to the
16 MiB single-record cap; counts and `count * element_size` products are
overflow-checked before reserving memory.

Record types (first slice; the number is the version-1 `record_type` value):

```text
1   TaskCreated
2   OptionsSnapshot
3   LayoutCommitted
4   GenerationStarted   (advances the generation; the only record that does)
5   LeaseStarted        (begins one provisional storage span)
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
19  CheckpointStart     (first record of a compaction checkpoint set)
20  CheckpointEnd       (validates the checkpoint state records)
21  LayoutChunk         (continuation of a large LayoutCommitted entry list)
22  FinalizeIntent      (declares the temp -> final rename about to happen)
23  FinalizeDone        (rename observed complete)
24  PieceStateChunk     (checkpoint-only compact durable-piece state)
25  HttpStrongValidator (strong ETag/resource binding for HTTP resume)
26  HttpRangeIdentity   (bounded digest/length binding for HTTP resume)
27  VerificationManifest (first part of the immutable checksum manifest)
28  VerificationManifestChunk (ordered manifest continuation)
29  ProtocolValidator   (source-local FTP/FTPS/SFTP resume evidence)
30  WholeFileVerified   (all required whole-file checksums matched)
31  HostKeyState        (exact pending challenge and trust decision)
32  MetadataComplete    (atomic Metalink parent/child expansion completed)
```

The payload of every first-version record is normative:

| Record | Payload fields, in order |
| --- | --- |
| `TaskCreated` | `durability:u8`, `creator_version:u16` |
| `OptionsSnapshot` | `scope:u8`, `patch_id:OptionalId`, `snapshot_hash:Hash32`, `options:OptionMap`; secret-valued entries are forbidden |
| `LayoutCommitted` | `layout_hash:Hash32`, `root_binding_hash:Hash32`, `root_display:PlatformPath`, `root_identity:Bytes`, `total_length:OptionalU64`, `piece_length:u64`, `total_file_count:u32`, `chunk_count:u32`, `inline_file_count:u32`, repeated inline `FileLayoutEntry` |
| `GenerationStarted` | `previous_generation:u64`, `reason:u8`, `next_snapshot_hash:Hash32`, `patch_id:OptionalId` |
| `LeaseStarted` | `transfer_attempt_id:Id`, `lease_id:Id`, `span:Span`, `validator_fingerprint:Hash32` |
| `PieceStarted` | `lease_id:Id`, `piece_id:Id`, `piece_span:Span` |
| `PieceWritten` | `lease_id:Id`, `piece_id:Id`, `written_span:Span` |
| `LeaseCommitted` | `lease_id:Id`, `span:Span`, `validator_fingerprint:Hash32`, `response_digest:OptionalDigest` |
| `LeaseAborted` | `lease_id:Id`, `reason:u8` |
| `PieceVerified` | `piece_id:Id`, `piece_span:Span`, `contributors_hash:Hash32`, `digest:Digest` |
| `PieceFailed` | `lease_id:OptionalId`, `piece_id:Id`, `piece_span:Span`, `error_class:u8`, `attempt:u32` |
| `PieceDurable` | `piece_id:Id`, `piece_span:Span`, `contributors_hash:Hash32`, `validator_set_fingerprint:Hash32`, `digest:OptionalDigest`, `data_barrier:u8` |
| `RetryState` | `scope:u8`, `scope_id:Id`, `attempt:u32`, `elapsed_before_wait_ms:u64`, `scheduled_at_unix_ms:u64`, `delay_ms:u64`, `error_class:u8`, `retry_reason:u8` |
| `TaskPaused` | `reason:u8` |
| `TaskComplete` | `layout_hash:Hash32`, `final_length:u64`, `final_digest:OptionalDigest`, `completed_at_unix_ms:u64` |
| `TaskError` | `error_class:u8`, `retriable:u8`, `diagnostic_id:u64` |
| `TaskRemoved` | `reason:u8` |
| `CleanShutdown` | `checkpoint_sequence:u64`, `shutdown_at_unix_ms:u64` |
| `CheckpointStart` | `checkpoint_id:[u8;16]`, `source_last_sequence:u64`, `source_segment_hash:Hash32`, `state_record_count:u32`, `created_at_unix_ms:u64` |
| `CheckpointEnd` | `checkpoint_id:[u8;16]`, `state_record_count:u32`, `state_hash:Hash32` |
| `LayoutChunk` | `layout_hash:Hash32`, `root_binding_hash:Hash32`, `chunk_index:u32`, `chunk_count:u32`, `file_count:u32`, repeated `FileLayoutEntry` |
| `FinalizeIntent` | `layout_hash:Hash32`, `root_binding_hash:Hash32`, `file_id:Id`, `temp_relative_path:Bytes`, `final_relative_path:Bytes`, `final_length:u64`, `file_identity:Bytes` |
| `FinalizeDone` | `layout_hash:Hash32`, `root_binding_hash:Hash32`, `file_id:Id`, `final_relative_path:Bytes` |
| `PieceStateChunk` | `layout_hash:Hash32`, `root_binding_hash:Hash32`, `chunk_index:u32`, `chunk_count:u32`, `first_piece_id:Id`, `covered_piece_count:u32`, `durable_bitmap:Bytes`, `evidence_run_count:u32`, repeated `DurableEvidenceRun` |
| `HttpStrongValidator` | `resource_fingerprint:Hash32`, `validator_fingerprint:Hash32`, `total_length:u64`, `etag:Bytes` (bounded strong ETag; exact bytes are retained for `If-Range`) |
| `HttpRangeIdentity` | `identity_fingerprint:Hash32`, `total_length:u64`, `representation_digest:Digest` (SHA-256 only; the fingerprint is domain-separated over the digest and exact length) |
| `VerificationManifest` | `fingerprint:Hash32`, `total_bytes:u32`, `chunk_count:u32`, `bytes:Bytes` |
| `VerificationManifestChunk` | `fingerprint:Hash32`, `chunk_index:u32`, `chunk_count:u32`, `bytes:Bytes` |
| `ProtocolValidator` | `protocol:u8`, `source:Hash32`, `total_length:u64`, `modified_unix_seconds:OptionalU64`, `host_key_present:u8`, optional `host_key:Hash32` |
| `WholeFileVerified` | `fingerprint:Hash32`, `digest_count:u8`, repeated `Digest` (at most two) |
| `HostKeyState` | `decision:u8`, `challenge_id:[u8;16]`, `canonical_host:Bytes`, `port:u16`, `algorithm:Bytes`, `fingerprint_sha256:[u8;32]`, `presented_public_key:Bytes`, `created_ms:u64` |
| `MetadataComplete` | `expansion:Bytes` (canonical UTF-8 expansion, at most 64 KiB), `completed_at_unix_ms:u64` |

Manifest parts carry at most 64 KiB each and reassemble to at most 64 MiB.
The canonical manifest encodes `version:u8=1`, `total_length:u64`,
`chunk_length:u64`, `chunk_digest_count:u32`, `whole_digest_count:u8`, then
chunk digests followed by whole-file digests. Each digest has an algorithm byte
(`1=md5`, `2=sha-1`, `3=sha-256`, `4=sha-512`) and its exact fixed-length value.
There are at most 1,048,576 chunk digests and two whole-file requirements.
`fingerprint` is SHA-256 over `ariax/verification-manifest/v1\0` followed by
these canonical bytes. Replay completes the ordered manifest and checks its
fingerprint before accepting dependent leases or verification evidence.

`ProtocolValidator.protocol` uses `1=ftp`, `2=ftps`, `3=sftp`. Only SFTP has
the required host-key hash. The source hash and modification time are
source-local resume evidence, not cross-mirror content identity.

The expansion text is
`1|parent_gid|generation|snapshot_hash|document_hash|document_bytes|retained|child_gids`.
Hashes are hexadecimal, `retained` is `0` or `1`, and child GIDs form an ordered
comma-separated list of 1–1,000 distinct IDs excluding the parent. Replay binds
the expansion to the exact parent generation and option snapshot. Retained XML
also requires its matching layout; metadata-only completion cannot manufacture
file durability. Approved or rejected trust requires the exact retained pending
challenge, including its bounded public key.

`validator_fingerprint` is a fixed hash of the canonical validator tuple, not a
raw cookie, credential, or header block. `contributors_hash` covers the sorted
committed `(LeaseId, Span, validator_fingerprint)` tuples that supplied a piece;
`validator_set_fingerprint` covers their canonical distinct validator set. Thus
a verification piece assembled from multiple leases does not pretend to have a
single source lease. `OptionsSnapshot.scope` is `CurrentGeneration` or
`NextAdmission`. It contains only the sanitized options needed to reproduce
layout, verification, and recovery decisions; sensitive values are never legal
journal payloads.

The only lease-free durable promotion is explicit different-identity rebind
readback. It uses the canonical empty contributor hash, a domain-separated
validator-set fingerprint over `(rebind, prior_root_binding_hash,
new_root_binding_hash, piece_digest)`, a required nonempty digest, and the
`RecoveryReadback` data-barrier tag. It is legal only during admission before a
network worker starts. Ordinary recovery without content proof cannot manufacture
this form.

Generation/patch crash rule:

- Generation 0 begins with one `CurrentGeneration` snapshot.
- A representation restart first appends and flushes
  `TaskPaused(reason=restarting)` while the old generation is drained. This is
  the durable restart-reason authority if the process exits before generation
  promotion; it is not a user-visible pause.
- Any restart first appends and flushes a `NextAdmission` snapshot while the old
  generation remains current. For an option patch it carries the accepted
  `OptionPatchId`; non-option restarts use no patch id. A newer staged snapshot
  supersedes an older one only through an explicitly accepted complete patch.
- After the old generation drains, the sole admission rollover appends
  `GenerationStarted` with the staged snapshot hash/patch id. Replay accepts the
  advance only when that exact earlier staged snapshot exists, then promotes it
  to current and clears the pending slot and restart marker. A representation
  restart promotion also invalidates every old durable piece before any new
  layout or lease. The worker starts only after this record is flushed.
- Automatic `retry_readmission` and `recovery_repair` rollovers retain bounded
  piece/span retry decisions when the complete option snapshot is unchanged.
  This preserves attempt caps and deadlines across released slots, slow-slot
  readmission and process recovery. Task/URI decisions are consumed by admission.
  An option change or explicit/representation restart clears prior retry state.
  The exact generation, staging hash, patch identity and drained-lease checks
  remain mandatory; retention never accepts an otherwise invalid record.
- A crash after the restarting marker but before staging resumes with the marker
  as reason authority. A crash after staging but before `GenerationStarted`
  retains the exact accepted pending restart and appends only the missing
  promotion suffix; a mismatched staged snapshot fails closed. A crash after
  `GenerationStarted` recovers the promoted generation with old representation
  progress already unpublishable. A missing/mismatched staged snapshot makes
  the generation record invalid at that point; replay stops before the advance
  rather than combining option versions.

For an active RPC option patch, the staged `NextAdmission` record is keyed by
the patch id and is written at most once. An uncertain accepted append or flush
stops the driver for recovery under `session-persistence.md`. Recovery reuses
the exact staged prefix; it cannot append another snapshot for the same patch
or an untagged snapshot over it. A newly accepted complete patch may supersede
it only with a distinct patch id. Promotion consumes the matching snapshot
once. The existing duplicate/hash/identity rejection rules stay strict;
`P4-04` fixes the producer sequence rather than accepting invalid journals.

`layout_hash` covers the canonical relative file map, lengths, selection, and
piece geometry but deliberately excludes the filesystem location.
`root_binding_hash` covers the platform tag, canonical root path, stable root
identity, and each opened file identity. Recovery may count journal progress
only after rebuilding the root capability and matching that binding. Finalize
records contain safe relative paths and the binding hash; a raw absolute path in
a finalization record is invalid and can never bypass `rename_safe`.

`RetryState` persists the scheduling decision, not a bare deadline:
`elapsed_before_wait_ms` is the capped generation retry-budget elapsed time,
`scheduled_at_unix_ms` is the wall-clock time the wait was chosen, `delay_ms`
is the chosen delay, and `retry_reason` distinguishes backoff, `Retry-After`,
and policy-clamped waits. Live waits and elapsed accounting always use the
monotonic clock;
`retry-policy.md` defines how these fields are validated against wall-clock
jumps at recovery. A `FinalizeIntent`/`FinalizeDone` pair brackets each final
rename; the Finalization section defines the idempotent crash rules.
`file_identity` is the platform identity evidence captured for the temp file
before rename (device/inode pair, Windows volume/file index, or empty when the
platform provides none).

A `LayoutCommitted` whose `FileLayoutEntry` list would exceed the 16 MiB record
cap uses deterministic entry-boundary chunking. The inline entries are chunk
index 0, and `chunk_count` is the total number of chunks including that inline
chunk (`1` when no continuation is needed). `LayoutChunk` records then use
indexes `1..chunk_count-1`, repeat the same `layout_hash`,
`root_binding_hash`, and `chunk_count`, and follow immediately with no
interleaved record. Packing is greedy in canonical file-index order up to the
record cap; an entry is never split, and component length limits guarantee one
entry fits one record. Replay rejects a duplicate, gap, binding/hash mismatch,
interleaving record, count overflow, or a sum of per-chunk counts different
from `total_file_count`. `layout_hash` covers the canonical reassembled layout
independent of record boundaries; `root_binding_hash` covers the reassembled
identity-bearing entries.

`PieceStateChunk` is legal only inside a complete checkpoint set. It replaces
one-record-per-piece checkpoint output; ordinary live progress still uses
`PieceDurable`. Encoding is deterministic:

- chunks are ordered by piece id and numbered `0..chunk_count-1`; each begins
  at the first durable piece not covered by the previous chunk,
- one chunk covers the largest prefix of at most 131,072 consecutive piece ids
  whose complete encoding fits the record cap; its bitmap is exactly
  `ceil(covered_piece_count / 8)` bytes, least-significant bit first,
- evidence runs cover every set bit exactly once in increasing order. A run
  contains consecutive durable pieces with the same validator-set fingerprint,
  digest algorithm, and digest length. With no per-piece digest, algorithm,
  length, and values are empty/zero; otherwise `digest_values` is exactly
  `piece_count * digest_value_len` canonical bytes,
- a run may be split only at a piece boundary to meet the record cap; gaps,
  overlaps, unset-bit evidence, missing evidence, noncanonical splitting, or a
  binding/layout mismatch reject the entire checkpoint set.

Lease contributor hashes are deliberately omitted from a checkpoint: they
prove the original live commit history, not future recovery state. The
checkpoint's `state_hash`, the retained validator-set fingerprint, optional
per-piece digest, layout/root binding, and data-before-journal invariant are the
canonical replacement evidence. This makes checkpoint replay proportional to
compact state rather than the historical count of `PieceDurable` records.

Checkpoint sets use the same ordinary layout records and therefore the same
rule. Other version-1 payloads have explicit bounded cardinalities and must fit
one record; version 1 does not imply an undefined generic continuation format.

Only `GenerationStarted` advances the generation. All other records use the
current generation. A `LeaseStarted` without `LeaseCommitted`, and any
`PieceStarted`/`PieceWritten` without a matching durable result, is provisional
and resets to pending during recovery. `LeaseCommitted` makes a span logically
accepted, but only `PieceDurable` makes a complete piece durable across restart.

### Appender Ownership And Rotation

Implementation status: the synchronous storage primitive and recovered-open
path are executable. Creation uses an exclusive `.tmp` candidate for
`segment-{index:010}.arxj`, syncs the header before install, syncs the parent
directory where supported, and never overwrites an existing segment path.
Recovered open accepts the exact ordered installed paths plus the expected task
and journal identities; it applies replay budgets before proportional
allocation. Its portable path checks are lexical and metadata-only, not a
filesystem security boundary. Async inbox ownership and storage-engine fact
wiring remain later integration work.

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

Write-amplification rule: storage accumulates provisional completion ranges in
memory per `(generation, lease, piece)` and submits a `PieceWritten` fact only
when a contiguous run closes, the lease commits/aborts, or a configured
checkpoint boundary requires a hint. It never sends one appender fact per
`WriteBlock`/network buffer. Thus the bounded appender inbox is protected before
enqueue, not merely by batching after it fills. The appender may additionally
merge adjacent same-lease facts before encoding. Coalescing changes only record
granularity, never ordering, sequence continuity, or the data-before-
`PieceDurable` barrier. The accumulator count is bounded by active leases and
pieces; cancellation flushes or discards each accumulator according to the
lease outcome. Coalescing plus segment rotation bounds a single segment; the
Checkpoint Compaction section below bounds lifetime size and replay cost.

`BeginLease`, provisional disk completions, `CommitLease`, and `AbortLease`
produce `LeaseStarted`, `PieceWritten`, `LeaseCommitted`, and `LeaseAborted`
facts through this same appender. An endgame commit candidate remains in memory
and produces no `LeaseCommitted` fact until every competing write is fenced. A
dirty overlap rollback appends `LeaseAborted` for every group member and no
`LeaseCommitted` or `PieceDurable`; replay therefore returns the physical bytes
to pending without an undo record. `LeaseCommitted` acknowledgement requires
the record to be accepted as `Appended`, but not flushed; if a crash loses that
tail, recovery safely treats the physical bytes as pending. A journal append
failure rejects/faults the transaction rather than allowing an unrecorded commit
to win endgame arbitration.

Rotation happens only after a flushed record boundary. The appender syncs and
closes the old segment, hashes its valid bytes, creates the next segment under a
temporary name with `segment_index + 1`, `first_sequence = last_sequence + 1`,
the current generation, and the previous segment's last-sequence/hash link,
then publishes the new segment with an atomic no-clobber hard link, syncs its
parent directory where supported, removes the private candidate name, syncs the
directory again, and only then appends to it. A publication collision preserves
the existing destination. A crash or barrier/unlink failure after publication
can leave the final name alone or both names for the candidate inode even though
the install did not return success. Startup must validate the exact expected
header/linkage at the final name and, when both names remain, remove the
candidate only after proving both names identify the same file. A foreign final
name or candidate fails closed. A filesystem that cannot provide the no-clobber
hard link fails with the typed install operation; it never falls back to an
overwriting rename. Old segments remain immutable and are removed only by
checkpoint retirement below.

`ControlJournalAppender::open_recovered` is legal only while the session owner
holds the task's exclusive persistence ownership. Every supplied path must be
the exact deterministically named file for its position in the supplied
directory and must pass the portable regular-file preflight; the first header
must match the task gid and installed `journal_id`. This lexical/path-metadata
check is not a descriptor-safe descendant open and does not bind later pathname
opens to the objects inspected by preflight. Native startup must acquire and
revalidate the private journal directory and segment descriptors through the
platform capability adapter, and must exclude namespace mutation for the full
recovered-open call, so a symlink, hard-link alias, or replacement race cannot
turn the portable preflight into authority. Empty
sets, reordered or missing indexes, header/identity/linkage mismatches,
sequence gaps between otherwise complete records, and replay-budget exhaustion
reject the open without publishing an appender.

At a clean end, recovered open reconstructs the exact next sequence and tail
fingerprint, reopens the final segment, validates its complete bytes, length,
header, and final record, and completes `sync_all` before those installed valid
bytes are represented as flushed or it may append at EOF. A record-invalid or
torn suffix is repairable only in the final
installed segment. Recovery truncates that file to the authoritative committed
record boundary, calls `sync_all`, and never writes after the rejected bytes.
When the repaired segment contains a committed record, it is sealed and hashed
at that valid length and a fresh no-clobber successor is installed through the
ordinary linkage protocol before any new fact is appended. If the authoritative
prefix of the first segment is header-only, version 1 cannot encode a successor
whose previous sequence is zero; recovery therefore reopens that trimmed empty
segment after validating it. A corrupt non-final segment, corrupt header,
reordered set, mismatched link, or pre-existing successor/candidate is not
silently replaced or overwritten.

### Checkpoint Compaction

Rotation bounds one segment; compaction bounds the segment set. Record types
`CheckpointStart`/`CheckpointEnd` are part of format version 1 — there is no
shipped version-1 reader that predates them.

Triggers, evaluated by the appender at flushed record boundaries (all values
registry-controlled internal defaults, visible in effective diagnostics):

- total live segment-set bytes exceed `journal-compact-min-bytes`
  (default 64 MiB) and exceed twice the estimated checkpoint size,
- total live record count exceeds 262,144 and exceeds twice the estimated
  checkpoint record count,
- segment count exceeds 16,
- the measured replay time of the most recent recovery exceeded 2 seconds
  (compact once after that recovery reaches a flushed boundary only when the
  canonical encoding is projected to reduce bytes or record count by at least
  25%).

The double-size conditions guarantee geometric shrink and prevent thrash; a
per-task minimum interval (default 60 s) plus exponential failure backoff
bounds retry cost. Pause, remove-with-retained-data, and clean shutdown may
also compact opportunistically when a size trigger holds.
The replay-time trigger is recorded against the installed `journal_id`; it does
not rewrite an already compact checkpoint on every startup when irreducible
canonical state itself takes longer than 2 seconds. That case is a measured
startup diagnostic and must remain within the metadata/admission budgets.

Checkpoint build, performed by the same serialized appender (facts arriving
during the build wait in its bounded inbox; the pause is sized by state, not
by history):

The builder streams canonical records and updates `state_hash` incrementally;
it never materializes a second full checkpoint/state copy. A bounded sizing
pass computes deterministic layout/piece chunk counts, then encoding retains at
most one 16 MiB record buffer plus iterator/hash state, all charged to
`journal_state_budget`. If the appender inbox fills during the task-local pause,
normal storage backpressure stops that task; no fact is dropped and other tasks/
control actors continue.

1. Complete the durability-mode flush for the active segment; freeze
   `source_last_sequence` and the current canonical state: generation,
   sanitized generation options, layout, durable pieces, retry state, pause
   marker, finalize intent/done state, and any terminal marker.
   Provisional/in-flight leases and non-durable pieces are excluded — they are
   pending by definition and compaction must not promote them.
2. Create a new segment set under a temporary name with a fresh `journal_id`,
   `segment_index = 0`, `first_sequence = 1`, and the frozen generation.
3. Append `CheckpointStart`, the state records encoded with the ordinary
   version-1 record types (`TaskCreated`, `OptionsSnapshot`,
   `LayoutCommitted`/`LayoutChunk`, `HttpStrongValidator` or
   `HttpRangeIdentity`, `RetryState`, the checkpoint-only compact
   `PieceStateChunk` set, and
   `TaskPaused`/`FinalizeIntent`/`FinalizeDone`/terminal markers as applicable),
   then `CheckpointEnd` whose `state_hash` covers the canonical encoded state
   records. Live `PieceDurable` history is represented by `PieceStateChunk` in
   the checkpoint rather than copied one record per piece. Layout and durable-
   piece state use only their defined deterministic chunking formats; every
   other version-1 payload must fit one bounded record.
4. `sync_all` the checkpoint segment and its directory where supported.

The v1 checkpoint state hash covers its domain, `state_record_count:u32`, then
each state record's `record_type:u16`, `generation:u64`, `payload_len:u32`, and
canonical payload bytes. It deliberately excludes sequence numbers, CRCs, and
commit markers so the same frozen state is deterministic when written into a
fresh segment set. Semantic recovery preserves the trusted prefix before an
invalid live record, but rejects an invalid checkpoint envelope or state set as
a whole.

Installation is a pointer/name switch with explicit crash points:

1. Write an `installing` intent row to SQLite: gid, old journal id and path,
   new checkpoint id, journal id, path, and `source_last_sequence`. The call
   returns a `JournalInstallToken` containing gid, checkpoint id, and new journal
   id; all later completion/clear commands must present that exact identity.
2. Install the new set as primary: in `central` mode, update the SQLite journal
   path/journal-id fields in the same transaction that changes the intent phase
   to `installed`; in `beside-output` mode, atomically rename the checkpoint
   segment over the companion base name, then perform that tokenized SQLite
   transaction. Completion re-decodes the stored intent and rechecks that the
   task still points to the recorded old id/path. The `both` replica is refreshed
   from the new primary before `installed`.
3. Only after `installed` is durable in SQLite: retire (delete) the old
   segments and any older orphaned checkpoint temporaries. Retirement failures
   are diagnostics, not correctness failures — stale sets are unreachable
   because their `journal_id` no longer matches the installed pointer. Clearing
   the installed intent also requires the same token and cannot clear a newer
   install for the gid.
4. The appender continues appending post-checkpoint facts to the new set;
   subsequent segments extend it with the normal rotation linkage.

Recovery with a pending `installing` intent validates the new set's complete
linked prefix; the checkpoint is acceptable only if that prefix ends at or
after its `CheckpointEnd` and the reassembled state matches `state_hash`.
If acceptable, recovery completes the installation; otherwise it deletes the
temporaries and recovers from the retained old set, which remains authoritative
until `installed`. A `CheckpointStart` without a matching valid `CheckpointEnd`
invalidates the whole candidate set, never just a suffix. Divergent copies at
the same checkpoint id are corruption and fail closed to the old set.
Rejecting a candidate also submits `abort_journal_install` with the exact
`JournalInstallToken`; the transaction re-decodes the `installing` row,
rechecks that the task still points to the recorded old id/path, and deletes
only that intent. Candidate cleanup deletes only objects whose opened
directory/file identities prove ownership. Foreign residue is left untouched
and diagnosed. Failure to token-abort the exact row prevents startup
publication so a newer install can never be cleared accidentally.
Opening the session database additionally validates the phase/pointer relation:
`installing` must retain the old id/path and `installed` must name the new
id/path. Ordinary task updates cannot bypass this protocol to change the
primary pointer.

A compaction failure (build, sync, or install) leaves the old set
authoritative, cleans up temporaries, backs off, and raises a diagnostic; it
never faults the task unless the active journal itself faults. Version
compatibility follows the ordinary rules: a checkpoint is written in the
writer's current format version, and a reader that rejects the version rejects
the whole set.

### Journal Descriptor Budget

Open journal files count against the process file budget
(`detailed-runtime.md`). The appender object (sequence counter, tail state,
rotation state) is always resident, but its file descriptor is closable: an
LRU cap (`journal-open-fds`, default derived from the file budget, minimum 16)
closes idle appender descriptors after a flushed boundary. Reopen validates
the tail (last record readable, sequence equals the in-memory expectation)
before the next append; a mismatch faults the task journal rather than
appending after unnoticed truncation. Closing and reopening never reorders
facts or skips the durability barrier because both are owned by the still-
resident appender object.

Diagnostics expose per task and globally: journal bytes, segment count, record
count, estimated checkpoint size, last replay duration, last compaction time
and outcome, compaction failure count, and open journal descriptors.

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

The balanced group closes at the first of its profile's byte, elapsed-time, or
piece-count thresholds in `performance-profiles.md`, and unconditionally on
pause, remove, finalization, clean shutdown, or a file-handle/failover barrier.
Only a task with a nonempty candidate group registers a deadline, so the time
bound does not create one idle timer/task per download. Threshold changes are
registry-controlled internal tuning and never weaken the ordering above.

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

## Admission, Root Binding, And Relocation

No protocol worker starts with an uncommitted filesystem target. Initial
admission appends and flushes `TaskCreated` and the current `OptionsSnapshot`,
opens the selected targets through `SafePath` capabilities, collects their
identities, then appends and flushes `LayoutCommitted` plus any `LayoutChunk`s
before polling a body. A later admission first appends and flushes
`GenerationStarted`; when its promoted option snapshot changes layout,
selection, or output root, the newly opened binding is also appended and
flushed before a worker starts. If those fields are unchanged, the prior
committed layout/binding may be reused after identity revalidation.

A companion journal is portable state, not portable filesystem authority.
Recovery/rebinding has two cases:

- **Identity-preserving relocation.** If the canonical display path changed
  because the same directory was renamed/moved but the stable root identity and
  every selected file identity still match, recovery may automatically rebuild
  the root capability after allowed-root checks. It starts a new generation and
  writes a new root binding (the path is part of the binding hash) before any
  I/O; existing durable pieces remain valid.
- **Different or missing identity.** A copied tree, another filesystem, or a
  replaced output requires an explicit session import/rebind operation. The
  caller supplies the new root and chooses content verification. Every relative
  path is rebuilt through `SafePathBuilder`; symlinks/reparse points, collisions,
  foreign extra targets, and paths outside policy fail closed. A durable piece
  is retained only when its recorded per-piece digest exists and readback under
  the new capability matches it. A piece without such content evidence returns
  to pending; file length, timestamps, adjacency to the control file, or a
  whole-file digest for an incomplete file are not proof. Cross-file pieces are
  retained only when the complete piece verifies.

The rebind sequence is: quiesce/drain the old generation, append and flush the
complete `NextAdmission` option snapshot naming the candidate root, perform all
safe opens and read-only verification, append and flush `GenerationStarted`,
append and flush the new `LayoutCommitted`/chunks, complete the selected
durability data barrier and append/flush one lease-free `PieceDurable` for each
digest-proven retained piece in a different-identity rebind, transactionally
update SQLite's path/index cache, then start workers. An identity-preserving
path-only relocation may carry prior durable state because the same root/file
identities were revalidated. A crash after generation
promotion but before the new layout binding leaves an incomplete admission:
replay never falls back to the previous generation's root for writes, and
recovery repeats safe allocation/rebinding from the promoted snapshot. A crash
after the layout flush but before SQLite update repairs SQLite from the journal.
If a crash interrupts digest-proven piece promotion, replay retains only the
new-generation promotions already flushed and leaves the rest pending.

Automatic scanning may propose a companion file and candidate root, but it may
not select the different-identity case without the explicit import/rebind
operation. `session-persistence.md` owns its control-plane/API surface; this
section owns the byte-trust rule.

## Recovery

Startup:

1. read SQLite queue membership and locate the task's segment set,
2. validate and replay the journal's global valid prefix,
3. rebuild and validate the recorded root binding (or enter the explicit
   relocation/rebind path above) before opening any descendant,
4. take generation, layout, validator snapshot, lease/piece progress, and task
   completion from that prefix,
5. use SQLite only for its authoritative queue/cross-task fields and reconcile
   any duplicated journal-owned fields from journal to SQLite,
6. reset begun/aborted/uncommitted leases and every non-durable piece to pending,
7. re-read and rehash fast-mode hints where a checksum can prove them; otherwise
   redownload them,
8. verify that each journal-durable span is readable and inside the recorded
   layout,
9. if persistence omitted an authenticated source or required credential,
   retain all verified/durable pieces, reconstruct `Waiting` or `Paused` from
   SQLite desired state, and set the scheduler `needs_credentials` admission
   condition; it cannot issue a new lease until the caller supplies a satisfying
   replacement source or credential,
10. otherwise create the scheduler task in a new generation, subject to
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
  -> if temp naming is active:
       append FinalizeIntent (root binding, safe temp/final relative paths,
                              layout/file identity)
       flush journal through FinalizeIntent
       rename temp to final
       fsync parent directory where supported
       append FinalizeDone
  -> append TaskComplete through ControlJournalAppender
  -> flush journal through TaskComplete
  -> persist stopped result
  -> publish terminal Complete snapshot
  -> optionally remove companion control file
```

Without temp naming there is no rename step; `TaskComplete` alone finalizes.

### Idempotent Rename Recovery

`FinalizeIntent` is appended and flushed strictly before the rename syscall;
`FinalizeDone` and `TaskComplete` may lag arbitrarily behind it. Recovery
therefore decides from `(intent, done, filesystem)`:

- No `FinalizeIntent` in the valid prefix: no rename can have happened
  (record-before-rename). Normal recovery; a stray file at the final path is an
  unrelated existing file protected by overwrite policy.
- `FinalizeIntent` without `FinalizeDone`:
  - temp exists, final absent: redo the rename, then append
    `FinalizeDone`/`TaskComplete`. The redo is safe because the intent proves
    the earlier attempt was ours and either failed or never ran.
  - temp absent, final exists: the rename happened before the crash. Accept the
    final file only if it matches the recorded `final_length`, the layout hash,
    and — where the platform records it — the `file_identity` evidence or the
    configured digest; then append `FinalizeDone`/`TaskComplete`. On mismatch,
    treat the final path as an unrelated existing file: fail finalization with
    a collision error rather than overwrite.
  - both exist: the final-path file was not produced by this intent (our rename
    would have consumed the temp). Verify per the previous point using identity
    /length/digest against the *temp* file for redo and treat the final path as
    a collision unless auto-renaming policy resolves it.
  - both absent: the output is gone; downgrade to the corruption/missing-output
    policy (revalidate/redownload in a new generation), never fabricate
    completion.
- `FinalizeIntent` and `FinalizeDone` present: rename is settled; recovery only
  finishes `TaskComplete`/stopped-result publication if the crash hit between
  them.

Collision policy at first finalization (not recovery) checks the final path
before appending `FinalizeIntent`: an existing file is resolved by
`allow-overwrite`/`auto-file-renaming` policy first, so the recovery rules
above can always treat an unexpected final-path object as foreign. On Windows,
a rename rejected with a sharing violation retries on a bounded schedule and
then fails finalization with a typed error; recovery may retry the same
idempotent step. Directory fsync ordering is: temp file data flush, rename,
parent directory sync, then `FinalizeDone`. Multi-file layouts repeat the
intent/rename/done triple per selected file in deterministic `FileId` order;
recovery replays the remaining files from the first missing `FinalizeDone`.

`TaskComplete` records task-local file completion. User-visible completion
requires both `TaskComplete` and the global stopped result transaction. Crash
recovery may recreate a missing stopped result from a valid `TaskComplete`, but
it must not trust a stopped result without a matching journal completion or
fresh finalization verification.

## Tests

Required tests:

- path traversal corpus across Unix/Windows forms,
- initial admission cannot start a worker before its layout/root binding is
  flushed,
- identity-preserving directory relocation writes a new binding and retains
  durable pieces; a copied/different-identity tree requires explicit rebind and
  retains only digest-proven pieces,
- crash at each rebind step never writes through the old root and repairs the
  SQLite cache from the journal after the binding is durable,
- `${out}` and Content-Disposition path sanitization,
- global offset overflow rejection,
- stale generation write rejection,
- duplicate write policy,
- exact-length commit, short/oversized abort, stale validator abort, and
  first-eligible endgame candidate arbitration,
- clean overlap settlement commits the candidate only after every loser is
  cancellation-confirmed without a completed write,
- dirty or cancellation-uncertain overlap settlement aborts all members, clears
  touched in-memory piece state, leaves physical bytes unchanged, and permits a
  same-generation overwrite,
- crash at every overlap-settlement point replays the touched pieces as pending,
- crash after provisional write and after `LeaseCommitted` but before
  `PieceDurable` resets the affected range to pending,
- hash mismatch appends `PieceFailed`, clears the whole verification range, and
  permits same-generation overwrite,
- resume open does not truncate,
- `200 OK` restart path cannot write at nonzero offset,
- torn journal tail replay,
- deterministic partial-fsync failure leaves `Flushed` unchanged and a
  simulated durable-prefix loss replays only the prior barrier,
- forced child-process exit and parent-driven kill after provisional write,
  `LeaseCommitted`, data sync, unflushed `PieceDurable` publication, and flushed
  commit recover exactly the expected pending or durable prefix on Linux and
  native Windows-GNU; the OS-surviving kill and simulated power-loss cut have
  intentionally different expectations for the unflushed journal record,
- corrupted journal CRC replay,
- duplicate/gapped sequence rejection and out-of-order disk completions through
  the single sequence-assigning appender,
- segment rotation, missing/reordered segment, and previous-segment hash-link
  rejection,
- a crash after successor publication that leaves both final and `.tmp` names
  adopts the candidate only after descriptor-bound same-file identity proof
  and complete installed header/linkage replay; foreign or invalid candidates
  remain untouched and fail closed,
- crash after disk write before journal,
- crash after balanced data sync but before journal sync,
- crash after journal bytes but before the required data barrier is never
  constructible through the appender API,
- crash after journal before final rename,
- checkpoint compaction: every size/count/replay trigger fires, provisional
  state is never promoted, and replay of checkpoint + tail equals replay of the
  full original set,
- crash at every compaction step (build, checkpoint sync, installing intent,
  pointer/rename install, retirement) recovers to exactly one authoritative
  set with no lost durable state,
- a checkpoint set with a missing/invalid `CheckpointEnd` or `state_hash`
  mismatch is rejected whole and the old set recovers,
- deterministic `LayoutChunk` packing/reassembly covers inline chunk index 0,
  exact total count, cap boundaries, missing/duplicate/interleaved chunks, and
  mismatched layout/root-binding hashes,
- checkpoint `PieceStateChunk` round-trips sparse/dense durable maps, canonical
  evidence runs, digest/no-digest pieces, and cap splits, and rejects every
  gap/overlap/noncanonical or binding-mismatched set,
- idle journal descriptors close under the FD cap and reopen validates the
  tail; a truncated tail faults instead of appending,
- compaction failure backs off, keeps the old set authoritative, and surfaces
  diagnostics,
- finalize intent/redo: crash before rename, after rename before
  `FinalizeDone`, after `FinalizeDone` before `TaskComplete`, and after
  `TaskComplete` before the stopped result each recover to exactly one
  outcome; final-path collision with a foreign file fails closed; Windows
  sharing violation retries bounded and fails typed; multi-file finalization
  resumes at the first missing `FinalizeDone`,
- disk-full write rejection returns/quarantines buffer,
- `trunc` logical length and zero-filled reads never create completed ranges,
- completion requires stopped result persistence.
