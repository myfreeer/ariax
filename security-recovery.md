# Security And Recovery Design

Status: reviewed contract with implementation in progress. Portable path
normalization/rejection, persisted root bindings, and bounded control-journal
framing/replay plus all 24 bounded typed payload codecs are executable.
Policy-gated option-secret rejection and typed semantic state recovery are also
executable, including digest-bound different-identity rebind records. Native
capability acquisition, safe descendant open/revalidation, and complete startup
filesystem reconciliation remain pending. The bounded pure cross-store startup
planner and atomic scheduler reconstruction are executable. Durable journal
append/flush, latched appender failure, tail/content-validated final-segment
reopen after portable exact-name preflight, and linked rotation are executable;
that reopen does not establish native namespace authority. SQLite session schema
v2 rejects newer/unversioned schemas fail-closed, preflights and privately backs
up exact v1 databases before their transactional migration, verifies hard limits
and bounded persisted records,
rechecks task-option and host-key semantics on read, retains stopped results
atomically with queue ownership, enforces private database artifacts, and
transactionally rechecks tokenized journal-install pointers. A bounded
session-store owner thread and exact persistence-effect composition boundary
are executable. The bounded SQLite-only application stage applies queue repairs
before terminal repairs, then exact journal-authority repairs, and cannot expose
the restored scheduler while a command is queued, in flight, failed, or
unexpectedly acknowledged. Journal-install work, appender recovery, native
capabilities, and final publication remain pending.

This document turns the safety requirements into enforceable design rules.

## Safe Path Builder

All metadata-derived and path-bearing option inputs use the single
`SafePathBuilder::build(SafePathInput) -> Result<SafePathOutput, PathError>` API
owned by `detailed-storage.md`. This document adds policy constraints; it does
not define a second builder shape.

```text
SafePathBuilder::build(SafePathInput) -> Result<SafePathOutput, PathError>
```

Input decoding and component construction are deterministic:

- Decode an external byte/percent-encoded name exactly once. Reject invalid
  UTF-8, including overlong encodings and surrogate encodings, rather than using
  lossy replacement.
- Normalize every Unicode component to NFC before validation, collision checks,
  persistence, and display. Never normalize after a path has been opened.
- `out` and `index-out` may express relative subdirectories. After rejecting an
  absolute/drive/UNC prefix, split them on both `/` and `\\`, collapse repeated
  separators by ignoring empty interior components, and pass every resulting
  component separately through the builder. `.` and `..` are rejected, not
  collapsed. A wholly empty result is invalid.
- Metadata formats that already supply component arrays do not get a second
  separator interpretation: a separator inside one supplied component is an
  error.

Reject:

- absolute paths,
- Windows drive prefixes and UNC prefixes,
- `.` and `..`,
- path separators inside a component,
- empty components except where the format explicitly permits them and they are
  ignored,
- NUL and control characters,
- bidirectional override/isolate formatting characters that can spoof displayed
  path order,
- `:` in every component. The portable policy deliberately rejects this on all
  platforms so a session restored on Windows cannot reinterpret `file:stream`
  as an alternate data stream,
- reserved Windows names: `CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`, and
  `LPT1`-`LPT9`, including when they carry an extension or stream suffix
  (`CON.txt`, `nul.log` are also reserved), and the superscript-digit forms
  `COM¹`/`COM²`/`COM³` and `LPT¹`/`LPT²`/`LPT³` that current Windows naming
  rules also reserve,
- trailing spaces or dots on Windows,
- symlink escapes when opening existing directories.

Windows path-length policy: the builder computes the full final path length and
uses the extended-length (`\\?\`) form when the classic `MAX_PATH` limit would
otherwise be exceeded and the platform supports it. Because the `\\?\` form
disables the Win32 normalization that the reject list above compensates for,
the builder's own component validation is mandatory before that form is used.
If the final path still exceeds the platform/filesystem limit, path
construction fails with a typed `PathError`, not a truncated or aliased name.

Reserved-name, trailing-dot/space, and collision checks run after NFC
normalization. On case-insensitive targets the builder uses the filesystem's
case-insensitive comparison key for collision detection. Visually confusable
Unicode characters are not treated as equivalent—there is no stable universal
homoglyph mapping—but security decisions never depend on rendered similarity,
and diagnostics escape non-ASCII/bidirectional-sensitive names unambiguously.

The builder retains a capability for the canonical output root and resolves or
creates every descendant relative to retained directory capabilities. The
display `PathBuf` is never reopened as authority. Linux uses `openat2` with
`RESOLVE_BENEATH`/no-symlink constraints when supported and a stepwise
`openat`/`O_NOFOLLOW` directory-fd walk otherwise. Other Unix targets use the
same held-directory-fd pattern. Windows opens each directory without
share-delete, rejects reparse points, retains the handle chain until the final
handle is acquired, and verifies volume/file identity. All platform-specific
unsafe/syscall code stays behind the project safe-open adapter.

This race-resistant open contract is required on supported production targets,
including the bounded blocking fallback. If the runtime probe cannot provide
it, metadata-derived output creation fails closed with `SafeOpenUnavailable`;
there is no silent best-effort check-then-open mode for an attacker-writable
root. `detailed-storage.md` owns the canonical capability-bearing API and
recovery reconstruction rule.

This builder is mandatory for:

- torrent file paths,
- Metalink names,
- Content-Disposition filenames,
- `out`,
- `index-out`,
- session restore,
- RPC upload metadata paths.

The initial libtorrent adapter is a documented boundary: libtorrent receives
sanitized relative paths rather than the project's open file capabilities. Its
output root therefore must not be writable by an untrusted local principal
while the session runs. Stronger local-attacker containment for BitTorrent
requires the deferred custom libtorrent storage backend; metadata sanitization
alone must not be described as providing that stronger guarantee.

## No RCE Policy

Default behavior:

- no shell command execution,
- no command templates,
- no metadata-controlled process launch,
- no RPC-enabled hook changes.

Safe event alternatives:

- structured WebSocket events,
- JSON event log,
- local plugin API with typed messages,
- webhook client with URL allowlist and no shell interpolation.

Unsafe compatibility:

- `--allow-exec-hooks=true` enables aria2-style local hooks.
- Hooks are startup-only unless a local admin RPC policy permits changes.
- Commands are executed without a shell when possible.
- Environment variables are allowlisted.
- Arguments are passed as argv fields, not concatenated command strings.
- Hook path must be absolute or under an allowlisted directory.
- Hook stdout/stderr size is capped.
- Hook timeout is enforced.

## RPC Security

Defaults:

- bind only loopback,
- require `rpc-secret` for non-loopback,
- cap request body by `rpc-max-request-size`,
- reject unauthenticated WebSocket event subscriptions,
- redact tokens, passwords, cookies, Authorization headers, and proxy
  credentials.

Remote RPC guardrails:

- Optional network allow/deny lists for downloads submitted over RPC:
  `network-allowlist` and `network-denylist`. When both are set, denylist wins
  (default-deny on conflict); an empty allowlist means "no restriction", a
  non-empty allowlist means "only these match". Defaults: both empty.
- Private-address blocking to reduce SSRF risk when RPC is exposed:
  `rpc-allow-private-address-downloads`, default `false`. When false, downloads
  whose resolved address is non-global or special-use are refused. Setting it
  true relaxes only RFC 1918 IPv4 and IPv6 unique-local destinations; loopback,
  link-local, unspecified, multicast, broadcast, documentation/benchmark,
  reserved ranges, and cloud-metadata endpoints still require an explicit
  startup administrator allowlist.
- CORS disabled by default; wildcard CORS requires explicit insecure opt-in.
- `changeGlobalOption` cannot mutate startup-only listener security.

## SSRF Guardrail

`README.md` promises that redirects, proxies, and DNS resolution obey SSRF
guardrails when RPC is remotely exposed. This section defines the mechanism.

The guardrail runs whenever a download target is submitted over a non-loopback
RPC listener (and always for redirect targets, see `redirect-policy.md`):

- Resolve the host, then evaluate every resolved address against a generated,
  pinned IANA special-purpose prefix table. Default remote-RPC policy permits
  only globally routable unicast and denies unspecified/current-network,
  loopback, RFC 1918/unique-local, carrier-grade NAT, link-local, protocol/
  benchmarking/documentation ranges, multicast, reserved/future-use, IPv4
  broadcast, IPv4-mapped forms of any denied IPv4 address, and known cloud
  metadata endpoints (including `169.254.169.254`) before connect. The table
  update is reviewed like a dependency update; code does not rely on an
  incomplete hand-written trio of private ranges.
- Canonicalize numeric hosts before classification, including unusual integer/
  legacy IPv4 spellings and IPv4-mapped IPv6. Policy is applied to the canonical
  address, so alternate textual forms do not bypass the deny set.
- DNS-rebinding protection: pin the address that passed the check and connect to
  that exact address, so a second resolution cannot swap in a blocked target
  between check and connect. A reconnect or DNS-cache refresh performs a new
  resolution/check and creates a new pin; it never reuses approval for a
  hostname with a different address. Happy Eyeballs may race connection
  attempts only among addresses that each individually passed the check; the
  winning connection's address is the pin.
- Composition: the guardrail applies after `network-allowlist`/`network-denylist`
  and before the connection is made. A user-configured mirror is not exempt when
  RPC is remotely exposed; an explicitly loopback-bound RPC instance may relax
  only RFC 1918/unique-local blocking via
  `rpc-allow-private-address-downloads=true`; other special-use ranges still
  require an explicit startup allowlist.
- Proxy interaction: the guardrail validates both the proxy endpoint and the
  final origin. For an untrusted remote-RPC task, locally resolve and validate
  the origin, pass its pinned numeric address to SOCKS5 or HTTP `CONNECT`, and
  retain the original hostname only for TLS SNI, certificate verification, and
  generated HTTP `Host`. `socks5h`, hostname-form `CONNECT`, or another
  proxy-resolved destination is refused unless a local administrator marked the
  proxy at startup as destination-enforcing with an allowlist at least as strict
  as this guardrail. A per-task option or RPC caller cannot grant that trust.
  See `protocol-modernization.md`.
- Redirect and proxy changes repeat the entire decision. Failure to express a
  pinned connect address separately from Host/SNI fails closed.

Output root guardrail:

- `allowed-output-root` may restrict all metadata-derived and user-provided
  output paths to one or more configured roots.
- `SafePathBuilder` runs after this policy; both checks must pass.

## HTTP Header Boundary

Generic custom headers cannot override fields that enforce framing, placement,
identity, or credential scope. Header names and values are syntax-validated and
reject CR/LF/NUL/control injection. Case-insensitive attempts to set `Host`,
`Content-Length`, `Transfer-Encoding`, `Range`, `If-Range`, `Accept-Encoding`,
`Authorization`, `Proxy-Authorization`, `Cookie`, integrity digests, or request-signature
fields reject the task. The HTTP builder generates those fields and rebuilds
them after each redirect; it never resolves conflicts using last-write-wins.
The canonical list and harmless-header behavior are in
`detailed-http-first-slice.md`.

## Correct Range And Placement Rules

HTTP range write acceptance:

- Request says `Range: bytes=A-B`.
- Response must be `206`.
- `Content-Range` must be `bytes A-B/T` or an equivalent valid form.
- Body length must equal `B - A + 1`.
- If total `T` is known, it must match the task total.
- If body length differs, no bytes are committed durable for that segment.
- After response-head validation, every attempt calls storage `BeginLease` with
  its `LeaseId` and writes only provisional blocks. Exact body length and all
  required validator/digest checks precede `CommitLease`; short/oversized body,
  redirect, cancellation, or validation failure calls `AbortLease`. These
  storage-owned operations, not file length or physical bytes, decide progress.
- An endgame overlap group cannot become committed or durable until all
  competing writes are fenced. If any losing attempt wrote or has uncertain
  cancellation, all touched pieces return to pending metadata and in-memory
  state; their physical bytes remain untrusted and are overwritten by the next
  lease rather than restored in place.

Sequential resume:

- Existing partial length is read without truncation.
- Resume request starts at existing durable length.
- `206` is required.
- `200` causes full restart only after the storage engine creates a new
  generation and explicitly truncates or renames according to overwrite policy.

Disk placement:

- Protocol workers cannot write files directly.
- Workers submit global offsets to the storage engine.
- Storage engine validates layout, selected files, generation, and current
  piece state.
- Disk ack includes exact offset and byte count written.

## Security Boundary Tests

- path decoding rejects invalid/overlong UTF-8, ADS colons, reserved names,
  traversal, bidi overrides, and NFC-equivalent collisions,
- `out`/`index-out` split relative subdirectories into validated components and
  reject absolute, drive, UNC, `.` and `..` forms,
- decimal/legacy IPv4 and IPv4-mapped IPv6 forms of private, loopback,
  link-local, special-use, and metadata addresses are denied,
- DNS rebinding on reconnect triggers a new resolution and policy decision,
- untrusted `socks5h` and hostname `CONNECT` are refused, while local resolution
  uses the pinned numeric connect address with the original Host/SNI,
- a redirect or proxy change re-runs both endpoint and final-origin checks,
- custom-header case changes, duplicates, and CR/LF injection cannot override a
  generated safety/credential field,
- a failed range attempt has one `AbortLease` and no committed progress.

## Control Journal

The normative journal format and record enum are defined in
`detailed-storage.md`. This section states only the security-relevant
properties; it does not restate the field layout, to avoid the divergence that
previously existed between the two documents.

Security-relevant properties:

- append-only or atomically replaced,
- a CRC covers the full record framing plus payload, and an explicit
  `max_record_len` bound is checked before any read, so a torn or garbage length
  field cannot drive an over-read,
- every record carries the task generation and a monotonically increasing
  per-task sequence,
- recovery scans until the last valid committed record; invalid tail bytes are
  ignored,
- a record is never trusted without a valid length, CRC, commit marker, and
  expected sequence,
- multiple records normally share one generation; only `GenerationStarted`
  advances the generation.

The record enum (including `GenerationStarted`, which advances the generation,
and `PieceStarted`, which marks a piece in-flight for recovery) is the union
enum in `detailed-storage.md`.

The primary persistence model is described in `session-persistence.md`: global
queue/session metadata in SQLite, crash-critical progress in per-task control
journals.

## Session Database Boundary

The SQLite file and backups live in a dedicated private directory. Every
existing path component must be a directory, not a symlink or Windows reparse
point. An existing configured parent with broad Unix permissions or an
inherited/foreign Windows allow ACL is rejected without being modified; a
missing owned chain is created privately at each step. Database, WAL, SHM,
rollback-journal, `${db}.ariax-owner-lock`, temporary, and backup artifacts must
be private, uniquely linked regular files. Symlinks, hard-link aliases, and
non-regular artifacts fail closed so a path-derived owner lock cannot protect a
different name for the same database inode. If the main database is missing or
empty, orphan `-wal`, `-shm`, or `-journal` files are rejected before SQLite can
initialize or recover it. Windows ACL operations are direct Win32 calls isolated in
`ariax-windows-security`; persistence startup does not spawn a shell or
PowerShell process.

Before SQLite opens an existing database, streaming, fixed-buffer raw preflight
recovers the committed header from a hot rollback journal's page-one
before-image, the main header, and valid committed WAL frames. This permits supported rollback
recovery even when the main page-one header is damaged. A legacy rollback
journal with encoded page size zero fails closed. A newer committed version is
rejected before chmod/ACL changes, owner-lock creation, SQLite recovery, or
journal-mode changes.

Ariax then exclusively holds `${db}.ariax-owner-lock` for the `SessionStore`
lifetime and repeats preflight under the lock. This is cooperative single-writer
exclusion among Ariax processes, not a SQLite-enforced boundary: external raw
SQLite writers bypass it and are unsupported. Supported v1 databases are then
opened read/write for rollback recovery, semantically preflighted, privately
backed up, and transactionally migrated to v2. Current v2 stores then undergo
exact schema, integrity, foreign-key, dense-queue, decoded task/stopped/host-key/
install record, and install-pointer validation.

Persisted task, stopped-result, host-key-challenge, and install reads have
explicit count/byte budgets, and option reads reapply the current persistence
policy. Queue moves, including cross-queue moves and slow-slot metadata changes,
shift source and target positions and validate dense final queues inside one
immediate transaction. Terminal retention and stopped-result deletion pair or
remove stopped task/result metadata atomically. Journal installation is the only
primary-pointer mutation path: completion is bound to
gid/checkpoint/new-journal identity and rechecks the old pointer in the same
transaction.

WAL and DELETE each receive a real `BEGIN IMMEDIATE` page-one write/rollback
probe; WAL falls back to DELETE only when DELETE passes the same check. Truncate
checkpoint reports busy rather than discarding WAL state. Backups are written
privately, integrity/schema/semantic validated, file-synced, and published with
a no-clobber hard link. Every destination filename ending in `-wal`, `-shm`, or
`-journal`, matched ASCII-case-insensitively, is rejected before filesystem
mutation; pre-existing destination sidecars are also rejected rather than
adopted. Unix syncs the parent directory; Windows does not yet claim
crash-durable directory-entry publication. The successful-return path removes
the temporary name, but a crash or unlink failure at any point from
destination-link publication until temporary-link removal is durably synced can
leave or resurrect both names for one inode. Normal unique-link validation then
rejects the backup until a future recovery step verifies and removes only the
generated same-file alias. A native atomic no-replace publication primitive is
also acceptable. This recovery, full crash-point window, and unlink-error
injection are required before production use or tagging. Backup residue
recovery and periodic backup/checkpoint scheduling on the dedicated store
thread remain pending.

## Bounded Cross-Store Startup Reconciliation

The `ariax-engine` composition layer owns the first executable reconciliation
boundary. It accepts one bounded `SessionStartupSnapshot`, exactly one
already-replayed semantic journal state for every SQLite task GID, and exactly
one credential-admission record for every task. The credential record carries
either the precise non-secret scheduler requirement or an explicit `None`;
omission is rejected rather than silently interpreted as "no credentials
required." The dedicated owner materializes one exact bounded source set per
task, and composition can deterministically derive these records from its
redacted or persistence-safe source rows. An explicit admission API remains for
a future reviewed encrypted credential provider, but the ordinary
plaintext-free startup path does not rely on caller guesswork.

Journal opening, segment-tail repair, native root-capability reconstruction,
install-candidate validation, and appender descriptor acquisition happen
outside this pure boundary. The reconciler returns typed repair and deferred
recovery requests for an external startup executor; it never treats a display
path as an opened capability or claims those requests were applied.

Before producing scheduler state it validates all of the following as one
atomic batch:

- the SQLite task GID set and supplied journal GID set are identical, with no
  duplicate GID, journal id, or persisted journal `TaskId`,
- each supplied journal id is the task's phase-authoritative primary journal
  id, pending install intents refer to an existing task, an `installed` row
  matches the recovered checkpoint id and frozen source sequence, an
  `installing` row cannot claim a source sequence beyond the recovered old
  prefix, and replica sequence evidence never exceeds that primary prefix,
- every task belongs to exactly one dense zero-based SQLite queue and every
  task references the one recovered session row,
- SQLite stopped rows/results require a matching journal terminal marker and
  cannot manufacture completion; a journal `TaskComplete` that won the crash
  race over SQLite instead normalizes to `StoppedResult` and emits bounded
  deferred `PersistStoppedResult` repairs in ascending GID order; every repair
  carries the exact intermediate source/stopped queue orders required to run
  the sequence directly; an already-stopped completion cache must exactly
  retain the journal's final length and layout hash rather than treating a
  missing optional cache field as a match,
- a retained host-key challenge appears exactly once, belongs to a nonterminal
  paused task, matches a journal `HostKeyApproval` pause marker, and passes the
  scheduler challenge constructor unchanged,
- persisted task retry, slow-readmission, and no-space wall decisions are
  valid under `PersistedDelayDecision`; the orthogonal no-space probe may
  coexist with a retry or slow-readmission timer, while an unrepresentable
  task-retry plus slow-readmission conflict fails closed.

Authority is applied in one direction. Journal generation, option-snapshot
identity, layout/root hashes, retry evidence, and terminal markers win over
SQLite caches. SQLite owns queue order and desired pause. A crash-time `Active`
row never recreates a live slot: it is normalized to `Waiting`, or `Paused`
when desired pause is set. Existing waiting rows precede normalized active rows
in the rebuilt waiting queue; existing paused rows precede crash-time rows
normalized into the paused queue. Demoted, stopped, and direct paused order is
otherwise preserved exactly. Journal `TaskPaused` is only a task-local marker;
it specializes a SQLite paused row as slow or host-key paused but does not
invent queue membership.

Normalization is a pre-publication draft, not an already-committed cross-store
state. Any crash-time `Active` row, or desired-pause row whose normalized queue
differs from SQLite, produces an exact bounded queue repair. Those repairs carry
the complete intermediate source/target orders and must be applied and
acknowledged in the returned order before terminal repairs. Journal-authorized
terminal repairs follow and likewise carry their exact intermediate orders.
No restore snapshot, timer, probe, or task admission may be published until the
whole repair sequence succeeds. A failed or out-of-order repair leaves startup
unpublished and must be retried or failed closed.

Exactly one task-scope retry record is representable as scheduler
`RetryWait`; URI/span/piece retries remain task-owned journal metadata. A
demoted row requires its persisted slow decision. Expired decisions produce a
fresh monotonic deadline equal to startup time so the normal correlated timer
path runs immediately. No-space recovery retains the exact native target only
in a bounded move-only target catalog and publishes a fixed redacted scheduler
description. The catalog entry is bound to the fresh task id, GID, generation,
and recovered deadline. It can be consumed once only by the matching scheduler
`ProbeNoSpace` effect; the scheduler remains the sole allocator of the fresh
probe correlation id, which is adopted from that effect rather than predicted
by recovery. Backwards clocks wait the bounded full persisted delay; forward
jumps cannot lengthen it.

Fresh process-local `TaskId` values are allocated in ascending GID order.
`RequestScheduler::restore` then validates the complete five-queue batch and
creates fresh timer correlation ids. The result contains the scheduler and its
bounded restore-effect plan, per-task journal state, journal-cache/root repair
metadata, exact ordered SQLite repairs, deferred appender-open requests,
pending install recovery requests, and the move-only no-space target catalog.

An `installing` intent suppresses a direct appender-open request because native
candidate validation may either retain the old set or install the new set.
Install recovery is bound to the fresh task identity and recovered authoritative
sequence; it must validate the candidate's native path/header, linked prefix,
checkpoint envelope/state hash, id, and frozen source sequence, then return the
final appender request. An `installed` intent must already match the supplied
recovered checkpoint, but native old-set retirement and path/capability checks
remain deferred. Native root identity and descendant revalidation likewise
remain mandatory before admission. No scheduler state is published if any
input, repair, native recovery step, or final restore batch is rejected.

## Durable Completion

A piece is complete only after:

1. all bytes are written at correct offsets,
2. checksum is valid when available,
3. disk write has been acknowledged,
4. journal durable record has been saved.

A download is complete only after:

1. all selected pieces are complete,
2. final full-file checksum is valid if configured,
3. final filenames are atomically moved into place if temp naming is used,
4. final file metadata and parent directory are flushed according to durability
   mode,
5. stopped result is persisted.

## Crash Scenarios

Power loss during network read:

- no durable journal record exists,
- segment returns to pending.

Power loss after disk write before journal:

- data may exist on disk,
- piece is not trusted,
- piece is revalidated or redownloaded.

Power loss after journal before final rename:

- piece remains complete,
- startup resumes finalization through the `FinalizeIntent` redo rule in
  `detailed-storage.md`.

Power loss during final rename:

- the flushed `FinalizeIntent` plus temp/final presence, recorded length, and
  file-identity evidence select exactly one finalization outcome
  (`detailed-storage.md` Idempotent Rename Recovery),
- no existing unrelated file is overwritten; a foreign final-path object fails
  closed as a collision.

Power loss during journal checkpoint compaction:

- until the SQLite pointer is `installed`, the old segment set remains
  authoritative and the candidate checkpoint is discarded on any validation
  failure,
- after `installed`, the checkpoint set is authoritative and stale old
  segments are unreachable garbage,
- stale completion/intent-clear commands cannot act on a newer install because
  they require the gid/checkpoint/new-journal token,
- compaction never promotes provisional/in-flight state to durable.

Power loss during control save:

- torn tail is ignored,
- previous committed state remains valid.

## Secrets And Logs

Sensitive values are stored in `Secret<T>` wrappers:

- no `Debug` reveal,
- redacted serialization,
- optional zeroize on drop,
- never included in panic messages.

Logs must redact:

- RPC secrets,
- HTTP Authorization,
- cookies,
- proxy credentials,
- netrc credentials,
- SFTP passwords and keys,
- signed URLs if configured.

## Unsafe Code And FFI

Pure Rust crates forbid unsafe code.

Allowed unsafe crates:

- `ariax-windows-security`: narrow Win32 security-descriptor creation and ACL
  verification used by otherwise-safe persistence code,
- `platform-io`: system calls, IOCP, io_uring, openat wrappers.
- `bt-libtorrent`: C++ bridge.

Requirements:

- every unsafe block has an invariant comment,
- Miri for pure code,
- ASAN/UBSAN/TSAN for FFI builds,
- fuzz tests cross FFI boundaries with small corpus inputs,
- panics do not unwind across FFI.
