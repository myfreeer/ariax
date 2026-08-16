# Detailed HTTP First-Slice Design

Status: reviewed Phase-3B implementation contract. The fresh sequential and
strong-ETag resume paths remain executable, and the public known-length
multi-mirror slice now drives ordinary `http`/`https` URIs through the real
scheduler. Task, source, and current-generation option metadata are admitted in
one SQLite transaction before scheduler publication; recovery reconstructs the
source catalog and safe output path before a task can run. The policy client
owns bounded Hickory/system DNS resolution, positive/negative TTL caches,
singleflight waiters, generated special-purpose-address admission, two-racer
Happy Eyeballs, redirect rebuilding, HTTP forward/CONNECT and SOCKS5 routing,
Basic/private-netrc credentials, and a bounded cookie jar backed by a verified
bundled Mozilla Public Suffix List. Non-overlapping range leases run across
submitted mirrors with per-range/per-source retry budgets, durable-piece
restart recovery, pre-network descriptor-bound SHA-256 readback of every
durable piece, exact strong-ETag/resource/length binding for single-source and
strict-fallback resume, packet-independent live speed sampling, and
scheduler-owned worker supervision. Five methods (`addUri`, `tellStatus`,
`pause`, `remove`, and `getGlobalStat`) are executable over loopback HTTP/1.1
and bounded Content-Length stdio framing. The older pinned-peer CLI remains as
a diagnostic harness. A persisted user `checksum=sha-256=<64 hex>` now gates
strict concurrent mirrors, verifies the descriptor-bound assembled file on a
bounded blocking worker before `TaskComplete`, and permits digest-bound restart
without a strong server validator. The persisted stale-validator policy is also
executable: `fail` terminates before unvalidated bytes become durable,
`revalidate` performs a fresh bounded range probe and requires the original
identity unless a configured whole-file digest gates completion, and
`restart-if-safe` drains the worker, stages the unchanged option snapshot,
persists a new representation generation, invalidates all old durable pieces,
and reopens only the exact task-owned output identity before rewriting from
offset zero. The executable boundary now includes bounded same-origin endgame
duplicates: only an original lease pinned by a strong ETag may be duplicated,
the duplicate uses that same source and validator, and storage withholds the
candidate commit until the losing attempt is cancellation-confirmed. Any loser
write rolls the whole overlap group back to pending metadata for a normal
same-generation overwrite. Cross-mirror endgame remains refused because the
implemented user SHA-256 is a whole-file identity proof, not a range-verifiable
digest. RFC 9530/Metalink digest identity, additional checksum algorithms,
Last-Modified and unsafe-override resume, HTTP/2, unknown-length or chunked
layouts, WebSocket/NDJSON and non-loopback RPC, and the rest of the Phase-4
control plane remain outside this checkpoint.

The process-capacity boundary is executable for this HTTP slice. A resolved
runtime profile creates one process-owned handle budget, one global resident-byte
budget, a bounded per-connection reservation, and a bounded HTTP-ingress budget.
Transport sockets (direct and proxy) consume the shared process/socket permits;
selected storage files consume two shared file permits for their capability and
blocking-lane descriptors; the transport pool, policy-client clones, worker
ingress, and storage buffer pool share the resident permit. Idle origin-cache
entries remain capped independently by the selected profile. The broader
evictable file-handle LRU remains outside this slice. The RPC
binary selects the profile with `--profile=...` and uses `auto`'s concurrency
baseline by default. A deterministic benchmark opens 10,000 real low-activity
loopback sockets and drives 1,000 simultaneous 64 KiB loopback `206` range
responses under the same permits; native release-platform runs remain a gate.

The finite HTTP discard guard is executable. One process-owned ledger charges
canonical final-origin host, task, and attempt scopes atomically for probe,
failed/aborted range, queued stale-chunk, endgame-loser/rollback, and checksum-
failure bytes. No abort or rollback refunds a charge. Credit is checked before
another body poll; crossing a limit accounts only the bounded accepted frame,
closes the attempt, and fails the retry cycle with
`http_discard_budget_exhausted`. The process host-key table is capped, and
`tellStatus` exposes cumulative budget consumption and remaining task credit.

This document defines HTTP(S) sequential download, resume, strict range
validation, storage integration, retry integration, and stats behavior.

## Scope

Included:

- HTTP/1.1 and HTTPS through the selected HTTP stack,
- single-file sequential download,
- resume from durable local state,
- range request validation,
- non-overlapping multi-mirror range workers,
- retry policy hooks,
- storage and control journal integration,
- JSON-RPC-visible snapshots.

Excluded from first slice:

- HTTP/2 multiplexing,
- HTTP/3,
- Metalink multi-source orchestration,
- BitTorrent web seeds,
- content decoding,
- unknown-length/chunked output, which requires the explicit growing-layout
  capability described below.

The executable code retains two intentionally different entry points. The
pinned-peer harness accepts an already-approved numeric `IP:port` for focused
transport/recovery diagnostics. The public RPC path accepts ordinary
`http`/`https` mirror URIs, rejects userinfo and malformed authorities, runs
every destination and proxy endpoint through policy, and passes only admitted
numeric addresses into connection establishment while retaining the original
authority for `Host`, SNI, and certificate verification.

## Executable Destination Admission

The public connector gate is closed and executable:

- only `http` and `https` are admitted; URI userinfo and port zero are rejected,
- bracketed IPv6 and ordinary dotted IPv4 are canonicalized, and legacy
  one-to-four-part decimal/octal/hexadecimal IPv4 forms are converted before
  classification so alternate spellings cannot bypass policy,
- registered names are resolved through the project-owned resolver. Hickory is
  the default backend and the system adapter remains selectable. Resolution is
  bounded by a ten-second default timeout, 128 in-flight names, 4,096 total
  waiters, 1,024 waiters per name, and 32 distinct answers,
- positive and negative caches are separately bounded (4,096 and 512 entries),
  clamp positive TTLs to one day and negative TTLs to 30 seconds, and preserve
  TTL-zero no-cache behavior. Concurrent requests for the same normalized name
  share one bounded singleflight result,
- IPv4-mapped IPv6 answers are canonicalized to IPv4 before de-duplication and
  policy evaluation,
- every distinct answer must pass the same generated classifier; a mixed
  allowed/denied answer set fails closed, and an over-cardinality set is
  rejected rather than partially authorized,
- the default permits only globally reachable unicast. Separate explicit
  policy bits may admit loopback for a local harness and RFC 1918/IPv6
  unique-local destinations for administrator-owned contexts. Carrier-grade
  NAT, link-local, metadata, documentation, benchmark, unspecified, multicast,
  broadcast, and reserved/future-use space remain denied,
- approved addresses enter a two-racer Happy Eyeballs connector with a 250 ms
  default fallback delay and at most 32 candidates. Losing racers are cancelled
  and closed; the winning `SocketAddr` is retained with the connection,
- the original URI authority remains the HTTP `Host` and TLS identity. A new
  physical direct connection, redirect hop, or proxy endpoint resolution runs
  the complete admission decision; approval is never transferred to an
  unclassified answer.

The generated classifier is derived from the LF-normalized, SHA-256-pinned IANA
IPv4 and IPv6 Special-Purpose Address Registry snapshots recorded in
`compat/iana-special-purpose.pin`, plus reviewed metadata, multicast, and
IPv4-compatible security overrides. `cargo xtask verify-contracts` fails if the
snapshots, generated Rust table, or `generated/http_destination_policy.json`
drift.

## Direct HTTP(S) Transport Gate

The executable Phase-3A direct transport is an immutable policy context shared
by fresh and resume workers. It owns destination admission, TCP/TLS establishment,
HTTP/1.1 connection reuse, and connection accounting; protocol workers own
request construction, response validation, body placement, and storage leases.

The public transport policy has these closed choices:

- schemes are `http` and `https`, with default ports 80 and 443,
- minimum TLS is `TLSv1.2` or `TLSv1.3`; TLS 1.2 is the default minimum,
- trust is the operating-system root store, one bounded custom PEM bundle, or
  their union,
- certificate verification is always enabled in this gate,
- the HTTP/1-only connector sends no ALPN extension and uses HTTP/1.1,
- client certificates, CA directories, native TLS, and HTTP/2 remain
  unavailable. Redirect and proxy policy are implemented above this direct
  transport and rebuild each request/route without weakening its pool identity.

One transport instance fixes the destination-policy, TLS, authentication, and
future proxy context used by its pool. A physical connection retains both a
socket permit and a worst-case connection-memory permit from connect admission
until it is closed, including while idle. Per-origin active/idle counts and the
idle timeout are bounded; disabling keep-alive sets the idle capacity to zero.
An incomplete response, cancellation, framing ambiguity, protocol error, TLS
error, or transport error permanently disqualifies that connection from reuse.
Only an exactly consumed and validated response may return its connection to
the pool.

A reused connection retains its original approved peer. Every new physical
connection performs fresh resolution and full destination admission before TCP
connect. For HTTPS, the original canonical URI host—not the numeric peer—is
used for SNI and certificate verification; an IP-literal URI is verified as an
IP subject alternative name. `Host` continues to use the original authority.

Custom PEM loading happens once while constructing the immutable TLS policy.
The file and certificate counts are hard bounded, an empty bundle is rejected,
and any malformed certificate rejects the entire bundle. CA bytes, private
keys, raw URLs, and credentials are not persisted or exposed in diagnostics.

## Executable Phase 3B Public Control Slice

`ariax --rpc-http SESSION_DB CONTROL_DIR OUTPUT_ROOT LOOPBACK_ADDR` serves only
`POST /jsonrpc` on an IP-loopback bind. `ariax --rpc-stdio` uses the same
dispatcher with one required, non-duplicated `Content-Length` header per frame.
Request bodies are capped at 2 MiB, responses at 8 MiB, stdio headers at 16 KiB,
and the HTTP listener at 64 established connection tasks. HTTP shutdown stops
accepting, gives current requests a five-second graceful window, drains worker
references, and then closes journals and the SQLite session owner.

The checkpoint implements only these request forms:

- `aria2.addUri([uris], [options])`, with one or more HTTP(S) mirrors and the
  reviewed options `dir`, `out`, `pause`, `split`,
  `max-connection-per-server`, `min-split-size`, `piece-length`,
  `connect-timeout`, `timeout`, `max-download-limit`, `lowest-speed-limit`,
  `checksum`, the bounded retry-policy options, and
  `verify-mirror-identity`,
- `aria2.tellStatus(gid)`, `aria2.pause(gid)`, and `aria2.remove(gid)`, each
  requiring exactly one full hexadecimal GID,
- `aria2.getGlobalStat()` with no arguments.

The configured output root is the only accepted `dir`; `out` must pass
`SafePathBuilder`. Unsupported options and malformed method arity fail before a
task row, source row, option snapshot, journal, or scheduler-visible task is
published. `tellStatus` reports durable and discarded lengths, discard-budget
consumption and remaining credit, active connections, retry count, and
packet-independent current/durable speed. Batch,
notifications, authentication, token handling, list methods, runtime option
mutation, and non-loopback listeners remain Phase 4 work.

## Request Preparation

Input:

```rust
pub struct HttpTaskInput {
    pub task: TaskId,
    pub gid: Gid,
    pub generation: Generation,
    pub uri: Uri,
    pub options: TaskOptions,
    // Present for resume/range; a fresh fixed-layout request settles this from
    // the validated response head before polling any body bytes.
    pub layout: Option<FileLayout>,
    pub output: SafePath,
}
```

Preparation:

1. apply URL-rule and per-download option snapshot before task creation,
2. resolve proxy/no-proxy policy,
3. resolve TLS trust config,
4. create the output path through the canonical `SafePathInput` /
   `SafePathBuilder` contract in `detailed-storage.md`,
5. open or create the journal and recover any durable layout and pieces,
6. decide fixed-identity or explicitly enabled growing-sequential mode,
7. for a fresh fixed-identity request, read only the response head, require an
   identity-coded known length, create and open the immutable `FileLayout`, and
   only then poll the response body,
8. for resume or range mode, require the recovered immutable layout before
   sending the request.

The path and journal are ready before network I/O. No response body byte is
accepted, buffered for placement, or submitted to storage before its layout is
open. A response head is not itself progress.

## User Header Policy

User-provided headers are parsed once into a validated, case-insensitive header
list. A field name must be an HTTP token, a value must contain no CR, LF, NUL, or
other forbidden control byte, and invalid syntax rejects the option/task before
any request is sent.

The request builder owns safety-critical and connection-specific fields. User
headers MUST NOT set `Host`, `Content-Length`, `Transfer-Encoding`, `Range`,
`If-Range`, `Accept-Encoding`, `Authorization`, `Proxy-Authorization`,
`Cookie`, `Content-Digest`, `Repr-Digest`, `Signature`, or `Signature-Input`. A
case-insensitive conflict or duplicate is rejected; it is never resolved by
last-write-wins. The generated value is authoritative inside the request
builder as a defense in depth.

Other custom headers retain their configured order. Duplicate fields are
allowed only where the HTTP field definition permits list/repeated semantics;
ambiguous singleton duplicates are rejected. Every redirect rebuilds the
request from the validated custom-header list and re-runs this policy rather
than forwarding the prior header map. Credentials and cookies are added only by
their origin-scoped subsystems (see `redirect-policy.md`).

## Content Encoding And Range Requests

Byte offsets in `Range`/`Content-Range` and the immutable `FileLayout` are in
representation-byte space. A decoded body is in a different byte space, and a
chunked body has no final extent when the layout must be opened. The first slice
therefore has one fixed-layout contract:

- Fresh sequential, range, split, and resume requests send
  `Accept-Encoding: identity`.
- The response must have no non-identity `Content-Encoding` and must establish a
  final length: `Content-Length` for `200`, or a validated known total in
  `Content-Range` for `206`.
- `Transfer-Encoding: chunked`, a missing final length, or a non-identity content
  coding is rejected before body polling in fixed-layout mode. Policy may try a
  different source, but it MUST NOT silently reinterpret the response as a
  fixed-layout stream or fall back from range to a decoded nonzero-offset write.

Unknown-length or decoded sequential downloads require the separate
`GrowingSequential` storage capability. That mode is explicit, is not enabled
in the first slice, and never activates as an automatic fallback. Its storage
contract must provide an append-only growing layout, an administrator/user
configured maximum decoded length, distinct wire-byte and decoded-file-byte
counters, and a durable final-extent commit before hashing, resume metadata, or
completion is published. `GrowingSequential` starts at offset `0` and cannot be
combined with range, split, or resume.

## Validators

Tracked validators:

```rust
pub struct EntityValidator {
    pub etag: Option<String>,
    pub last_modified: Option<HttpDate>,
    pub content_length: Option<u64>,
    pub digest: Option<ContentDigest>,
    pub weak: bool,
}
```

Resume requires one of:

- strong ETag match,
- Last-Modified plus unchanged length when no better validator exists,
- configured checksum/digest,
- explicit unsafe user override.

`If-Range` is populated only when a strong validator (strong ETag, or a
Last-Modified the server can be trusted to compare strongly) is available. When
only a weak validator exists, `If-Range` is omitted and resume relies on the
durable-length plus Last-Modified comparison, with any change classified as
`StaleValidator` below.

If validator changes, classify as `StaleValidator`, not transient network
failure.

The current executable checkpoint implements the first item for sequential
downloads and for range tasks that settle on one source: a strong ETag whose
raw quoted value, exact resource fingerprint, and settled total length are
persisted in `HttpStrongValidator`. It also implements configured SHA-256
digest-bound range recovery. Recovery first verifies every durable piece
against its journaled SHA-256 digest without opening a network connection.
Strong-validator recovery then requires the newly probed source to reproduce
the exact binding before pending ranges can run; checksum-bound recovery may
continue across a weak or changed server validator and verifies the complete
descriptor-bound file before terminal publication. A fully durable checksum-
bound file is reverified and completed locally without a network connection.
For a mid-generation stale response, `fail` stops immediately; `revalidate`
aborts the current lease, probes the submitted source again, and only retries
when the original final URI, total length, and validator identity are restored.
A configured whole-file checksum may accept a changed validator at that same
resource because terminal publication still requires the exact expected
digest. `restart-if-safe` is bounded by the task attempt cap and is authorized
only after descriptor-bound reopening proves that the partial file is the
task-owned output. The new `GenerationStarted(reason=representation_restart)`
record clears old durable evidence before any replacement layout or lease can
be admitted; crashes before or after the replacement layout therefore cannot
publish mixed bytes. Last-Modified and unsafe-override resume remain
design-level policy and are not accepted by the checkpoint runner.

## Cross-Mirror Entity Identity

Multi-URI HTTP treats TAB-separated URIs as mirrors of the same entity. Like
aria2, the default trusts that assertion: concurrent split runs range workers
against all listed mirrors. This preserves aria2 behavior and the common workflow
of pasting a mirror list with no checksum.

HTTP metadata cannot *prove* cross-mirror byte-identity, and it is worth being
explicit about why, because it bounds what the safe modes can offer:

- `ETag` is opaque and origin-scoped (RFC 9110). It is only comparable against
  the same resource on the same server; two mirrors serving identical bytes
  routinely emit different ETags (different derivation from inode/mtime/size, or
  different CDN schemes), and the algorithm is not specified. Comparing ETags
  across mirrors is therefore meaningless — it both rejects valid mirrors and
  fails to prove identity on a coincidental match.
- `Last-Modified` and `Content-Length` are even weaker; equal length with a
  differing body is exactly the residual corruption case.

The only thing HTTP metadata *can* do cheaply is reject an obvious mismatch: a
mirror whose total length disagrees with the task total is rejected by the
`Content-Range` known-total check regardless of mode. What no metadata catches is
two mirrors of equal length serving different bytes; only content-level
verification catches that.

The behavior is selected by `--verify-mirror-identity`:

- `off` (default, aria2-compatible): trust the mirror list. Concurrent split runs
  across all mirrors. If a whole-file or Metalink checksum is configured it is
  still verified at the end, so corruption is detected before completion, just not
  before bytes are written. This matches aria2's guarantee.
- `strict` (opt-in): concurrent multi-mirror split is admitted only when
  the assembled result will be verified by a shared content digest — Metalink
  per-chunk checksums (see `metalink-chunking.md`, preferred because divergence is
  caught per chunk rather than at end-of-file), a `Content-Digest`/`Repr-Digest`
  (RFC 9530) identity tuple that matches on every mirror, or a user-supplied
  whole-file checksum. The RFC 9530 tuple includes field kind, algorithm, digest
  value, covered representation, content coding, and covered byte range (whole
  entity or the exact same range); matching only the algorithm is insufficient.
  When no such digest is available, split is restricted to a single mirror
  (whose own `ETag`/`Last-Modified` via `If-Range` keeps it self-consistent across
  its own range responses — a valid per-origin guarantee); the other URIs remain
  sequential-download or restart fallbacks.

The executable strict-mode checkpoint implements both the single-mirror strong-
ETag fallback and concurrent mirrors backed by a user-supplied SHA-256 checksum.
The checksum is persisted in the immutable task option snapshot; the assembled
file is read through a duplicated descriptor capability on a process-bounded
blocking hash worker, and a mismatch leaves all piece evidence nonterminal.
RFC 9530 identity tuples, Metalink chunk hashes, and additional user checksum
algorithms remain pending.

A redirect target never joins the mirror pool merely because a redirect was
followed. Its admission and per-lease exclusivity follow `redirect-policy.md`.

## Sequential Download

Sequential fresh request:

```text
GET uri
Accept-Encoding: identity
```

Acceptance:

- `200 OK` allowed,
- a valid `Content-Length` is required and fixes the immutable layout before
  the body is polled,
- `Transfer-Encoding: chunked`, missing length, and non-identity content coding
  are rejected in the first slice,
- body bytes start at global offset `0`,
- one `TransferAttemptId` identifies the response stream, which remains open and
  is continuously read into bounded buffers and submitted to storage at exact
  offsets,
- the stream advances through piece-aligned storage `LeaseId`s. Reaching a lease
  boundary commits/checkpoints that exact span and immediately opens the next
  lease on the same response; it does not issue another HTTP request or buffer a
  whole lease,
- client download-rate tokens gate response-body polling/`read`. After bytes are
  accepted and charged, their `write_at`/disk completion is governed only by
  storage backpressure and is never delayed a second time by the rate limiter.

Completion:

- each complete non-final checkpoint lease commits after its exact span reaches
  disk under the validated response head,
- the final lease commits only when received length exactly matches the settled
  response length and EOF/framing is valid,
- premature EOF, cancellation, or transport failure aborts only the current
  incomplete lease. Earlier committed/durable checkpoints remain resumable if
  the representation validator remains valid,
- a validator or required whole-representation digest failure invalidates the
  affected representation through the normal restart/hash-failure rules rather
  than trusting earlier checkpoints,
- final validators are persisted,
- all pieces durable,
- finalization completes through storage.

## Resume Download

Resume request:

```text
Range: bytes=<durable_length>-
If-Range: <strong validator when available>
Accept-Encoding: identity
```

Acceptance:

- `206 Partial Content` required,
- exactly one `Content-Range: bytes start-end/total` is required; `start` equals
  the durable length, `end` equals `total - 1`, and `total` equals the recovered
  layout length,
- exactly one `Content-Length` is required and equals `total - start`; the first
  milestone intentionally rejects a shorter valid sub-range response,
- the response carries one matching strong ETag and identity encoding,
- local existing bytes are not truncated.

The resume response is one new `TransferAttemptId` and a streaming series of
piece-aligned storage leases beginning at the durable prefix. Intermediate exact
spans checkpoint on the same response/data stream. Premature EOF, cancellation,
or transport failure aborts only the current incomplete lease; earlier
checkpoints remain resumable when `If-Range`/validator policy still proves the
same representation. The final lease still requires exact EOF/framing.
Redirect or validator failure before body acceptance aborts the current lease;
a representation-level validation failure restarts/invalidates the affected
generation rather than preserving checkpoints from a different entity.

If server returns `200 OK`:

- never write it at nonzero offset,
- if policy allows restart, create a new generation and full restart from
  offset `0`,
- otherwise fail with resume/range unsupported error.

If server returns `416`:

- the runner classifies it as an invalid-range restart-generation result;
- a fully durable local representation is completed locally before a network
  request, so the first milestone does not rely on a server `416` as a
  completion proof.

## Range Worker

```rust
pub struct RangeLease {
    pub id: LeaseId,
    pub task: TaskId,
    pub generation: Generation,
    pub start: u64,
    pub end_inclusive: u64,
    pub mirror: UriId,
    pub attempt: u32,
}
```

Request:

```text
Range: bytes=start-end
Accept-Encoding: identity
```

Acceptance:

- status must be `206`,
- `Content-Range` must exactly match start/end and known total,
- body length must equal `end - start + 1`,
- `Content-Encoding` must be absent or identity (see Content Encoding And Range
  Requests); a content-coded body is rejected, not decoded,
- total length must match the task total (via `Content-Range` known-total). In
  strict mode (`--verify-mirror-identity=strict`), concurrent multi-mirror split
  additionally requires the task to satisfy the digest requirement in Cross-Mirror
  Entity Identity before this lease is admitted.

After the response head passes these checks, the worker issues `BeginLease` for
the unique `(generation, LeaseId, attempt)` before polling body bytes. Every
body write is provisional under that `LeaseId`; endgame duplicates also carry
their shared `OverlapGroupId`. The worker may issue `CommitLease` only after the
exact expected body length and all applicable validator/digest checks pass. For
an overlap group this selects an in-memory candidate, cancels competitors, and
waits for storage to fence all competing writes before a final
`LeaseCommitted` acknowledgement.

If a competitor wrote any bytes, failed validation after writing, or cannot be
cancellation-confirmed, storage rolls back the entire overlap group to pending
metadata and clears the touched pieces' in-memory written/verified state. It
does not restore the file contents; the next ordinary lease overwrites those
untrusted offsets. No affected piece is eligible for `PieceDurable` before this
settlement.

For the executable HTTP/1.1 boundary, `endgame-max-duplicates` is a bounded
task option with default 2, accepted range 0 through 8, and at most one extra
attempt for any original lease. Zero disables endgame. Admission begins only
when remaining non-durable work is at most `max(8 MiB, 2 * piece_length)` and an
opened original is within `max(1 second, 25% of response_body_timeout)` of its
between-byte deadline. The duplicate consumes the task-wide duplicate cap,
normal split/origin/transport permits, ingress and rate tokens, and retry
attempt accounting. It never targets a durable piece.

The first exact-length completion receives `LeaseCommitPending`. The worker
then cancels the one competitor and waits for that attempt's join/cancellation
confirmation. A clean loser with zero accepted disk bytes is journal-aborted
before the candidate receives `LeaseCommitted` and `PieceDurable`. If the loser
wrote any bytes, storage journal-aborts both members, returns
`SpanRolledBack`, and the coordinator exposes the piece only as pending. A
process crash before settlement replays only provisional lease starts and
therefore also exposes the piece as pending.

Each body buffer is mapped to:

```text
global_offset = lease.start + bytes_already_accepted_in_lease
```

Oversized body:

- stop reading body,
- issue `AbortLease`,
- reject response,
- penalize mirror,
- do not expose any provisional byte as progress.

Short body:

- issue `AbortLease` for the entire attempt,
- return the lease span to retry policy; the first slice does not promote a
  partial response into a new committed suffix/prefix lease.

## HTTP Status Handling

Status handling delegates retry decisions to `RetryPolicy`, but correctness
checks run first.

Examples:

- `301/302/303/307/308`: redirect policy (`redirect-policy.md`), budgeted
  separately from retry; cross-origin credential stripping and validator
  revalidation apply.
- `401/407`: auth challenge only if credentials policy can change request.
- `404`: `max-file-not-found` unless user status-code policy overrides.
- `408/425/429/500/502/503/504`: retryable only if policy says so; `501` is
  not in the conservative default set.
- `503` with `Retry-After`: retry only if status is retryable, then cap delay.
- invalid `206`: range-invalid mirror, not successful response.
- `200` to range: sequential fallback only at offset `0` and policy allows.

## Retry Integration

Worker failure emits:

```rust
pub struct RetryEvent {
    pub transfer_attempt: Option<TransferAttemptId>,
    pub lease: Option<LeaseId>,
    pub span: GlobalSpan,
    pub uri: UriId,
    pub class: RetryClass,
    pub http_status: Option<u16>,
    pub retry_after: Option<RetryAfter>,
}
```

Scheduler decides:

- same mirror retry,
- different mirror retry,
- smaller lease retry,
- sequential fallback,
- stale-validator fail, fresh revalidation, or fenced representation restart,
- terminal error.

Retry wait releases buffers and does not block worker threads.

## Stats Integration

Counters:

- raw body bytes read from the transport,
- bytes accepted by HTTP validator,
- provisional bytes submitted to storage,
- bytes committed by `CommitLease` (useful logical progress),
- bytes durable,
- discarded bytes and discard-budget consumption (already charged at body
  ingress),
- retry bytes,
- current lease progress.

Rules:

- speed sampler runs on monotonic tick,
- no packet arrival means current speed reaches zero,
- backpressure and rate-limit diagnostic conditions are distinct from stalled,
- a worker acquires rate credit before body polling. Once a frame/chunk is read
  and charged, its disk write is not rate-delayed; discarded bodies keep the
  debit and also consume the separate discard guard,
- the finite discard hierarchy is checked before each body poll and charged
  atomically at process, final-origin host, task, and attempt scopes; short,
  oversized, cancelled, retry, checksum-failed, stale queued, and endgame-loser
  bytes have no refund path,
- lease retry waits remain visible in status.

## Storage Integration

Every HTTP response attempt has a unique `TransferAttemptId`. A closed range
response normally owns one `LeaseId`; a fresh sequential or open-ended resume
response advances through many piece-aligned storage leases while the same body
stream remains open. After response-head validation and before polling bytes for
each span, the worker submits a storage-owned `LeaseWritePlan` through
`BeginLease`. For each accepted body chunk:

```rust
let block = WriteBlock {
    task,
    generation,
    lease: lease.id,
    global_offset,
    expected_len: chunk.len(),
    buffer,
    piece,
};
storage.write(block).await
```

At an exact intermediate sequential checkpoint, the worker commits that storage
lease and opens the next one without another HTTP request. A closed range or the
final sequential lease additionally requires exact response completion. On an
unsuccessful exit it aborts the current incomplete lease; earlier sequential
checkpoints remain only when representation-validator policy permits. These
commands and their journal/recovery semantics are owned by
`detailed-storage.md`; the HTTP adapter does not create an alternate
provisional-state format. `PieceDurable` cannot be emitted for bytes belonging
only to a begun or aborted lease.

HTTP worker must not hold a mutable buffer after submitting it to storage.

If storage rejects:

- stop worker,
- issue `AbortLease` unless storage has already returned a terminal
  `LeaseAborted` disposition for that `LeaseId`,
- classify disk/storage error,
- do not retry as network unless rejection is stale generation from
  cancellation.

## Shutdown/Pause

Pause:

- scheduler cancels generation,
- workers stop reading,
- every begun attempt receives `AbortLease`,
- in-flight storage completions drain,
- non-durable spans return pending,
- journal records `TaskPaused`.

Shutdown:

- follows durability mode,
- can stop accepting new tasks,
- waits for configured graceful timeout,
- persists session state before exit.

## Tests

Required tests:

- fresh `200 OK` sequential writes at offset `0`,
- fresh fixed-layout request sends `Accept-Encoding: identity`, requires a
  valid `Content-Length`, and opens the layout before polling the body,
- chunked, missing-length, gzip, and deflate responses are rejected in the first
  slice without accepting a body byte,
- resume sends range from durable length,
- strong ETag/resource/length material survives live replay and compact
  checkpoint replay, while weak/malformed/oversized values are rejected,
- recovery reads back every trusted durable piece and rejects local digest
  corruption before opening a network connection,
- resume `206` validates `Content-Range`,
- resume sends the exact persisted strong ETag in `If-Range` and requires the
  full remaining tail in the first milestone,
- resume `200 OK` never writes at nonzero offset,
- range `200 OK` rejected for nonzero lease,
- invalid `Content-Range` rejected,
- a short body aborts the current incomplete checkpoint lease, preserves prior
  committed checkpoints under a valid validator, and exposes no committed
  progress for the aborted span; a short closed-range body aborts its whole
  lease,
- an oversized body stops at the bounded validation read, aborts the current
  lease, and penalizes the source,
- `Retry-After` capped and visible,
- stuck socket speed drops to zero,
- disk backpressure stops reads,
- pause races with body read and disk completion,
- stale validator creates restart/fail decision,
- `fail` publishes a terminal stale-validator error without releasing the
  rejected range,
- `revalidate` restores the original validator through a fresh probe before a
  retry and fails closed when that identity is not restored,
- `restart-if-safe` advances exactly one persisted generation, clears old
  durable pieces at the generation record, reopens the exact task-owned file,
  redownloads every piece, and leaves the final file with no mixed old bytes,
- a crash after the staged next-admission snapshot or after
  `GenerationStarted(reason=representation_restart)` replays without admitting
  old progress into the new generation,
- range/resume requests send `Accept-Encoding: identity`,
- content-coded body to a range request is rejected, not written,
- strict concurrent multi-mirror split is refused unless the task has a shared
  content digest (Metalink chunk hashes, an exactly matching RFC 9530 identity
  tuple on every mirror, or a whole-file checksum); otherwise strict mode is
  restricted to a single mirror,
- a user SHA-256 checksum admits strict concurrent mirrors, survives option
  persistence/recovery, writes the matching final digest into `TaskComplete`,
  and rejects a divergent equal-length mirror without terminal completion,
- a fully durable checksum-bound task rehashes and completes or rejects locally
  without reconnecting, and partial checksum-bound recovery can retain verified
  local pieces despite a weak server ETag,
- equal-length mirrors with different RFC 9530 digest values, coverage, or
  representations fail the strict identity gate even if their algorithms match,
- normal split under `off` retains the documented aria2-compatible residual
  risk, but cross-mirror endgame races and implicit redirect-target pool
  admission are prohibited without the stronger gates in `split-download.md`,
- a dirty endgame group exposes no committed progress, clears touched in-memory
  piece state, and is overwritten by a later ordinary range request without
  truncating or restoring the file,
- a mirror whose total length disagrees with the task total is rejected by the
  known-total check,
- every storage lease begun by `BeginLease` ends in exactly one `CommitLease` or
  `AbortLease`, including redirect, pause, short body, and storage rejection,
- interruption at a checkpoint boundary, one byte before a boundary, and one
  byte into a new lease resumes from the exact committed prefix,
- RPC status reflects durable bytes, not just received bytes.
