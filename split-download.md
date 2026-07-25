# Split Download Design

Status: draft.

Decision: use dynamic non-overlapping range leases, not overlapping
"connection 1 downloads 0-100%, connection 2 downloads 50-100%" style ranges.

Each active connection owns one leased byte range at a time. When it finishes,
it asks the segment scheduler for another range. This keeps placement simple,
avoids duplicated writes, and lets the scheduler adapt lease size and mirror
choice.

Terminology:

- `piece` or `verification chunk`: fixed durability/checksum unit.
- `range lease`: dynamic transfer unit assigned to a worker.
- `block`: optional smaller bookkeeping unit inside a piece for partial retry.

Retry is primarily bounded to the failed range lease or failed byte span, not to
a fixed piece. A fixed piece is redownloaded only when checksum/recovery rules
cannot safely reuse its partial bytes.

## Range Model

A file is represented as ordered fixed pieces for durability:

```text
[0..1MiB) [1MiB..2MiB) [2MiB..3MiB) ...
```

The scheduler then creates dynamic range leases over pending byte spans:

```text
lease 17: [0..512KiB)
lease 18: [512KiB..2MiB)
lease 19: [8MiB..10MiB)
```

Each byte span has state:

```text
Pending
Leased { worker, mirror, lease_id, deadline }
WritingProvisional { worker, lease_id, offset, len }
Committed { lease_id }
Verifying
Durable
RetryWait { reason, next_time, attempts }
Failed
```

Only one worker may own a byte span unless the task enters explicit endgame
mode. Network completion alone never advances a span from provisional writing
to committed: the storage-owned `CommitLease` operation does so atomically after
the protocol's exact-length and validator/digest checks pass.

## Worker Assignment

Normal HTTP split:

```text
worker A: lease [0..1MiB)
worker B: lease [1MiB..2MiB)
worker C: lease [2MiB..3MiB)
...
```

After worker A completes its lease, it requests the next pending span. The
scheduler may assign `[8MiB..10MiB)`, not necessarily the next contiguous span,
depending on piece
selection, mirror health, sequentiality, and file selection.

For a single file with `split=8`, this means up to 8 non-overlapping ranges are
in flight. It is not one permanent connection per fixed piece for the whole
download. It is a range-worker pool.

## Lease Size

Initial range lease size derives from:

- `min-split-size`,
- `piece-length`,
- Metalink chunk checksum boundaries,
- BitTorrent piece boundaries when cross-protocol sharing is active,
- file size,
- profile,
- disk pressure.

The scheduler can resize future leases:

- larger leases for high-throughput stable mirrors,
- smaller leases for many mirrors or fairness,
- sequential leases for HDD-like disks,
- checksum-aligned leases for Metalink when beneficial,
- smaller leases near the tail or after failures.

Already issued leases are not resized in place. If a lease fails, its remaining
span can be split into smaller retry leases.

When Metalink chunk hashes are active, fixed verification chunks come from the
Metalink metadata. Leases should prefer those boundaries, but may cover multiple
Metalink chunks if streamed sub-hashing can verify each chunk without disk
readback. See `metalink-chunking.md`.

## Endgame Mode

Near completion, slow leased chunks can dominate finish time. Endgame mode is a
controlled exception to the one-owner rule.

Allowed behavior:

- duplicate request only for a range whose original lease is slow or near
  timeout,
- each duplicate has a distinct `LeaseId`; `BeginLease` and all writes remain
  provisional until one attempt is eligible for `CommitLease`,
- a duplicate against the same mirror is allowed only when both requests are
  pinned by the same strong per-origin validator (`If-Range` where applicable),
- a duplicate against a different mirror is allowed only when the exact range is
  covered by a shared per-chunk/range digest that can be checked before commit.
  Strict mode backed only by a whole-file checksum is not sufficient for this
  same-offset race,
- when a piece/chunk digest exists, the first exact-length, hash-valid attempt
  whose `CommitLease` becomes the candidate wins the network race. With no
  range-verifiable digest, endgame is confined to the same validator-pinned
  mirror,
- every duplicate belongs to one `OverlapGroupId`. A candidate commit freezes
  the group, cancels competitors, and waits for all accepted competing disk
  operations to complete or receive cancellation confirmation before it can
  become `LeaseCommitted`,
- if no competing attempt wrote physical bytes, the candidate commits normally.
  If a competitor wrote, failed validation after writing, or has uncertain
  cancellation, storage aborts the whole overlap group, clears in-memory
  downloaded/verified state for every touched piece, and returns those pieces to
  pending,
- overlap rollback does not restore or zero the file. The untrusted physical
  bytes remain just like preallocation contents and are overwritten by the next
  ordinary download lease. No `PieceDurable` record is emitted for them,
- loser bytes consume normal received-payload rate tokens and the discard guard;
  the bounded duplicate cap prevents unbounded waste,
- duplicate data is never written over a durable piece,
- endgame duplicate budget is small and bounded by an explicit
  `endgame-max-duplicates` cap (default small, e.g. 2 concurrent duplicates).

This is similar in spirit to BitTorrent endgame requests, but for HTTP/SFTP
ranges. It is not used for the whole download, and FTP does not use it because
FTP sources are sequential in this design. The conservative dirty-overlap rule
may discard a candidate that was actually correct; correctness takes precedence
over preserving ambiguous shared-file bytes, and no scratch/undo store is
required.

## Retry Model

Retries are per range lease or failed byte span, per mirror, and per error
class. Piece-level retry is a fallback when the verification unit cannot be
trusted.

The full user-configurable retry surface is defined in `retry-policy.md`.
This section defines how retry decisions map back to range leases and
verification pieces.

Retry state tracks:

- lease id and byte range,
- covered piece ids,
- mirror/URI,
- HTTP status or transport error,
- attempts on this mirror,
- attempts across all mirrors,
- backoff deadline,
- validator state,
- bytes written before failure,
- storage lease disposition (`provisional`, `committed`, or `aborted`).

Errors:

- transient network reset: retry the same byte span on the same mirror only if
  retry policy permits that error class and mirror; otherwise choose another
  eligible mirror or fail the lease when retry limits are exhausted.
- timeout/lowest-speed violation: retry same span on another mirror if
  available.
- `5xx`: backoff mirror, retry span elsewhere.
- `404`/file-not-found: increment file-not-found counter for that mirror.
- invalid `206`/`Content-Range`: mark mirror range-bad and do not use it for
  split unless revalidated.
- `200 OK` to range: if at offset 0 and policy allows, switch task to
  sequential fallback; otherwise mark mirror range-unsupported for split.
- short body: issue `AbortLease` for the complete attempt and retry the lease
  span; no prefix from that response becomes committed progress, and its bytes
  keep their ingress rate debit, are surfaced as discarded, and consume the
  discard guard.
- oversized body: stop the bounded validation read, issue `AbortLease`, reject
  the response, and penalize the mirror; the bounded overrun keeps its rate
  debit and consumes the discard guard.
- disk full/permission: task-level failure or pause, not network retry.
- checksum failure: redownload the affected verification piece or failed spans
  from a different mirror when
  possible; repeated failures can mark mirror corrupt.

Backoff:

- jittered exponential backoff per mirror/error,
- global `max-tries`, lease-level caps, and optional piece-level caps after
  checksum failure,
- retry budget visible in status/RPC,
- retry wait does not hold transfer buffers.

## Partial Chunk Failure

If a worker fails after writing some bytes:

- issue `AbortLease` for that `LeaseId`; its bytes remain physically
  overwriteable but are invisible to progress and recovery,
- return the complete lease span to pending/retry and create a new `LeaseId` for
  the next attempt,
- redownload the whole fixed verification piece only when checksum rollback
  invalidates other committed spans in that piece.

The first slice does not promote a short response's prefix into a second lease.
A future partial-reuse optimization must be an explicit storage operation that
proves span, generation, and hash state; a protocol worker cannot infer reuse
from file contents or from acknowledged provisional writes.

## Mirror Selection

The scheduler considers:

- URI selector mode: inorder, feedback, adaptive,
- server stats,
- current per-host connection count,
- observed throughput,
- error rate,
- range support,
- checksum failures,
- proxy/no-proxy constraints,
- protocol priority from Metalink.

A mirror can be:

```text
Healthy
Slow
RangeUnsupported
RangeInvalid
TemporarilyFailed
Corrupt
Exhausted
```

Range unsupported mirrors can still be used for sequential fallback or
single-connection downloads if policy allows.

## Cross-Mirror Entity Identity

Concurrent split across multiple mirror URIs is safe only if every mirror serves
the byte-identical entity. TAB-separated URIs are *asserted* to be mirrors of the
same entity (`configuration.md`); like aria2, this assertion is trusted by
default. The residual risk is narrow but real: two mirrors serving
same-length-but-different content would interleave into silent corruption, and no
HTTP metadata can detect it.

HTTP metadata cannot prove cross-mirror identity. `ETag` is opaque and
origin-scoped (RFC 9110): two mirrors serving identical bytes commonly return
different ETags, and the derivation is unspecified, so comparing ETags across
mirrors is meaningless. `Last-Modified`/`Content-Length` are weaker still. Only a
shared content digest can actually establish identity.

Default behavior (`--verify-mirror-identity=off`, aria2-compatible):

- Trust the mirror list and split concurrently across all eligible mirrors, as
  aria2 does.
- A total-length mismatch is still rejected by the `Content-Range` known-total
  check, so obviously-different mirrors are dropped; only the equal-length
  differing-content case slips through undetected.
- If the task has a whole-file or Metalink checksum, final verification catches a
  divergent mirror at end-of-file (Metalink per-chunk checksums catch it per
  chunk, much earlier).

Strict opt-in (`--verify-mirror-identity=strict`):

- Concurrent multi-mirror split is admitted only when the assembled result will
  be verified by a shared content digest: Metalink per-chunk checksums (see
  `metalink-chunking.md`, preferred because divergence is caught per chunk), a
  `Content-Digest`/`Repr-Digest` (RFC 9530) identity tuple present on every
  mirror, or a user-supplied whole-file checksum. An RFC 9530 identity tuple
  matches only when field kind, algorithm, digest value, covered
  representation, content coding, and covered range all match; equal algorithms
  alone do not establish identity.
- Without such a digest, split is restricted to a single mirror. That origin's own
  `ETag`/`Last-Modified` (via `If-Range`) keeps it self-consistent across its own
  range responses — a valid per-origin guarantee; only the cross-origin
  comparison is invalid. Other URIs remain sequential-download or restart
  fallbacks, not concurrent split sources.

A redirect target is not added as another eligible mirror implicitly. Under
`off`, it is at most an exclusive replacement for the source of the redirected
lease; under `strict`, it must pass the same full identity gate before joining
the pool. Cross-mirror endgame has the stronger range-verifiable digest rule
above. See `redirect-policy.md`.

## Fallback To Sequential

Split download requires:

- known total length,
- range support,
- exact range validation,
- output layout ready,
- identity content-coding: range/split requests send `Accept-Encoding: identity`,
  because `Range`/`Content-Range` offsets are in encoded space while a decoded
  body is in decoded space. A chunked, unknown-length, or content-decoded
  response cannot enter split mode. The explicit growing-sequential capability
  is never a split fallback; see `detailed-http-first-slice.md`.

If unavailable:

- with multiple mirrors, try another mirror for split,
- with one mirror returning valid `200 OK` at offset 0, fall back to sequential
  if resume policy allows,
- if resuming and server cannot provide `206`, fail or restart according to
  `always-resume`, `continue`, and overwrite policy,
- never write a full `200 OK` response at a nonzero offset.

## Cross-Protocol Downloads

For Metalink or mixed sources:

- HTTP and SFTP workers lease random-access chunks from the same scheduler,
- FTP sources are sequential fallback/failover sources and do not participate
  in simultaneous arbitrary-range leasing or endgame; see
  `detailed-ftp-sftp.md`,
- checksum boundaries drive chunk states,
- BitTorrent full build may run in libtorrent and report completed pieces
  separately; cross-protocol shared storage is a later advanced phase.

## Cancellation

When a task is paused or removed:

- scheduler stops issuing leases,
- active workers receive cancellation,
- each begun but uncommitted attempt receives `AbortLease`; disk in-flight
  writes drain but cannot become committed after the abort/generation change,
- non-durable chunks return to pending on resume,
- durable chunks remain complete.

## Status Reporting

Expose per task:

- total pieces and active leases,
- pending/leased/provisional/committed/verifying/durable/retry/failed counts,
- active workers,
- lease size range,
- retry counts by reason,
- mirrors disabled for range or corruption,
- endgame duplicate count.

## Tests

- every begun normal, retry, redirected, and endgame attempt terminates in
  exactly one `CommitLease` or `AbortLease`,
- a short/oversized response leaves the complete lease span pending and exposes
  no committed prefix,
- same-mirror endgame requires one strong validator and commits at most one
  attempt,
- an endgame candidate commits only when all competitors are fenced without a
  completed competing write,
- a competitor that wrote, failed validation after writing, or has uncertain
  cancellation rolls every touched piece back to pending while leaving physical
  bytes in place for the next lease to overwrite,
- crash during overlap settlement leaves no `LeaseCommitted`/`PieceDurable` and
  replays the group as pending,
- cross-mirror endgame is refused without an exact range-verifiable digest,
- two RFC 9530 responses with the same algorithm but different value, coverage,
  representation, or range fail the strict identity gate,
- a redirect target under `off` cannot become an additional concurrent mirror,
- FTP sources are never assigned noncontiguous leases or endgame duplicates.
