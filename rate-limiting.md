# Rate Limiting Design

Status: draft.

`max-overall-download-limit`, `max-download-limit`, `max-overall-upload-limit`,
and `max-upload-limit` are listed as implemented-live options, and a token bucket
is referenced by the stats and runtime docs, but the limiter itself was
unspecified. This document defines the token-bucket hierarchy, fairness,
precedence relative to read-backpressure, and reconciliation with libtorrent.

## Bucket Hierarchy

Rate limiting is a hierarchy of token buckets, checked from broad to narrow. A
rate-accounted action may proceed only when every bucket on its path has tokens.
For downloads, the rate-accounted action is publication through `CommitLease`;
raw receive is separately bounded by backpressure and the discard guard.

```text
global (max-overall-*-limit)
  -> per-host bucket
       -> per-task bucket (max-*-limit)
```

- The global bucket is the hard ceiling for user-rate-accounted committed
  progress (both directions have their own global bucket); raw discarded input
  is instead limited by the separate discard guard.
- Per-host buckets are optional fairness buckets so one host cannot starve
  others under a shared global cap.
- Per-task buckets enforce `max-download-limit`/`max-upload-limit`.

The configured download buckets account only for bytes that `CommitLease`
accepts toward a lease/piece. That is the ariax user-facing rate-limit semantic:
short or oversized responses, aborted provisional writes, losing endgame
attempts, checksum rollback, redirect/cancellation races, and other discarded
payload do not consume `max-overall-download-limit` or
`max-download-limit`. Stats separately report transport, committed, durable, and
discarded bytes so users can distinguish useful progress from wasted bandwidth.

For accounting purposes, the covered bytes are HTTP body bytes after transfer
framing and before any content decoding, FTP data-channel bytes, SFTP file-data
payload, and the corresponding libtorrent payload counter. Protocol headers,
TLS records, and TCP/IP overhead are outside the aria2-compatible application
rate limit.

User rate tokens are acquired at `CommitLease`, after exact response framing and
validator/digest checks have selected the committed span. A completed provisional
lease waits for bucket credit without holding a socket buffer; when credit is
available, its commit and the corresponding token debit happen atomically from
the scheduler's perspective. An aborted lease consumes no user-rate tokens.

Large validated leases accumulate a pending commit credit in bounded token
quanta under the same fair scheduler; no bucket must hold the whole lease length
at once. Only after credit equal to the exact committed span has accrued does the
scheduler issue the atomic `CommitLease`. This can delay publication of a fast
raw response, but it preserves the configured committed-progress rate without
holding payload buffers or treating an aborted response as user-rate usage.

The read path is instead bounded by storage/backpressure and the separate
discard guard below. It cannot issue unbounded provisional work: per-task
in-flight lease and buffer limits still apply, and a worker does not start a
replacement lease until the prior attempt commits or aborts. This deliberately
makes `max-*-limit` a committed-progress limit rather than a raw-wire limit.

## Bucket Parameters

- Each bucket has a rate (bytes/sec) and a burst capacity (max accumulated
  tokens). Default burst is a small multiple of the rate (e.g. 1 second worth),
  bounded so a long-idle bucket cannot release a huge burst.
- Refill is computed from the monotonic clock on acquisition (lazy refill), not a
  timer thread, so the limiter adds no periodic wakeups.
- A rate of 0 means unlimited (the bucket is bypassed), matching aria2.

## Fairness Across Many Streams

When ~1,000 completed/provisional range streams share one global cap:

- Tokens are handed out in bounded chunks with a fair queue (round-robin or
  deficit round-robin across waiting workers), so no worker is starved and no
  worker monopolizes a refill.
- A lease whose validated commit cannot get tokens does not spin; it registers
  for wakeup when the bucket next refills enough for its committed span.
- Fairness is per-host first, then per-task, so a single multi-connection host
  does not crowd out others under the global cap.

## Precedence Relative To Read-Backpressure

Backpressure controls socket reads; the user rate limiter controls publication of
validated provisional work, so their precedence must be explicit:

- Backpressure (disk saturated, buffer budget exhausted) takes precedence: if the
  storage path cannot accept bytes, the worker does not read even if a later
  commit would have rate credit. Reading would only fill buffers that cannot be
  drained.
- After a response completes validation, the user limiter applies before
  `CommitLease`. If tokens are not available, the worker releases its payload
  buffers and waits with only bounded lease metadata; it does not keep reading
  another range for that worker.
- The runtime read flow therefore uses storage queue credit and buffer lease
  checks, while the scheduler/commit flow performs the user-token acquisition.
  `detailed-runtime.md` must model this as a commit gate rather than charge
  uncommitted body bytes.

## Discard Bounds

Discarded bytes are excluded from the configured user limit, but they must not
become an unbounded raw-network bypass. A separate discard guard records raw
payload and enforces finite per-attempt, per-task, per-host, and global discard
budgets. These budgets are safety policy, not hidden debits against
`max-*-limit`; exceeding one cancels the attempt/source or fails the task. The
resolved budgets and their consumption are visible in diagnostics.

The guard is bounded at the protocol layer:

- reject a known framing/range-length mismatch from response headers before
  polling the body where possible,
- after a streamed response exceeds its expected span, read at most the current
  bounded parser buffer, close/reset the stream, and penalize the source; never
  drain an unbounded oversized body,
- cancel endgame losers as soon as one `CommitLease` wins, with the global
  `endgame-max-duplicates` bound from `split-download.md`,
- abort provisional attempts promptly on redirect, cancellation, or validation
  failure,
- FTP has no artificial split tail to drain because each source uses one
  sequential stream (`detailed-ftp-sftp.md`).

The discarded-byte counter includes every raw payload byte that does not enter a
committed lease. In addition to the fixed discard budgets, oversized responses
are limited to one parser-buffer validation overrun, endgame is bounded by its
duplicate cap, and retry/lease-size limits bound short-body waste. A source that
continues sending after cancellation/reset cannot take effect promptly consumes
the discard guard and is terminated.

## Reconciliation With libtorrent

libtorrent has its own internal rate limiter, which historically causes
double-counting when a global limit must cover both HTTP and BitTorrent.

- The global bucket is the single source of truth. libtorrent's session rate
  limits (`set_download_rate_limit`/`set_upload_rate_limit`) are driven from the
  global budget: the BT lane is allocated a share of the global rate and
  libtorrent is configured to that share, rather than running its own independent
  uncoordinated limit.
- The BT lane reports bytes that become accepted pieces back through the event
  bridge so the global accounting reflects committed BT progress and the HTTP
  share is adjusted. Rejected blocks are accounted by the BT discard guard, not
  by user rate tokens. This keeps committed HTTP + BT progress within
  `max-overall-download-limit`.
- If precise unified accounting is not achievable in the first full build, the
  conservative fallback is to partition the global cap into an HTTP share and a
  BT share so the sum never exceeds the global limit, and document that the split
  is static until dynamic reconciliation lands.

## Options

```text
--max-overall-download-limit=SIZE   (global download bucket; 0 = unlimited)
--max-download-limit=SIZE           (per-task download bucket)
--max-overall-upload-limit=SIZE     (global upload bucket)
--max-upload-limit=SIZE             (per-task upload bucket)
```

These are runtime-live: changing a limit re-parameterizes the bucket without
restarting the task. `lowest-speed-limit` is a retry trigger, not a rate limiter,
and is handled by `retry-policy.md`.

## Tests

- global committed-progress accuracy under 1 stream and under ~1,000 streams
  (measured within tolerance of the configured rate),
- per-host fairness: one host cannot starve others under a shared global cap,
- per-task limit enforced within a higher global limit,
- a validated lease waiting for tokens holds no socket buffer and does not start
  a replacement range,
- limit change at runtime takes effect without task restart,
- committed HTTP + BT combined stays within the global cap in the full build,
- short, oversized, checksum-failed, cancelled, and losing-endgame bytes are
  excluded from user rate accounting but consume the finite discard guard,
- an oversized response is reset within the bounded discard budget rather than
  drained to EOF,
- stats expose raw transport, rate-accounted committed goodput, durable bytes,
  discarded bytes, and discard-budget consumption as distinct counters.
