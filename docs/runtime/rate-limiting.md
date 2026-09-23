# Rate Limiting Design

[Documentation](../README.md)

Status: reviewed streaming contract. The HTTP download limiter, ordered read
gate, stall diagnostics, and finite hierarchical discard guard are executable.
The rate arbiter has deterministic virtual-time coverage for bounded overshoot
debt repayment, FIFO refill fairness, explicit-scope persistence across default
reconfiguration, cancellation cleanup, and 1,000 tracked streams; FTP/SFTP and
libtorrent integration remain later adapter work.

`max-overall-download-limit`, `max-download-limit`,
`max-overall-upload-limit`, and `max-upload-limit` are live token-bucket
controls. The limiter acts at protocol ingress/egress, not at disk completion or
storage commit.

## Accounting Point

Download tokens are consumed when application payload bytes are accepted from a
protocol read:

- HTTP response-body poll/read,
- FTP data-channel read,
- SFTP file-data response,
- the corresponding libtorrent payload ingress controlled by its session
  limiter.

Upload tokens are consumed when application payload bytes are accepted by the
transport send/write path. Protocol headers, TLS records, and TCP/IP overhead
are outside the aria2-compatible application payload limit.

Once download bytes have been read and charged, their storage `write_at` is not
rate-limited. It proceeds as quickly as disk/backpressure credit permits. A
filled buffer never waits for a second user-rate permit, and `CommitLease` never
waits for rate tokens.

Tokens are not refunded when a response is aborted, a checksum fails, an
endgame group rolls back, or bytes are otherwise discarded. This makes the
configured limit a limit on actual application payload accepted by the client,
not merely on useful/durable progress. Stats separately expose received/sent
payload, committed progress, durable progress, and discarded payload.

## Bucket Hierarchy

Rate-accounted reads/sends pass through a hierarchy of token buckets from broad
to narrow:

```text
global (max-overall-*-limit)
  -> per-host fairness bucket
       -> per-task bucket (max-*-limit)
```

- The global download/upload buckets are the hard application-payload ceilings
  across project-owned protocols and the allocated libtorrent share.
- Per-host buckets are internal fairness controls so one origin cannot consume
  every global token through many streams.
- Per-task buckets enforce `max-download-limit` and `max-upload-limit`.
- A rate of `0` means unlimited and bypasses that bucket, matching aria2.

A read/send may proceed only with a `RatePermit` covering every configured
bucket on its path. Permit acquisition is fair and cancellable. Unused reserved
bytes are returned; bytes actually accepted by the protocol are consumed and
never refunded.

## Atomic Hierarchical Admission

One project-owned `RateArbiter` per direction owns the project HTTP/FTP/SFTP
bucket clocks and waiter queues. A request names its global/host/task/stream
path and maximum quantum. In one arbiter turn it either reserves the same byte
count from every enabled bucket and returns one move-only `RatePermit`, or
reserves nothing and registers exactly one cancellable waiter at the computed
earliest deadline. Workers never acquire the hierarchy sequentially, never
hold global tokens while waiting for a host/task bucket, and never hold a
buffer/queue slot merely because a partial rate reservation succeeded.

The arbiter uses lazy monotonic refill, a timer wheel for the next eligible
deadlines, and deficit-round-robin ready queues; it emits work per permit
quantum/wakeup, not per byte. It may be internally sharded after measurement,
but sharding must preserve one atomic reservation at the global root and the
same starvation bound. Runtime limit changes are versioned in the arbiter and
invalidate/recompute sleeping deadlines without revoking already consumed
bytes. Libtorrent's separately allocated share remains outside these project
bucket paths as described below.

## Streaming Read Gate

For project-owned downloads, the normal order before a protocol read is:

1. task/generation and cancellation are valid,
2. storage queue/byte credit is reserved,
3. an empty `BufferLease` or bounded HTTP ingress slot is available,
4. discard-guard credit remains,
5. a bounded `RatePermit` is acquired,
6. the protocol performs `response.read`, body polling, or offset-response
   acceptance,
7. accepted bytes are immediately charged and streamed to storage.

If any precondition is unavailable, the worker does not read. It releases
tentative reservations that cannot safely be held and registers one wakeup for
the limiting resource. This is a read gate, not a post-read sleep.

Raw FTP/socket reads request no more than the available permit, remaining
storage span, and buffer capacity. An SFTP `SSH_FXP_READ` is sent only after a
permit for its requested data length is reserved; the bounded response cannot
exceed that request, accepted bytes consume the permit, and timeout/cancellation
returns only the unused remainder. The small per-request quantum/outstanding cap
prevents high-latency SFTP from hoarding an unbounded share. A sequential HTTP/FTP response remains one
continuous transport stream even while it rotates through piece-aligned storage
leases; storage checkpoint boundaries do not create new requests or rate waits.

Hyper can already own a bounded body frame before the adapter observes it. The
adapter therefore:

- stops polling the body when rate or downstream credit is unavailable,
- bounds HTTP/1 body chunks and HTTP/2 connection/stream windows through the
  separate ingress budget,
- charges the complete yielded frame immediately,
- permits at most one bounded frame/window overshoot by recording token debt and
  polling no further body data until refill repays it,
- splits/copies the charged frame into storage buffers without another rate
  check.

The maximum overshoot is an explicit diagnostic and memory/rate test parameter,
not an unbounded consequence of the HTTP library.

## Bucket Parameters

- Each bucket has a rate in bytes/second and a bounded burst capacity.
- Default burst is a small multiple of the rate, normally up to one second of
  tokens, with a fixed maximum so long-idle tasks cannot release a huge burst.
- Refill uses monotonic lazy accounting on permit acquisition/wakeup. There is
  no per-bucket timer thread or per-byte message.
- Runtime limit changes re-parameterize the bucket and wake affected waiters
  without restarting the task.

## Fairness Across Many Streams

When many streams share a bucket:

- ready readers/senders receive bounded quanta through deficit round-robin (or
  an equivalent starvation-free scheduler),
- fairness is applied across hosts, then tasks, then active streams,
- one stream cannot reserve a whole large lease in advance,
- a waiter sleeps until cancellation, downstream recovery, or the next computed
  refill deadline; it does not spin,
- scheduler messages occur per permit quantum/wakeup, never per byte.

The implementation may adapt quantum size to rate and active-stream count, but
it remains capped by buffer/frame and burst limits.

## Precedence Relative To Backpressure

Backpressure and rate limiting both stop protocol reads, but for different
reasons:

- Downstream storage, queue, CPU/hash, journal, and memory credit is checked
  first. Reading without a place to send bytes would only retain filled buffers.
- Rate credit is checked immediately before the protocol read/body poll.
- Once the read succeeds, disk submission and completion are never delayed by
  the user rate limiter; only normal storage backpressure applies.
- A worker that is deliberately waiting for a permit reports `RateLimited`, not
  `Stalled` or `Backpressured`.
- A worker blocked on downstream resources reports `Backpressured`, even if a
  rate bucket also happens to be empty.

This ordering keeps memory bounded, makes the network-facing limit effective
during one long sequential response, and avoids a post-download commit delay.

## Discard And Retry Bounds

Discarded payload consumes normal user-rate tokens because it was actually read
from the peer. It also consumes a separate finite discard guard. The two controls
serve different purposes:

- rate buckets cap accepted application bandwidth,
- discard budgets cap cumulative waste/abuse per attempt, task, host, and
  process and can disable a source or fail a retry cycle.

Protocol rules:

- reject known framing/range mismatches from headers before body polling,
- after an oversized response crosses its expected span, account the bounded
  parser/frame overrun, close/reset promptly, and never drain an unbounded tail,
- charge and cancel endgame losers promptly; dirty overlap rollback does not
  refund their tokens,
- charge short, checksum-failed, cancelled, and retry payload normally,
- expose discard bytes, budget consumption, and source penalties in diagnostics.

The HTTP slice now uses a process-owned `HttpDiscardBudget` with atomic
process/host/task/attempt charging and no refund operation. Host identity is the
canonical final `scheme://authority`; the retained process host table is capped
at 4,096 entries, after which new origins share one fail-closed overflow host
bucket instead of allocating more metadata or bypassing the guard. Effective
attempt credit is the configured piece length plus one bounded ingress frame.
Effective task and host ceilings multiply that
attempt bound by the applicable total/per-mirror retry cap, the configured
endgame duplicate factor, and an internal factor of 64, then clamp to the
registry-controlled maxima. Current maxima are 16 GiB process-wide, 4 GiB per
host, 2 GiB per task, and 1 GiB per attempt. These are cumulative traffic
guards, not resident-memory reservations. Retained task scopes are capped at
100,000 entries and use the same fail-closed overflow rule.

Probe payload, short and oversized responses, lowest-speed aborts, retry and
cancellation cleanup, queued stale chunks, endgame losers/rollbacks, and
whole-file checksum failure all charge the same hierarchy. The worker checks
that credit remains before another body poll, charges the bounded frame already
accepted if a limit is crossed, closes the attempt, and returns the terminal
`http_discard_budget_exhausted` resource-limit error instead of admitting
another retry. `tellStatus` exposes `discardBudgetConsumed` and
`discardBudgetRemaining` separately from `discardedLength`; the remaining value
is the current task-scope credit, while a process/host exhaustion is identified
by the terminal error scope and does not get hidden by that task-level number.

A user-facing override requires a normal option-registry addition; the current
limits remain internal executable policy.

## Reconciliation With libtorrent

Libtorrent performs its own ingress/egress pacing. The project global limiter is
the allocation source of truth:

- the scheduler assigns a bounded download/upload share to the BT lane,
- libtorrent session rate limits are set to that share,
- project-owned HTTP/FTP/SFTP buckets use only the remaining global share,
- allocated shares always sum to no more than the configured global limit,
- demand and observed throughput may rebalance shares at a bounded control
  interval without per-packet events.

BT payload is not debited a second time through project token buckets. Accepted
and rejected BT payload already consumes the libtorrent share; BT events report
received, useful, discarded, and uploaded counters for reconciliation and
diagnostics.

If dynamic allocation cannot meet accuracy/fairness gates in the first full
build, use a documented static HTTP/BT partition whose sum never exceeds the
global cap. Do not run two independent full-size limiters.

## Options

```text
--max-overall-download-limit=SIZE   (global download bucket; 0 = unlimited)
--max-download-limit=SIZE           (per-task download bucket)
--max-overall-upload-limit=SIZE     (global upload bucket)
--max-upload-limit=SIZE             (per-task upload bucket)
```

`lowest-speed-limit` is a retry trigger, not a rate limiter, and is handled by
[retry-policy.md](../protocols/retry-policy.md).

## Tests

- one sequential HTTP/FTP stream stays near the configured rate while
  continuously streaming response reads to disk,
- disk writes never wait for user-rate tokens after bytes are read,
- per-task limits compose under a lower global limit,
- an unavailable host/task bucket leaves the global bucket unchanged; cancel,
  timeout, and runtime reconfiguration cannot leak a partial hierarchy
  reservation,
- queued streams are served FIFO at deterministic refill boundaries, and ~1,000
  active stream scopes remain within the hard tracking bound,
- short, oversized, checksum-failed, cancelled, retry, and endgame-loser bytes
  consume both rate tokens and the appropriate discard budget,
- abort/rollback never refunds consumed tokens,
- a low limit below one Hyper frame has a measured overshoot bounded by the
  configured frame/window budget; virtual-time tests prove the resulting debt
  is reported accurately and no new permit is granted until it is repaid,
- backpressure is reported instead of rate limiting when downstream credit is
  the first unavailable resource,
- runtime limit changes take effect without task restart, while explicit scoped
  limits survive default reconfiguration,
- HTTP/FTP/SFTP plus BT allocated shares never exceed the global cap,
- stats expose received/sent payload, committed, durable, discarded, rate debt,
  and discard-budget counters separately.
