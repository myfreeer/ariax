# Stats And Stall Detection

Status: draft.

Problem: many downloaders update speed only when bytes arrive. If a socket gets
stuck, the displayed speed can freeze at the previous value until another packet
arrives. This misleads users and delays timeout decisions.

Decision: stats are sampled on a monotonic clock tick independent of packet
arrival. A connection or task can report zero current speed while still open.

## Principles

- Packet arrival updates byte counters, not display speed directly.
- A periodic stats sampler computes rates from monotonic time and counter
  deltas.
- No bytes in a sample interval means current speed decays or becomes zero.
- Stalled, idle, backpressured, and rate-limited are distinct connection/lease
  diagnostic conditions. They never introduce a new `TaskState` value.
- RPC/CLI status comes from snapshots, not live socket callbacks.

## Counters

Each connection tracks atomics or lane-local counters:

- total raw payload bytes received from the transport,
- total bytes accepted by protocol,
- total provisional bytes submitted to disk,
- current provisional bytes in flight (gauge),
- total bytes committed by `CommitLease`,
- total bytes made durable,
- total discarded bytes (received minus bytes that enter a committed lease,
  reconciled when an attempt commits/aborts),
- discard-budget consumption and remaining budget,
- last byte arrival time,
- last committed-progress time,
- last successful write time,
- last progress time,
- current connection/lease diagnostic condition,
- stall reason if known.

Each task aggregates:

- active connections,
- leased ranges,
- completed durable bytes,
- verified bytes,
- retry bytes,
- discarded bytes,
- upload bytes for BitTorrent.

Global stats aggregate task snapshots.

Counter meanings are intentionally independent:

- `receivedPayloadBytes` is raw transport telemetry and includes bytes later
  discarded because of short/oversized bodies, aborted leases, checksum failures,
  redirects, cancellation races, or losing endgame attempts. These are the
  download bytes debited from the configured user rate buckets.
- `acceptedBytes` passed protocol framing/range checks but may still be
  provisional.
- `committedBytes` won `CommitLease`, count as logical download progress, and
  are useful-progress accounting rather than the rate-debit point.
- `durableBytes` also passed the configured storage/journal durability barrier.
- `discardedBytes` is diagnostic waste that already consumed user-rate tokens
  when read and is additionally charged to the separate finite discard guard.

Cumulative received/accepted/submitted counters are monotonic; the in-flight
provisional gauge decreases when an attempt commits or aborts. Current committed
progress may roll back only through the explicit checksum/recovery transition,
which also increments discarded/rollback diagnostics. Aggregate accounting must
not double-count an endgame winner and loser.

## Sampler

The stats sampler runs at a fixed interval, for example 250 ms or 1 s depending
on profile.

Sampler cost is O(active), not O(total): idle connections and idle tasks
carry no per-tick work. Connections/leases register with the sampler only
while they have activity to report (counter deltas since last tick or a
condition change); a task whose counters are unchanged republishes nothing
except a cheap sample-age bump on its existing snapshot. This keeps a 250 ms
tick affordable at 10,000 mostly idle connections.

For each connection/task, aria2-compatible rate uses rate-accounted received
application payload bytes:

```text
delta_bytes = received_payload_total - previous_received_payload_total
delta_time = now - previous_sample_time
instant_rate = delta_bytes / delta_time
```

If `delta_bytes == 0`, instant rate is `0` for that interval. The display may
also show EWMA speed, but the current speed must not remain frozen.

Recommended fields:

- `currentSpeed`: aria2-compatible short-window received-payload rate; it reaches
  zero quickly and matches the bytes governed by `max-*-limit`.
- `wireSpeed`: extension short-window transport-payload rate before useful/
  discarded reconciliation (normally the same byte basis as `currentSpeed`).
- `usefulSpeed`: extension short-window newly committed bytes.
- `durableSpeed`: extension short-window rate of newly durable bytes.
- `avgSpeed`: task lifetime or active-window average.
- `smoothedSpeed`: EWMA for stable UI.
- `lastProgressAt`: monotonic or wall-clock timestamp.
- `taskState`: the closed canonical lifecycle state from `detailed-core.md`.
- `connectionCondition`: `none`, `idle`, `stalled`, `backpressured`, or
  `rateLimited`; this is diagnostic metadata, not a task-state/status value.

## Stale Socket Detection

Each connection has progress deadlines:

- connect deadline,
- TLS/handshake deadline,
- first-byte deadline,
- between-bytes idle deadline,
- lowest-speed deadline,
- write-to-disk deadline when buffers are held.

If no bytes arrive before the configured deadline:

- connection condition becomes `stalled`,
- current speed becomes zero on the next sample,
- scheduler may cancel/retry the lease,
- UI/RPC reports the stall reason.

Timeout decisions use monotonic timers, not display speed alone.

Receiving bytes that are continually discarded is transport activity but not
useful progress. `lowest-speed-limit` and the no-progress retry decision use
accepted/committed progress (as selected by retry policy), not discarded bytes,
so a peer cannot keep a broken lease alive by streaming invalid data. The
discard safety budget in `rate-limiting.md` independently resets a source that
continues sending after rejection.

## Connection And Lease Conditions

No bytes arriving is not always a network stall.

The following are diagnostic conditions, not `TaskState` variants:

- `Backpressured`: downloader intentionally stopped reading because disk,
  buffer, CPU, or journal pressure is high.
- `RateLimited`: the downloader is deliberately not polling/reading the protocol
  because ingress rate credit is unavailable; it holds no filled transfer buffer
  while waiting and creates no new task state.
- `Stalled`: socket expected progress but no bytes arrived.
- `Idle`: connection kept alive or waiting for next lease.

`RetryWait`, `PausedSlow`, and other scheduler lifecycle outcomes remain in the
closed core task-state model. Stats may attach a `connectionCondition` and reason
to their snapshots, but must not serialize a condition as the task's status.

This distinction prevents false alarms when the downloader itself paused reads.
It also lets `download-scheduling.md` free slots only for remote slowness, not
for local backpressure.

## Display Semantics

CLI:

- show current speed from sampler,
- show stalled/backpressured markers when relevant,
- do not leave old speed visible indefinitely,
- optionally show last progress age.

RPC:

- expose aria2-compatible string speeds for compatibility,
- add diagnostic fields when extension metadata is requested:
  `lastProgressAt`, `stallReason`, `backpressureReason`, `smoothedSpeed`,
  `sampleAge`, `slotState`, `slotReason`, `readmitAfter`, `wireSpeed`,
  `usefulSpeed`, `durableSpeed`, `receivedPayloadBytes`, `committedBytes`,
  `durableBytes`, `discardedBytes`, and discard-budget consumption.

Logs:

- emit task-state or connection-condition transitions, not one log line per
  sample.

## BitTorrent

Libtorrent stats are imported on a periodic tick as snapshots. If libtorrent
does not emit a fresh alert, the adapter still republishes a snapshot with
updated sample age and zero/decayed current speeds as appropriate.

The main UI must not depend on receiving a peer packet or libtorrent alert to
update displayed rates.

## Tests

Required tests:

- speed drops to zero when no bytes arrive for a sample interval,
- EWMA decays while current speed is zero,
- backpressured sockets show backpressured, not stalled,
- rate-limited sockets show rateLimited,
- backpressured/rateLimited diagnostics do not change the closed task state or
  aria2-compatible task-status value,
- failed/aborted/endgame-loser bytes appear in received/discarded totals, consume
  `max-*-limit` tokens, and never enter committed/durable progress,
- discarded bytes also exhaust the separate discard guard at its configured
  finite budget,
- repeated invalid bytes cannot satisfy useful-progress/lowest-speed checks,
- one endgame winner plus its losers are reconciled without double-counting
  committed bytes,
- stuck socket triggers retry timeout without waiting for another packet,
- RPC status snapshot changes over time even with no network events,
- libtorrent adapter stats age and speed update without alerts.
