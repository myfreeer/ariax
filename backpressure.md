# Backpressure Design

Status: draft.

Backpressure means the downloader responds to actual network, disk, CPU, and
memory conditions. It is not just a fixed rate limit.

The main goal is to keep control/RPC responsive and memory bounded while using
as much throughput as the machine can sustain.

## Feedback Loops

```text
Network reads -> BufferPool -> StorageEngine -> DiskBackend
       ^             |             |              |
       |             v             v              v
       +------ Scheduler <---- Metrics <---- Completions
```

Each protocol worker reads only when it has:

- connection budget,
- buffer budget,
- disk queue budget,
- task generation still active,
- finite discard-guard credit,
- configured user rate credit for the next protocol read/body poll,
- no cancellation/pause signal.

These signals cross worker-pool boundaries through bounded channels. The
threading layout is defined in `threading-model.md`.

## Disk Signals

Measured continuously:

- queued bytes,
- queued operations,
- write completion latency,
- fsync latency,
- allocation latency,
- buffer wait time,
- per-task disk wait time,
- write error rate,
- journal save latency.

The scheduler uses rolling windows rather than single samples to avoid
oscillation. Adaptive adjustments also use hysteresis: the enter threshold for
a pressure state is higher than its exit threshold, and window/queue targets
change by bounded steps per evaluation tick, so the controller converges
instead of flapping between `Busy` and `Healthy` on a noisy latency signal.

## Network Signals

Measured continuously:

- socket read readiness,
- bytes/sec per connection,
- stalled reads,
- TLS/decompression CPU cost,
- retry/error rate,
- server range correctness,
- per-host connection pressure.

If network is slower than disk, disk queues stay shallow and connections keep
reading. If disk is slower than network, reads are reduced before buffers grow.

Stats must distinguish this intentional read reduction from a stuck socket. See
`stats-and-stalls.md`.

This distinction also protects optional slow-slot scheduling. A task slowed by
local disk, CPU, memory, journal, or user rate limits retains its normal
scheduler state but reports a `Backpressured` or `RateLimited` connection/stall
diagnostic; it must not be demoted as a remote-slow task. See
`download-scheduling.md`.

## CPU Signals

Measured continuously:

- hash queue latency,
- decompression latency,
- XML/bencode parser queue latency,
- worker saturation,
- event-loop lag.

If CPU hashing is the bottleneck, disk writes may finish quickly but pieces are
not marked durable until hash work catches up. Network workers then slow down
through buffer and piece-window pressure.

## Memory Signals

Measured continuously:

- free buffers per size class,
- total bytes in flight,
- RPC response snapshot memory,
- metadata parser allocation caps.

If buffers are scarce, protocol workers stop reading before allocation grows.

## Response Actions

Soft actions:

- delay new segment dispatch,
- temporarily lower per-task segment window,
- lower per-host active connections for disk-bound tasks,
- increase write coalescing window within latency target,
- prefer sequential pieces,
- schedule slower mirrors later.

Hard actions:

- remove read interest from sockets,
- stop accepting new body chunks for a task,
- pause a task and save control state,
- fail on disk-full or repeated unrecoverable write error,
- reject new RPC adds when configured global budgets are exhausted.

## Responsiveness Targets

Control plane targets are independent of transfer throughput:

- RPC status should not wait for disk fsync.
- Pause/remove should enqueue immediately (coalescing a duplicate for the same
  task) and cancel workers cooperatively; overflow of the urgent queue is a
  typed busy error, not silent blocking.
- Backpressure never rejects or delays the completion of already accepted
  disk/CPU/journal work: internal completion lanes are permit-reserved and
  external producers cannot consume them.
- Queue operations should read snapshots, not block on active write locks.
- Event-loop p99 lag is a health metric and can trigger backpressure.

## User Configuration

Relevant options:

- `disk-cache`
- `profile`
- `split`
- `max-connection-per-server`
- `max-concurrent-downloads`
- `max-overall-download-limit`
- `max-download-limit`
- `disk-workers`
- `net-workers`
- `durability`
- `disk-queue-bytes`
- `disk-queue-ops`
- `adaptive-backpressure=true|false`
- `slow-slot-policy=off|demote|pause`
- `retry-wait-consumes-slot=true|false|auto`

Defaults are adaptive. Turning off adaptive backpressure keeps hard caps but
uses static windows, mainly for reproducible benchmarking.

The selected unified profile sets the guardrails for these adaptive
decisions. See `performance-profiles.md`.
