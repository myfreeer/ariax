# Disk Adapter Design

Status: draft.

Decision: build the downloader's disk adapter in-house, but do not hand-roll
raw platform syscalls when a small, maintained wrapper is enough.

The disk adapter is correctness-critical. It owns file layout, offset mapping,
resume state, durability, and corruption prevention. A generic third-party file
I/O abstraction cannot know enough about aria2-style segmented downloads,
Metalink chunk checksums, BitTorrent selected files, `.aria2` recovery files, or
RPC pause/resume semantics. For that reason the adapter is project-owned.

## What Is Project-Owned

Implement from scratch in this project:

- `SafePathBuilder`
- `FileLayout`
- `GlobalOffsetMapper`
- `ControlJournal`
- `StorageEngine`
- `DiskScheduler`
- `BufferPool`
- write coalescing and backpressure policy
- allocation policy
- recovery state machine
- checksum-to-durable-piece transition
- selected-file and shared-piece edge handling

These pieces define downloader behavior and must be fully testable without real
network I/O.

## What Uses Libraries Or OS APIs

Use existing libraries or system APIs for low-level mechanics:

- Linux io_uring: use `io-uring` crate or a small audited wrapper around
  `liburing`/syscalls.
- Windows: use a narrow overlapped I/O / IOCP wrapper.
- macOS/BSD: use POSIX `pread`, `pwrite`, `fcntl`, `fsync`, and `ftruncate`
  through `std`/`libc`/`rustix`-style wrappers on a bounded disk pool.
- Path and fd safety: use `openat`/no-follow capable APIs where available.
- BitTorrent full build: let libtorrent manage its own swarm disk internals
  unless we need a custom storage backend for unified placement.

Avoid depending on a broad storage framework that hides open flags, fsync,
rename, allocation, or offset writes. Those details are part of the correctness
contract.

## Why Not `tokio::fs` As The Adapter

`tokio::fs` is useful for simple ordinary file operations, but the official
Tokio docs state that it currently uses blocking file operations behind
`spawn_blocking` because most operating systems do not provide async filesystem
APIs. It also may change implementation details later.

So:

- `tokio::fs` is acceptable in tests, setup code, config reads, small metadata
  reads, and fallback utilities.
- It is not the high-performance disk adapter for transfer data.
- Transfer data goes through `DiskBackend`, which may use io_uring, IOCP, or a
  bounded blocking pool with explicit queue limits.

## Disk Backend Interface

The normative backend contract is defined in `detailed-runtime.md` ("Disk
Backend Contract"). Runtime selection uses the concrete `DiskBackendKind` enum
over io_uring, IOCP, and blocking implementations; it does not construct a
trait object with native async methods and does not box one future per write.
This document does not restate its methods. The payload type is always the
move-only `BufferLease`; there is no separate `Buffer` type.

The backend contract is intentionally offset-based. No protocol worker gets a
mutable file cursor. Cursor-based writes are a corruption risk in segmented
downloads.

## Storage Engine Interface

Protocol workers do not call `DiskBackend` directly. They call
`StorageEngine`:

```rust
pub struct WriteBlock {
    pub task: TaskId,
    pub generation: Generation,
    pub lease: LeaseId,
    pub global_offset: u64,
    pub expected_len: usize,
    pub buffer: BufferLease,
    pub piece: PieceId,
}
```

The transactional commands and acknowledgements (`BeginLease`, provisional
write, `CommitLease`/`AbortLease`, and durable-piece completion) are defined
normatively in `detailed-storage.md`; this document does not restate their
shape. The buffer field is always the move-only `BufferLease`.

Every backend write returns the runtime-defined
`DiskWriteOutcome { lease, result }`. Both successful and failed outcomes carry
exactly the submitted `BufferLease`; a short write is a failure outcome that
still carries it. Only `StorageEngine` can turn a successful low-level result
into a committed lease or durable-piece acknowledgement after layout,
generation, exact-length, checksum, and journal rules pass.

`StorageEngine` verifies:

- task id, generation, and active `LeaseId`,
- offset is inside `FileLayout`,
- length does not overflow `u64`,
- selected-file policy,
- shared-piece edge policy,
- duplicate/overlap policy,
- expected piece hash state,
- current pause/remove/cancel state.

## Backend Selection

Backend selection follows `event-backends.md`.

Disk-specific order:

- Linux `auto`: io_uring if runtime probe passes, otherwise bounded blocking
  pool.
- Windows `auto`: overlapped file I/O/IOCP if probe passes, otherwise bounded
  blocking pool.
- macOS/BSD `auto`: bounded blocking pool with `pread`/`pwrite`.
- Any platform: `sync` only for tests and very small single-file tools.

If the user requests a backend:

- `--disk-io-backend=uring` on unsupported Linux: fallback or clean config
  error based on `--event-backend-fallback`.
- `--disk-io-backend=iocp` off Windows: same.
- `--disk-io-backend=blocking`: always allowed on desktop platforms.

## Bounded Blocking Fallback

The fallback is not "just spawn unbounded blocking tasks".

It has:

- fixed worker count,
- bounded submission queue,
- bounded bytes in flight,
- cancellation tokens,
- priority classes for journal/control writes over bulk writes,
- per-task fairness,
- metrics for queue depth and wait time.

If the disk queue is full, protocol workers stop reading more network bytes
until buffers return. This is how memory remains bounded.

## Adaptive Backpressure

Backpressure is responsive to observed disk behavior, not a fixed assumption
about HDD, SATA SSD, NVMe, network filesystem, or removable media.

Signals:

- disk queue depth,
- bytes queued,
- buffer-pool pressure,
- write completion latency p50/p95/p99,
- fsync latency,
- allocation latency,
- short write or retry rate,
- OS disk-full and quota errors,
- event-loop lag caused by disk completions,
- per-file sequentiality and seek distance.

Controls:

- pause or resume socket reads,
- reduce per-download active segments,
- reduce per-host connection use for disk-bound tasks,
- coalesce small writes more aggressively,
- increase flush interval within durability limits,
- prefer sequential piece selection when random writes hurt,
- move hash work away from disk completion threads,
- reserve queue slots for journal/control writes.

The storage engine classifies current disk pressure:

```text
Healthy     queue low, latency stable
Busy        queue growing or p95 latency rising
Saturated   queue near cap, p99 high, buffers scarce
Faulted     repeated I/O errors, ENOSPC, permission/quota failure
```

Network workers react to this classification. In `Busy`, they keep established
connections but reduce read interest or range dispatch. In `Saturated`, they
stop pulling more body bytes except for small control/protocol frames and let
TCP backpressure propagate. In `Faulted`, affected downloads follow the ENOSPC
policy for disk-full/quota (pause with durable state intact) and fail for
non-recoverable errors such as permission denial.

## HDD, SSD, And NVMe Behavior

The defaults differ by observed behavior rather than a user-declared drive
type.

HDD-like behavior:

- higher p95 latency for random writes,
- write coalescing target increases,
- active segment count per file may be reduced,
- piece selection prefers longer sequential runs,
- fsync grouping becomes more important in `balanced` mode.

SATA SSD-like behavior:

- moderate queue depth is useful,
- random writes are less harmful,
- coalescing remains useful but does not force long waits,
- segment concurrency can stay higher if latency is stable.

NVMe-like behavior:

- higher queue depth may be useful,
- io_uring registered buffers and batched submissions are preferred,
- small random writes may be acceptable if p99 latency remains low,
- CPU hashing or network may become the bottleneck instead of disk.

Network/removable filesystem behavior:

- latency spikes and fsync cost are treated as first-class signals,
- queue caps are conservative,
- strict durability mode may significantly reduce concurrency.

Users can still override budgets, but the scheduler will not keep reading
unbounded network data just because the configured connection count is high.

## Write Coalescing

The adapter coalesces adjacent writes only after correctness checks:

- same task,
- same generation,
- same lease, unless the backend batch preserves a separate outcome and buffer
  owner for every constituent lease,
- adjacent global offsets,
- same target file or valid multi-file span,
- same durability class,
- no hash boundary that requires an immediate verification step.

Coalescing target:

- 64 KiB to 1 MiB for general HTTP/FTP writes,
- piece/block-aligned for BitTorrent,
- chunk-hash-aligned for Metalink.

On Unix fallback, coalesced writes prefer vectored I/O where practical.

## Buffer Ownership

Buffers are owned by the global `BufferPool`. The canonical `BufferLease`
fields and state enum are defined normatively in `detailed-runtime.md`;
`buffer-pool.md` defines allocation and lifecycle policy without a second type.

A buffer cannot be reused until a `DiskWriteOutcome` (success or failure) or a
confirmed cancellation returns it and hash processing releases it. Cancellation
whose OS ownership is uncertain moves the same lease to bounded quarantine
until completion or cancellation confirmation. Backend shutdown drains or
quarantines every submitted lease exactly once; it cannot emit a lease-less
error or recover ownership through a side channel.

Zero-copy variants are permitted only through the same ownership state machine.
For example, io_uring registered buffers or OS page-cache transfer APIs may be
used after the storage engine has already validated offset, length, generation,
and piece ownership. A zero-copy path must return the same backend
`DiskWriteOutcome` and be promoted to durable state only through the same
`StorageEngine` hashing and journal ordering as the portable path.

## File Allocation

Allocation modes:

- `none`: no preallocation.
- `trunc`: set logical file length; physical allocation is unspecified.
- `falloc`: true allocation where supported.
- `prealloc`: portable zero-fill fallback, only on disk workers.

Rules:

- allocation never runs on network/control runtime threads,
- allocation progress is visible in status,
- disk-full handling follows the single ENOSPC policy below,
- selected-file and shared-piece edges drive which files must exist,
- no allocation mode marks a byte written, committed, verified, or durable.

`trunc` may create sparse extents, eagerly allocated zero-filled blocks, or
another filesystem-specific representation. Neither `set_len`, file metadata,
allocated-block counts, nor reads returning zero prove that download bytes were
received. If sparse allocation is an optimization goal, each backend must probe
and test its platform operation explicitly; portability does not infer it from
logical length.

## Disk-Full (ENOSPC) Policy

ENOSPC is handled by one policy across the disk adapter, retry engine, and
backpressure, so the task is not simultaneously described as terminal and
pausable:

- Mid-transfer ENOSPC pauses the task with durable state intact. It is not a
  terminal error and not a network retry. Every affected in-flight lease is
  aborted, and each returned buffer is released or quarantined through its
  `DiskWriteOutcome`; provisional spans become overwriteable. Already-durable
  pieces stay durable.
- Preallocation-time ENOSPC fails the allocation cleanly before any transfer
  starts (nothing is durable yet), with an actionable error.
- A paused-for-space task resumes on explicit user action (or an optional
  auto-retry timer) after space is freed, re-leasing the pending pieces.
- Quota errors are treated the same as ENOSPC. Permission errors remain terminal
  because freeing space does not resolve them.

`retry-policy.md` and `backpressure.md` reference this policy rather than
restating a terminal-vs-pause rule.

## Fsync And Rename Policy

Durability is explicit:

- `fast`: ordinary progress records are provisional; finalization flushes data
  and journal and promotes verified pieces. Recent pieces may be rehashed or
  redownloaded after crash.
- `balanced`: default; verified pieces are grouped, touched data files complete
  `sync_data` before their `PieceDurable` records are appended, and the
  append-growing journal completes one `sync_all` per interval/group before any
  durable acknowledgement.
- `strict`: each verified piece completes data-file `sync_all`, then its
  `PieceDurable` append and journal `sync_all`, before acknowledgement.

These modes specify ordering and flush frequency. They do not assume a portable
performance ordering between `sync_data` and `sync_all`; actual group sizes and
costs must be benchmarked on native Linux filesystems and Windows NTFS. In all
modes, a journal flush on one descriptor never substitutes for the required
data-file flush, and no `PieceDurable` may precede that data barrier.

Finalization:

- write to final path only when safe by overwrite policy, or use temp path,
- atomically rename into final path,
- fsync parent directory where supported,
- remove control file only after final state is durable.

Platform note: POSIX `rename` replaces an existing destination atomically. On
Windows the backend must use `MoveFileEx`/`SetFileInformationByHandle` with
the replace-existing semantics (`ReplaceFile` when both files exist and
overwrite policy allows); a plain rename fails when the destination exists.
Sharing violations from concurrent open handles (antivirus, indexers) are a
retryable finalization error with bounded backoff, not an immediate terminal
failure. Parent-directory fsync is a POSIX durability requirement; on Windows
it is a no-op and NTFS metadata journaling covers the rename.

## Multi-File Mapping

`GlobalOffsetMapper` maps one write to one or more file writes:

```text
global [900, 1300)
  file A [900, 1000)
  file B [0, 300)
```

The mapper rejects out-of-range writes before they reach the backend. It also
handles zero-length files and unselected file gaps explicitly.

## BitTorrent Storage

Initial full-build plan:

- Use libtorrent's default disk I/O for torrent payloads.
- Use our safe path builder before handing storage parameters to libtorrent.
- Import libtorrent resume data into our task result/session model.
- Keep aria2-compatible queue/RPC status in our scheduler.
- Run libtorrent outside the main control and HTTP event loops. The adapter owns
  a libtorrent session lane and communicates with the scheduler by bounded
  channels.

Optional later plan:

- Implement a custom libtorrent storage backend that delegates file placement
  to our `StorageEngine`.

Do not write a native BitTorrent disk/piece engine until there is a dedicated
phase for peer protocol, piece picker, choking, DHT, PEX, resume data, and
fuzz and internal stress validation.

## Tests

Required tests:

- offset mapping property tests,
- adjacent and cross-file write tests,
- unselected-file rejection tests,
- path traversal tests,
- short write and disk-full injection,
- success, short-write, cancellation, and backend-shutdown paths return or
  quarantine each submitted `BufferLease` exactly once,
- forced crash after every journal/write/finalize step,
- backend fallback tests,
- concurrent writes to same and different pieces,
- buffer reuse after cancellation,
- fsync/rename behavior with temp files,
- `trunc` tests prove only logical sizing and never infer completed ranges;
  platform-specific sparse-allocation claims require separate backend tests.

Benchmarks:

- 4 KiB, 64 KiB, 256 KiB, 1 MiB write sizes,
- random vs sequential offsets,
- single large file and many small files,
- disk queue latency under 1,000 active network streams,
- memory cap behavior when disk is slower than network,
- real balanced-group and strict per-piece data/journal barriers on native Linux
  filesystems and Windows NTFS, including small pieces.
