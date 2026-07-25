# Zero-Copy Policy

Status: draft.

Zero-copy is allowed by this design, but only as a backend optimization. It is
not allowed to bypass the storage engine, checksum rules, recovery journal, rate
limits, or cancellation.

## Definition

In this project, "zero-copy" can mean several different things:

- avoiding a copy from protocol buffer to disk queue by transferring ownership
  of a pooled buffer,
- using vectored I/O to avoid coalescing copies,
- using registered buffers with io_uring,
- using OS-assisted file transfer APIs where applicable,
- avoiding user-space copies inside a BitTorrent library.

It does not mean that bytes skip validation. Every byte still belongs to an
explicit task generation, global offset, piece/chunk, and durable state.

## Allowed Zero-Copy Paths

Allowed:

- Network read fills a `BufferPool` buffer, then ownership moves to
  `DiskQueued` without copying.
- Hashing borrows immutable slices from the same buffer before release.
- Disk backend writes from that buffer and returns it after completion.
- io_uring registered buffers are used for repeated reads/writes.
- Vectored writes submit several validated adjacent buffers in one syscall.
- BitTorrent full build uses libtorrent's internal zero-copy/cache strategies
  inside the isolated BT lane.

All of these still go through `StorageEngine::write_block` or an equivalent BT
adapter checkpoint.

## Disallowed Shortcuts

Not allowed:

- protocol worker writes directly to a file descriptor,
- cursor-based writes where current file position decides placement,
- accepting a range body before `206` and `Content-Range` are validated,
- marking a piece complete before disk ack and hash validation,
- reusing or mutating a buffer before disk completion,
- treating a provisional block write as committed before its response lease
  passes exact-length and validator checks,
- `sendfile`/splice-style transfer from network to final file that prevents
  piece hashing, rate limiting, cancellation, or exact length checks,
- mmap writes that let unrelated code mutate pages after validation.

## HTTP/FTP/SFTP Data Path

Default path:

```text
socket -> pooled Buffer -> validate per-block boundaries -> StorageEngine
       -> provisional WriteBlock -> DiskBackend write_at -> DiskWriteOutcome
       -> exact response validation -> CommitLease or AbortLease
       -> piece hash -> data barrier -> journal flush -> BufferPool
```

This has one kernel-to-user copy and one user-to-kernel disk submission in the
portable fallback. It avoids extra heap copies and whole-segment buffers.

Linux optimized path:

```text
socket -> registered pooled Buffer -> StorageEngine -> io_uring write
       -> DiskWriteOutcome -> lease commit/abort -> hash/journal -> BufferPool
```

This still does not do direct network-to-file transfer by default, because the
downloader must inspect bytes for checksums, content encoding, exact length,
and recovery state.

## When True Kernel Zero-Copy Can Be Used

True kernel zero-copy APIs may be used only for cases where all constraints are
already satisfied:

- target offset and length are known,
- no decompression/filtering is active,
- no per-chunk hash needs user-space bytes, or hashing can be performed by a
  verified alternate path,
- the commit gate can debit the exact validated range without retaining
  unbounded user-space payload buffers; aborted bytes consume only discard
  budget, not user rate tokens,
- cancellation can still prevent durable commit,
- recovery journal can distinguish begun, written, lease-committed, verified,
  and durable bytes.

For ordinary HTTP range downloads with checksums, user-space buffer ownership is
the safer default.

## Transaction And Failure Rules

Zero-copy changes neither the `LeaseId` transaction nor the ownership result.
Every write is provisional until the response validator issues `CommitLease`.
Short/oversized bodies, stale validators, cancellation, and losing endgame
attempts issue `AbortLease`; their physical bytes are invisible to recovery and
may be overwritten. A crash treats every lease lacking `LeaseCommitted` as
aborted.

Every backend path returns `DiskWriteOutcome { lease, result }`, so success,
short write, I/O error, and confirmed cancellation all return the submitted
`BufferLease`. Uncertain OS cancellation quarantines that same lease until a
completion or cancellation confirmation resolves ownership. No zero-copy
backend may report a lease-less error.

If a required piece/chunk hash fails after bytes were written, storage appends
`PieceFailed`, clears committed/provisional state for that verification range,
and returns it to pending. The same-generation retry may overwrite those
offsets because no `PieceDurable` was committed. A zero-copy path cannot retain
or publish the failed bytes through an alternate cache.

## mmap Policy

mmap is not the default write path.

Allowed:

- read-only verification for large files when it improves hashing and does not
  exceed `max-mmap-limit`,
- explicit user-enabled write mapping after preallocation, with strict region
  ownership.

Disallowed:

- shared mutable mappings exposed to protocol workers,
- mapping files larger than configured limits,
- relying on mmap dirty-page timing as durable completion.

## Libtorrent

Libtorrent may perform its own optimized disk/cache behavior inside the BT lane.
That is acceptable because libtorrent owns BT piece scheduling and validation
inside its session.

The main downloader still requires:

- safe output paths before session creation,
- bounded event bridge,
- periodic resume data import,
- normalized status snapshots,
- no blocking calls from libtorrent into the control plane,
- no direct mutation of main scheduler state from libtorrent callbacks.
