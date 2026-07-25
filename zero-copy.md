# Zero-Copy Policy

Status: reviewed pre-implementation contract. Implementation pending.

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

- Raw protocol reads that accept a caller buffer (including FTP) fill a
  `BufferPool` lease, then ownership moves to `DiskQueued` without copying.
  Hyper instead yields a framework-owned immutable `Bytes` frame and
  russh-sftp 2.3.0 yields an owned `SSH_FXP_DATA` vector; each is separately
  ingress-budgeted and copied/split into the pool in the baseline.
- Hashing borrows immutable slices from the same buffer before release.
- Disk backend writes from that buffer and returns it after completion.
- io_uring registered buffers are used for repeated reads/writes.
- Vectored writes submit several validated adjacent buffers in one syscall.
- BitTorrent full build uses libtorrent's internal zero-copy/cache strategies
  inside the isolated BT lane.

All of these still go through the storage engine's `WriteBlock` command
(`detailed-storage.md`) or an equivalent BT adapter checkpoint.

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

For Hyper, insert `Hyper Bytes frame -> BufferLease`; for SFTP, insert
`SSH_FXP_DATA Vec -> BufferLease` before `StorageEngine`. Each is one explicit
bounded user-space copy accepted by the baseline. Exposed HTTP window/buffer
controls and reserved SFTP request lengths bound the configurable framework
memory. Unexposed stack overhead is measured and included in
admission/diagnostics rather than described as zero-copy.

## When True Kernel Zero-Copy Can Be Used

True kernel zero-copy APIs may be used only for cases where all constraints are
already satisfied:

- target offset and length are known,
- no decompression/filtering is active,
- no per-chunk hash needs user-space bytes, or hashing can be performed by a
  verified alternate path,
- protocol ingress can acquire/debit rate credit before accepting bytes without
  retaining unbounded user-space payload buffers; aborted bytes retain that
  debit and also consume discard budget,
- cancellation can still prevent durable commit,
- recovery journal can distinguish begun, written, lease-committed, verified,
  and durable bytes.

For ordinary HTTP range downloads with checksums, user-space buffer ownership is
the safer default.

## Transaction And Failure Rules

Zero-copy changes neither the `LeaseId` transaction nor the ownership result.
Every write is provisional until its storage lease commits: a range lease after
exact response validation, a sequential checkpoint lease after its exact span
is written under the validated response head, and the final sequential lease
after exact EOF/framing. Short/oversized bodies, stale validators,
cancellation, and losing endgame attempts issue `AbortLease` for the current
incomplete lease; their physical bytes are invisible to recovery and may be
overwritten. A crash treats every lease lacking `LeaseCommitted` as aborted.

An overlapping endgame candidate is not journal-committed until all competitors
are fenced. If any competitor wrote or remains cancellation-uncertain, the whole
overlap group rolls back to pending metadata and clears affected in-memory
piece state. Zero-copy backends do not undo the bytes; the next lease overwrites
them, exactly as it overwrites untrusted preallocation contents.

Every backend path returns
`DiskWriteOutcome { backend_epoch, lease, result }`, so success, short write,
I/O error, and confirmed cancellation all return the submitted `BufferLease`.
Uncertain OS cancellation quarantines that same lease until a completion or
cancellation confirmation resolves ownership. A stale backend epoch can release
ownership but cannot commit storage state, and no zero-copy backend may report a
lease-less error.

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
