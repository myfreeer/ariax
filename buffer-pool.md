# Buffer Pool Design

Status: draft.

Decision: use a bounded hybrid buffer pool. It is lazy/on-demand within hard
budgets by default, with optional preallocation for benchmarks, high-throughput
profiles, and io_uring registered-buffer mode.

The pool is the normal data-transfer target between network and disk. Protocol
workers read into pool buffers, storage writes from the same buffers, hashers
borrow immutable slices, and buffers return to the pool only after all owners
release them.

Inter-thread messages carry `BufferLease` ownership tokens and offsets, not
copied payload bytes. Queue selection is defined in `messaging-model.md`.

## Allocation Policy

Default: lazy bounded allocation.

- At startup, allocate only small metadata structures and optionally a small
  warm reserve per size class.
- On demand, allocate buffers until the configured byte cap or count cap is hit.
- When demand falls, keep a bounded hot reserve and release surplus buffers
  after an idle timeout.
- Allocation failure applies backpressure; it does not grow unbounded and does
  not allocate ad hoc `Vec<u8>` in protocol code.

Optional preallocation:

- `--buffer-pool-prealloc=SIZE` preallocates a fixed budget.
- `--buffer-pool-lock-memory=true` may be supported only where safe and
  privileged.
- io_uring registered-buffer mode preallocates/registers selected buffers after
  backend probing succeeds.

Rationale:

- Lazy allocation keeps small downloads and idle RPC mode compact.
- Hard caps prevent C10k from turning into unbounded memory use.
- Preallocation is useful for stable p99 latency and registered I/O.

Free-list order is LIFO per size class: the most recently released buffer is
reused first, so hot buffers stay cache/TLB-warm and cold surplus naturally
sinks to the tail where the idle-timeout release trims it. Per-lane/thread
small local caches in front of the shared free list are a permitted
optimization as long as budget accounting stays global. The baseline cap is
two free leases per size class per lane; registered/quarantined leases are never
eligible. A global waiter or high-watermark signal flushes lane-local surplus
back to the shared list before allocating or reporting exhaustion, so local
cache warmth cannot starve another lane.

## `disk-cache` Compatibility Cache

`disk-cache` is an optional verified-span readback cache implemented by
retaining immutable `BufferLease`s inside this same pool, not a second
allocator. The default is `0` in every profile because the OS page cache already
retains ordinary written file data; users may opt in for measured repeated
Metalink/checksum readback workloads.

Rules:

- the key is `(layout_hash, generation, global_span, verification_identity)`;
  cached bytes never establish downloaded/durable progress and are not returned
  for a mismatched generation/layout/hash,
- only exact committed-and-verified spans enter; abort, hash failure, overwrite,
  layout/generation invalidation, or finalization identity change evicts the
  affected entries,
- lookup may satisfy only an internal verification/readback request that would
  otherwise read the same exact span from disk; protocol workers never use it
  as a source of network progress,
- byte accounting is part of `buffer_budget` and the configured `disk-cache`
  sublimit; entry metadata is charged to `piece_metadata_budget`,
- cache leases are lowest-priority reclaimable ownership. A transfer-buffer
  waiter, quarantine pressure, or parent high watermark evicts LRU entries
  synchronously before admission fails; cache retention never reserves a hard
  minimum,
- diagnostics expose hit/miss/eviction bytes and distinguish cache retention
  from free/in-flight/quarantined pool bytes.

This gives the aria2-named option observable bounded behavior without claiming
that duplicating the OS file cache is normally beneficial.

## Size Classes

Default classes:

```text
16 KiB    headers, small protocol frames, slow streams
64 KiB    default transfer chunk
256 KiB   high-throughput stream chunk
1 MiB     large sequential writes or coalesced disk batches
```

The scheduler chooses class by:

- protocol,
- active bandwidth,
- disk pressure,
- TLS/decompression behavior,
- piece/chunk boundary,
- memory pressure.

Large buffers are not assigned to idle or slow connections just because they
are available.

## Is It The Only Copy Target?

For transfer payloads, yes by policy:

- Raw FTP body bytes enter a `BufferPool` buffer. Hyper may first yield a
  framework-owned immutable `Bytes` frame, and russh-sftp returns an owned
  `SSH_FXP_DATA` vector; the adapters account these against their separate HTTP
  or SFTP ingress budgets and copy/split them into a `BufferLease`.
- The same buffer is submitted to `StorageEngine`.
- Hashing borrows from the same immutable buffer when possible.
- Disk backend writes from the same buffer or from validated vectored slices.

Exceptions:

- small protocol metadata may use stack or small owned allocations,
- HTTP headers and RPC JSON/XML bodies have separate size-capped parsers,
- decompression may require a second output buffer because bytes change,
- TLS libraries may maintain internal buffers,
- Hyper/h2 may maintain configured/hidden ingress buffers and flow-control
  windows; their effective knobs and measured overhead are reported separately
  and included in connection admission,
- libtorrent uses its own buffers inside the isolated BT lane,
- OS zero-copy paths may use registered pool buffers or backend-owned kernel
  buffers, but still report through the same ownership model.

Protocol code must not allocate arbitrary payload `Vec<u8>` for segments or
whole files.

## Lifecycle

```text
Free
  -> Reserved
  -> NetworkFill
  -> TransformOwned
  -> Filled
  -> Validating
  -> DiskQueued
  -> DiskInFlight
  -> DiskDone
  -> HashBorrowed
  -> JournalPending
  -> Releasable
  -> Free
```

Some transitions are skipped when no transform, hash, or journal update is
needed. This is an explanatory flow, not a second enum definition: the
normative `BufferState` union and `BufferLease` fields live in
`detailed-runtime.md`.

State meaning:

- `Free`: available to any worker.
- `Reserved`: counted against a task before issuing a read.
- `NetworkFill`: socket/TLS/protocol worker can mutate it.
- `TransformOwned`: one bounded transform owns mutable output construction.
- `Filled`: immutable payload length is set.
- `Validating`: response/range/chunk boundary checks are using it.
- `DiskQueued`: queued to storage; protocol worker no longer owns it.
- `DiskInFlight`: backend owns it until completion.
- `DiskDone`: write ack received with exact byte count.
- `HashBorrowed`: CPU has immutable borrow, possibly before or after disk ack.
- `JournalPending`: durable state needs control journal update.
- `Releasable`: all references are gone; buffer can be wiped if required and
  returned.

Invalid transitions panic in debug builds and return typed internal errors in
release builds.

## Ownership Object

The single normative move-only `BufferLease` is the stable-storage,
pointer/capacity/owner form defined in `detailed-runtime.md`. This document does
not redefine it. In particular, a lease does not contain a reallocatable or
splittable `BytesMut`; registered io_uring and overlapped-I/O buffers retain a
stable address for the entire registration/in-flight lifetime.

If a decoder or other transform genuinely requires a relocatable allocation,
it uses a distinct bounded `TransformBuffer`. `TransformBuffer` is never sent to
`DiskBackend` and is not interchangeable with `BufferLease`; its output must be
moved or copied into a reserved lease before entering `Filled`/`Validating`.

Rules:

- mutable access exists only in `NetworkFill` or explicitly owned transform
  states,
- disk and hash stages receive immutable views unless a transform owns a new
  output buffer,
- `Drop` returns leaked leases to quarantine, not directly to free list,
- every `DiskWriteOutcome` returns the submitted lease on both success and
  failure; a lease-less disk error is not a valid backend result,
- cancelled buffers move through `Releasable` only after backend cancellation
  or completion is observed,
- cancellation uncertainty moves the one owned lease to bounded quarantine;
  completion, cancellation confirmation, and backend shutdown must each resolve
  it exactly once,
- debug builds track owner thread/lane and transition history.

## Backpressure

The pool enforces:

- total bytes cap,
- per-size-class count cap,
- per-task buffer cap,
- per-host buffer cap,
- disk-queued byte cap,
- hash-reorder-held byte cap, charged to both the task and global pool budgets,
- optional global memory watermark from the OS/process.

If a worker cannot reserve a buffer:

- it does not read from the socket,
- it yields or waits on a bounded notification,
- the scheduler may reduce segment windows,
- RPC/control remains responsive because it does not need transfer buffers.

## Scaling

C10k target:

- idle connections do not own large buffers,
- slow connections use small buffers or no buffer until readable,
- active streams borrow at most a small number of buffers each,
- disk pressure shrinks per-task windows before memory grows,
- buffer metrics guide adaptive sizing.

Example envelope:

```text
10,000 low-activity sockets: near-zero payload buffers
1,000 active streams * 64 KiB: about 64 MiB payload buffers
plus disk/hash/journal in-flight caps, bounded by config
```

The system scales by bounding active payload buffers, not by allocating one
large buffer per possible connection.

## Registered Buffers

For io_uring:

- registered buffers come from a dedicated size class or arena,
- registration happens after runtime probe succeeds,
- registered buffers have stable memory addresses until unregistered,
- they cannot be shrunk or released while registered,
- fallback mode can still use the same `BufferLease` API without registration.

For Windows:

- overlapped I/O buffers must remain pinned/stable until completion,
- those buffers stay in `DiskInFlight` until IOCP completion.

## Security

- Sensitive metadata buffers can be zeroed before reuse.
- Payload buffers are not normally zeroed for performance unless configured.
- Debug poisoning can catch use-after-release in tests.
- A normal protocol/checksum validation failure does not quarantine a buffer:
  after all owners are known to have released it, the lease is optionally
  zeroed/poisoned according to policy and returns to its size class. Quarantine
  is reserved for unresolved external/OS ownership (or an internal ownership
  invariant violation), so malicious bad payloads cannot exhaust the backend
  cancellation budget merely by failing validation.
- Logs never dump payload contents by default.

## Metrics

Expose:

- total pool bytes,
- allocated bytes,
- free bytes,
- in-flight bytes by state,
- wait time for buffer reservations,
- allocation count and release count,
- quarantine count,
- per-task buffer use,
- registered-buffer use,
- hash-reorder-held bytes and readback fallbacks,
- peak memory since start.
