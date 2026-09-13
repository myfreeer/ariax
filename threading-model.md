# Threading Model

Status: Tokio HTTP/RPC execution, the bounded blocking disk lane, a dedicated
session-owner thread, process budgets and bounded shutdown are implemented.
The full CPU/BitTorrent lane topology and adaptive worker allocation below
remain roadmap contracts. Current evidence is in `implementation-readiness.md`.

Decision: use split worker pools by default, coordinated by one global resource
budget. Do not run event loops, disk I/O, libtorrent, and hashing on one shared
pool in the normal build.

## Why Split Pools

The workload classes have different latency and blocking behavior:

- control/RPC needs low latency and must not wait behind bulk hashing or disk
  fsync,
- network event loops should not block on regular file I/O,
- disk I/O can block or stall on HDDs, network filesystems, allocation, fsync,
  antivirus, or OS writeback,
- hashing/checksum work is CPU-bound and can starve async reactors if mixed,
- libtorrent may run its own Asio loop and disk threads.

A single general pool is simpler, but under load it creates priority inversion:
slow disk work can delay RPC, hash work can delay socket readiness, and swarm
callbacks can interfere with HTTP downloads.

## Default Pools

```text
main/control thread
  CLI, signals, scheduler commands, cheap state transitions

network runtime
  Tokio/Mio event loops, TCP/UDP/TLS, HTTP/FTP/SFTP protocol tasks

disk pool or completion lane
  io_uring completion, IOCP file completion, or bounded blocking pwrite/fsync

cpu pool
  hashing, Metalink chunk checksums, full-file checksums, decompression,
  bencode/XML parsing, expensive validation

session-store lane
  one bounded SQLite owner thread; never runs on a network/control reactor

bt lane
  libtorrent session thread(s), libtorrent callbacks, BT status bridge

rpc accept/runtime
  may share network runtime, but heavy request processing moves to control/cpu
```

The pools are separate executors or lanes, not necessarily one OS thread each.
Worker counts are configurable and capped by the global resource manager.

## Global Coordinator

Even though pools are split, they are not independent silos. A `ResourceManager`
owns process-wide budgets:

- maximum total threads,
- maximum network workers,
- maximum disk workers,
- maximum CPU workers,
- maximum blocking tasks,
- maximum in-flight buffers,
- maximum queued disk bytes,
- maximum active connections,
- maximum open files.

The scheduler makes admission decisions using those budgets.

## Default Sizing

Defaults come from one project-owned OS-thread budget rather than sizing every
lane independently:

```text
fixed_service_threads = 1 control/network + 1 session-store (persistent builds)
thread_budget = configured max-threads, otherwise max(3, cpu_count)
reserve control/network progress first
reserve the session-store owner next
assign remaining workers across cpu and blocking-disk/completion lanes
io_uring/IOCP completion work uses the reserved runtime lane, not an additional
  independently sized pool
bt lane is feature-gated and its configured internal threads count against the
  full-build budget
```

`memory` session mode used by tests/no-resume builds has no session-store thread
and may use the older minimum of 2. A persistent build rejects `max-threads < 3`
instead of spawning an unreported extra thread. At the minimum, the compact
profile uses one bounded shared disk/CPU worker and the blocking disk fallback;
the split Rayon and completion lanes begin only when the total budget can hold
them without violating the cap.

Small-machine defaults:

```text
1 core, persistent budget 3:
  1 combined control/network current-thread runtime
  1 dedicated session-store thread
  1 shared bounded blocking worker for disk/CPU jobs

2 cores, persistent budget 3:
  1 combined control/network current-thread runtime
  1 dedicated session-store thread
  1 shared bounded blocking worker for disk/CPU jobs

4 cores, budget 4:
  1 combined control/network progress thread
  1 dedicated session-store thread
  1 CPU worker
  1 blocking-disk worker (or backend completion lane)
```

At larger budgets, after the two fixed persistent services and at least one
disk/CPU progress worker, the network pool grows to at most 8 workers; CPU/disk
workers split the remainder according to the selected profile. A lane may be a
logical executor without a dedicated OS thread. The sum of all
downloader-owned live OS threads, including the session-store owner and the
configured libtorrent session/disk quota, must not exceed `thread_budget`. A
full build reports the fixed/session/BT shares separately and reduces other
workers or requires an explicit larger budget before startup.

The exact larger-machine formula is an implementation default, not ABI. It must
be visible through diagnostics, covered by 1/2/4-core tests in persistent and
memory modes, and overrideable.

## User Options

```text
--profile=auto|concurrency|throughput|latency|compact
--max-threads=N
--net-workers=N
--disk-workers=N
--cpu-workers=N
--bt-workers=N
--shared-worker-pool=true|false
```

`--profile` is the single user-facing preset for coordinated defaults across
threading, buffer pool, disk queue, and scheduler behavior. Worker-specific
options are advanced overrides.

`auto` is split-pool mode by default.

`compact` lowers thread counts and may share CPU/disk workers where safe.

`throughput` allows more disk/network concurrency within memory limits.

`latency` reserves more headroom for control/RPC and lowers bulk queue depths.

`shared-worker-pool=true` is for constrained builds or embedded use. It is not
the default and must still keep event-loop threads free from blocking disk calls.

## Work Placement Rules

Control lane:

- queue mutation,
- option validation,
- pause/remove admission,
- snapshot publication,
- signal handling.

Never here:

- hashing large buffers,
- XML/bencode parsing of untrusted large metadata,
- fsync,
- file allocation,
- blocking FFI.

Network runtime:

- socket reads/writes,
- TLS progress,
- protocol state machines,
- timers,
- lightweight header parsing.

Never here:

- regular file blocking I/O,
- CPU-heavy hashing,
- libtorrent session polling,
- long RPC response formatting over large collections.

Disk lane:

- `write_at`,
- `read_at`,
- allocation,
- fsync,
- rename/finalization,
- journal persistence.

Never here:

- network reads,
- RPC handlers,
- CPU-heavy digest loops except tiny metadata checks.

CPU pool:

- piece and chunk hashing,
- full-file verification,
- decompression filters when expensive,
- XML and bencode parsing,
- signature/checksum verification.

Never here:

- blocking disk operations,
- waiting on network sockets.

BT lane:

- libtorrent session operations,
- libtorrent alerts/callbacks,
- BT resume data extraction,
- normalized BT snapshot generation.

Never here:

- main scheduler locks,
- RPC handler work,
- shell hooks.

## Communication

Pools communicate with bounded channels:

- control -> network: start/cancel protocol workers,
- network -> disk: validated write blocks,
- disk -> cpu: hash/verify requests when not done inline,
- cpu -> control: verification result,
- bt -> control: normalized events and snapshots.

Queue implementations are selected per lane, not globally. See
`messaging-model.md` for the concrete choices: Tokio channels for control,
bounded Tokio wrappers for first-slice transfer lanes, optional measured SPSC/
thingbuf replacements, a reserved completion drain, and crossbeam bounded
channels for blocking workers.

Each channel has:

- operation count cap,
- byte cap where buffers are carried,
- structural control/journal priority: split urgent/bulk external lanes with
  bounded-burst fairness, plus permit-reserved internal completion lanes that
  external producers cannot consume (`messaging-model.md`),
- overload error or backpressure behavior.

## Avoiding Thread Explosion

Every subsystem requests workers from `ResourceManager`.

Rules:

- libraries with internal pools must be configured to fit `--max-threads`,
- libtorrent defaults are overridden in full build when possible,
- blocking fallback workers are fixed, not spawned per operation,
- CPU tasks are batched and cancellable,
- metrics expose live OS thread counts.
- automatic sizing tests assert that project-owned threads fit the one budget on
  1-, 2-, and 4-core hosts.

## When A Single Pool Is Acceptable

A single shared pool can be used in `compact` or test builds if:

- event-loop operations still use non-blocking APIs,
- blocking disk operations run through a bounded blocking facility,
- control/RPC tasks have priority,
- max threads and queue caps are low and explicit,
- C10k performance is not claimed for that build profile.

Production C10k builds should use split pools.

## Diagnostics

Expose:

- selected unified profile,
- explicit worker overrides,
- worker counts per pool,
- live threads per pool,
- queue depth per pool,
- task wait latency per pool,
- p99 event-loop lag,
- p99 disk completion latency,
- p99 hash queue latency,
- libtorrent adapter channel depth.
