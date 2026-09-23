# Event Backend And I/O Selection

[Documentation](../README.md)

Status: production HTTP/RPC uses Tokio and the bounded blocking disk lane.
The selection options, native io_uring/IOCP probes and disk failover below remain
target contracts; those native disk backends are not implemented. Supporting
native CI remains required before enabling them.

The downloader uses Tokio/Mio as its one network reactor architecture and
selects disk I/O independently. It must not crash when a preferred disk API is
missing, blocked by policy, or unsupported by the running kernel.

## User Options

```text
--event-backend=auto|tokio
--event-backend-fallback=true|false
--disk-io-backend=auto|uring|iocp|blocking
--disk-failover-drain-timeout=SEC
--net-workers=N
--disk-workers=N
--cpu-workers=N
```

Defaults:

- `event-backend=auto`
- `event-backend-fallback=true`
- `disk-io-backend=auto`
- worker counts derived from CPU count and memory budget

`event-poll` is accepted as a deprecated alias for compatibility. Its legacy
values (`epoll`, `kqueue`, `port`, `poll`, `select`) express an expected platform
selector, not a request to instantiate a second raw reactor. The alias succeeds
only when Tokio/Mio reports the corresponding supported selector; otherwise the
normal fallback/error policy applies and one warning is emitted.

If the user explicitly selects an unavailable supported backend:

- With fallback enabled, log a warning, expose the fallback reason in RPC
  diagnostics, and continue with the next viable backend.
- With fallback disabled, exit with a clean configuration error. Never panic,
  assert, abort, or segfault.

## Selection Order

Network on every supported platform:

1. construct the configured Tokio runtime,
2. record Mio's selected platform selector/capabilities for diagnostics,
3. if construction fails, return a typed startup error. There is no raw
   epoll/kqueue/poll/select reactor hidden behind fallback.

Disk selection is independent:

- Linux: io_uring after runtime probe, otherwise bounded blocking pool.
- Windows: overlapped/IOCP file backend after runtime probe, otherwise bounded
  blocking pool.
- macOS/BSD/other supported targets: bounded blocking pool until a maintained
  completion backend passes its own design and benchmark gate.

`sync` is not a production option: test harnesses may instantiate a synchronous
fake directly, but release configuration never permits transfer file I/O on a
network/control caller thread.

## Runtime Probe Requirements

Compile-time availability is not enough. Startup must run a small runtime probe.

`io_uring` probe:

- call `io_uring_setup` with a small queue,
- submit a no-op or harmless operation,
- handle `ENOSYS`, `EPERM`, `EINVAL`, `ENOMEM`, and seccomp denial,
- close all fds on failure,
- record disabled features such as registered buffers, fixed files, SQPOLL, or
  network operations.

`IOCP` probe:

- create a completion port,
- associate a harmless handle where possible,
- close all handles.

## Backend Interface

There is no project-owned `EventBackend` trait. Tokio owns task scheduling,
socket registration, wakeups, timers, and cancellation. Diagnostics expose a
read-only `NetworkRuntimeInfo` snapshot containing Tokio/Mio version, platform
selector, worker count, and relevant capability/initialization errors.

`DiskBackendKind` and `DiskWriteOutcome` are defined normatively in
[detailed-runtime.md](detailed-runtime.md). This document only defines the capability/probe/fallback
requirements for its concrete variants. The buffer type is the move-only
`BufferLease`, never a separate `Buffer` type.

The rest of the engine depends on capabilities, not concrete system APIs.

Important disk capabilities:

- regular file support,
- vectored read/write,
- cancellation,
- timeouts,
- buffer registration,
- file allocation,
- fsync support,
- maximum descriptors,
- known unsupported operations.

## Tokio Integration

Tokio is the network executor/reactor because it is mature and has the strongest
Rust ecosystem support. It is not the regular-file I/O strategy.

Tokio responsibilities:

- task scheduling,
- timers,
- TCP/UDP readiness on supported platforms,
- channels and cancellation,
- async RPC server.

Non-Tokio responsibilities:

- Linux io_uring disk operations where available,
- Windows overlapped file I/O where available,
- blocking disk pool fallback,
- backend probing and fallback diagnostics.

No blocking file I/O is allowed on network reactor threads.

## Graceful Degradation

When a disk backend falls back:

- log one concise warning at startup,
- include `requested_backend`, `selected_backend`, `reason`, and
  `performance_impact`,
- expose the same fields through `aria2.getVersion` extension metadata and a
  `getBackendInfo` diagnostic RPC method,
- keep user-configured hard limits unless they exceed backend capabilities, in
  which case reject them with a clean error.

Startup probe fallback and live failover are distinct. A startup probe has no
accepted operations and may select the fallback immediately. A live backend
must first enter the disk-adapter failover barrier: stop submissions, cancel and
drain every accepted operation, return/quarantine every lease exactly once,
abort provisional spans, and close backend-bound file handles. Only a fully
settled task may reopen its files through safe capabilities on the fallback and
requeue under a fresh generation/backend epoch. If any accepted write remains
completion/cancellation-uncertain, that task fails closed with recoverable
partial state and receives no overlapping fallback I/O in the same process;
unaffected/new tasks may use the fallback. [disk-adapter.md](../storage/disk-adapter.md) owns the sequence.

Example:

```text
requested disk backend io_uring is unavailable: EPERM from io_uring_setup,
seccomp or policy denial detected. network remains Tokio/Mio; selected
blocking-pool for disk.
```

## No Hard Crash Rule

Backend selection must never use `assert`, `unwrap`, `expect`, or abort paths
for runtime capability failures. Fatal errors are represented as typed
configuration errors with actionable messages.

This explicitly avoids the aria2 pattern in aria2's
`src/DownloadEngineFactory.cc` where unsupported or failed event poll selection
can throw during construction and the final path asserts.

## Benchmarks Per Backend

Each backend needs benchmark gates:

- idle socket scalability,
- active transfer throughput,
- event-loop p99 delay,
- disk write throughput with 4 KiB, 64 KiB, 1 MiB buffers,
- mixed RPC latency under load,
- fallback startup behavior in forced failure tests.

Linux CI should include a test that blocks `io_uring_setup` and verifies blocking
disk fallback while Tokio networking remains available. Windows CI should verify
overlapped disk selection/fallback. macOS CI should verify Tokio/Mio kqueue
network diagnostics plus the bounded disk pool. Legacy `event-poll` aliases need
platform compatibility tests and must never instantiate a raw reactor.
