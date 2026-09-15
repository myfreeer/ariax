# Performance Profiles

## Control Plane Measurement Protocol

`ariax-engine/benches/rpc_active_profile.rs` measures the real HTTP worker and
shared dispatcher in a separate engine process. The loopback origin and RPC
client run outside that process so its RSS/working-set samples exclude fixture
memory. One known-length task owns 1,000 concurrent HTTP/1.1 ranges across 125
loopback origins, with eight connections per origin, 1,000-way splitting and
64 KiB ingress frames. The fixture respects the transport's eight-connection
per-origin ceiling and the unchanged concurrency-profile process limits.

Each HTTP, WebSocket, Content-Length stdio and NDJSON scenario collects exactly
20,000 measured round trips: 12,000 status queries; 4,000 list projections of
128 tasks; 1,000 each of file, URI and option queries; and 1,000 actual mutations
of a separate auxiliary task. Mutations cycle through resume, pause, option
change, queue move, URI change, remove, result removal and fresh admission
(125 calls each). File and URI queries project 32 sources of about 2 KiB each.
Every mutation gets an additional state-verification call, counted against
the same burst limit. Reports include per-operation latency and actual response
sizes. List requests use supported status fields.

Runs use at most 1,000 calls or 500 ms per burst, followed by a 250 ms cooldown.
No new measured request starts after 400 ms, leaving time for its response and
any paired verification before the hard 500 ms limit.
Each scenario has a 90-second overall deadline; setup, barrier queries and
shutdown also have deadlines so a failed fixture cannot run indefinitely.
After each warm-up the origin pulses every open response and the harness waits
for both its 1,000-response acknowledgement and the engine's received-byte and
connection barriers before timing calls. The barrier is checked again afterward.
Every measured status response must also report 1,000 connections. Reports
separate total measured burst time, round-trip time and whole-scenario time.
The aggregate and every ordinary operation's p99 gate is 50 ms; missing range,
RSS or budget evidence fails the run. Incomplete runs remain failures.

A second consumer stops reading large source-list replies throughout the
measured bursts. A separate WebSocket consumer stops reading 512 KiB coalesced
fixture notifications through the production event broker; an event is refreshed
before each warm-up. Both retained response and event credits must release after
their respective consumers disconnect. For stdio, the measured client uses native OS pipes and the
second framed runner uses a loopback socket as its stalled writer. Both runners
share the production dispatcher and process budgets. Reports distinguish this
fixture from a second process-stdio handle, and record retained-credit growth,
release after disconnect, and the maximum sampled engine RSS and reservations.
Enable with `ARIAX_RUN_ACTIVE_RPC_BENCH=1`; select one bounded scenario with
`--scenario=http|websocket|content-length|ndjson`.
The separate `--administrative` scenario uses the production in-process backend
to import 128 tasks, resume/pause them in bulk, export/save, create 128 real
stopped results, purge them, and shut down. It reports zero active ranges.
Concurrent queries and urgent probes follow observed bulk progress; final
state checks establish captured membership and later-action precedence.
Concurrent query p99 and urgent acknowledgement must stay within 50 ms.
Total operation and shutdown durations are reported separately from those
acknowledgements. Administrative calls retain the burst, cooldown and
90-second scenario limits.
On Linux, run in a shell with a 20,000-file-descriptor soft limit, as for the
capacity harness; the default 1,024 limit cannot hold the origin listeners and
1,000 live responses. This changes only the benchmark shell and its children.

Native Linux acceptance is deferred at the user's request on September 13,
2026, until CI is ready. WSL 1 timings do not close that platform gate. The
manually dispatched `native-linux-rpc-benchmarks.yml` workflow runs the same four
optimized transport scenarios plus the administrative scenario, and fails on a missing barrier, incomplete call count,
unreleased stalled-consumer credit, memory overflow or latency-gate failure.
It preserves JSON reports and failed-run diagnostics as artifacts. A successful
native CI result must be recorded before closing native Linux acceptance.
The current implementation moves projection outside the owner and runs bounded
mutation continuations through the managed runtime; deterministic tests and the
expanded campaign validate those changes separately from the older results.

### Native Windows Phase 5 Evidence

The September 15, 2026 campaign passes on Phase 5 implementation `6a55b1f`,
using native Windows-GNU Rust 1.97.1, two Tokio workers per process, the
concurrency profile and `BlockingDiskLane`. `ARIAX_BENCH_METALINK=1` admits the
1,000 active HTTP ranges through Metalink with a complete SHA-256 chunk
manifest. Each transport completes 20,000 measured calls and 1,000 additional
mutation-verification calls. Every operation's p99 meets the 50 ms gate.

| Transport | Measured Calls | Aggregate p99 (ms) | Worst Operation p99 (ms) | Longest Burst (ms) | Scenario Time (s) | Peak Sampled Working Set (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP | 20,000 | 17.260 | 34.435 | 414 | 44.650 | 139.09 |
| WebSocket | 20,000 | 17.064 | 28.010 | 417 | 44.910 | 140.52 |
| Content-Length stdio | 20,000 | 17.424 | 35.674 | 420 | 51.655 | 142.63 |
| NDJSON stdio | 20,000 | 17.605 | 38.946 | 419 | 50.184 | 140.48 |

The worst operation is `addUri` for HTTP and Content-Length, and `changeUri`
for WebSocket and NDJSON. No burst exceeds 426 calls or 420 ms; cooldowns are
250 ms. All renewed range barriers, stalled-credit releases, resource limits
and clean shutdown checks pass. Peak RPC and resident reservations are
37.77 MiB against 64 MiB and 194.98 MiB against 896 MiB, respectively.
Transport owner turns use at most 12 steps. Maximum observed owner turn and
lock wait are 6.514 ms and 0.524 ms; the approximately 1 ms owner budget remains
cooperative rather than a hard wall-clock deadline.

The separate administrative scenario completes in 10.298 seconds with zero
active ranges. It imports/exports/saves 128 tasks, preserves later per-task
intent during bulk resume/pause, creates and purges 128 real stopped results,
and shuts down cleanly. Concurrent query p99 peaks at 3.790 ms, urgent
acknowledgement at 17.380 ms, and the longest query burst at 402.025 ms.
Shutdown acknowledgement and drain take 0.016 ms and 50.337 ms. Total operation
durations remain separate from the query and urgent acknowledgement gates.

All five scenarios pass on their first attempt. Compilation and fuzzing finish
before measurement; no compiler process is observed by preflight or the
five-second native process samples. The
[raw reports](performance-evidence/phase5-windows-gnu-2026-09-15.json) retain
commands, source/binary hashes, runtime/resource observations and fuzz attempts.
The [validation record](performance-evidence/phase5-validation-2026-09-15.md)
also covers protocol security, OpenSSH interoperability, workspace checks and
the bounded fuzz campaign. Native Linux acceptance stays deferred until CI is
ready. The P4 reports below retain their original scope and results.

### Native Windows Control Plane Evidence

The September 14, 2026 campaign passes all four transports on the P4-11
implementation with native Windows-GNU Rust 1.97.1, two Tokio workers per
process, the concurrency profile and `BlockingDiskLane`. Each transport completes
20,000 measured calls and 1,000 additional mutation-verification calls. Every
ordinary operation's p99 meets the 50 ms limit. Stalled response/event credits
release, range barriers remain satisfied, and all engine shutdowns are clean.

| Transport | Measured Calls | Aggregate p99 (ms) | Worst Operation p99 (ms) | Longest Burst (ms) | Scenario Time (s) | Peak Sampled Working Set (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP | 20,000 | 17.403 | 30.318 | 421 | 43.995 | 138.44 |
| WebSocket | 20,000 | 17.682 | 37.543 | 420 | 48.252 | 139.57 |
| Content-Length stdio | 20,000 | 17.444 | 26.032 | 419 | 50.213 | 139.25 |
| NDJSON stdio | 20,000 | 17.282 | 27.704 | 420 | 43.800 | 139.04 |

The worst operation is `addUri` in each scenario. The 128-row list response is
20,609 bytes; the 32-source file and URI responses are 67,561 and 67,415 bytes.
The largest sampled RPC reservation is 37.77 MiB against 64 MiB, and resident
reservation is 192.97 MiB against 896 MiB. Owner lock wait peaks at 19 microseconds;
transport owner turns use at most 12 steps. The largest observed turn is
1.815 ms: the approximately 1 ms scheduling budget is cooperative, not a hard
wall-clock deadline. Productive turns yield without an added timer wait;
unready completions retain polling backoff.

The separate administrative scenario completes in 8.988 seconds with zero active
ranges. It imports/exports/saves 128 tasks, preserves later per-task intent during
resume-all and pause-all, creates and purges 128 real stopped results, and shuts
down cleanly. Import takes 1.979 seconds; resume-all 0.733 seconds; pause-all
1.444 seconds; export 1.942 ms; configured save 8.660 ms; and purge 0.535 seconds.
Concurrent query p99 peaks at 1.523 ms and urgent acknowledgement at 16.631 ms.
Its longest query burst is 401.048 ms, owner turns use at most 13 steps, and
shutdown acknowledgement/drain take 0.010/38.849 ms.

The [raw reports](performance-evidence/p4-11-windows-gnu-2026-09-14.json) record
per-operation counts, response sizes, runtime metrics, budgets, shutdown
boundaries and binary identity. They also retain the earlier failed HTTP run:
it completed all samples but `addUri` p99 was 55.507 ms. Removing timer delays
between ready continuation turns produced the passing campaign above; the
latency, burst and scenario limits were unchanged. The
[validation record](performance-evidence/p4-11-validation-2026-09-14.md) covers
tests, toolchains, fuzzing and deferred coverage. Native Linux acceptance remains
deferred until CI is ready.

### Historical September 13 Evidence

The historical September 13, 2026 run used native Windows-GNU Rust 1.97.1, two Tokio
workers per process, the concurrency profile and `BlockingDiskLane` storage.
Each scenario completed 19,000 status calls and 1,000 global-template mutations
in 20 bursts, with 250 ms cooldowns and renewed barriers after warm-up. All
stalled response/event credits released, and every engine shutdown was clean.

| Transport | Measured Calls | p99 (ms) | Longest Burst (ms) | Measured Burst Time (s) | Scenario Time (s) | Peak Working Set (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP | 20,000 | 0.371 | 169 | 2.587 | 14.508 | 128.94 |
| WebSocket | 20,000 | 0.331 | 131 | 2.110 | 13.614 | 127.70 |
| Content-Length stdio | 20,000 | 0.542 | 343 | 5.833 | 17.093 | 128.81 |
| NDJSON stdio | 20,000 | 0.344 | 155 | 2.524 | 14.036 | 127.31 |

The largest sampled RPC reservation was 36.40 MiB against 64 MiB; the largest
shared resident reservation was 192.65 MiB against 896 MiB. Working set stayed
below the 1 GiB profile target. Measured burst time excludes warm-up, barriers,
cooldowns and cleanup; scenario time includes them. These are sampled memory
maxima, with admission limits enforced independently by permits. The
[raw reports](performance-evidence/phase-4b-windows-gnu-2026-09-13.json) include
all limits, retained-credit release, elapsed times, toolchain identity and
the tested binary hash. Stdio's second stalled writer uses the socket fixture
described above; the measured stdio round trips use native OS pipes.
This report predates the P4-11 progress implementation and does not validate it.

## Profile Implementation

Status: executable profile and HTTP-capacity slice implemented and recorded;
optimized Linux and native Windows-GNU HTTP-capacity runs are recorded below.
The native Windows control-plane runs above also pass, including Phase 5
Metalink admission. FTP/FTPS and SFTP share the implemented resource owners.
Adaptive tuning, native Linux control-plane acceptance, and the remaining
release-platform matrix remain pending; native Linux acceptance is deferred
until CI is ready.

The runtime resolver now owns the exact preset matrix below, subtracts the
64-handle control reserve from the native soft handle limit, and derives shared
process/socket/file and resident-byte budgets. The RPC binary accepts
`--profile=auto|concurrency|throughput|latency|compact`; its HTTP transport,
HTTP ingress, and storage buffer pool share the resolved resident budget, while
transport sockets consume the process/socket handle permits. Selected storage
files share file-handle permits, as do the Phase-5 protocol adapters; the
evictable file-handle LRU remains later work. `auto` currently resolves to the
concurrency baseline; adaptive movement inside the guardrails is a later
milestone.

C10k and maximum throughput are related but not identical goals.

- C10k means the downloader can keep many connections/tasks alive without
  memory blowup or control-plane stalls.
- Maximum throughput means the downloader pushes the active bottleneck, often
  disk, CPU hashing, TLS, NIC, or remote servers, as hard as possible.

One automatic profile can be good for both in many cases, but there are real
tradeoffs. The design exposes presets and lets the adaptive scheduler move
within each preset.

## Unified User Profile

```text
--profile=auto|concurrency|throughput|latency|compact
```

`auto`:

- default,
- starts with balanced limits,
- adapts based on memory, disk, CPU, and network signals,
- should be best for most users.

`concurrency`:

- optimized for many tasks/connections,
- lower per-connection buffer use,
- smaller per-task segment windows,
- stricter queue caps,
- fair scheduling across tasks,
- lower memory per socket,
- slightly lower peak throughput for a single large file.

`throughput`:

- optimized for a smaller number of active large transfers,
- larger buffers where useful,
- deeper disk queue if latency stays healthy,
- more active segments per large file,
- more aggressive write coalescing and batching,
- may use more memory and can increase control p99 latency if caps are too high.

`latency`:

- optimized for responsive RPC/control and interactive pause/remove,
- reserves more headroom for control work,
- lower disk queue caps,
- lower batch windows,
- useful when aria2 RPC clients poll frequently.

`compact`:

- optimized for small binaries and low idle memory,
- fewer worker threads,
- smaller hot buffer reserves,
- optional shared worker pool,
- no C10k claim.

## Can One Profile Be Optimal For Both?

Sometimes, yes:

- NVMe disk,
- fast CPU,
- enough RAM,
- stable remote servers,
- large sequential files,
- no expensive checksum/decompression bottleneck.

In that case `auto` may converge close to throughput mode while preserving
C10k-safe caps.

Sometimes, no:

- HDD or high-latency network filesystem,
- many small files,
- expensive Metalink chunk hashes,
- slow CPU or TLS bottleneck,
- thousands of slow connections,
- tight memory limits,
- heavy RPC polling.

In those cases, max throughput for one task can harm fairness and responsiveness
for many tasks. The scheduler must choose based on profile.

## How C10k Affects Throughput

C10k design improves throughput under mixed load by preventing collapse:

- idle sockets do not consume large buffers,
- slow peers do not hold scarce disk buffers forever,
- disk backpressure stops network reads before memory explodes,
- control plane remains able to pause/remove/reprioritize work,
- per-host and per-task budgets avoid one source monopolizing the process.

But C10k safety can reduce peak single-download throughput if limits are too
conservative:

- smaller buffers mean more syscalls,
- shallower disk queues may underutilize NVMe,
- fair scheduling may reduce one task's segment count,
- frequent journal/fsync in strict durability mode can cap speed.

That is why throughput profile exists.

## Baseline Queue And Budget Defaults

These are the registry-controlled internal defaults the profiles start from.
They are normative as defaults — CI asserts the resolved values — but every one
is a registry entry that a profile or explicit option may override. Sizes
assume a host with ≥ 4 GiB RAM; `compact` halves byte budgets and queue depths
twice (quarter), and `auto` scales between `concurrency` and `throughput`
inside these guardrails.

| Lane / budget | concurrency | throughput | latency | compact |
| --- | --- | --- | --- | --- |
| accounted resident target / permit limit | 1 GiB / 896 MiB | 2 GiB / 1792 MiB | 768 MiB / 672 MiB | 128 MiB / 112 MiB |
| process handle target | 16384 | 8192 | 8192 | 1024 |
| logical socket / file subcaps | 12288 / 4096 | 4096 / 4096 | 4096 / 2048 | 512 / 512 |
| `control_urgent` capacity (commands) | 256 | 256 | 256 | 64 |
| `control_bulk` capacity (commands) | 1024 | 1024 | 512 | 128 |
| `urgent_burst` (drain fairness) | 32 | 32 | 16 | 8 |
| write lane `HotMpscLane` (items / bytes) | 512 / 64 MiB | 1024 / 256 MiB | 256 / 32 MiB | 128 / 8 MiB |
| disk submission in flight (`disk-queue-ops` / `disk-queue-bytes`) | 128 / 64 MiB | 256 / 256 MiB | 64 / 32 MiB | 32 / 8 MiB |
| `CompletionDrain` capacity | = disk-queue-ops (permit-reserved) | = | = | = |
| hash lane (jobs / bytes) | 64 / 32 MiB | 128 / 128 MiB | 32 / 16 MiB | 16 / 4 MiB |
| journal appender inbox (facts) | 1024 | 2048 | 512 | 256 |
| balanced durability group (bytes / max age / pieces) | 16 MiB / 1 s / 1024 | 64 MiB / 2 s / 4096 | 4 MiB / 250 ms / 256 | 8 MiB / 2 s / 512 |
| `BufferPool` total (`buffer_budget`) | 256 MiB | 1 GiB | 128 MiB | 32 MiB |
| quarantine budget (within pool total) | 32 MiB | 64 MiB | 16 MiB | 8 MiB |
| `disk-cache` retained-span default (within pool total) | 0 | 0 | 0 | 0 |
| `http_ingress_budget` | 64 MiB | 256 MiB | 32 MiB | 8 MiB |
| `sftp_ingress_budget` | 32 MiB | 128 MiB | 16 MiB | 4 MiB |
| `piece_metadata_budget` | 128 MiB | 256 MiB | 64 MiB | 16 MiB |
| `task_metadata_budget` | 64 MiB | 256 MiB | 32 MiB | 8 MiB |
| `transform_budget` (baseline feature set) | 0 | 0 | 0 | 0 |
| RPC pending work (`rpc_budget`, items / bytes) | 128 / 64 MiB | 128 / 64 MiB | 256 / 128 MiB | 32 / 32 MiB |
| `journal_state_budget` | 32 MiB | 64 MiB | 32 MiB | 20 MiB |
| `sqlite_cache_budget` | 16 MiB | 32 MiB | 8 MiB | 4 MiB |
| `metadata_cache_budget` | 32 MiB | 64 MiB | 16 MiB | 4 MiB |
| `cpu_scratch_budget` | 32 MiB | 128 MiB | 16 MiB | 4 MiB |
| downloader worker stack reservation | 2 MiB/thread | 2 MiB/thread | 2 MiB/thread | 1 MiB/thread |
| default HTTP/2 stream / connection window | 256 KiB / 1 MiB | 2 MiB / 8 MiB | 128 KiB / 512 KiB | 64 KiB / 256 KiB |
| per-client event queue (events / serialized bytes) | 256 / 4 MiB | 256 / 4 MiB | 512 / 8 MiB | 64 / 1 MiB |
| stopped-result retention (`max-download-result`) | 1000 | 1000 | 1000 | 250 |

Phase 4B now enforces shared profile RPC/resident reservations, four client
request leases, bounded parsing, and response credit through transport release.
Large source/session results use borrowed preflight views and typed input
preparation reserves its temporary copies; native calls retain projection
credit through typed conversion. Scheduler simulation and status-draft copies
reserve before mutation and retain credit while driver work is pending. The
1,000-active-task forecast test covers allocation contracts. The native Windows
active-download/RSS and latency evidence above closes that platform's `P4-11`
measurement; native Linux acceptance remains deferred until CI is ready.

Cache/cardinality defaults are also registry-owned and admission-visible:

| Cache / metadata cap | concurrency | throughput | latency | compact |
| --- | --- | --- | --- | --- |
| HTTP idle connections (global / per origin) | 512 / 2 | 256 / 8 | 128 / 2 | 32 / 1 |
| HTTP idle-pool estimated memory / timeout | 32 MiB / 60 s | 32 MiB / 60 s | 16 MiB / 30 s | 4 MiB / 30 s |
| DNS positive / negative entries | 4096 / 512 | 4096 / 512 | 2048 / 256 | 512 / 64 |
| cookie jar total entries | 3000 | 3000 | 3000 | 512 |
| server-stat entries | 4096 | 4096 | 2048 | 512 |
| exported per-entity metric top-N | 100 | 100 | 100 | 32 |

Additional hard cardinality rules are profile-independent: at most 32 addresses
from one DNS answer enter Happy Eyeballs; TTL=0 is not cached, a positive TTL is
clamped to at most 86400 seconds, and a negative entry lives no longer than 30
seconds. A cookie is at most 4096 bytes, at most
180 cookies and 64 KiB of cookie bytes are retained per registrable domain, and
expired cookies are evicted before LRU pressure. One task accepts at most 1024
source URIs/mirrors; a larger metadata/input set fails with `ResourceLimit`
before task creation. One task has at most 262,144 file-layout entries and
64 MiB of canonical layout data; all variable task metadata also needs a
`task_metadata_budget` reservation. Server statistics expire under
`server-stat-timeout`
(default 86400 seconds) and then LRU-evict. Detailed diagnostics retain aggregate
counters plus only the 16 most recent per-task error/retry events; exported
metrics use the top-N plus one `other` bucket, never an unbounded URL/host label.

HTTP idle-pool memory is an admission sublimit on `conn_overhead`, and cache
entries are sublimits on `metadata_cache_budget`; they are not additional
resident-equation terms. Raising a count without enough parent memory budget
reduces admission rather than permitting silent growth.

Derived invariants CI must assert on the resolved defaults:

- `CompletionDrain` capacity equals accepted-submission capacity (permits make
  overflow unrepresentable),
- write-lane bytes ≤ `buffer_budget − quarantine`, and disk-queue bytes ≤
  write-lane bytes. These are overlapping ownership-stage sublimits on the same
  pooled leases, not three additive resident allocations,
- HTTP/2 admission reservation per connection
  (`min(connection_window, streams × stream_window)` + frame + header
  allowance) times the connection cap fits `http_ingress_budget`,
- SFTP admission proves `(packet_buffer_cap + requested_data_len)` for every
  outstanding request fits `sftp_ingress_budget`; the default per-channel
  request count is 8 and the hard count maximum is 64,
- RPC serialization reserves one response item and bytes before producing
  chunks; one client is capped at one 16 MiB response, four accepted requests /
  8 MiB request state, plus its event-queue byte share and cannot consume the
  process budget,
- logical socket/file subcaps share the process handle target and resolve down
  to the native `RLIMIT_NOFILE`/handle capability minus a 64-handle control
  reserve; C10k admission fails explicitly when the OS limit is lower,
- every queue capacity above is finite and visible in diagnostics,
- a balanced durability group closes at the first resolved byte, age, or piece
  threshold (and always on pause/remove/finalize/shutdown), so tuning cannot
  leave verified data waiting indefinitely for a data/journal barrier.

## Resident Memory Equation

C10k claims are evaluated against the whole resident budget, not `BufferPool`
alone. Every allocation takes a domain permit and a global resident permit.
Domain caps are workload guardrails and may sum above the resident target;
actual simultaneous reservations may not. The global gate is:

```text
accounted_resident_limit ≥
    buffer_reserved                   (pool incl. disk-cache retention + quarantine)
  + http_ingress_reserved             (Hyper frames/read buffers, h2 windows,
                                       response headers, dynamic TLS buffers)
  + sftp_ingress_reserved             (russh-sftp DATA vectors and bounded
                                       response framing before pool copy)
  + conn_count × conn_overhead        (fixed HTTP/TLS/socket/pool-entry state only;
                                       excludes ingress-reserved bytes;
                                       measured per platform in Phase 0, budgeted
                                       ≤ 32 KiB idle / ≤ 96 KiB active TLS)
  + task_count × task_base_overhead   (fixed snapshot/scheduler shell;
                                       budgeted ≤ 32 KiB for the base task,
                                       excluding variable metadata below)
  + task_metadata_reserved            (layouts, sources, options, bounded history)
  + piece_metadata_reserved           (packed durable/verified maps and bounded
                                       sparse active-piece state)
  + transform_reserved                (relocatable decode/decompress output;
                                       zero while the feature is gated)
  + rpc_response_reserved             (serializer chunks and transport-pending bytes)
  + queue_metadata_reserved           (ring slots, descriptors, permits, and bounded
                                       non-pooled message payloads; queued
                                       BufferLeases stay in buffer_reserved;
                                       excludes RPC bytes above)
  + journal_state_reserved            (appender buffers + indexes; bounded by
                                       compaction triggers and the FD cap)
  + sqlite_cache_reserved             (page cache, bounded by PRAGMA cache_size)
  + metadata_cache_reserved           (DNS/cookies/server stats, bounded counts)
  + cpu_metadata_and_scratch_reserved (job descriptors and non-pooled private
                                       scratch only; hash input leases stay in
                                       buffer_reserved)
  + bt_reserved                       (libtorrent session budget, full build only;
                                       configured into libtorrent settings)
  + fixed_process_reserve             (TLS roots and runtime stacks:
                                       thread_count × stack size)
```

Every term is an allocation domain counted exactly once, individually bounded,
and exported in diagnostics with the same names. `accounted_resident_limit` is
7/8 of `resident_target`; the remaining 1/8 is non-allocatable headroom for
allocator fragmentation and measured framework/process overhead. Stage byte
caps (write lane, disk queue, hash reorder, disk-cache retention) may overlap as
sublimits on the same pooled lease; diagnostics show both the stage charge and
its parent allocation domain but the resident sum uses only the parent once.
RSS sampling is a defensive earlier-stop signal, not permission to allocate
outside these charges.

The `concurrency` C10k gate is 10,000 concurrently open low-activity sockets,
not 10,000 retained HTTP keep-alive pool entries (that pool remains capped at
512). The profile idle-connection reserve is at most 320 MiB, leaving the rest
of the 896 MiB accounted limit for buffers, active ingress, task state, queues,
caches, and fixed reserves. Admission reduces those active domains as the
socket count rises. Exceeding any domain or the global limit is backpressure or
admission refusal, never silent growth.

### Executable Capacity Evidence

The checked-in `ariax-engine` benchmark harness uses the same process-owned
transport and ingress permits as the RPC worker. It requires an operating-system
RSS/working-set sample, rejects a sample above the profile target, and holds a
second barrier while all 1,000 HTTP responses, sockets, and ingress permits are
simultaneously live. Missing platform instrumentation is a benchmark failure,
not an optional `None` result.

The optimized Rust 1.97.1 harness was run on August 18, 2026 with the
repository-local Linux toolchain and an isolated 20,000-handle soft limit. It
opened 10,000 real loopback sockets and then drove 1,000 simultaneous real
loopback HTTP `206` range responses, each 64 KiB:

```text
profile=concurrency sockets=10000 active_ranges=1000 connect_ms=1658 active_reservation_ms=37 resident_reserved_bytes=393216000 accounted_limit_bytes=939524096 resident_target_bytes=1073741824 rss_kib=47828
profile=concurrency http_ranges=1000 http_bytes=65536000 http_ms=477 active_http_resident_bytes=98304000 active_http_sockets=1000 accounted_limit_bytes=939524096 rss_kib=43956
```

The same optimized harness was run natively with the repository's Windows-GNU
Rust 1.97.1 distribution through MSYS2 `MINGW64` on August 18, 2026. The safe
Windows adapter reads the current process working set through
`K32GetProcessMemoryInfo`:

```text
profile=concurrency sockets=10000 active_ranges=1000 connect_ms=910 active_reservation_ms=21 resident_reserved_bytes=393216000 accounted_limit_bytes=939524096 resident_target_bytes=1073741824 rss_kib=74260
profile=concurrency http_ranges=1000 http_bytes=65536000 http_ms=417 active_http_resident_bytes=98304000 active_http_sockets=1000 accounted_limit_bytes=939524096 rss_kib=56720
```

Both runs prove native socket/ingress/resident-permit admission, the full
active-range transfer, and measured process residency below the 1 GiB
concurrency-profile target. The socket fixture actively closes first and the
client observes EOF before releasing its permits, so repeated harness runs do
not exhaust client ephemeral ports with 10,000 `TIME_WAIT` entries.

With the ordinary 1,024-handle WSL limit, the same C10k-specific admission
returns an explicit rejection (`resolved 960` after the control reserve). The
service may still run at its scaled profile caps; a C10k claim requires the
explicit capacity gate and a native platform benchmark. The Linux and
Windows-GNU runs above are release-mode platform evidence; macOS and the
complete release matrix remain separate gates.

## Tunables By Profile

Concurrency profile:

- lower default transfer buffer class, often 16-64 KiB,
- low buffers per connection,
- low per-task segment window,
- strict per-host fairness,
- conservative disk queue bytes,
- shorter status snapshot intervals.

Throughput profile:

- larger buffer class, often 256 KiB-1 MiB for fast streams,
- higher per-task segment window,
- deeper disk queue when p99 latency is stable,
- more write coalescing,
- larger hash batches,
- registered buffers where backend supports them.

Latency profile:

- reserve control CPU time,
- reserve journal/control disk queue slots,
- lower bulk batch sizes,
- cap RPC response work per tick,
- avoid long synchronous formatting.

Compact profile:

- fewer worker threads,
- lazy buffer allocation with small hot reserve,
- lower max idle connections unless user overrides,
- minimal optional features.

## Adaptive Scheduler

The scheduler observes:

- active vs idle connection ratio,
- disk latency and queue depth,
- buffer wait time,
- hash queue latency,
- CPU utilization,
- event-loop lag,
- per-host throughput,
- retry/error rate.

It adjusts:

- segment window,
- buffer size class,
- disk queue target,
- write coalescing window,
- per-host connection share,
- piece selection strategy,
- new task admission.

The profile sets guardrails. Adaptive logic moves inside those guardrails.

## One Profile, Many Subsystems

There should be one user-facing profile option, not separate profile knobs for
workers, buffers, disk, and scheduling. `--profile` selects coordinated defaults
for:

- worker counts,
- buffer pool hot reserve and max size class,
- HTTP ingress budget, HTTP/1 read-buffer policy, and HTTP/2 window/frame
  defaults (explicit user overrides still win),
- disk queue bytes and operation caps,
- adaptive backpressure guardrails,
- per-task segment windows,
- per-host fairness,
- RPC/control headroom,
- write coalescing,
- hash batch sizing,
- optional feature defaults in compact builds.

Advanced users can still override individual knobs. Once overridden, diagnostics
should show both:

```text
profile: throughput
override: disk-queue-bytes=256M
override: cpu-workers=8
```

The profile remains the baseline, and explicit options override only their
specific setting.

## Success Metrics

The Phase-4B control-plane acceptance target is p99 at most 50 ms for status
queries and ordinary control acknowledgements under 1,000 simultaneously active
HTTP ranges on native optimized Linux and Windows-GNU builds. Warm up before
collecting 20,000 measured calls per transport scenario. Include stalled event
and response consumers alongside responsive clients, and record latency, lock
wait, budget peaks, and RSS/working set. Downloads must remain active throughout
measurement. Administrative import/export and final shutdown drain are timed
separately from ordinary control acknowledgements. WSL/DrvFS timings remain
diagnostic rather than native release evidence.

Local collection uses short bursts to avoid CPU-frequency degradation during
sustained load. Default to at most 1,000 calls or 500 ms of measurement per
burst, with a brief warm-up and at least 250 ms of unloaded cooldown between
bursts. Reestablish the active-range barrier before every measured burst. The
20,000-call scenario target is an aggregate across those bursts; retain all
samples and report actual counts, elapsed load time, and any incomplete scenario.
A time cap never converts an incomplete run into passing acceptance evidence.

The P4-11 campaign includes substantial list/metadata projection
and real per-task mutations, with per-operation counts and latency summaries.
Keep ordinary mutation targets separate from the task owning the active-range
fixture. Measure pause-all/resume-all/purge and administrative import/export or
shutdown separately: bulk operations may change the active population and must
not claim a continuously maintained 1,000-active-range barrier. Report their
actual task cardinalities, completion counts, and total durations, as well as
concurrent query and urgent-command progress. Keep native Linux acceptance
deferred until CI is ready.

Administrative urgent probes follow observed bulk progress so their ordering
is established by published state. Concurrent query p99 and urgent probe
acknowledgements use the same 50 ms limit. Measure engine shutdown through
process exit separately from origin-fixture cleanup. List projections request
supported status fields; file and URI metadata projections report their actual
source counts and response sizes.

C10k profile success:

- 10,000 concurrent low-activity sockets with bounded memory; no claim to keep
  10,000 fully idle reusable HTTP connections,
- RPC p99 latency under target,
- no event-loop starvation,
- no unbounded queues.

Throughput profile success:

- saturates disk/NIC/server bottleneck for large downloads,
- keeps CPU hashing pipeline fed,
- avoids unnecessary extra copies,
- maintains correctness and recovery guarantees.

Auto profile success:

- approaches throughput profile when resources are healthy,
- backs off toward concurrency/latency behavior when pressure rises,
- never exceeds hard memory/thread/file/socket budgets.
