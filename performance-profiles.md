# Performance Profiles

Status: executable profile and HTTP-capacity slice implemented; optimized Linux
and native Windows-GNU HTTP-capacity runs are recorded below. Adaptive tuning,
non-HTTP resource wiring, and the remaining release-platform matrix remain
pending.

The runtime resolver now owns the exact preset matrix below, subtracts the
64-handle control reserve from the native soft handle limit, and derives shared
process/socket/file and resident-byte budgets. The RPC binary accepts
`--profile=auto|concurrency|throughput|latency|compact`; its HTTP transport,
HTTP ingress, and storage buffer pool share the resolved resident budget, while
transport sockets consume the process/socket handle permits. File-handle and
non-HTTP consumers are not wired yet. `auto` currently resolves to the
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
transport and ingress permits as the RPC worker. With an isolated Linux soft
handle limit of 20,000, the concurrency profile opened 10,000 real loopback
sockets and then drove 1,000 simultaneous real loopback HTTP `206` range
responses, each 64 KiB:

```text
profile=concurrency sockets=10000 active_ranges=1000
resident_reserved_bytes=393216000
accounted_limit_bytes=939524096 resident_target_bytes=1073741824
rss_kib=Some(47828)
```

The same run's active transfer phase reported:

```text
profile=concurrency http_ranges=1000 http_bytes=65536000 http_ms=446
active_http_resident_bytes=98304000 active_http_sockets=1000
accounted_limit_bytes=939524096 rss_kib=Some(156636)
```

The optimized Rust 1.97.1 harness was rerun on August 18, 2026 with the
repository-local Linux toolchain and an isolated 20,000-handle soft limit. It
reported:

```text
profile=concurrency sockets=10000 active_ranges=1000 connect_ms=2894 active_reservation_ms=48 resident_reserved_bytes=393216000 accounted_limit_bytes=939524096 resident_target_bytes=1073741824 rss_kib=Some(47812)
profile=concurrency http_ranges=1000 http_bytes=65536000 http_ms=549 active_http_resident_bytes=98304000 active_http_sockets=1000 accounted_limit_bytes=939524096 rss_kib=Some(156468)
```

The same optimized harness was run natively with the repository's Windows-GNU
Rust 1.97.1 distribution through MSYS2 `MINGW64` on August 18, 2026:

```text
profile=concurrency sockets=10000 active_ranges=1000 connect_ms=920 active_reservation_ms=21 resident_reserved_bytes=393216000 accounted_limit_bytes=939524096 resident_target_bytes=1073741824 rss_kib=None
profile=concurrency http_ranges=1000 http_bytes=65536000 http_ms=455 active_http_resident_bytes=98304000 active_http_sockets=1000 accounted_limit_bytes=939524096 rss_kib=None
```

The Windows-GNU run proves native socket/ingress/resident-permit admission and
the full active-range transfer, but this harness currently has no Windows RSS
reader and therefore reports `None`; it is not treated as RSS evidence.

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
