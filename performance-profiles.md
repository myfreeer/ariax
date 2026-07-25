# Performance Profiles

Status: draft.

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
| `control_urgent` capacity (commands) | 256 | 256 | 256 | 64 |
| `control_bulk` capacity (commands) | 1024 | 1024 | 512 | 128 |
| `urgent_burst` (drain fairness) | 32 | 32 | 16 | 8 |
| write lane `HotMpscLane` (items / bytes) | 512 / 64 MiB | 1024 / 256 MiB | 256 / 32 MiB | 128 / 8 MiB |
| disk submission in flight (`disk-queue-ops` / `disk-queue-bytes`) | 128 / 64 MiB | 256 / 256 MiB | 64 / 32 MiB | 32 / 8 MiB |
| `CompletionDrain` capacity | = disk-queue-ops (permit-reserved) | = | = | = |
| hash lane (jobs / bytes) | 64 / 32 MiB | 128 / 128 MiB | 32 / 16 MiB | 16 / 4 MiB |
| journal appender inbox (facts) | 1024 | 2048 | 512 | 256 |
| `BufferPool` total (`buffer_budget`) | 256 MiB | 1 GiB | 128 MiB | 32 MiB |
| quarantine budget (within pool total) | 32 MiB | 64 MiB | 16 MiB | 8 MiB |
| `http_ingress_budget` | 64 MiB | 256 MiB | 32 MiB | 8 MiB |
| `piece_metadata_budget` | 128 MiB | 256 MiB | 64 MiB | 16 MiB |
| default HTTP/2 stream / connection window | 256 KiB / 1 MiB | 2 MiB / 8 MiB | 128 KiB / 512 KiB | 64 KiB / 256 KiB |
| per-client event queue (events) | 256 | 256 | 512 | 64 |
| stopped-result retention (`max-download-result`) | 1000 | 1000 | 1000 | 250 |

Derived invariants CI must assert on the resolved defaults:

- `CompletionDrain` capacity equals accepted-submission capacity (permits make
  overflow unrepresentable),
- write-lane bytes ≤ `buffer_budget − quarantine`, and disk-queue bytes ≤
  write-lane bytes. These are overlapping ownership-stage sublimits on the same
  pooled leases, not three additive resident allocations,
- HTTP/2 admission reservation per connection
  (`min(connection_window, streams × stream_window)` + frame + header
  allowance) times the connection cap fits `http_ingress_budget`,
- every queue capacity above is finite and visible in diagnostics.

## Resident Memory Equation

C10k claims are evaluated against the whole resident budget, not `BufferPool`
alone. The global memory gate is:

```text
resident_target ≥
    buffer_budget                     (pool incl. disk-cache retention + quarantine)
  + http_ingress_budget               (Hyper frames/read buffers, h2 windows,
                                       response headers, dynamic TLS buffers)
  + conn_count × conn_overhead        (fixed HTTP/TLS/socket/pool-entry state only;
                                       excludes ingress-reserved bytes;
                                       measured per platform in Phase 0, budgeted
                                       ≤ 32 KiB idle / ≤ 96 KiB active TLS)
  + task_count × task_base_overhead   (snapshots, retry/lease/source metadata;
                                       budgeted ≤ 32 KiB for the base task,
                                       with caps on URIs/redirects/cookies/stats)
  + piece_metadata_budget             (packed durable/verified maps and bounded
                                       sparse active-piece state)
  + queue_metadata                    (ring slots, descriptors, permits, and bounded
                                       non-pooled message payloads; queued
                                       BufferLeases stay in buffer_budget)
  + journal_state                     (appender buffers + indexes; bounded by
                                       compaction triggers and the FD cap)
  + sqlite_cache                      (page cache, bounded by PRAGMA cache_size)
  + dns_cache + metadata_caches       (bounded entry counts)
  + cpu_job_metadata_and_scratch      (job descriptors and non-pooled private
                                       scratch only; hash input leases stay in
                                       buffer_budget)
  + bt_share                          (libtorrent session budget, full build only;
                                       configured into libtorrent settings)
  + fixed_process_overhead            (code, TLS roots, runtime stacks:
                                       thread_count × stack size)
```

Every term is an allocation domain counted exactly once, individually bounded,
and exported in diagnostics with the same names. Stage byte caps (write lane,
disk queue, hash reorder, disk-cache retention) may overlap as sublimits on the
same pooled lease; diagnostics show both the stage charge and its parent
allocation domain but the resident sum uses only the parent once. The C10k gate
for `concurrency` is: 10,000 mostly idle connections with default budgets fit in
approximately 1 GiB resident (320 MiB idle-connection overhead + 256 MiB pool +
64 MiB ingress + metadata/caches/queues/fixed), and control p99 stays within
target. Exceeding a term is backpressure or admission refusal, never silent
growth.

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

- 10,000 idle sockets with bounded memory,
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
