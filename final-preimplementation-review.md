# Final Pre-Implementation Review

Status: final review record and implementation gate.

Review date: 2026-07-25.

Scope: the complete `design/` set as a standalone Rust downloader design,
independent of the aria2 C++ implementation repository.

## Outcome

The overall architecture is coherent and worth implementing, but the transfer,
storage, scheduler-state, and rate-control contracts are not yet safe to code as
written. The result is a **conditional no-go** for those modules until the P0
items below are amended in their normative documents.

Phase-0 repository scaffolding, generated inventories, option-registry work,
pure parsing, `SafePathBuilder`, and pure layout/offset types may begin. Starting
the HTTP/storage vertical slice before the P0 amendments risks silent corruption,
unbounded recovery cost, incorrect throttling, and state-machine divergence.

This file is a review record, not a replacement source of truth. A finding is
closed only when the named normative documents contain the chosen rule, their
generated matrices agree, and the listed tests exist.

## General Architectural Check

The strongest parts of the design are:

- The scheduler, protocol adapters, storage engine, session store, CPU pool,
  disk backends, and libtorrent adapter have explicit ownership boundaries.
- Payload memory is bounded and ownership is expressed through move-only buffer
  leases rather than copied queue payloads.
- Storage placement uses global offsets and explicit provisional lease
  commit/abort instead of cursor-based writes.
- Task generations provide a sound basis for rejecting stale completions after
  pause, restart, option mutation, and cancellation.
- Configuration, RPC, CLI, runtime mutation, and documentation are intended to
  come from one typed registry rather than duplicated option handling.
- Safe path construction, reserved-header authority, redirect/proxy checks,
  secret redaction, and the isolated BitTorrent boundary are treated as core
  contracts rather than optional hardening.
- Backpressure is resource-oriented: socket, buffer, disk, CPU, journal, RPC,
  and client-event pressure are separate observable causes.
- The recovery ordering correctly distinguishes written, committed, flushed,
  and durable data.

The principal architectural problem is not the top-level decomposition. It is a
small set of cross-document contract mismatches at the boundaries between a
network attempt, a storage lease, physical output bytes, durable progress, and
task state. Those mismatches must be fixed before the types become expensive to
change.

## P0 Findings

### P0-1 (resolved): Endgame Overlap Uses Metadata Rollback

Affected documents: `split-download.md`, `detailed-storage.md`,
`detailed-http-first-slice.md`, `metalink-chunking.md`, and `configuration.md`.

The original endgame design permitted duplicate attempts for one span while both
attempts wrote provisional bytes to the same final offsets. Commit arbitration
alone could not undo physical bytes written by a loser after candidate
validation.

The normative storage/split/HTTP/Metalink/zero-copy documents now adopt the
chosen rollback model:

1. The first valid `CommitLease` is only an in-memory candidate; it is not
   journal-committed or durable yet.
2. Storage freezes the overlap group, cancels competitors, and drains or
   cancellation-confirms every accepted disk operation.
3. If no competitor completed a write, the candidate commits normally.
4. If a competitor wrote, failed validation after writing, or remains
   cancellation-uncertain, every group member is aborted and all touched pieces
   are marked not-downloaded/pending in metadata and in-memory state.
5. Rollback does not restore, zero, or truncate physical bytes. They remain
   untrusted like fallocate/`SetFileValidData` contents and the next ordinary
   connection overwrites them. No `PieceDurable` record can cover the group.

This is conservative and may discard a correct candidate, but it needs no
scratch file or undo buffer and cannot expose ambiguous shared bytes as progress.
Required tests cover clean settlement, a loser write after candidate validation,
validation failure, uncertain cancellation, crash during settlement, and
same-generation overwrite.

### P0-2: A Sequential Transfer Cannot Be One All-Or-Nothing Storage Lease

Affected documents: `detailed-http-first-slice.md`, `detailed-ftp-sftp.md`,
`detailed-storage.md`, `rate-limiting.md`, `retry-policy.md`, and
`implementation-readiness.md`.

The current sequential HTTP/FTP contract uses one lease for the entire remaining
file and commits only at exact EOF. A disconnect near the end of a large file
therefore aborts all progress made by that attempt. It also prevents balanced
durability from advancing a resumable prefix during a long stream.

Chosen resolution:

1. Distinguish a protocol `TransferAttemptId` from a storage `LeaseId`.
2. A sequential transport attempt opens successive fixed storage subleases,
   normally aligned to the piece or durability-checkpoint size.
3. Each complete sublease is validated and committed independently. A premature
   EOF aborts only the current incomplete sublease; previously durable subleases
   remain resumable when the representation validator still matches.
4. The final partial sublease commits only when the advertised representation
   extent and protocol EOF/framing agree exactly.
5. A normal range response can remain one transfer attempt to one storage lease.

Required tests include interruption at every sublease boundary, interruption one
byte before a boundary, crash after data flush but before the durability record,
validator change on resume, and final partial extent mismatch.

### P0-3: Rate Limits Must Charge Received Payload, Not Only Committed Bytes

Affected documents: `rate-limiting.md`, `detailed-runtime.md`,
`detailed-http-first-slice.md`, `detailed-ftp-sftp.md`, `split-download.md`,
`retry-policy.md`, `stats-and-stalls.md`, `backpressure.md`, and `zero-copy.md`.

The current hierarchy debits user token buckets at `CommitLease`. Bytes later
discarded after an invalid response or failed attempt therefore do not consume
the configured download limit. Repeated invalid or aborted responses can use
real network bandwidth outside the user's limit, while a large valid lease can
wait at commit time even though the network transfer already occurred.

Chosen resolution:

1. Charge download buckets when application payload bytes are accepted from the
   protocol body/data channel, including bytes later discarded.
2. Charge upload buckets as payload bytes are handed to the transport for send.
3. Consumed tokens are never refunded on abort. Durable/committed goodput is a
   separate progress and efficiency counter.
4. The discard budget remains a waste/abuse guard, not an alternative rate
   limiter.
5. Raw FTP/SFTP reads are sized by available token credit. Hyper body frames are
   charged immediately and may be split without copying; any unavoidable
   high-level-stack overshoot is bounded by the separately budgeted maximum
   ingress frame/connection window and appears in diagnostics.
6. aria2-compatible displayed download speed follows the rate-accounted payload
   stream; extended diagnostics expose durable goodput and discarded throughput.

Required tests include endless bad range bodies, hash failures, retries,
per-task/global bucket composition, pause/resume, low limits below one frame,
and bounded burst/overshoot behavior for HTTP/1.1 and HTTP/2.

### P0-4: Lease Retry And Slow-Slot Demotion Need Separate Task States

Affected documents: `detailed-core.md`, `retry-policy.md`,
`download-scheduling.md`, `apis-and-embedding.md`, and generated state/wire
matrices.

One retryable split-lease failure currently transitions the whole task from
`Active` to `RetryWait`, although other leases may still be transferring. Slow
slot `demote` is also represented by `PausedSlow`, while one document says it is
automatically readmitted and another projects it to aria2 `paused`.

Chosen resolution:

- A lease/span has its own retry-wait substate. The task remains `Active` while
  any work is runnable or in flight.
- Task-level `RetryWait` is used only when no work is runnable/in flight, or for
  a sequential task-level retry.
- Add an internal `WaitingSlow`/`Demoted` state that projects to aria2 `waiting`
  and is automatically readmitted by policy.
- Keep `PausedSlow` only for the explicit slow-slot `pause` policy, projecting
  to aria2 `paused`.
- Cancellation of old workers completes and the task generation increments
  before either state is readmitted. No old-generation completion can become
  current merely because the task was automatically resumed.

Model tests must cover mixed active/retrying leases, last-active-lease failure,
demotion/readmission, explicit pause, option restart, and every wire projection.

### P0-5: Internal Completions Cannot Share Saturable External Urgent Capacity

Affected documents: `detailed-runtime.md`, `messaging-model.md`,
`backpressure.md`, and `threading-model.md`.

The split urgent/bulk queue protects urgent work from bulk admission, but the
urgent queue itself can fill, and a permanently biased drain can starve bulk
commands. Journal-critical outcomes and accepted disk completions must not
compete with externally produced pause/remove requests.

Chosen resolution:

- Accepted disk/CPU/journal operations carry a move-only `CompletionPermit`
  reserved at submission and consumed by exactly one outcome.
- Internal completion/journal progress uses a non-rejecting reserved lane that
  external RPC/CLI producers cannot consume.
- Shutdown uses an out-of-band cancellation/watch signal rather than requiring a
  free ordinary command slot.
- Duplicate per-task pause/remove/cancel commands are coalesced where their
  semantics permit it, and external urgent producers have bounded admission.
- The scheduler drains external urgent work in bounded bursts, then services at
  least one bulk command when present. Hard internal completion progress remains
  independent of that fairness quota.

Model tests must prove exactly-once outcome delivery, permit return on rejected
submission, shutdown with every ordinary queue full, no bulk starvation, and no
buffer leak during receiver close.

### P0-6: Recovery Cost Needs Journal Compaction And Bounded Replay

Affected documents: `detailed-storage.md`, `session-persistence.md`,
`security-recovery.md`, and `implementation-plan.md`.

Segment rotation bounds one file but not a task journal's lifetime size or
startup replay work. Repeated retries, option snapshots, pause/resume cycles, and
long-lived tasks can grow journals indefinitely. A production design also needs
a cap on idle journal file descriptors.

Chosen resolution requirements:

- Define record-count, byte-count, and measured replay-time compaction triggers.
- Build a recovery-equivalent checkpoint journal set from canonical current
  state, sync it, atomically install/update the SQLite pointer, and retain the
  old set until installation is durable. Old segments are retired only after
  successful installation.
- Provisional/in-flight work is not promoted by compaction; it returns to
  pending unless existing durability rules prove it durable.
- Define checkpoint chunking for state larger than the maximum record payload,
  schema/version compatibility, crash points for every install step, and how a
  partially installed checkpoint is rejected.
- Cap open journal descriptors and close/reopen idle appenders without violating
  sequence assignment or durability ordering.
- Expose journal bytes, segment count, replay time, last compaction, and
  compaction failure in diagnostics.

No implementation should invent the checkpoint format ad hoc. The exact v1
record/header changes must be added to `detailed-storage.md` first.

### P0-7: HTTP Ingress Memory Is Outside The Current Buffer-Pool Claim

Affected documents: `zero-copy.md`, `buffer-pool.md`, `detailed-runtime.md`,
`detailed-http-first-slice.md`, `backpressure.md`, and `performance-profiles.md`.

Hyper exposes response body data as stack-owned immutable `Bytes`; it does not
fill a project `BufferLease` directly. HTTP/2 also has connection/stream flow
control and internal buffering. The current wording overstates direct
socket-to-pool zero-copy and omits those bytes from the memory budget.

Chosen resolution:

- Baseline HTTP permits one bounded body-frame-to-`BufferLease` copy.
- Add an explicit global/per-task HTTP ingress budget, maximum frame/chunk size,
  and bounded HTTP/2 connection and stream windows.
- Stop polling response bodies before downstream storage, buffer, CPU, and rate
  credit is exhausted. Delivered frames remain charged to ingress memory until
  split/copied/released.
- Do not enable whole-body aggregation. A future immutable foreign-buffer lease
  is an optimization and cannot bypass registered-buffer, hashing, placement,
  rate, or cancellation contracts.
- C10k memory gates include HTTP-stack connection state, TLS buffers, ingress
  frames, queue descriptors, task metadata, and libtorrent's separately bounded
  memory, not only `BufferPool` bytes.

### P0-8: Finalization And Persisted Retry Time Need Exact Crash Semantics

Affected documents: `detailed-storage.md`, `retry-policy.md`, and
`session-persistence.md`.

A crash can occur after the temporary path is renamed to the final path but
before `TaskComplete` is durable. Recovery needs an unambiguous rule that cannot
overwrite an unrelated final file. Persisting only `next_retry_unix_ms` also
cannot support the stated guarantee that wall-clock jumps never skip a wait.

Chosen resolution requirements:

- Add explicit finalization intent/installed state or an equivalent idempotent
  recovery algorithm tied to task id, layout identity, and file identity.
- Specify rename-before-record and record-before-rename crash handling, existing
  final-path collision behavior, directory sync ordering, and Windows sharing
  violations.
- Persist retry scheduling wall time plus the chosen delay and scheduling
  context. Live waits use monotonic time. After restart, validate wall-clock
  plausibility and use a conservative bounded fallback on large jumps.
- State honestly that monotonic time cannot survive process restart/reboot
  exactly; recovery preserves policy conservatively rather than claiming the
  impossible.

## Phase-Specific Blockers And Caveats

### SFTP Security Contract Before Phase 5

`detailed-ftp-sftp.md` defines offsets and validators but not the security
contract implied by the requirements traceability document. Before SFTP code:

- host-key verification is strict by default,
- known-hosts format, default locations, explicit trust-on-first-use behavior,
  changed-key rejection, and `ssh-host-key-md` compatibility are defined,
- password, public-key, encrypted-key, and agent authentication order and secret
  lifetimes are defined,
- accepted/minimum algorithms, timeouts, rekey, proxy behavior, server limits,
  path encoding, and remote symlink policy are defined,
- raw offset requests use project-owned bounded pipelining and integrate with
  global memory, rate, retry, and cancellation budgets.

The selected baseline is russh plus russh-sftp. libssh2 is only an explicit
fallback after an interoperability matrix demonstrates a blocking gap.

### BitTorrent Path Semantics Before Full Build

The libtorrent adapter must reject or explicitly map torrent symlink entries,
not merely sanitize ordinary path components. It also needs deterministic tests
for sanitized-name collisions, case-folding collisions, reserved Windows names,
selected-file roots, and late metadata replacement.

### Release Artifact Panic Policy

One profile cannot both use `panic=abort` and catch panics at a C ABI boundary.
CLI-only artifacts may abort. Any staticlib/cdylib/C-ABI artifact promising
containment must use unwind and catch at every export. The Cargo profile/artifact
matrix must encode this distinction.

## Resolved Implementation Choices

The detailed rationale and research snapshot are in `library-choice.md`.

| Area | Choice | Remaining gate |
| --- | --- | --- |
| Language/toolchain | Rust 2024, bootstrap Rust 1.97.0, initial MSRV 1.88 | Pin and test in CI; WSL 1.85 is below the full graph's MSRV |
| Async network runtime | Tokio/Mio | Backend and lag tests on every target |
| HTTP/1.1 and HTTP/2 | Hyper + hyper-util + hyper-rustls | Custom connector, ingress-budget, and exact-body prototype |
| TLS | Stable rustls 0.23.x | Platform trust and custom-CA matrix |
| Async DNS | Hickory Resolver 0.26.x; `trust-dns` input alias only | SSRF pinning, cache, custom resolver tests |
| Linux disk | tokio-uring behind `DiskBackend` | Fall internally to low-level io-uring if cancellation/secure-open gates fail |
| Windows disk | Overlapped/IOCP adapter | Native MSVC and all-MinGW secondary tests |
| Portable disk fallback | Bounded blocking worker pool | Queue/cancellation/fault gates |
| SFTP | russh + russh-sftp raw offset pipeline | Security and interoperability matrix |
| Session DB | rusqlite + bundled SQLite on one bounded thread | WAL/locking/filesystem fallback tests |
| CPU work | Dedicated project-owned Rayon pool | Bounded admission and cancellation tests |
| Queues | Bounded Tokio + crossbeam baseline | thingbuf/rtrb only after a measured topology |
| HTTP/3 | Quinn + h3/h3-quinn experiment | Remains feature-gated until maturity gates pass |
| BitTorrent | libtorrent-rasterbar isolated adapter | ABI, memory, path, shutdown, and resume gates |

Direct versions observed on 2026-07-25 are a research snapshot and must be
resolved through the workspace lockfile, audit, license policy, and target build
matrix. Do not use an h3 `0.0.x` or rustls development release as an implicit
stability promise.

## Performance And Resource Review

### Event Efficiency

- Keep progress, speed, and queue metrics in atomics/snapshots sampled on a
  timer. Never send per-byte or per-packet scheduler messages.
- Coalesce replaceable external events by `(gid, event kind)` and disconnect
  bounded slow consumers for lossless replies/terminal events as already
  designed.
- Use `CompletionPermit` for accepted backend work and separate internal
  completion progress from externally admitted urgent commands.
- Use a bounded urgent burst rather than an indefinitely biased select, so
  status/add/reload work has a finite service bound.
- Use one timer wheel/delay queue per runtime shard for retries, stalls, idle
  connections, and lease expiry instead of one sleeping task per deadline where
  scale tests show timer/task overhead.
- Batch journal notifications and metrics publication at durability/sampling
  boundaries; do not wake the control actor for each buffer write.

### Cache And Memory

- `disk-cache` remains retained `BufferPool` capacity, not a second allocator.
  LIFO size-class reuse and small bounded lane-local caches are sensible for
  cache/TLB locality.
- Account HTTP/TLS ingress, DNS cache, connection-pool state, queue storage,
  retry metadata, journal indexes, CPU jobs, and libtorrent memory outside the
  transfer pool. Diagnostics need both component totals and the global resident
  budget.
- Avoid a second project-level file-data read cache for ordinary HTTP/FTP writes;
  the OS page cache already retains written pages. Add readback cache only for a
  measured Metalink/hash workload and charge it to the same global memory cap.
- Bound connection-pool idle entries by both count and estimated memory, not
  only per-origin count. C10k idle sockets must not retain transfer-size buffers.
- Bound metadata cardinality: URIs, redirects, DNS answers, cookies, per-host
  statistics, diagnostic top-N entries, and retry history all need explicit
  caps.
- Buffer quarantine remains part of the pool total. When its cap is reached,
  stop accepting cancellation-uncertain I/O instead of allocating replacement
  buffers indefinitely.

### Network I/O

- The downloader-owned connector is required to keep DNS answer validation,
  address pinning, Happy Eyeballs, proxy policy, Host, and TLS SNI consistent.
- HTTP/2 connection and stream windows must shrink effective ingress when disk,
  CPU, memory, or rate credit is unavailable; large default windows can defeat
  application backpressure even when body polling stops.
- Pool identity includes scheme, authority, resolved-policy context, proxy, TLS
  identity, credentials, and relevant local binding. Ambiguous protocol errors
  close rather than reuse the connection.
- SFTP throughput requires multiple bounded offset requests; the high-level
  sequential reader is not the segmented engine.
- Retries consume fresh connection/stream budgets and cannot create unbounded
  parallel speculative attempts.

### Disk I/O And Recovery

- Offset writes, allocation, flush, rename, and completion ownership remain
  behind `DiskBackend`; protocol code never holds a final file descriptor.
- io_uring/IOCP submission depth is bounded by operations and bytes. Accepted
  operations drain to outcomes after cancellation; stale generations discard the
  result without losing buffer ownership.
- Sequential subleases are necessary for resumability and for balanced
  durability to advance without an entire-file commit.
- Balanced mode should batch data flush and journal sync by explicit byte/time
  thresholds. Strict mode preserves its per-piece ordering even when slower.
- Preallocation is policy-driven and must handle sparse files, ENOSPC timing,
  filesystems without allocation support, and multi-file layouts.
- Journal compaction, replay time, segment count, and idle file descriptors are
  hard resource concerns, not maintenance work that can be deferred forever.
- Hash/readback scheduling must not evict active write buffers or create
  unbounded random I/O on HDD profiles.

### CPU Efficiency

- Use a dedicated bounded Rayon pool for CPU-heavy pure work and incremental
  hash state where ordering permits it.
- Do not move protocol parsing or small hash updates to the CPU pool unless the
  enqueue/wakeup cost is lower than inline work.
- Hash jobs carry task/generation/span identity and return through bounded
  completions; cancellation is cooperative and late results are rejected.
- XML/Metalink parsing remains size-capped and streaming where possible. One
  metadata document cannot occupy all CPU workers or memory credit.

## Missing Implementation Detail Checklist

The following types/artifacts need to be added to the normative module designs
or generated in Phase 0:

- `TransferAttemptId`, distinct from `LeaseId` for sequential one-to-many
  storage subleases.
- Lease/span retry substate and the `WaitingSlow`/`Demoted` task state.
- `CompletionPermit` ownership and every submit/reject/complete/close path.
- HTTP ingress-budget type, frame/chunk cap, HTTP/2 window defaults, and memory
  accounting fields.
- Rate-accounting counters that distinguish received/sent payload, durable
  goodput, discarded bytes, and protocol overhead if exposed.
- Journal checkpoint/compaction records or equivalent versioned snapshot format,
  atomic install protocol, and replay limits.
- Retry persistence fields for scheduled wall time, chosen delay, reason, and
  conservative recovery fallback.
- Finalization intent/installed state and idempotent crash recovery.
- SFTP host-key, known-hosts, auth, algorithm, proxy, timeout, rekey, and path
  policy.
- Torrent symlink and sanitized-collision policy.
- Artifact-specific Cargo panic profiles and the native ABI matrix.
- `rust-toolchain.toml`, committed lockfile, target feature matrix, license and
  advisory policy, reproducible native dependency strategy, and SBOM generation.
- Exact queue defaults and memory equations that include non-pool ingress and
  third-party adapter memory.

## Required Amendment Order

1. Resolve P0-2 through P0-4 in storage, HTTP, rate, retry, and state documents;
   regenerate the state/wire and option behavior matrices. P0-1 is resolved by
   the endgame metadata-rollback contract.
2. Resolve P0-5 and P0-7 in runtime, messaging, buffer, zero-copy, and
   backpressure documents; prototype Hyper ingress and completion ownership.
3. Specify P0-6 and P0-8 in the journal/recovery schema before implementing
   `ControlJournal` or finalization.
4. Complete SFTP security before Phase 5 and BitTorrent path semantics before
   the full build.
5. Pin/build/audit the selected dependency graph and target matrix, then run the
   fault/model/performance gates.

## Final Go/No-Go Checklist

Implementation of the affected modules is ready only when all are true:

- [x] Dirty/uncertain endgame overlap rolls all touched pieces back to pending;
      untrusted physical bytes cannot become progress and are overwritten later.
- [ ] Sequential attempts commit bounded storage subleases.
- [ ] User rate limits charge received/sent payload independently of commit.
- [ ] Lease retry, task retry, demotion, and pause have distinct typed states.
- [ ] Accepted internal work has non-rejecting, exactly-once completion credit.
- [ ] Journal lifetime size, replay work, and open descriptors are bounded.
- [ ] Hyper/TLS/HTTP2 ingress memory is included in global budgets.
- [ ] Finalization and retry-time recovery are unambiguous after crashes.
- [ ] SFTP security policy is complete before SFTP implementation.
- [ ] Cargo toolchain, target ABI, panic profile, lockfile, license, advisory,
      and SBOM artifacts are generated and tested.

After those changes, the design is architecturally ready for the first
HTTP/storage vertical slice. Until then, the correct status is “reviewed, with
blocking contract amendments,” not “implementation ready.”
