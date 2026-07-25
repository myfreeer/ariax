# Final Pre-Implementation Review

Status: closed review record. All P0 findings and phase blockers are resolved
in their normative documents; the design is go for implementation.

Review date: 2026-07-25. Amendments completed and committed the same day.

Scope: the complete `design/` set as a standalone Rust downloader design,
independent of the aria2 C++ implementation repository.

## Outcome

The overall architecture is coherent and ready to implement. The review
initially returned a **conditional no-go** because the transfer, storage,
scheduler-state, and rate-control contracts were not safe to code as first
written. Every P0 item below has since been amended in the named normative
documents, the outcome is now **go**, and the remaining work is the ordinary
phase gating in `implementation-plan.md` and `implementation-readiness.md`
(generated matrices and listed tests come into existence with their phases).

This file is a review record, not a replacement source of truth. Each finding
records the chosen rule; the named normative documents own it. A finding
regresses to open only if its normative text is weakened.

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

The principal architectural problem found by the review was not the top-level
decomposition. It was a set of cross-document contract mismatches at the
boundaries between a network attempt, a storage lease, physical output bytes,
durable progress, and task state. The amendments below close those mismatches
before the types become expensive to change.

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

### P0-2 (resolved): A Sequential Transfer Cannot Be One All-Or-Nothing Storage Lease

Affected documents: `detailed-http-first-slice.md`, `detailed-ftp-sftp.md`,
`detailed-storage.md`, `rate-limiting.md`, `retry-policy.md`, and
`implementation-readiness.md`.

The original sequential HTTP/FTP contract used one lease for the entire
remaining file and committed only at exact EOF. A disconnect near the end of a
large file therefore aborted all progress made by that attempt.

Adopted resolution (now normative in the affected documents):

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

### P0-3 (resolved): Rate Limits Must Charge Received Payload, Not Only Committed Bytes

Affected documents: `rate-limiting.md`, `detailed-runtime.md`,
`detailed-http-first-slice.md`, `detailed-ftp-sftp.md`, `split-download.md`,
`retry-policy.md`, `stats-and-stalls.md`, `backpressure.md`, and `zero-copy.md`.

The pre-review hierarchy debited user token buckets at `CommitLease`. Bytes
later discarded after an invalid response or failed attempt therefore did not
consume the configured download limit, while a large valid lease could wait at
commit time although the network transfer had already occurred. The earlier
non-debiting wire-pacing compromise is superseded.

Adopted resolution (now normative in the affected documents):

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

### P0-4 (resolved): Lease Retry And Slow-Slot Demotion Need Separate Task States

Affected documents: `detailed-core.md`, `retry-policy.md`,
`download-scheduling.md`, `apis-and-embedding.md`, and generated state/wire
matrices.

One retryable split-lease failure previously transitioned the whole task from
`Active` to `RetryWait` although other leases were still transferring, and slow
slot `demote` was conflated with `PausedSlow`.

Adopted resolution (now normative; `detailed-core.md` defines
`PlannedSpanState` and the `WaitingSlow` state):

- A lease/span has its own retry-wait substate. The task remains `Active` while
  any work is runnable or in flight.
- Task-level `RetryWait` is used only when no work is runnable/in flight, or for
  a sequential task-level retry.
- The internal `WaitingSlow` state is produced by slow-slot `demote`, projects
  to aria2 `waiting`, and is automatically readmitted by policy.
- `PausedSlow` is kept only for the explicit slow-slot `pause` policy, projects
  to aria2 `paused`, and never readmits automatically.
- Admission into `Allocating` increments the generation after the previous
  generation's cancellation drain completes, uniformly across retry, demotion,
  pause, and host-key readmission. No old-generation completion can become
  current merely because the task was automatically resumed.

Model tests must cover mixed active/retrying leases, last-active-lease failure,
demotion/readmission, explicit pause, option restart, and every wire projection.

### P0-5 (resolved): Internal Completions Cannot Share Saturable External Urgent Capacity

Affected documents: `detailed-runtime.md`, `messaging-model.md`,
`backpressure.md`, and `threading-model.md`.

The split urgent/bulk queue protected urgent work from bulk admission, but the
urgent queue itself could fill, and a permanently biased drain could starve bulk
commands.

Adopted resolution (now normative; `messaging-model.md` owns the
`CompletionPermit` lifecycle):

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

### P0-6 (resolved): Recovery Cost Needs Journal Compaction And Bounded Replay

Affected documents: `detailed-storage.md`, `session-persistence.md`,
`security-recovery.md`, and `implementation-plan.md`.

Segment rotation bounded one file but not a task journal's lifetime size or
startup replay work.

Adopted resolution (normative in `detailed-storage.md` Checkpoint Compaction
and Journal Descriptor Budget; v1 record types 19–21 and 24):

- Record-count, byte-count, segment-count, and measured replay-time compaction
  triggers with geometric-shrink conditions and failure backoff.
- A recovery-equivalent checkpoint set is built by the serialized appender from
  canonical current state, synced, and installed through an explicit SQLite
  `installing`/`installed` pointer transaction; old segments are retired only
  after installation is durable.
- Provisional/in-flight work is never promoted by compaction.
- `CheckpointStart`/`CheckpointEnd` with a state hash validate the set whole;
  `LayoutChunk` bounds large file-layout snapshots and `PieceStateChunk` bounds
  the durable piece map; every install crash point recovers to exactly one
  authoritative set.
- Idle journal descriptors close under an LRU cap with tail revalidation on
  reopen.
- Journal bytes, segment count, replay time, last compaction, failures, and
  open descriptors are diagnostics.

### P0-7 (resolved): HTTP Ingress Memory Is Outside The Current Buffer-Pool Claim

Affected documents: `zero-copy.md`, `buffer-pool.md`, `detailed-runtime.md`,
`detailed-http-first-slice.md`, `backpressure.md`, and `performance-profiles.md`.

Hyper exposes response body data as stack-owned immutable `Bytes`; it does not
fill a project `BufferLease` directly, and HTTP/2 flow-control windows buffer
outside the pool.

Adopted resolution (now normative in the affected documents):

- Baseline HTTP permits one bounded body-frame-to-`BufferLease` copy.
- `http_ingress_budget` plus HTTP/1 read-buffer and HTTP/2 window/frame options
  (`protocol-modernization.md`) bound framework memory; connection/stream
  admission reserves against the budget; adaptive windows stay feature-gated.
- Body polling stops when storage, buffer, CPU, or rate credit is exhausted;
  delivered frames stay charged to ingress until split/copied/released.
- Whole-body aggregation stays disabled; a future foreign-buffer lease cannot
  bypass registered-buffer, hashing, placement, rate, or cancellation
  contracts.
- The C10k memory gate is the resident-memory equation in
  `performance-profiles.md`, which includes HTTP-stack, TLS, ingress, queue,
  task-metadata, and libtorrent terms, not only `BufferPool` bytes.

### P0-8 (resolved): Finalization And Persisted Retry Time Need Exact Crash Semantics

Affected documents: `detailed-storage.md`, `retry-policy.md`, and
`session-persistence.md`.

A crash could occur after the temporary path was renamed to the final path but
before `TaskComplete` was durable, and persisting only a wall-clock retry
deadline could not honor wait guarantees across clock jumps.

Adopted resolution (normative in `detailed-storage.md` Finalization and
`retry-policy.md` Clock rule; v1 record types 22–23):

- `FinalizeIntent` (flushed before rename, carrying safe temp/final relative
  paths, root/layout identity, length, and file-identity evidence) plus
  `FinalizeDone` make
  recovery a pure function of `(intent, done, filesystem)`; the redo matrix
  covers both crash orders, foreign final-path collisions (fail closed),
  directory sync ordering, Windows sharing violations, and multi-file order.
- `RetryState` persists `scheduled_at_unix_ms`, `delay_ms`,
  `elapsed_before_wait_ms`, and the reason.
  Live waits use monotonic time; recovery clamps elapsed time into
  `[0, delay_ms]`, re-waits fully on implausible clocks, caps by
  `retry-max-wait`, and explicitly documents that restart-surviving waits are
  bounded-conservative, not exact.

## Final Cross-Pass Amendments (resolved)

The final architecture/resource pass found no new decomposition change, but it
did close four implementation-critical boundary sets that were under-specified
in the earlier P0 record:

- **Persistence identity and compact state.** `detailed-storage.md` and
  `session-persistence.md` now bind progress to a canonical output root and
  stable file identities, require explicit identity-preserving relocation or
  per-piece-digest rebind, define checkpoint-only `PieceStateChunk`, and freeze
  the exact SQLite v1 schema, pragmas, migration, backup, and install-pointer
  crash rules. Names, adjacency, mtimes, and copied control files convey no
  ownership.
- **Resident resources and amplification.** `performance-profiles.md`,
  `configuration.md`, `buffer-pool.md`, and `apis-and-embedding.md` now require
  named-domain plus global resident permits, reserve headroom, bound task/piece
  metadata and caches, account SFTP's external vectors and transform output,
  cap handles, and cap RPC request/response/batch/list/per-client work before
  amplification.
- **Live disk failover.** `disk-adapter.md` and `event-backends.md` now use a
  `BackendEpoch` stop/drain/abort/close/reopen barrier, identity-check every
  reopened handle, and readmit only under a fresh task generation. A file-local
  error does not spuriously fail over the process backend, and cancellation
  uncertainty fails closed.
- **Protocol boundary hardening.** FTP passive/active data endpoints are tied to
  the approved control peer and full destination policy; special-use IP policy
  is generated from pinned IANA data; SFTP host-key approval names the exact
  current challenge/fingerprint and generic resume cannot approve it; pinned
  russh-sftp and SuppaFTP patches enforce SFTP framing and FTP control-reply
  limits before allocation; every cookie jar receives a pinned Mozilla Public
  Suffix List; DNS
  cache/singleflight/Happy-Eyeballs work and libtorrent bridge/resume queues are
  explicitly bounded.

## Phase-Specific Blockers And Caveats

### SFTP Security Contract Before Phase 5 (resolved)

`detailed-ftp-sftp.md` now defines the host-key verification order (pin,
known-hosts, `ssh-host-key-md` compat, explicit insecure bypass, paused
approval), the `PausedHostKey` state with task-scoped pinning, the complete
`sftp-*` option set, interactive/non-TTY CLI behavior, authentication order
and secret lifetimes, the pinned algorithm policy (no SHA-1 KEX/`ssh-rsa`/CBC
/weak MACs outside `unsafe_compat`), timeouts/rekey/proxy/server-limit/path
and remote-symlink rules, and the bounded offset pipeline integrated with
rate, memory, retry, and cancellation budgets. The Phase-5 interoperability
matrix remains the implementation gate. A generic Resume never approves a host
key; approval must carry the current challenge id and displayed fingerprint.
The baseline stays russh plus a pinned inbound-frame-cap patch for russh-sftp
2.3.0 and uses russh's exact re-exported ssh-key 0.7.0-rc.11 representation;
libssh2 remains only the documented interoperability fallback. Unpatched
russh-sftp 2.3.0 cannot satisfy the inbound allocation contract.

### BitTorrent Path Semantics Before Full Build (resolved)

`libtorrent-integration.md` now rejects torrent symlink entries by default
with a typed error (future opt-in constrained to validated intra-root
targets), resolves sanitized-name/case-folding/reserved-name/file-vs-directory
collisions deterministically in file-index order with a persisted mapping, and
defines selected-file roots, magnet late-metadata checks, and
metadata-replacement rejection, with the listed deterministic tests.

### Release Artifact Panic Policy (resolved)

The README artifact/profile/panic matrix now encodes the distinction: CLI
artifacts abort; staticlib/cdylib/C-ABI artifacts use unwind profiles and
catch at every export; library crates never force a strategy. Phase 0 asserts
in CI that no C-ABI artifact builds under `panic=abort`.

## Resolved Implementation Choices

The detailed rationale and research snapshot are in `library-choice.md`.

| Area | Choice | Remaining gate |
| --- | --- | --- |
| Language/toolchain | Rust 2024, bootstrap Rust 1.97.1, initial MSRV 1.88 | Pin and test in CI; WSL 1.85 is below the full graph's MSRV |
| Async network runtime | Tokio/Mio | Backend and lag tests on every target |
| HTTP/1.1 and HTTP/2 | Hyper + hyper-util + hyper-rustls | Custom connector, ingress-budget, and exact-body prototype |
| TLS | Stable rustls 0.23.x, single ring provider (aws-lc optional exclusive) | Platform trust and custom-CA matrix; provider-unification CI check |
| Async DNS | Hickory Resolver 0.26.x; `trust-dns` input alias only | SSRF pinning, cache, custom resolver tests |
| Linux disk | low-level io-uring crate behind `DiskBackend` | Fall to the bounded blocking backend on probe/cancellation/secure-open failure |
| Windows disk | Overlapped/IOCP adapter on windows-sys | Native MSVC and all-MinGW secondary tests |
| Portable disk fallback | Bounded blocking worker pool | Queue/cancellation/fault gates |
| FTP/FTPS | patched SuppaFTP 10.0.1 (Tokio, rustls-ring) under owned validation | Control-reply bounds, no-secret-log provenance, offset/EOF, and FTPS matrix |
| SFTP | russh defaults-off ring+flate2+rsa; patched russh-sftp 2.3.0 raw pipeline; exact ssh-key 0.7.0-rc.11 re-export | Feature/provider, inbound frame-cap/provenance, security, external-vector memory, and interoperability matrix |
| Session DB | rusqlite 0.40.1, defaults off, bundled+backup+cache+limits, one bounded thread | SQLite-limit, backup, WAL/locking/filesystem fallback tests |
| CPU work | Dedicated project-owned Rayon pool; compact may use its one bounded shared disk/CPU worker | Bounded admission and cancellation tests |
| Queues | Bounded Tokio + crossbeam baseline | thingbuf/rtrb only after a measured topology |
| Timers | tokio-util DelayQueue per shard | Scale test vs per-deadline tasks |
| Journal CRC | crc32c crate (crc-fast fallback) | Throughput check in Phase 0 baseline |
| Digests | RustCrypto sha2/sha1/md-5 0.11 | asm/hw feature matrix per target |
| XML | quick-xml streaming, no DTD | Fuzz targets |
| Cookies | cookie_store/publicsuffix behind an owned jar with pinned Mozilla PSL and owned SameSite filtering | PSL load-fail-closed, schemeful-site redirects, and aria2 cookie-file compatibility tests |
| netrc | Project parser (no maintained crate) | Fuzz + aria2 semantics tests |
| Syscall layer | rustix (Unix) / windows-sys (Windows) | Secure-open probe per platform |
| Decompression | flate2 (miniz_oxide; zlib-rs upgrade path) | Growing-layout phase only |
| Supply chain | cargo-deny/audit/auditable/cyclonedx | deny.toml policy in Phase 0 CI |
| HTTP/3 | Quinn + h3/h3-quinn experiment | Remains feature-gated until maturity gates pass |
| BitTorrent | libtorrent-rasterbar via project cxx bridge (no maintained crates.io binding) | ABI, memory, path, shutdown, and resume gates |

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

- Every accounted allocation requires both its named-domain permit and the
  global resident permit. Profile limits reserve headroom below the target; the
  fact that domain maxima sum above the limit never authorizes overcommit.
- `disk-cache` remains retained `BufferPool` capacity, not a second allocator.
  Its optional verified-span LRU defaults to zero; LIFO size-class reuse and
  small bounded lane-local caches are sensible for cache/TLB locality.
- Account HTTP/TLS ingress, DNS cache, connection-pool state, queue storage,
  retry/task/piece metadata, journal/SQLite state, CPU scratch/transform jobs,
  SFTP external vectors, RPC work, file handles, and libtorrent memory outside
  the transfer pool. Diagnostics need component totals and the global resident
  budget.
- Avoid a second project-level file-data read cache for ordinary HTTP/FTP writes;
  the OS page cache already retains written pages. Add readback cache only for a
  measured Metalink/hash workload and charge it to the same global memory cap.
- Bound connection-pool idle entries by both count and estimated memory, not
  only per-origin count. C10k low-activity sockets must not imply 10,000 retained
  idle-pool entries or transfer-size buffers.
- Bound metadata cardinality: URIs, redirects, DNS answers, cookies, per-host
  statistics, diagnostic top-N entries, and retry history all need explicit
  caps.
- RPC response serialization reserves bounded output before work, produces into
  a byte-counting chunk sink, and permits one full response per client;
  immutable membership indexes keep list queries out of scheduler actor turns.
- Buffer quarantine remains part of the pool total. When its cap is reached,
  stop accepting cancellation-uncertain I/O instead of allocating replacement
  buffers indefinitely.

### Network I/O

- The downloader-owned connector is required to keep DNS answer validation,
  address pinning, Happy Eyeballs, proxy policy, Host, and TLS SNI consistent.
- DNS uses bounded singleflight, answer counts, TTLs, and at most the configured
  immediate Happy-Eyeballs racers; reconnects re-resolve and re-run policy.
- HTTP/2 connection and stream windows must shrink effective ingress when disk,
  CPU, memory, or rate credit is unavailable; large default windows can defeat
  application backpressure even when body polling stops.
- Pool identity includes scheme, authority, resolved-policy context, proxy, TLS
  identity, credentials, and relevant local binding. Ambiguous protocol errors
  close rather than reuse the connection.
- SFTP throughput requires multiple bounded offset requests; the high-level
  sequential reader is not the segmented engine. The packet-buffer plus owned
  returned-vector peak is admitted before each request, and the patched receive
  driver rejects oversized framing before payload allocation.
- FTP EPSV/PASV and active-mode callbacks accept only the approved control peer
  by default; an administrator override still re-runs SSRF/special-use policy.
- FTP control/passive connections use only the downloader connector and owned
  builder. The SuppaFTP patch validates active peers before TLS, closes bounded
  mismatches, and keeps accepting until the approved peer or deadline.
- FTP greeting/reply/FEAT parsing has fixed line, aggregate, and line-count caps
  in the pinned SuppaFTP patch; all retained control bytes consume metadata and
  global resident permits.
- The SuppaFTP patch never formats raw commands/replies, credentials, paths,
  FEAT text, or listings; diagnostics are safe verb/status/length/count metadata.
- Retries consume fresh connection/stream budgets and cannot create unbounded
  parallel speculative attempts.

### Disk I/O And Recovery

- Offset writes, allocation, flush, rename, and completion ownership remain
  behind `DiskBackend`; protocol code never holds a final file descriptor.
- io_uring/IOCP submission depth is bounded by operations and bytes. Accepted
  operations drain to outcomes after cancellation; stale generations discard the
  result without losing buffer ownership.
- File handles are under a process/profile budget and an identity-checked LRU;
  in-flight or dirty handles cannot be evicted. Live backend failover changes
  epochs only after the full settlement barrier.
- Sequential subleases are necessary for resumability and for balanced
  durability to advance without an entire-file commit.
- Balanced mode should batch data flush and journal sync by explicit byte/time
  thresholds. Strict mode preserves its per-piece ordering even when slower.
- Preallocation is policy-driven and must handle sparse files, ENOSPC timing,
  filesystems without allocation support, and multi-file layouts.
- Journal compaction, replay time, segment count, and idle file descriptors are
  hard resource concerns, not maintenance work that can be deferred forever.
- Checkpoints stream canonical `PieceStateChunk` records rather than cloning the
  complete piece book, and recovery validates the persisted root binding before
  opening descendants or trusting progress.
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
- Relocatable decode/decompression output is admitted through the separate
  `transform_budget`; the fixed-layout baseline keeps that budget at zero.

## Implementation Detail Checklist (resolved)

Every item now has a normative owner; Phase 0 generates the corresponding
machine-readable artifacts:

- `TransferAttemptId` distinct from `LeaseId` — `detailed-core.md`,
  `detailed-storage.md`.
- Span retry substate (`PlannedSpanState`) and `WaitingSlow` — `detailed-core.md`.
- `CompletionPermit` lifecycle — `messaging-model.md`.
- `http_ingress_budget`, frame/window options, admission formula —
  `detailed-runtime.md`, `protocol-modernization.md`.
- Rate-accounting counters (received/sent, committed, durable, discarded, rate
  debt) — `stats-and-stalls.md`, `rate-limiting.md`.
- Checkpoint/compaction records, install protocol, replay bounds —
  `detailed-storage.md`.
- Retry persistence (`scheduled_at`, `delay_ms`, `elapsed_before_wait_ms`,
  reason) and conservative recovery — `detailed-storage.md`, `retry-policy.md`.
- `FinalizeIntent`/`FinalizeDone` idempotent recovery — `detailed-storage.md`.
- SFTP host-key/auth/algorithm/session policy — `detailed-ftp-sftp.md`.
- Torrent symlink and collision policy — `libtorrent-integration.md`.
- Artifact/panic/ABI matrices — `README.md`, `implementation-plan.md` Phase 0.
- Toolchain/lockfile/license/advisory/SBOM artifacts — `library-choice.md`,
  `implementation-plan.md` Phase 0.
- Queue defaults and the resident-memory equation —
  `performance-profiles.md`.
- Persisted root binding, explicit relocation/rebind, and exact SQLite v1 schema
  — `detailed-storage.md`, `session-persistence.md`.
- Compact checkpoint `PieceStateChunk` encoding — `detailed-storage.md`.
- Global resident/domain permits, parser/cardinality/cache caps, SFTP external
  vectors, RPC work/response bounds, and handle budgets —
  `performance-profiles.md`, `configuration.md`, `apis-and-embedding.md`.
- Backend epoch, live-failover barrier, and identity-checked handle LRU —
  `disk-adapter.md`, `event-backends.md`.
- FTP data-endpoint/control-reply caps, explicit SFTP host-key approval, inbound
  SFTP frame cap, and pinned cookie Public Suffix List rules —
  `detailed-ftp-sftp.md`,
  `protocol-modernization.md`, `library-choice.md`.

## Amendment History

The amendments were applied in this order and committed in the design
repository:

1. P0-1 endgame metadata rollback (prior commit).
2. P0-2/P0-3/P0-7 plus SFTP host-key approval across storage, HTTP, FTP/SFTP,
   rate, runtime, and stats documents.
3. P0-4 state-machine separation.
4. P0-5 completion-permit and queue-fairness contracts.
5. P0-6/P0-8 journal compaction and finalization/retry-time crash semantics.
6. BitTorrent path semantics.
7. Crate research snapshot and remaining-choice decisions.
8. Artifact/panic/target matrices, supply-chain wiring, queue defaults, and
   memory equations.
9. Cross-document state, path, protocol, and dependency-choice reconciliation
   (`abf32c3`).
10. Persistence identity/schema/checkpoint, resource/cardinality/RPC, FTP/SFTP,
    and live-failover boundaries (`3600223`).
11. Final crate-source gates for FTP control replies/active-peer policy/secret
    logging, SFTP inbound framing, russh's exact ssh-key/features, and cookie
    Public Suffix List plus SameSite policy.

Phase 0 must regenerate the state/wire, option-behavior, and journal-record
matrices from the amended normative documents; the listed tests come into
existence with their owning phases.

## Final Go/No-Go Checklist

All contract amendments are complete:

- [x] Dirty/uncertain endgame overlap rolls all touched pieces back to pending;
      untrusted physical bytes cannot become progress and are overwritten later.
- [x] Sequential attempts commit bounded storage subleases.
- [x] User rate limits charge received/sent payload independently of commit.
- [x] Lease retry, task retry, demotion, and pause have distinct typed states.
- [x] Accepted internal work has non-rejecting, exactly-once completion credit.
- [x] Journal lifetime size, replay work, and open descriptors are bounded.
- [x] Hyper/TLS/HTTP2 ingress memory is included in global budgets.
- [x] Finalization and retry-time recovery are unambiguous after crashes.
- [x] SFTP security policy is complete before SFTP implementation.
- [x] Persisted progress is root/file-identity bound; relocation/rebind is
      explicit and digest-proven when identities differ.
- [x] Compact checkpoints bound durable-piece state and the exact SQLite v1
      schema/migration/install protocol is defined.
- [x] Global resident/domain permits bound transfer, metadata, cache, transform,
      RPC, SFTP, SQLite, journal, stack, and CPU-scratch memory.
- [x] Backend epochs, live-failover settlement, and file-handle LRU/reopen rules
      are explicit and fail closed on uncertainty.
- [x] FTP data endpoints, generated special-use IP policy, DNS work/cache, and
      SFTP challenge approval close their network-trust boundaries.
- [x] Unpatched SuppaFTP/russh-sftp parser allocation paths and SuppaFTP raw
      secret logging are rejected by the dependency gate, and cookie jars cannot
      start without the pinned Public Suffix List.
- [x] RPC request/response/batch/list and per-client work are bounded without
      blocking the scheduler or constructing an unbounded response tree.
- [x] Cargo toolchain, target ABI, panic profile, lockfile, license, advisory,
      and SBOM policies are specified; Phase 0 generates and tests the
      artifacts.

The design is architecturally ready for the first HTTP/storage vertical slice.
The correct status is “implementation ready, gated by the phase exit criteria
in `implementation-plan.md`.”
