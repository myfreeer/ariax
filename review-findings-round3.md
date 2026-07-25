# Design Review Round 3: Consolidated Findings Through Round 4

Status: review of the `design/` set after the round-2 fixes (F1–F18) were applied,
including the supplemental cross-document pass and the externally verified round-4
corrections.
The underlying round-3 pass re-read the 34 documents then present in five thematic
clusters (core runtime, storage/recovery, protocols/download,
config/API/integration, and process/traceability), then cross-checked findings
between clusters for contradictions.
Verdicts and line references describe the current post-round-2 state. Round 4 was
not copied verbatim: its claims were rechecked against the aria2 source/manual and
small WSL/MinGW Rust experiments, and its overclaims are corrected below.

The design remains architecturally strong and clearly written against the
earlier `aria2_rust` review's failure classes. The issues below cluster at the
same two seams round 2 identified — (1) two documents specifying the same
artifact differently, and (2) a stated guarantee with no owning mechanism — plus
a new third category this round surfaced: **guarantees that hold in `strict`
mode but are violated in the default mode**. Two of those (balanced-durability
data fsync, cross-mirror content-blind writes) are integrity defects in the
path most users will actually run. A final cross-document pass found additional
seams at the HTTP layout/commit boundary, proxy boundary, runtime selection
boundary, wire-compatibility boundary, and repository build boundary. Each
actionable item below has a concrete proposed fix; the few architectural choices
are called out explicitly rather than left implicit.

## Severity And Status Summary

| ID | Finding | Severity | Verdict | Fix |
| --- | --- | --- | --- | --- |
| R3-1 | Balanced durability marks pieces durable without a data fsync | High | CONFIRMED | G1 |
| R3-2 | No rollback path for a piece that fails hash after being written | High | CONFIRMED | G2 |
| R3-3 | Endgame/redirect commit content-blind writes under default identity mode | High | CONFIRMED | G3 |
| R3-4 | SPSC disk-completion lane has multiple producers and is a backpressure inversion | High | CONFIRMED | G4 |
| R3-5 | Journal `sequence` assignment under out-of-order acks is undefined; append ownership doubled | High | CONFIRMED | G5 |
| R3-6 | `BufferLease` still defined two incompatible ways | High | CONFIRMED | G6 |
| R3-7 | `active_restart` matches aria2, but `PausedRestarting` needs the correct wire projection | Medium | CORRECTED | G7 |
| R3-8 | `active_restart` model collides with BT isolation; no `bt-*` runtime class | Medium | CONFIRMED | G8 |
| R3-9 | FTP split leases stream to EOF; split offset math unspecified | High | CONFIRMED | G9 |
| R3-10 | aria2 `token:<secret>` auth convention never specified | Medium | CONFIRMED | G10 |
| R3-11 | `event-poll` option name renamed, not aliased | Low | CONFIRMED | G11 |
| R3-12 | libtorrent self-drops alerts; no-drop guarantee unmet | Medium | CONFIRMED | G12 |
| R3-13 | Journal record payload layouts unspecified | Medium | CONFIRMED | G5 |
| R3-14 | Segment rotation unmodeled; conflicts with gap-stop replay | Medium | CONFIRMED | G5 |
| R3-15 | Option/layout snapshot duplicated (journal vs SQLite) with no precedence | Medium | CONFIRMED | G13 |
| R3-16 | Runtime-selected async `DiskBackend` needs dyn-compatible or enum dispatch | Medium | CORRECTED | G14 |
| R3-17 | Discarded-byte accounting vs rate limiter undefined | Medium | CONFIRMED | G15 |
| R3-18 | First-slice needs Phase-4 machinery; Phase-0 gate is circular | Medium | CONFIRMED | G16 |
| R3-19 | "No parsed-only options" invariant has no CI detection mechanism | Medium | CONFIRMED | G17 |
| R3-20 | `requirements-traceability.md` stale: no rows for SSRF/redirect/cookie/proxy/rate | Medium | CONFIRMED | G18 |
| R3-21 | Worker sizing oversubscribes small (2-core) machines | Low | CONFIRMED | G19 |
| R3-22 | Path-safety gaps: Windows ADS colon, no Unicode normalization | Low | CONFIRMED | G20 |
| R3-23 | Three option-change error schemes still unreconciled | Low | CONFIRMED | G21 |
| R3-24 | Over-engineering for slice 1 (5 queue families, unused `concurrent-queue`) | Low | note | G22 |
| R3-25 | Unknown/chunked or decoded HTTP bodies do not fit the immutable fixed layout | High | CONFIRMED | H1 |
| R3-26 | Range writes have no provisional commit/abort transaction | High | CONFIRMED | H2 |
| R3-27 | SSRF protection is bypassable for proxy-resolved destinations | High | CONFIRMED | H3 |
| R3-28 | User headers can override range, encoding, and other safety-critical headers | High | CONFIRMED | H4 |
| R3-29 | `DiskBackend` error ownership cannot return its `BufferLease` | High | CONFIRMED | H5 |
| R3-30 | Runtime event-backend fallback conflicts with Tokio and the no-custom-reactor decision | High | CONFIRMED | H6 |
| R3-31 | Internal task states have no normative aria2 wire-status projection | Medium | CONFIRMED | H7 |
| R3-32 | GID wire serialization and identity rules are unspecified | Medium | CONFIRMED | H8 |
| R3-33 | Credentials and secrets at rest have no persistence policy | Medium | CONFIRMED | H9 |
| R3-34 | Metalink incremental hashing has no out-of-order reassembly rule | Medium | CONFIRMED | H10 |
| R3-35 | BT shutdown checkpoint ordering contradicts the BT-specific shutdown contract | Medium | CONFIRMED | H11 |
| R3-36 | WebSocket/stdio event queues have no slow-consumer policy | Medium | CONFIRMED | H12 |
| R3-37 | The Rust engine has no build/packaging integration plan in this autotools repository | Medium | CONFIRMED | H13 |
| R3-38 | The task state graph omits cancellation/error transitions for non-Active states | Medium | CONFIRMED | H14 |
| R4-1 | aria2 restarts active downloads for `split`/connection/split-size changes | High | VERIFIED; CORRECTS R3-7 | G7 |
| R4-2 | An option-change restart reports `waiting`, with no pause event | Medium | VERIFIED; CORRECTS R3-7 | G7 |
| R4-3 | Live-update compatibility divergences need an explicit matrix | Medium | VERIFIED WITH CORRECTION | V1 |
| R4-4 | Native async traits are not dyn-compatible; boxed erasure really allocates | Medium | VERIFIED WITH CORRECTION | G14 |
| R4-5 | Journal `sync_data`/`sync_all` cost is platform-specific; mode taxonomy still stands | Medium | PARTIALLY VERIFIED | V2 |
| R4-6 | Length-only completion is invalid; physical sparseness is not portable | — | PARTIALLY VERIFIED | — |

Verdict key: CONFIRMED — reproduced against the text. VERIFIED — checked against
source/manual or a compiled experiment. CORRECTED / VERIFIED WITH CORRECTION — the
original finding contained a factual overclaim and the wording below supersedes it.
note — an observation, not a contradiction or missing mechanism.

## High-Severity Findings

### R3-1 — Balanced durability marks pieces durable without a data fsync

CONFIRMED. `detailed-storage.md` "Write And Journal Ordering" gives the balanced
path as `write_at -> ack -> update memory -> [piece complete] verify -> append
PieceDurable -> fsync journal`. There is no data fsync in this path; only `strict`
does `fsync data` before `PieceDurable`. An fsync on the journal fd does not flush
the data fd, so the `PieceDurable` record can reach disk while the piece's data
pages are still dirty in the page cache. After power loss, recovery's "journal
wins / completed pieces are trusted" rule then trusts bytes that never reached
disk. This directly breaks the doc's own stated invariant "piece data reaches disk
before the PieceDurable record that claims it." Because balanced is the default
mode, this is a corruption hole in the path most users run, not an edge case.

This is the load-bearing gap behind the round-2 R2-6/F5 note and the downgraded
"fast-durability self-contradiction" item, now confirmed as a real ordering defect
in `balanced`, not just `fast`.

Proposed fix G1 — enforce write-ahead-log ordering in every mode that trusts the
journal on recovery:

- Before appending `PieceDurable` for a checksummed piece, the data for that piece
  must be durably on disk. In `balanced`, call `sync_data` (fdatasync) on the piece
  range before the `PieceDurable` append + journal fsync. In `strict`, keep
  `sync_all`. In `fast`, either (a) do not write `PieceDurable` at all and recover
  by re-hashing on restart, or (b) mark such pieces `provisional` in the journal so
  recovery re-validates rather than trusts them.
- State the invariant as a hard rule in `detailed-storage.md`: no record may claim
  durability for data that has not been flushed with at least `sync_data`. Recovery
  trusts `PieceDurable` only for records written under this ordering.
- Add a fault-injection test: kill between data write and journal fsync in each
  mode; recovery must never count an un-flushed piece as complete.

### R3-2 — No rollback path for a piece that fails hash after being written

CONFIRMED. `buffer-pool.md` allows `HashBorrowed` validation "before or after disk
ack," and `zero-copy.md` forbids completing a piece before hash validation. But no
document specifies what happens when a piece has already been written (and, under
`strict`, fsynced) and its hash then fails. The recovery state machine
(`security-recovery.md`, `detailed-storage.md`) has states for in-flight → pending
and pending → durable, but no "written-but-invalid → pending" transition, and no
statement that the offsets are safe to overwrite in place. Interacts with G1: if
the un-verified data was flushed, a naive re-download must reset the durable bitset
and the journal state for that piece.

Proposed fix G2 — define the hash-failure transition explicitly:

- On hash failure for a piece, do not append `PieceDurable`; instead append a
  `PieceFailed`/reset record that returns the piece to `pending` and clears any
  provisional durable state.
- Specify that the piece's byte range may be overwritten in place by the retry
  (same generation), since no `PieceDurable` was committed for it.
- Bound retries per piece via the existing retry taxonomy (a distinct
  `HashMismatch` error class, sibling to `StaleValidator`); after the cap, fail the
  task or, in a multi-mirror split, re-lease the range to a different mirror.
- Document in `detailed-storage.md` (Recovery + Write ordering) and cross-link from
  `zero-copy.md`.

### R3-3 — Endgame and redirect commit content-blind writes under the default identity mode

CONFIRMED, and this reopens R2-4/F4 rather than being separate from it. Round 2
made cross-mirror identity opt-in with `--verify-mirror-identity=off` as the
aria2-compatible default, accepting that whole-file/Metalink checksums are the only
backstop and corruption is caught "not before bytes are written." Two mechanisms
added since interact badly with that default:

- `split-download.md` "Endgame Mode" duplicates a slow range on another mirror and
  lets storage arbitrate on "first valid write wins," where "valid" means
  range/length-valid, not content-valid. Under `off`, two equal-length mirrors
  serving different bytes are explicitly permitted, so endgame can commit whichever
  divergent copy arrives first.
- `redirect-policy.md` "Validator And Range Interaction" has a redirect target
  "replace the current mirror for that lease and join the mirror pool." Under `off`
  this admits an attacker- or CDN-influenced target into a live split behind only a
  length check — reachable via a single 302.

Both contradict `retry-policy.md` "Safety Rules," which forbid racing writes of
unverified duplicate data over the same range. So the endgame/redirect docs and the
retry-policy safety rules disagree about whether content-blind duplicate writes are
allowed. Round 2's F4 note ("under `off`, a redirect target does not silently join
the mirror pool without passing the F4 identity check") was written but the
endgame and redirect docs do not enforce it.

Proposed fix G3 — make endgame and redirect obey the F4 identity gate, and make
the arbitration content-aware where it can be:

- Endgame duplication of a range across mirrors is permitted only when the range is
  covered by a per-chunk digest (Metalink) or the task is running under
  `--verify-mirror-identity=strict`. Under `off` with no per-chunk digest, endgame
  is restricted to duplicating within a single self-consistent mirror
  (`If-Range`-pinned), not across divergent mirrors.
- When `Content-Digest`/`Repr-Digest` is used as that gate, require the digest
  algorithm, value, covered representation, and covered byte range to match;
  merely seeing the same algorithm on two responses is not an identity check.
- "First valid write wins" becomes "first *hash-valid* write wins" whenever a piece/
  chunk hash is available; the length-only acceptance is used only when no digest
  exists, and in that case the write is confined to a single mirror per range.
- A redirect target joins the mirror pool only after passing the same F4 gate; under
  `off` with no whole-file/Metalink checksum, a cross-origin redirect during a split
  lease restarts that lease from the redirect target as a single mirror rather than
  racing it against the others.
- State the shared rule once (in `retry-policy.md` Safety Rules) and have
  `split-download.md` and `redirect-policy.md` reference it, so the three docs stop
  disagreeing.

### R3-4 — SPSC disk-completion lane has multiple producers and is a backpressure inversion

CONFIRMED. `messaging-model.md` ("SPSC Ring Lanes" / "Default Topology") mandates
`disk completion lane -> SPSC ring -> storage ack`, single-producer/single-consumer.
But `threading-model.md` "Default Sizing" specifies a blocking-disk fallback of
`min(4, max(1, cpu_count/2))` workers — up to 4 completion producers feeding one
SPSC ring, which is a data race by construction. Separately, a *bounded* completion
lane is a backpressure inversion: the write has already happened by the time a
completion exists, so a full ring cannot slow the producer usefully — it can only
stall the io_uring/IOCP reactor or strand completions, which risks the very
quarantine/leak path R2-3/F3 tried to bound.

Proposed fix G4 — separate the completion lane from the submission lane:

- Completions are drained on an MPMC (or per-worker SPSC-to-single-drainer) lane
  that is effectively unbounded-drain: it must always accept a completion. Bound the
  *submission* side (WriteBlocks into the disk queue) instead — that is where
  backpressure belongs and where it already exists.
- For the blocking-pool fallback, either give each worker its own SPSC ring merged
  by a single drainer, or use one MPMC completion queue; do not point N workers at
  one SPSC ring.
- Update `messaging-model.md` "Default Topology" so the completion lane's producer
  count matches the disk-worker count in `threading-model.md`, and state explicitly
  that completions are never rejected for lack of capacity.

### R3-5 — Journal sequence assignment is undefined under out-of-order acks; append ownership is doubled; payloads unspecified

CONFIRMED (folds R3-13 and R3-14). Three related journal gaps:

- Recovery depends on gap-free monotonic `sequence` ("replay stops at the first
  sequence gap"), but multiple pieces are in flight via the disk queue and acks
  arrive out of order, and no document names a single serialized appender that
  assigns `sequence`. Concurrency hazard between disk queue, journal, and completion.
- Ownership of the `PieceDurable` append is specified twice: `detailed-storage.md`
  has the StorageEngine append it inline and emit `WriteAck::PieceDurable`, while
  `detailed-core.md` "Persistence Hooks" maps a scheduler-level `PieceDurable` event
  to the journal record. Two owners risk double-append or races. `detailed-core.md`
  also says persistence is "asynchronous but ordered per task," which conflicts with
  `strict` mode's need for a synchronous journal fsync before a piece is treated
  durable.
- Record payloads are unspecified: `detailed-storage.md` "Control Journal Format"
  makes the framing normative but leaves `payload: [u8]` per-type layout undefined.
  `LayoutCommitted` must carry the piece length ("must be persisted in the journal")
  and `PieceStarted`/`PieceDurable` need piece id, offsets, and validator state for
  the reset-to-pending step. Segment rotation is named in `session-persistence.md`
  ("append-only or segment-rotated") but `detailed-storage.md` defines only a single
  append-only file, with no segment header or cross-segment sequence continuity — so
  "stop at first gap" would misfire at a rotation boundary.

Proposed fix G5 — one journal owner, one appender, defined payloads:

- Name a single serialized journal appender (one task/actor) that assigns `sequence`
  and performs all appends. Completions hand it durability facts; it owns ordering.
  Remove the inline-append path from `detailed-storage.md` or the mapped-event path
  from `detailed-core.md` — keep exactly one.
- Resolve sync vs async: the appender is async for `fast`/`balanced` batching but
  exposes a `flush()` the scheduler awaits before treating a piece/task durable in
  `strict` (and before returning from a durability checkpoint).
- Define each record's payload layout in `detailed-storage.md` as the normative
  table (piece id, generation, offset, len, validator snapshot, layout hash where
  relevant).
- Specify segment rotation: per-segment header carrying the starting `sequence` and
  generation, and a continuity rule so replay spans segments; "stop at first gap"
  applies within the reconstructed global sequence, not per file.

### R3-6 — `BufferLease` is still defined two incompatible ways

CONFIRMED — R2-1/F1 reconciled the `DiskBackend` trait but not the `BufferLease`
type it carries. `buffer-pool.md` "Ownership Object" defines the lease as `bytes:
BytesMut`; `detailed-runtime.md` "Buffer Pool" defines it as `ptr`, `capacity`,
`owner`. `BytesMut` is reallocatable/splittable and is incompatible with
`buffer-pool.md`'s own "Registered Buffers" requirement that registered memory has
"stable memory addresses until unregistered … cannot be shrunk or released." The
state sets also diverge: `buffer-pool.md` lists `DiskDone` and `Free`;
`detailed-runtime.md` drops both and adds `TransformOwned`. `disk-adapter.md` says
it removed its own copy "to avoid divergence," but the two remaining definitions
still disagree, so a coder cannot implement the type.

Proposed fix G6 — one normative `BufferLease`:

- Make `detailed-runtime.md`'s pointer-based form (`ptr`/`capacity`/`owner`) the
  single definition, since it satisfies the stable-address requirement for
  registered buffers; remove the `BytesMut` form from `buffer-pool.md` and have it
  reference the runtime definition.
- Reconcile the state set into one enum (union of `DiskDone`, `Free`,
  `TransformOwned`, etc.), documented once, referenced everywhere.
- Where an owned `BytesMut`-like view is genuinely needed (e.g. a transform that
  reallocates), model it as a distinct owned type, not the same `BufferLease`.

### R3-7 — `active_restart` matches aria2, but `PausedRestarting` needs the correct wire projection

CORRECTED by R4-1/R4-2. The original round-3 finding got aria2's behavior wrong.
Real aria2 does restart an active download when `split`,
`max-connection-per-server`, or `min-split-size` changes:

- `src/RpcMethod.cc:150-175` routes options marked `changeOptionForReserved` into
  a pending option set rather than applying them live.
- `src/OptionHandlerFactory.cc:439-448,502-509,972-979` marks all three options
  `setChangeOptionForReserved(true)` and does not mark them `setChangeOption(true)`.
- `src/RpcMethodImpl.cc:1120-1131` stores the pending options, requests a pause,
  and sets `restartRequested` for an active group.
- `doc/manual-src/en/aria2c.rst:3282-3291` states that every changeable option
  except six named live exceptions restarts an active download; these three are
  not exceptions.

Therefore `configuration.md` is correct to classify the three options as
`active_restart`. The remaining compatibility issue is only the wire projection.
During aria2's restart path, `src/RequestGroupMan.cc:434-451` moves the group to
`STATE_WAITING`, applies the pending options, clears `pauseRequested`, and skips
the pause hook/event. `src/RpcMethodImpl.cc:1019-1029,1065-1070` consequently
reports `waiting`, not `paused`, and never exposes a restart-specific status value.

Corrected proposed fix G7 — preserve the restart and match its observable contract:

- Keep `active_restart` for `split`, `max-connection-per-server`, and
  `min-split-size`; withdraw the earlier proposal to apply them live.
- `PausedRestarting` may remain an internal implementation state, but map it to
  aria2's `waiting` status. Never serialize `PausedRestarting` in
  `tellStatus.status`.
- Do not emit the pause event/hook for a restart-driven transition. Record the
  restart reason only in an opt-in/namespaced extension field.
- Add a compatibility test that changes each option on an active download and
  observes an automatic restart, a transient `waiting` state, no pause event, and
  eventual resume with the pending option applied.

### R3-9 — FTP split leases stream to EOF; split offset math unspecified

CONFIRMED. `detailed-ftp-sftp.md` "Resume Offset (REST)" states `REST <offset>` +
`RETR` makes "the server stream from the restart offset to EOF; there is no
server-side end bound, so the client stops at the lease end." For an N-way split
this means each of the N data connections streams from its start offset all the way
to end-of-file and discards everything past its lease end — O(N²) wasted bandwidth,
and it interacts badly with per-host connection budgets and the rate limiter
(the discarded tail bytes, see R3-17). The doc also conflates the resume case
(`REST durable_length`) with the split case (`REST lease.start`) and never actually
specifies the split offset math.

Proposed fix G9 — bound FTP concurrency to what the protocol supports:

- Since FTP `RETR` has no end bound, do not run a many-way concurrent split over a
  single FTP source by default: either (a) restrict FTP to sequential resume
  (`REST durable_length` + `RETR`), or (b) allow at most a small number of leases
  and account for the tail explicitly, closing each data connection (ABOR/close)
  once its lease end is reached rather than draining to EOF.
- Prefer concurrency across *distinct* FTP mirrors (each a full sequential stream)
  over intra-source splitting, matching what the protocol can do efficiently.
- Specify the offset math for both the resume case and the (bounded) split case, and
  state that discarded tail bytes do not count against the user's rate limit (R3-17).
- Document in `detailed-ftp-sftp.md` and reconcile with `split-download.md`'s
  protocol-agnostic lease language, which is not mechanically true for FTP.

## Medium-Severity Findings

### R3-8 — `active_restart` collides with BT isolation; no `bt-*` runtime class

CONFIRMED. `active_restart` means cancel workers and requeue as a new generation.
For a BitTorrent task the "workers" are the libtorrent session; requeuing implies
remove + re-add, discarding swarm/peer/piece state libtorrent owns. No `bt-*` or
`dht-*` option is assigned any `runtime_update` class, so live option changes into
the BT lane are entirely unspecified. This blocks implementation of BT option
mutation. Proposed fix G8 — define a BT-specific runtime-update class that maps
option changes to libtorrent `settings_pack` updates through the command channel
(no session teardown), and mark BT options that genuinely cannot change live as
requiring an explicit stop/restart, never a silent generation requeue. Document in
`libtorrent-integration.md` and `configuration.md`.

### R3-10 — aria2 `token:<secret>` auth convention is never specified

CONFIRMED. `apis-and-embedding.md` and `configuration.md` list `rpc-secret`,
`rpc-user`, `rpc-passwd` as implemented but never describe aria2's actual
convention — the `token:<secret>` value prepended as the first positional parameter
of every RPC call — nor how `rpc-secret` and the legacy `rpc-user`/`rpc-passwd`
basic-auth interact. Without this, "aria2-compatible JSON-RPC" is not implementable
to spec. Proposed fix G10 — specify the `token:<secret>` first-parameter scheme for
every method (including the `system.multicall` per-call token rule), the legacy
HTTP basic-auth path and its deprecation, and precedence when both are present.
Document in `apis-and-embedding.md`.

### R3-12 — libtorrent self-drops alerts; the no-drop guarantee is unmet

CONFIRMED — R2-11/F10 protects durability-critical alerts by stalling the alert
pump, but `libtorrent-integration.md` does not account for libtorrent dropping its
*own* alerts when its internal `alert_queue_size` overflows. Stalling the consumer
does not prevent upstream loss of `save_resume_data_alert`, so the stated durability
guarantee is not met by the described mechanism. Proposed fix G12 — do not rely on
never-dropping alerts for durability: request resume data on an explicit cadence and
on shutdown (`save_resume_data` calls whose completion is awaited), size
`alert_queue_size` generously, and treat resume-data as pull-on-demand at
checkpoints rather than a push that must not be missed. Document in
`libtorrent-integration.md` Event Channel.

### R3-15 — Option/layout snapshot duplicated across journal and SQLite with no precedence

CONFIRMED. Both stores hold a per-task options snapshot (journal `OptionsSnapshot`/
`TaskCreated` vs `session-persistence.md`'s SQLite per-task options), and both hold
layout information (journal `LayoutCommitted` vs SQLite "safe relative paths or
layout hash"). Recovery precedence on disagreement is unspecified. Proposed fix G13
— declare the per-task control journal authoritative for layout and durable state,
and SQLite authoritative for queue membership/position and cross-task session state;
state the precedence rule for each duplicated field and have recovery reconcile in
one direction only. Document in `session-persistence.md`.

### R3-16 — Runtime-selected async `DiskBackend` needs dyn-compatible or enum dispatch

CORRECTED by R4-4 and the verification pass. `detailed-runtime.md` uses native
`async fn write_at(…)` on a trait selected at runtime. Native async trait methods
are not dyn-compatible: rustc 1.85.0 on WSL and rustc 1.97.0 MinGW both reject
`&dyn DiskBackend` with E0038 because `write_at` is async. Runtime selection must
therefore use enum/static dispatch or replace the method with an explicitly
dyn-compatible erased-future signature.

The original allocation statement remains structurally true for the usual erased
future/`async_trait` alternative, but it should not be presented as an unmeasured
disk-throughput claim. A custom counting-allocator experiment over 1,000,000
dispatch-only calls observed exactly one allocation per boxed call and zero for
enum dispatch; the boxed path was about 20.8 ns/call on WSL and 69–78 ns/call on
MinGW, versus about 2 ns/call for the enum. Real disk I/O will usually dominate
those nanoseconds, but the allocation still violates a stated no-per-write-
allocation objective.

Corrected proposed fix G14 — use enum dispatch over concrete backends (io_uring,
IOCP, blocking pool) so runtime selection is compile-valid and allocation-free.
Justify the decision first by dyn compatibility and ownership/type clarity, and
second by the structural allocation count. Do not publish the microbenchmark as a
real write-path speedup until it is repeated through the actual adapter.

### R3-17 — Discarded-byte accounting vs the rate limiter is undefined

CONFIRMED. Tokens are acquired before reading (`rate-limiting.md`), but short-body,
oversized-body, endgame-loser, and FTP-tail (R3-9) bytes are read-then-discarded,
and `stats-and-stalls.md` tracks "discarded bytes" separately. Whether discarded
bytes count against `max-*-limit` is never stated. Proposed fix G15 — define that
bytes counted against the user's rate limit are only bytes committed toward a lease/
piece; discarded bytes are surfaced in stats but excluded from the user's configured
limit (they still consume real network capacity, so also cap the discard volume via
the oversized-body and endgame rules). Document in `rate-limiting.md`.

### R3-18 — First slice needs Phase-4 machinery; Phase-0 gate is circular

CONFIRMED (round 2 flagged this in Coverage Caveats without carrying a fix).
`implementation-readiness.md` "First Implementation Slice" requires HTTP sequential
download plus live JSON-RPC `addUri`/`tellStatus`/`pause`/`remove`/`getGlobalStat`
"against the real scheduler," but `implementation-plan.md` builds only the
`TaskState`/queue model in Phase 1, defers the real scheduler and all JSON-RPC to
Phase 4, and puts the HTTP downloader in Phase 3. Separately, Phase 0 cannot exit
without `changeOption`/`changeGlobalOption` and config reload/dump tests, which are
themselves Phase 4 deliverables — a circular gate. Proposed fix G16 — reconcile the
phase plan with the first-slice contract: either pull a minimal real scheduler +
JSON-RPC surface into the slice's phase, or redefine the first slice to match what
Phase 1–3 actually deliver. Move the `changeOption`/config-reload gate out of Phase
0 to the phase that builds those, and give Phase 0 an exit criterion it can actually
meet. Update `implementation-plan.md` and `implementation-readiness.md`.

### R3-19 — "No parsed-only options" invariant has no CI detection mechanism

CONFIRMED (round 2 declined to confirm/fix). This is the backbone anti-regression
guarantee (`configuration.md`, Phase 0 exit, hard invariant #1), but the only stated
mechanism (`detailed-config.md`: a test-id string per implemented option, plus an
`owner: Owner` enum tag) proves neither that behavior is wired nor that the option's
value is consumed at runtime. A test id can exist without exercising behavior.
Proposed fix G17 — define an enforceable check: a runtime/integration harness that,
for each option marked `implemented`, sets a non-default value and asserts an
observable behavioral difference (a "behavioral fingerprint" test per option),
failing CI if the option has no such test or the value has no observable effect.
Static registry diffing is insufficient; state this in `detailed-config.md`.

### R3-20 — `requirements-traceability.md` is stale

CONFIRMED. It does not reference `redirect-policy.md`, `rate-limiting.md`,
`detailed-ftp-sftp.md`, or the five `detailed-*` first-slice docs, and has no rows
for SSRF (F12), redirect/credential stripping (F8/F15), cookie jar (F14), proxy
flows (F18), or rate limiting (F13) — all established as first-class in round 2. It
also leans on `README.md` as design coverage for Performance/Security/Compactness,
which is thin. Proposed fix G18 — regenerate the traceability matrix to reference
every current doc and add rows for the round-2 security/protocol concerns, mapping
each to its owning doc and its testing-strategy entry.

## Low-Severity Findings

### R3-11 — `event-poll` option name renamed, not aliased

CONFIRMED. `configuration.md` replaces `event-poll` with `event-backend`, and
`event-backends.md` defines only `--event-backend`. The value aliases exist
(`epoll` etc.) but the option *name* does not, so existing aria2 configs containing
`event-poll=epoll` hit "unknown option." Proposed fix G11 — accept `event-poll` as a
deprecated alias of `event-backend` (name and values), warn once, and document it in
`configuration.md`.

### R3-21 — Worker sizing oversubscribes small machines

CONFIRMED. `threading-model.md` "Default Sizing": on 2 cores, `network_workers =
min(max(2,1),8) = 2` consumes both cores, then `cpu_workers = max(1, cpu_count -
network_workers) = max(1,0) = 1`, plus 1 control and up to 4 blocking-disk threads —
~8 threads on 2 cores. The cpu formula ignores the control and disk lanes. Proposed
fix G19 — size all lanes from one budget so the totals fit core count on small
machines (e.g. reserve control first, split the remainder across network/cpu/disk
with a documented floor), and add a 1–2 core sizing example to the doc.

### R3-22 — Path-safety gaps

CONFIRMED. `detailed-storage.md`/`security-recovery.md` `SafePathBuilder` reject
lists omit the Windows alternate-data-stream colon (`file:stream`), have no Unicode
normalization / overlong-UTF-8 / homoglyph handling, and leave `out`/`index-out`
containing path separators (legal in aria2 for subdirs) with undefined component
splitting. The two docs also differ in API shape (`SafePathInput` struct vs
`SafePathBuilder::new().component()` builder). Proposed fix G20 — add the ADS colon
and a Unicode-normalization rule to the reject/normalize list, define separator
splitting for `out`/`index-out`, and pick one `SafePathBuilder` API. Fold into the
existing round-2 R2-10/F9 path work.

### R3-23 — Three option-change error schemes still unreconciled

CONFIRMED — round 2 downgraded this to low (error codes are generated/CI-enforced)
but did not reconcile the vocabulary. `configuration.md` returns per-option
`OptionRequiresNewGeneration`/`OptionNotRuntimeMutable`; `detailed-config.md` returns
a grouped `OptionPatchRejected`. Proposed fix G21 — reconcile into one vocabulary
when generating `error_codes.json`; state which error `changeOption`/
`changeGlobalOption` returns. Corrected G7 retains controlled restart behavior, so
the vocabulary still has to distinguish an accepted automatic restart from a
change that requires an explicit new-generation request.

### R3-24 — Over-engineering for the first slice

note, not a defect. `library-choice.md`/`messaging-model.md` commit to five queue
families (tokio, thingbuf, rtrb, crossbeam, concurrent-queue) and four wrappers,
while `detailed-runtime.md` admits the slice may "start with Tokio bounded channels";
`concurrent-queue` appears in no topology diagram. The HDD/SATA/NVMe latency-
inference adaptivity (`disk-adapter.md`) is premature before any benchmark baseline
exists, and the `endianness` byte that never changes behavior (R2-5) is slice-1
overhead. Proposed fix G22 — mark these as post-slice-1: start with Tokio bounded
channels, drop `concurrent-queue` unless a topology needs it, and gate the disk
latency-inference behind a measured baseline. This is a scoping recommendation, not
a correctness fix.

## Supplemental Findings From Final Cross-Document Pass

These findings were added after a final pass that followed the data and control
paths across the HTTP, storage, security, runtime, API, and repository documents.
They are not replacements for R3-1 through R3-24; they expose additional
implementation seams that the earlier thematic pass did not make explicit.

### R3-25 — Unknown/chunked or decoded HTTP bodies do not fit the immutable fixed layout

CONFIRMED. `detailed-http-first-slice.md` creates the layout and journal before
the request is sent, then permits chunked responses when the total is unknown and
submits each body buffer at an exact offset. `detailed-storage.md` makes
`FileLayout` immutable for a generation, requires `global_start`/`global_end` for
mapping, and rejects writes outside the known layout. At the same time,
`protocol-modernization.md` permits a decoded body whose length differs from the
wire `Content-Length` (for example, gzip/deflate). Those rules cannot all hold:
there is no final extent for chunked data, and a decoded stream cannot use the wire
length as its fixed file extent. A split or resume offset can therefore be mapped
against the wrong byte space, rejected after bytes have arrived, or silently
produce a file whose hash covers a different representation than the layout says.

Proposed fix H1 — choose and state one representation contract before enabling the
first slice:

- Restrict range/split/resume and immutable-layout writes to identity-encoded,
  known-length responses (`Content-Length` or a validated `Content-Range`). Reject
  chunked and content-decoded bodies for that path until a final extent exists.
- If chunked or decoded sequential downloads are required, define a separate
  append/growing layout with a maximum size, wire-byte and decoded-byte counters,
  finalization, and a journal record that commits the final extent before hashing
  or advertising completion.
- Keep wire offsets and decoded-file offsets distinct, and explicitly prohibit
  range requests against a representation whose decoding changes byte positions.
  Add tests for chunked, gzip/deflate, truncated, and over-limit bodies.

### R3-26 — Range writes have no provisional commit/abort transaction

CONFIRMED. The HTTP design streams body chunks to storage and validates the exact
body length only at the end. It describes oversized bodies as rejected and short
bodies as non-durable, but `StorageCommand::WriteBlock` has no lease/attempt
identifier or transaction handle, and `WriteAck` can report `PieceDurable` without
an explicit commit operation. `split-download.md` consequently promises that
partial bytes are not durable without specifying how already-issued writes are
withdrawn, hidden from recovery, or safely overwritten. A crash between the first
block and the final length check can leave bytes that look like a valid prefix of a
later lease.

Proposed fix H2 — make a range attempt transactional:

- Give every lease attempt a `LeaseId`/generation and record writes as
  provisional. Add explicit `CommitLease` and `AbortLease` (or an equivalent
  prepare/commit result) to the storage contract.
- Only a successful exact-length and digest/validator check may commit the lease
  and emit `PieceDurable`; an abort makes all provisional blocks invisible to
  recovery and eligible for overwrite.
- Persist the provisional/committed state in the journal and define crash recovery
  for an in-flight attempt. Tie endgame arbitration to the same commit operation,
  rather than to the arrival of the first length-valid block.

### R3-27 — SSRF protection is bypassable for proxy-resolved destinations

CONFIRMED. `security-recovery.md` requires resolving and pinning a target before
connecting and discusses checking the proxy endpoint, while
`protocol-modernization.md` supports HTTP CONNECT and SOCKS5/socks5h remote DNS.
The documents do not say which component resolves the final hostname or how the
pinned IP is conveyed through a proxy. An untrusted RPC caller can therefore use a
remote-DNS SOCKS proxy or CONNECT target to reach a private address that passed a
local pre-check, defeating the SSRF boundary. Redirects make the ambiguity recur
after the initial check.

Proposed fix H3 — define the trust boundary at the final connection hop:

- For untrusted remote RPC, disallow `socks5h`/remote-DNS and arbitrary CONNECT
  targets unless the proxy is explicitly trusted and enforces its own destination
  allowlist. Prefer local resolution, IP-range validation (including IPv4-mapped
  IPv6), and a pinned IP with separately specified Host/SNI.
- Apply the same policy to every redirect and to proxy changes; log the validated
  destination class without logging credentials. State whether DNS rebinding is
  prevented by reconnect-time pin checks.
- Add tests for private/link-local/loopback targets, decimal and mapped address
  forms, CONNECT, socks5h, and redirect-to-private cases.

### R3-28 — User headers can override range, encoding, and other safety-critical headers

CONFIRMED. `configuration.md` lists the user `header` option as implemented, but
the HTTP documents also require generated `Range`, `If-Range`, `Accept-Encoding:
identity`, `Host`, and length/transfer headers for split and resume requests.
`redirect-policy.md` specifies credential and cookie propagation but not custom
header merge order or re-evaluation after a redirect. A caller can consequently
replace a safety header, inject a second framing header, or replay an
`Authorization` value to a new origin while the design still claims exact-range
and identity semantics.

Proposed fix H4 — define a reserved-header policy:

- Reject CR/LF and invalid field syntax in user headers. Reserve
  `Host`, `Content-Length`, `Transfer-Encoding`, `Range`, `If-Range`,
  `Accept-Encoding`, `Authorization`, and any integrity/signature headers; either
  reject user overrides or make the generated value authoritative and visible in
  diagnostics.
- Define case-insensitive merge and duplicate behavior, then re-run the policy on
  every redirect/origin change. Credential and cookie headers must use the
  existing origin/scope rules, never blind replay.
- Add compatibility tests proving that custom harmless headers survive while
  framing, range, identity, and credential invariants cannot be overridden.

### R3-29 — `DiskBackend` error ownership cannot return its `BufferLease`

CONFIRMED. `detailed-runtime.md` declares `write_at(..., BufferLease) ->
Result<DiskCompletion>` and says the backend must return the same lease on both
completion and error; `disk-adapter.md` repeats that ownership requirement. The
ordinary `Err` branch, however, contains no lease or ownership token. An adapter
cannot implement the contract without either leaking the buffer, recovering it
through an undocumented side channel, or risking a double return. This is
independent of the type-definition conflict in R3-6 and affects every I/O error
path.

Proposed fix H5 — make ownership total in the type system:

- Return an outcome whose success and failure variants both carry the lease, for
  example `DiskResult { lease, completion: Result<...> }`, or define
  `DiskError { lease, kind }` and prohibit lease-less errors.
- Specify who returns/quarantines a lease after cancellation, short write, and
  backend shutdown, and make the rule identical for io_uring, IOCP, and the
  blocking fallback. Add fault-injection tests that count every lease exactly once.

### R3-30 — Runtime event-backend fallback conflicts with Tokio and the no-custom-reactor decision

CONFIRMED. `event-backends.md` promises runtime probing among raw epoll, kqueue,
poll, and select backends and exposes an `EventBackend` abstraction.
`library-choice.md` chooses Tokio/Mio for networking and explicitly rejects a
custom reactor, while the Tokio section assigns task and TCP ownership to Tokio.
No adapter or build split explains how a raw runtime-selected backend supplies
Tokio's reactor, socket registration, wakeups, and cancellation semantics. The
async trait used for runtime selection also runs into the object-safety/allocation
issue noted in R3-16. This is an architectural contradiction, not just a missing
option alias.

Proposed fix H6 — select one reactor architecture and make the selection testable:

- Either use Tokio/Mio's platform backends (with a documented compile-time or
  runtime feature selection) and remove the promise of an independent raw reactor,
  or own the reactor/task/socket stack and remove Tokio/Mio from the ownership
  contract.
- If both are retained for separate builds, define the adapter boundary, feature
  flags, wakeup and cancellation semantics, and which binary/library artifacts
  each build produces. Use enum/static dispatch where possible and document the
  supported fallback matrix in CI.

### R3-31 — Internal task states have no normative aria2 wire-status projection

CONFIRMED. `detailed-core.md` defines internal states including `Accepted`,
`Allocating`, `RetryWait`, `PausedSlow`, `Verifying`, `StoppedResult`, and
`Removed`, while `configuration.md` adds `PausedRestarting`. The API document
promises aria2-compatible response shapes, but it supplies no mapping table or
wire rule. aria2 clients expect the closed status set `active`, `waiting`,
`paused`, `error`, `complete`, and `removed`; leaking an internal value breaks
existing clients just as R3-7 does.

Proposed fix H7 — keep the compatibility surface closed: define a normative table
mapping every internal state to an aria2 status, an optional numeric/error reason,
and visibility rules for transitional states. Expose restart, throttling, and
verification reasons only as extension fields; never serialize them as a new
`status` value. The table must include `PausedRestarting -> waiting` with no pause
event, as verified by R4-2 and corrected G7.

### R3-32 — GID wire serialization and identity rules are unspecified

CONFIRMED. The core model uses `Gid(NonZeroU64)`, but neither the core nor API
document specifies the 16-character hexadecimal wire form, parsing of `--gid`,
short-prefix lookup, leading zeroes, or collision behavior. The existing aria2
manual requires a 16-character hexadecimal GID, so a numeric JSON serialization or
variable-width formatter would violate the stated compatibility claim.

Proposed fix H8 — define one GID codec and identity policy: serialize as exactly
16 lowercase hexadecimal characters, parse the documented CLI/API forms, reject
zero and ambiguous prefixes deterministically, and specify whether a supplied GID
is persisted across restart or regenerated on collision. Add round-trip and
multi-task lookup tests.

### R3-33 — Credentials and secrets at rest have no persistence policy

CONFIRMED. `session-persistence.md` stores URI lists, mirror metadata, option
snapshots, validators, and identity state in SQLite/journal artifacts. The
`Secret<T>` rules in `security-recovery.md` redact logs and may zeroize memory, but
they do not say whether RPC passwords, proxy credentials, cookies, or signed
headers can enter those artifacts, what file permissions apply, or how an exported
session is protected. Redaction in a config dump is not at-rest protection.

Proposed fix H9 — choose an explicit policy before persistence is implemented:
omit secrets and require re-authentication; encrypt them with an OS keyring or
user-supplied key; or document an equivalent encrypted store. In all cases specify
0600/ACL requirements, export/backup behavior, crash leftovers, key rotation, and
what recovery does when a secret is unavailable. Add tests that inspect SQLite,
journal, temporary, and exported-session files.

### R3-34 — Metalink incremental hashing has no out-of-order reassembly rule

CONFIRMED. `metalink-chunking.md` hashes streamed pieces incrementally and avoids
readback, while `split-download.md` permits smaller or misaligned dynamic leases
that can complete out of order. The design does not define how a checksum chunk is
fed when its leases arrive as A/B (or in many fragments), nor the memory bound for
holding later fragments. Hashing each lease independently is not equivalent to
hashing the declared contiguous Metalink chunk.

Proposed fix H10 — either serialize writes/hash updates in checksum-chunk order,
or add a bounded reorder buffer keyed by chunk offset and feed the digest only
when all preceding bytes are present. State behavior on a gap, retry, endgame
winner, and memory-limit breach; readback is the fallback when ordering cannot be
guaranteed.

### R3-35 — BT shutdown checkpoint ordering contradicts the BT-specific shutdown contract

CONFIRMED. `detailed-runtime.md` orders global shutdown as network, disk, journal,
session, then BT, which persists the session before the BT session has paused and
saved resume data. `libtorrent-integration.md` instead requires pause/save-resume,
persist, then stop the session. The two orders can produce a session checkpoint
that lacks the final torrent resume state or races destruction of the alert pump.

Proposed fix H11 — define a single shutdown barrier: quiesce BT commands, request
and await resume data, fold it into the task checkpoint, then stop BT; only after
that may the global journal/session checkpoint be finalized. Document timeout and
failure behavior (including an explicit dirty checkpoint) and use the same barrier
for orderly shutdown and crash-recovery tests.

### R3-36 — WebSocket/stdio event queues have no slow-consumer policy

CONFIRMED. WebSocket output is required, and stdio shares the dispatcher and
streams notifications, but `messaging-model.md` only bounds hot internal queues.
There is no per-client queue limit, event coalescing rule, disconnect behavior, or
statement that a blocked stdout/WebSocket client cannot stall the scheduler. A
slow remote client can therefore turn an external event stream into unbounded
memory growth or global backpressure.

Proposed fix H12 — define a bounded queue per external client, with a documented
coalescing/drop policy for status/stat events, lossless treatment for replies and
durability/error notifications, and deterministic disconnect/overflow errors.
Never block scheduler or storage actors on a client write; expose a resubscribe or
snapshot mechanism so a client can recover after coalescing.

### R3-37 — The Rust engine has no build/packaging integration plan in this autotools repository

CONFIRMED, conditional on the design being intended for this repository. The
repository currently builds the C++ `aria2c` through autotools (`Makefile.am` and
`src/Makefile.am`), with no Cargo manifest or Rust source tree. The design instead
names Rust crates (`ariax-core` and related components) and describes a Rust core,
but does not specify whether it replaces `aria2c`, is linked as a library, or is a
parallel experimental artifact. Cross-compilation, generated bindings, packaging,
release tarballs, and CI toolchain requirements are consequently undefined.

Proposed fix H13 — add a repository integration decision before implementation:
define the artifact and ownership boundary, add Cargo/autotools bridge targets (or
explicitly keep a separate repository), document cross-build and packaging rules,
and add CI jobs that build the exact release artifacts on supported platforms.
Until then, label the Rust design as a parallel prototype rather than an
implementation-ready replacement for the existing binary.

### R3-38 — The task state graph omits cancellation/error transitions for non-Active states

CONFIRMED. The state list includes `Allocating`, `RetryWait`, `Paused`,
`PausedSlow`, `Verifying`, and terminal states, and the scheduler exposes Pause
and Remove commands. The transition text and cancellation rules do not enumerate
what happens when a task is paused, removed, cancelled, or fails while in each of
those non-Active states (including whether leases are aborted, BT state is saved,
and which terminal result is observable). Without a complete graph, different
workers can legally emit incompatible terminal events or leave a lease/journal
record stranded.

Proposed fix H14 — publish a transition table covering every state × command/error
combination, including lease abort/rollback, retry classification, journal events,
wire status, and idempotence. Include BT/seeding and shutdown paths, and make the
table the source for model-based tests and the RPC projection in H7.

## Round-4 External Verification Merge

Round 4 checked external premises rather than re-reading every design document.
For this merge, its source claims were rechecked against the aria2 C++ source and
manual, and its Rust/filesystem claims were repeated with rustc 1.85.0 on WSL and
rustc 1.97.0 from the MSYS2 MINGW64 tool directory. Both compilers used
`-O --edition 2021` for timing experiments. Round 4 reused fix labels H1–H5,
which already name R3-25 through R3-29 fixes in this consolidated document; new
round-4-only fixes are therefore named V1 and V2 below.

### R4-1 — aria2 does restart the three active download options

VERIFIED; this corrects R3-7. `RpcMethod::gatherChangeableOption` sends an option
to the pending set when its handler has `changeOptionForReserved` but not
`changeOption`. `OptionHandlerFactory.cc` configures `split`,
`max-connection-per-server`, and `min-split-size` exactly that way.
`ChangeOptionRpcMethod` then stores the pending set, calls `pauseRequestGroup`, and
sets `restartRequested`. The `aria2.changeOption` manual independently says that
all changeable options except six named live exceptions restart an active
download; none of these three is an exception.

Result: retain their `active_restart` classification. The original G7 proposal to
apply them live is withdrawn, and the corrected G7 above is authoritative.

### R4-2 — option-change restart is observed as `waiting`, not `paused`

VERIFIED; this also corrects R3-7. In `RequestGroupMan.cc`, the restart path moves
the group to `STATE_WAITING`, applies pending options, clears `pauseRequested`, and
skips the pause hook/event. `TellStatusRpcMethod` and `TellWaitingRpcMethod` report
`paused` only while `pauseRequested` is set; otherwise a non-active live group is
`waiting`.

Result: an internal `PausedRestarting` state must project to `waiting`, and a
restart must not emit a pause notification. This rule is also a required row in
the R3-31/H7 state-to-wire mapping.

### R4-3 — runtime-update divergences need a matrix, with corrected scope

VERIFIED WITH CORRECTION. Round 4 says "four options" but lists five:
`lowest-speed-limit`, `retry-on`, `retry-on-http-status`, `retry-after`, and
`slow-slot-policy`. Only `lowest-speed-limit` is an aria2 option in this source;
its handler at `src/OptionHandlerFactory.cc:853-860` is reserved-only, so changing
it on an active download restarts aria2. The other four are new design extensions
and do not exist in aria2, so it is not meaningful to say aria2 restarts those
exact options. They may deliberately be live, but that behavior is an extension
rather than strict compatibility.

Proposed fix V1 — add a generated runtime-compatibility table to
`configuration.md`, referenced by `apis-and-embedding.md`, with these columns:

- option name and whether aria2 implements it,
- aria2 active-change behavior (`live`, restart, or unavailable),
- this design's behavior and compatibility mode,
- whether a difference is required, intentional, or unresolved.

Audit every real aria2 option against the manual's six live exceptions. Mark the
four new retry/slow-slot controls as extensions instead of inferring an aria2
restart behavior for names aria2 does not know.

### R4-4 — object-safety is verified; boxing cost is real but path-dependent

VERIFIED WITH CORRECTION; incorporated into R3-16/G14. Both available compilers
reject a native async-method trait used as `&dyn DiskBackend` with E0038. Enum
dispatch is therefore the simplest compile-valid runtime selection mechanism.

Round 4's stronger claim that per-call boxing cost is "unmeasurable" was not
reproduced. A custom global allocator around 1,000,000 erased-future calls counted
exactly 1,000,000 allocations, while the enum path counted zero. Dispatch-only
timings were approximately:

| Toolchain | Boxed dyn future | Enum future |
| --- | ---: | ---: |
| WSL rustc 1.85.0 | 20.8 ns/call | 2.1–2.2 ns/call |
| MinGW rustc 1.97.0 | 69–78 ns/call | 2.0–2.2 ns/call |

Those figures isolate dispatch and allocation, not disk I/O, so they must not be
advertised as end-to-end throughput gains. They do verify the structural
allocation that the design says it wants to avoid.

### R4-5 — journal sync cost is platform-specific; the mode taxonomy is not refuted

PARTIALLY VERIFIED. A 64-byte append benchmark (300 records, median of three runs)
produced:

| Execution/filesystem | No sync | `sync_data` | `sync_all` |
| --- | ---: | ---: | ---: |
| WSL `lxfs` (`/tmp`) | 4.3 µs | 1738.4 µs | 1716.9 µs |
| WSL `drvfs` over NTFS (`/mnt/d`) | 3.6 µs | 218.2 µs | 1782.4 µs |
| Native MinGW on NTFS (`D:`) | 2.4 µs | 1648.8 µs | 1653.0 µs |

The WSL `lxfs` and native Windows results support round 4's narrow observation:
for an append that changes file length, `sync_data` and `sync_all` can cost about
the same. The `drvfs` result refutes the claim that this is universal. More
importantly, round 4 overreads `README.md`: balanced durability batches journal
flushes at a piece-group/time boundary, while strict durability syncs data and
control state for every piece. Equal cost for one journal flush does not make the
two modes equivalent or invalidate their taxonomy.

Proposed fix V2 — describe durability modes by ordering and flush frequency, not
by assuming a portable performance ordering between `sync_data` and `sync_all`:

- keep G1's data-before-`PieceDurable` correctness requirement,
- state balanced batching and strict per-piece data/control guarantees explicitly,
- benchmark the real journal/data path on native Linux filesystems and Windows
  NTFS during Phase 0, including small-piece batching,
- do not adopt round 4's blanket recommendation that balanced always use
  `sync_all`; select the primitive from required durability semantics and verified
  platform behavior.

### R4-6 — length-only completion is invalid; physical sparseness is not portable

PARTIALLY VERIFIED. On WSL `lxfs`, WSL `drvfs`, and native MinGW/NTFS,
`set_len(10 MiB)` immediately produced a 10 MiB logical length and reads from the
unwritten middle returned zeroes. Thus file length cannot prove that download data
was received or that a resume prefix is durable.

The physical-allocation part of round 4 was not reproduced: `stat`/`du` reported
the full 10 MiB allocated in all three environments, rather than an empty sparse
extent. Physical sparseness is platform/backend-dependent and is not needed to
prove the logical flaw. The current storage design already says unknown bytes are
never complete by length alone; keep that invariant and add a test. If `trunc` is
intended to guarantee sparse allocation, `disk-adapter.md` must define and test the
platform-specific operation rather than infer it from `set_len`.

## Cross-Cluster Observations

- The recurring root cause is the same one round 2 named: the `detailed-*` docs were
  added after the topic docs and re-define types the topic docs already own, which is
  how the `BufferLease` (R3-6), journal-ownership (R3-5), and error-scheme (R3-23)
  divergences arose. A single normative-source rule per artifact — the `detailed-*`
  doc owns the type, topic docs reference it — would prevent the next round of these.
- A new pattern this round: guarantees stated for `strict` that silently do not hold
  in the default mode (R3-1 durability, R3-3 identity). Any guarantee the README
  advertises unconditionally ("no silent corruption", "poweroff recovery") should be
  audited for whether it holds in `balanced`/`off`, not just `strict`.
- The supplemental pass found a fourth pattern: a precondition is checked in one
  layer but can be invalidated in the next. HTTP layout assumptions change after
  response framing/decoding (R3-25), writes become observable before range
  validation (R3-26), SSRF checks stop before proxy-side resolution (R3-27), and
  generated request invariants can be replaced by user headers (R3-28). These need
  end-to-end contracts, not another local validation helper.
- Ownership and backpressure need to remain explicit on failure as well as success.
  The completion topology (R3-4), buffer model (R3-6), lease-less disk error
  (R3-29), external subscriber queues (R3-36), and incomplete state transitions
  (R3-38) are all versions of the same missing rule: every accepted unit of work
  must have exactly one bounded owner until commit, abort, or terminal delivery.
- Compatibility is promised at the API boundary but is not yet projected from the
  richer internal model. R3-7, R3-10, R3-31, and R3-32 should be resolved as one
  wire-contract exercise, with a closed status set, exact GID codec, authentication
  convention, and extension-field policy.
- `event-backends.md` and the repository build remain architectural sources of
  truth that do not match the chosen Rust/Tokio implementation (R3-30, R3-37).
  Those decisions should be settled before module boundaries or CI gates harden.
- Round 4 exposed a review-process boundary: claims about aria2 behavior require a
  source/manual citation, and claims about Rust or filesystem cost require a
  reproducible experiment plus platform scope. "Reproduced against the design
  text" is insufficient for either. R3-7 and R3-16 were corrected by that rule;
  R4-5 shows why one-host timings must not become portable architecture claims.
- Compatibility should distinguish implemented aria2 behavior from deliberate
  extensions. V1's generated matrix is the common source for runtime mutation,
  wire projection, strict compatibility mode, and behavioral tests.

## Recommended Sequencing

Resolve the decisions that shape every implementation layer first:

1. H6 and H13 — choose the reactor architecture and repository/build artifact.
2. G6, H5, and G4 — define one buffer/ownership model and a valid completion lane.
3. G5 and G13 — establish one journal owner, payload/rotation format, and snapshot
   precedence.
4. G1, G2, H2, and V2 — close the durability, hash rollback, provisional range
   transaction, and mode-semantics paths.
5. H1 — settle the body representation and growing-vs-fixed layout contract.
6. G3, H3, and H4 — enforce identity, proxy-destination, redirect, and generated-
   header safety end to end.
7. Corrected G7, G10, H7, H8, H14, and V1 — freeze the aria2 wire/runtime-
   compatibility contract and complete the internal state-transition table.

Then schedule the remaining fixes with their owning phases:

- G8, G12, and H11 for BT runtime mutation, resume durability, and shutdown.
- G9 and G15 for FTP limits and discarded-byte accounting.
- Corrected G14 for dyn-compatible, allocation-free disk-backend dispatch.
- H9 for secret persistence before SQLite/journal/session export is enabled.
- H10 for Metalink hashing before out-of-order checksum-chunk leases are enabled.
- H12 before WebSocket or stdio notifications are exposed to arbitrary clients.
- G16, G17, and G18 for phase/slice reconciliation, behavioral option tests, and
  traceability regeneration.
- G11, G19, G20, G21, and G22 as compatibility, sizing, path-hardening, error-
  vocabulary, and scope cleanup.

Feature gates should be explicit: H1/H2 must land before chunked, decoded, range,
or resume HTTP writes; H3/H4 before untrusted remote RPC with proxies or custom
headers; G3 before concurrent multi-mirror endgame/redirect writes; G9 before FTP
splitting; and corrected G7 plus H7/H8/H14/V1 before claiming aria2 RPC
compatibility. The superseded G7 proposal to make the three restart-only options
live must not be implemented.
