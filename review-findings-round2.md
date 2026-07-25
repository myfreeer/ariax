# Design Review Round 2: Findings and Proposed Fixes

Status: review of the `design/` set, with proposed resolutions. Fixes F1–F18
have been applied to the affected docs; this document is retained as the rationale
and audit trail for those changes. Verdicts and line references describe the
pre-fix state.

This round reviewed all 32 design documents in five thematic clusters, then ran
an adversarial verification pass that re-read the actual document text and ruled
each load-bearing finding CONFIRMED, PARTIAL, or REFUTED with quotes. Only
findings that survived verification are carried here. Overstated or refuted
first-round claims are recorded in "Downgraded and Rejected Findings" so they are
not acted on by mistake.

The design set is architecturally strong and internally coherent. The issues
below are concentrated at two seams: (1) places where two documents specify the
same artifact differently, and (2) goals stated without an owning mechanism.
None require an architectural change. Each has a concrete proposed fix.

## Severity And Status Summary

| ID | Finding | Severity | Verdict | Fix |
| --- | --- | --- | --- | --- |
| R2-1 | `DiskBackend` trait specified two incompatible ways | High | CONFIRMED | F1 |
| R2-2 | Control-plane priority required, no priority primitive selected | High | CONFIRMED | F2 |
| R2-3 | Buffer quarantine has no cap or reclamation path | High | CONFIRMED | F3 |
| R2-4 | Cross-mirror split has no entity-identity check without checksums | High | CONFIRMED | F4 |
| R2-5 | Journal record-type set diverges across docs | Medium | CONFIRMED | F5 |
| R2-6 | Session-persistence journal format conflicts | Medium | PARTIAL (real) | F5 |
| R2-7 | `disk-cache` memory is in no budget | Medium | CONFIRMED | F6 |
| R2-8 | No `Accept-Encoding: identity` rule for range/resume | Medium | PARTIAL (real) | F7 |
| R2-9 | Redirect policy referenced but undefined | Medium | CONFIRMED | F8 |
| R2-10 | Per-file torrent path validation unspecified | Medium | PARTIAL (real) | F9 |
| R2-11 | BT event-channel overflow policy unspecified | Medium | CONFIRMED | F10 |
| R2-12 | FTP/SFTP have no correctness contract | High | completeness | F11 |
| R2-13 | SSRF guardrails promised, not mechanized | High | completeness | F12 |
| R2-14 | Rate limiter has no design doc | High | completeness | F13 |
| R2-15 | Cross-origin redirect credential stripping undefined | High | completeness | F8 |
| R2-16 | Cookie jar semantics undesigned | Medium | completeness | F14 |
| R2-17 | Observability metric cardinality unbounded | Medium | completeness | F15 |
| R2-18 | Cross-lane graceful-shutdown ordering undefined | Medium | completeness | F16 |
| R2-19 | Disk-full handling contradicts itself | Medium | completeness | F17 |
| R2-20 | Proxy CONNECT/SOCKS/auth flows are one-liners | Medium | completeness | F18 |

Verdict key: CONFIRMED — reproduced against the text. PARTIAL (real) — the core
defect holds but the first-round framing overstated it; the corrected form is
recorded below. completeness — surfaced by the cross-cutting pass, not a
contradiction between existing statements but a goal with no owning mechanism.

## High-Severity Findings

### R2-1 — `DiskBackend` trait is specified two incompatible ways

CONFIRMED. `event-backends.md:119-123` defines the trait with `buf: Buffer`,
`read_at -> Buffer`, `fsync(handle, mode: FsyncMode)`, `allocate(...)`, plus
`name()`/`capabilities()`/`open(path, mode)`. `detailed-runtime.md:216-222`
defines the same trait with `buf: BufferLease`, `read_at -> BufferLease`,
`sync_data`/`sync_all` (no mode), `rename(...)`, `open(req: OpenRequest)`, and no
`allocate`, `name`, or `capabilities`. The `Buffer` vs `BufferLease` divergence
is fundamental: the entire ownership model is built on move-only `BufferLease`
tokens, so `Buffer` is stale. This is the central storage contract; left as-is it
forks into two implementations.

Proposed fix F1 — declare one canonical `DiskBackend` trait and reference it
everywhere:

- Buffer type is `BufferLease` in both `write_at` and `read_at`. Remove all
  `Buffer` uses from `event-backends.md`.
- Keep the union of operations that are actually needed: `open(req: OpenRequest)`,
  `write_at`, `read_at`, `sync_data`, `sync_all`, `allocate(off, len, mode)`
  (required by the preallocation modes in `disk-adapter.md`), `rename(from, to)`
  (required by temp→final finalization in `detailed-storage.md`), and the backend
  descriptors `name()` / `capabilities()` (required by the probe/fallback logic in
  `event-backends.md`).
- Drop the `fsync(handle, mode: FsyncMode)` form in favor of explicit
  `sync_data` / `sync_all`, which map 1:1 to `fdatasync` / `fsync` and to the
  data-vs-metadata distinction the durability modes already need. Durability mode
  chooses which to call; the backend does not need a mode enum.
- Make `detailed-runtime.md` the normative definition (it is the first-slice
  contract). `event-backends.md` states capability/probing semantics and refers to
  the normative trait rather than restating the signature.

Also fold in the related type-name findings (`Buffer` vs `BufferLease` in
`disk-adapter.md` `WriteBlock.buffer`, and `Buffer` in the event-backend trait):
`BufferLease` is the single name.

### R2-2 — Control-plane priority is required but no priority primitive is selected

CONFIRMED. `threading-model.md:208` lists "priority for control/journal
messages" as a required channel property and `detailed-runtime.md:51` says
"control/journal priority reserves cannot be consumed by bulk writes."
`backpressure.md:121` says "pause/remove should enqueue immediately." But the
`messaging-model.md:170-178` selection matrix picks only FIFO/unordered
primitives (`tokio::sync::mpsc`, `thingbuf`, `rtrb`, `crossbeam`,
`concurrent-queue`), and `detailed-runtime.md:89` routes both `addUri` and
pause/remove through one `ControlQueue<SchedulerCommand>`. The
no-blocking-control-plane guarantee has no mechanism behind it. (The first-round
"reject new RPC adds" causal link was loose; the actual risk is a shared bounded
FIFO with no priority lane, so a burst of queued adds can delay a pause/remove.)

Proposed fix F2 — split the control queue into two bounded channels rather than
introducing a priority primitive:

- `control_urgent`: small, bounded, carries pause, remove, cancel, shutdown, and
  journal-critical commands. Its capacity is reserved and never shared with adds.
- `control_bulk`: carries `addUri`/`addTorrent`/status-scan commands, subject to
  admission rejection when budgets are exhausted (this is what `backpressure.md`
  "reject new RPC adds" protects).
- The control loop drains `control_urgent` first using a biased `tokio::select!`
  (poll urgent, then bulk), so an urgent command never waits behind queued adds.
- State the invariant in `detailed-runtime.md` and `messaging-model.md`: urgent
  control capacity is a reserve that bulk commands cannot consume, and admission
  backpressure applies only to `control_bulk`.

This keeps every selected primitive FIFO and satisfies "pause/remove enqueue
immediately" without a new dependency.

### R2-3 — Buffer quarantine has no cap or reclamation path

CONFIRMED. `detailed-runtime.md:226-228` quarantines a lease "until completion is
observed" when the OS may still own it after a cancellation; `detailed-runtime.md:82`
sends leaked leases to quarantine on close; the `Quarantine` type
(`detailed-runtime.md:110`) has no fields. No document specifies a size cap,
timeout, or forced reclamation when a completion is *never* observed (a hung
io_uring op, a closed fd, a stuck IOCP completion). There is a `quarantined bytes`
metric (`detailed-runtime.md:236`), so the condition is observable, and if
quarantined bytes count against the budget the failure mode is pool starvation
rather than literally unbounded RSS — but it is still an unreclaimed-resource leak
under sustained cancellation against an unhealthy backend. (First-round
"unbounded memory" was slightly strong for this reason.)

Proposed fix F3 — give quarantine a budget, a cancellation protocol, and a
faulted-backend fallback:

- Add `quarantine_budget` to `ResourceManager`; quarantined bytes count against
  `total_budget`.
- On cancellation, the backend must issue an explicit kernel cancel
  (`IORING_OP_ASYNC_CANCEL` / `CancelIoEx`) and reclaim the lease only after
  cancel-confirmation or completion. Define a bounded `quarantine_timeout`.
- If a completion is neither observed nor cancel-confirmed within the timeout,
  the lease's memory is retired from the pool (leak-accounted, not returned) and a
  replacement is allocated only within `total_budget`.
- When `quarantine_budget` is exhausted, transition the backend to `Faulted` and
  stop issuing new I/O on it (fall back per `event-backends.md`) rather than
  growing memory. Surface this transition in metrics.

### R2-4 — Cross-mirror split has no entity-identity check without checksums

CONFIRMED. `split-download.md:71` runs up to `split=8` non-overlapping leases
across different mirror URIs; `configuration.md:334` only *asserts* that
TAB-separated URIs are "mirror URIs for the same entity" — an assumption, not a
validated rule. The per-response acceptance checks
(`detailed-http-first-slice.md:156-161`) validate status, `Content-Range`, body
length, and `Content-Encoding` per response but never cross-check that two mirrors
serve the byte-identical entity. For plain multi-URI HTTP with no per-chunk
checksum, two mirrors serving same-length-but-different content interleave into
silent corruption and pass every specified check. (A total-length mismatch *would*
be caught by the "known total" check, so corruption requires equal-length differing
content — narrow, but real, and common with dynamically generated or freshly
rebuilt artifacts.)

Proposed fix F4 — gate concurrent multi-mirror split on a shared content digest.

Correction to the first draft of this fix: comparing `ETag` across mirrors does
NOT work. `ETag` is opaque and origin-scoped (RFC 9110) — two mirrors serving
byte-identical content routinely return different ETags (different derivation
from inode/mtime/size, different CDN schemes) and the algorithm is unspecified,
so a cross-mirror ETag comparison both rejects valid mirrors and fails to prove
identity on a coincidental match. Only content-level verification proves
cross-mirror byte-identity.

Decision on default: the strict behavior is *opt-in*; the default is
aria2-compatible. Forcing verified-or-single-mirror by default would break the
common aria2 workflow of pasting a mirror list with no checksum, so that is a
compatibility regression the project explicitly declined. The applied rule is
selected by `--verify-mirror-identity`:

- `off` (default, aria2-compatible): trust the TAB-separated mirror list and
  split concurrently across all eligible mirrors, exactly as aria2 does. A
  total-length mismatch is still rejected by the `Content-Range` known-total
  check. If a whole-file or Metalink checksum is configured it is verified (per
  chunk for Metalink, at end-of-file otherwise), so corruption is caught before
  completion — just not before bytes are written. This matches aria2's guarantee.
- `strict` (opt-in): concurrent multi-mirror split is admitted only when the
  assembled result will be verified by a shared content digest — Metalink
  per-chunk checksums (preferred, divergence caught per chunk), a `Content-Digest`/
  `Repr-Digest` (RFC 9530) present on every mirror with a matching algorithm, or a
  user-supplied whole-file checksum. Without such a digest, split is restricted to
  a single mirror (its own `ETag`/`Last-Modified` via `If-Range` keeps it
  self-consistent — a valid per-origin guarantee); other URIs remain
  sequential-download or restart fallbacks.
- Documented in `split-download.md` (Cross-Mirror Entity Identity),
  `detailed-http-first-slice.md`, and registered in `configuration.md`. Add a
  fault-injection test: two mirrors, equal length, differing content, no digest —
  under `strict` must be detected or refused; under `off` corruption is caught
  only if a whole-file/Metalink checksum is configured.

## Medium-Severity Findings

### R2-5 / R2-6 — Journal record-type set and format diverge

R2-5 CONFIRMED. `detailed-storage.md:234-248` lists `TaskCreated`,
`OptionsSnapshot`, `LayoutCommitted`, `GenerationStarted`, `PieceWritten`,
`PieceVerified`, `PieceDurable`, `RetryState`, `TaskPaused`, `TaskComplete`,
`TaskError`, `TaskRemoved`, `CleanShutdown`. `security-recovery.md:139-148` omits
`OptionsSnapshot`, `GenerationStarted`, `RetryState`, `TaskRemoved`,
`CleanShutdown` and instead adds `PieceStarted`. Worse, `security-recovery.md:153`
says "only generation-changing records advance the generation" but never lists
`GenerationStarted`, the record whose name implies exactly that.

R2-6 PARTIAL (real). `session-persistence.md:316-325` describes a third framing
that introduces a `layout hash` field present in no other description and drops
`generation` and `sequence`, both of which the other two docs carry
(`detailed-storage.md:224-225`, `security-recovery.md:134`). The first-round
"three incompatible formats" was inflated — `detailed-storage.md` and
`security-recovery.md` actually agree with each other; the outlier is
`session-persistence.md`.

Also in this area, CONFIRMED but low severity: `detailed-storage.md:211-215`
orders the `endianness: u8` byte after the multi-byte `version: u16`, so a reader
cannot know the byte order of `version` until after reading it.

Proposed fix F5 — one normative journal spec:

- Make `detailed-storage.md` the single normative source for the record enum and
  the on-disk layout. `security-recovery.md` and `session-persistence.md` reference
  it and stop restating fields.
- Adopt the union enum. Keep both `GenerationStarted` (advances the generation)
  and `PieceStarted` (marks a piece in-flight so recovery can reset it) if both
  roles are wanted; explicitly annotate which record advances the generation, so
  `security-recovery.md:153` has a concrete referent.
- Remove the `layout hash` field from `session-persistence.md`'s per-record
  framing. The layout hash belongs in the `LayoutCommitted` payload and/or the
  control header, not in every record. Restore `generation` and `sequence`.
- Fix endianness: define all multi-byte fields as fixed little-endian and keep the
  endianness byte only as an assertion, or move the endianness byte ahead of every
  multi-byte field. Fixed-endian is recommended for a control format.
- While here, also close the related torn-write gap: a CRC covering the full
  record framing (`record_len`, `record_type`, `generation`, `sequence`) plus
  payload, an explicit `max_record_len` bound checked before any read, and a precise
  definition of the commit marker (sentinel vs checksum).

### R2-7 — `disk-cache` memory is in no budget

CONFIRMED. `disk-cache` is a user option (`backpressure.md:128`) but appears in
neither the Memory Signals list (`backpressure.md:88-92`) nor any `ResourceManager`
budget (`threading-model.md:55-64`, `detailed-runtime.md:35-43`). Total memory can
therefore exceed the coordinated bound.

Proposed fix F6 — make `disk-cache` a retention policy over the pooled buffers:

- Treat `disk-cache` as retained-after-ack `BufferLease`s, counted against
  `total_budget`, not a separate cache layer with its own allocator.
- Add "disk-cache retained bytes" to the Memory Signals list in `backpressure.md`.
- If a separate cache layer is genuinely wanted, give it an explicit
  `ResourceManager` budget and count it in the global total; document the
  relationship either way.

### R2-8 — No `Accept-Encoding: identity` rule for range/resume

PARTIAL (real). The baseline is correctly conditioned — `protocol-modernization.md:21`
says "gzip/deflate only where range semantics remain correct" — and range workers
require identity (`detailed-http-first-slice.md:160-161`). But no document sends
`Accept-Encoding: identity` on range/split/resume requests (grep across `design/`
finds zero `Accept-Encoding` references), and the resume request block
(`detailed-http-first-slice.md:108-110`) has no content-encoding rule at all. Byte
offsets in `Range`/`Content-Range` are in encoded space while a decoded body is in
decoded space, so a content-coded response to a range/resume request corrupts.
(First-round "encoded-vs-decoded corruption mechanism" is a correct inference, but
it was not stated in the docs — this fix states it.)

Proposed fix F7 — make the request-side rule explicit:

- Range, split, and resume requests MUST send `Accept-Encoding: identity`.
- If a server ignores it and returns a content-coded body to a range request,
  reject the response (do not write); fall back to sequential-from-zero or fail per
  retry policy.
- Sequential downloads from offset 0 may accept `gzip`/`deflate` (the "only where
  range semantics remain correct" case). State that compression and concurrent
  split/resume are effectively mutually exclusive.
- Document in the range-worker and resume blocks of `detailed-http-first-slice.md`
  and in the baseline of `protocol-modernization.md`.

### R2-9 / R2-15 — Redirect policy is referenced but undefined, including credential handling

CONFIRMED. `detailed-http-first-slice.md:188` and `retry-policy.md:204` both defer
to a "redirect policy," and `library-choice.md:144` references it, but no
redirect-policy document exists (only `README.md:410` mentions redirects, in an
SSRF context). Validator revalidation after redirect, loop/budget limits, and
whether a redirect target becomes a mirror are all unspecified. The completeness
pass adds the security dimension: whether `Authorization`, cookies, and proxy
credentials are stripped on a cross-origin redirect is undefined — a classic
secret-exfiltration CVE class — and `security-recovery.md` covers only log
redaction, not header propagation.

Proposed fix F8 — add `redirect-policy.md` (and cross-link from
`protocol-modernization.md` and `security-recovery.md`):

- Max redirect depth (default bounded, e.g. 20) and loop detection.
- On redirect during a range/resume request, re-establish validators against the
  new response. A validator (ETag) is origin-scoped and cannot be compared across
  a cross-origin redirect, so a cross-origin redirect does not justify continuing
  at the prior offset: continue only if a whole-file checksum will verify the
  result, otherwise restart from offset 0. (A same-origin redirect can compare
  the origin's own validator via `If-Range` as usual.)
- Cross-origin credential rules: drop `Authorization` when the redirect changes
  origin; scope cookies by host through the cookie jar (F14); re-evaluate proxy
  credentials for the new host. Refuse `https -> http` downgrade by default.
- A redirect target replaces the current mirror for that lease; it does not
  silently join the mirror pool without passing the F4 identity check.
- Feed redirect resolution through the SSRF guardrail (F12).

### R2-10 — Per-file torrent path validation is unspecified

PARTIAL (real). `libtorrent-integration.md:87` does say "downloader validates and
sanitizes output paths before passing them in," so the first-round "unguarded" was
too strong. The genuine gap is narrower: the design never states that
`SafePathBuilder` is applied to each per-file path a multi-file torrent encodes
internally, versus only the save-root/output path. `security-recovery.md:39` lists
"torrent file paths" as mandatory for `SafePathBuilder`, and BT disk ownership is
delegated to libtorrent with the `StorageEngine`-delegating backend explicitly
deferred to "a later phase" (`libtorrent-integration.md:97-101`). So in the
first-slice full build, per-file torrent paths flow through libtorrent's own path
construction without the mandated builder.

Proposed fix F9 — validate every torrent-encoded path at add time, even before the
delegating storage backend exists:

- At torrent-add, iterate the file list and run each relative path through
  `SafePathBuilder`. Reject the torrent if any path cannot be resolved safely
  (absolute components, `..` escape, symlink escape, Windows reserved names).
- For paths that are unsafe but resolvable to a safe sanitized name, use
  libtorrent's per-file rename API to pin the sanitized name inside the output root
  before the session starts writing.
- Document this as the interim guarantee in `libtorrent-integration.md`, with the
  delegating `StorageEngine` backend as the later, stronger phase.

### R2-11 — BT event-channel overflow policy is unspecified

CONFIRMED. The command channel defines overflow behavior
(`libtorrent-integration.md:46-48`: backpressure or typed overload error), but the
event channel (`libtorrent-integration.md:50-64`, carrying "piece complete",
"state changed", "resume data ready") is bounded (diagram line 24) yet never states
what happens when full. (First-round "desync durable resume state" was overstated —
libtorrent owns resume semantics — but a dropped event still causes status-snapshot
staleness and, for a dropped resume-data-persistence trigger, a missed durability
checkpoint.)

Proposed fix F10 — classify events and define overflow per class:

- Coalescible events (stats, progress, speed): drop-oldest / keep-latest under
  pressure is acceptable.
- Durability- and correctness-critical events (resume-data-ready, terminal
  state changes): must not be dropped. Carry them on a separate small reliable
  channel, or apply backpressure to the libtorrent alert pump so it stalls rather
  than dropping these.
- State the policy in `libtorrent-integration.md` Event Channel.

## Cross-Cutting Gaps (Goals With No Owning Mechanism)

These were surfaced by the completeness pass. They are not contradictions between
existing statements; they are commitments with no document that owns them. Each
needs either a new doc or a section, scheduled with its implementation phase.

### R2-12 — FTP/SFTP have no correctness contract (F11)

Every resume/validation document is HTTP-centric: `EntityValidator` is
`{ etag, last_modified, content_length, digest }`, and the `StaleValidator` model
(`retry-policy.md:44-53`) is HTTP-shaped. FTP and SFTP appear only as one-liners
(`README.md:171`, `split-download.md:238`, `implementation-plan.md:124-129`), yet
FTP ships in the `standard` profile. Undesigned: FTP `REST`-based resume offset,
PASV/EPSV/PORT data-channel setup (each FTP range needs its own data connection, so
"HTTP/FTP/SFTP workers lease chunks from the same scheduler" is not mechanically
true for FTP), `SIZE`/`MDTM` as the only resume validators (no ETag equivalent),
FTPS explicit/implicit TLS on control and data channels, `ftp-type` ASCII/binary
(ASCII mode corrupts byte offsets), and SFTP random-access reads vs libssh2/russh
windowing.

Proposed fix F11 — add `detailed-ftp-sftp.md` defining resume offset
semantics, data-channel/connection model per range, the `SIZE`/`MDTM` validator
mapping, ASCII-mode prohibition for ranged transfers, FTPS TLS handling, and the
SFTP read model. Add FTP/SFTP data-channel-drop fault-injection tests.

### R2-13 — SSRF guardrails are promised, not mechanized (F12)

`README.md:410` promises "Redirects, proxies, and DNS resolution obey SSRF
guardrails when RPC is remotely exposed," and `requirements-traceability.md` maps
Security to `security-recovery.md`, but that doc never defines the guardrail.
Remote RPC is a first-class use case (`apis-and-embedding.md`), so this is direct
exposure.

Proposed fix F12 — add an SSRF-guardrail section to `security-recovery.md`:
private/link-local/loopback denial, cloud metadata endpoint (169.254.169.254)
blocking, DNS-rebinding protection (resolve-and-pin the address used), and how the
guardrail composes with user-configured mirrors and with the redirect policy (F8).
Reconcile with the existing `rpc-allow-private-address-downloads`,
`network-allowlist`, and `network-denylist` options, including their defaults and
allow-vs-deny precedence, which are currently listed without values.

### R2-14 — Rate limiter has no design doc (F13)

`max-overall-download-limit`, `max-download-limit`, `max-overall-upload-limit`,
`max-upload-limit` are listed "implemented live" (`configuration.md:467`) and a
"token bucket" is mentioned in passing (`stats-and-stalls.md:101`,
`detailed-runtime.md:179`), but no document specifies the limiter: global vs
per-task vs per-host composition, bucket/burst sizing, fairness across ~1,000
streams sharing one global cap, and — historically bug-prone in aria2 —
reconciliation with libtorrent's independent internal rate limiter in the full
build (BT + HTTP double-counting). It also overlaps ambiguously with
read-backpressure, since both act by removing socket read interest, with no stated
precedence.

Proposed fix F13 — add `rate-limiting.md`: token-bucket hierarchy
(global -> per-host -> per-task), fairness policy, precedence relative to
read-backpressure, and how the global limit is enforced across both the HTTP lanes
and libtorrent (single accounting point, or libtorrent's own limiter driven from
the global budget). Add a rate-limiter accuracy test to the testing strategy.

### R2-16 — Cookie jar semantics are undesigned (F14)

`load-cookies`/`save-cookies` are listed "implemented" (`configuration.md:421-422`)
but no document defines the jar: Netscape/Mozilla file parsing, domain/path
matching, public-suffix enforcement, secure/HttpOnly/SameSite handling, expiry and
session-cookie rules, non-propagation to unrelated mirror hosts, and behavior across
the redirect flow (F8). Parsing an attacker-influenced cookie file is an
untrusted-input surface with no fuzz target listed.

Proposed fix F14 — add a cookie-jar section (in `protocol-modernization.md` or a
new doc) covering the above, with host-scoping tied to F4/F8 and a cookie-file fuzz
target added to the testing strategy.

### R2-17 — Observability metric cardinality is unbounded (F15)

`README.md:417-429` and `threading-model.md:234-246` require per-host active
connections, per-host recent error rate, and per-task retry causes, plus per-lease
diagnostics (`download-scheduling.md:197-206`). At C10k across many hosts/tasks
these are high-cardinality label sets that blow up a metrics backend's memory and
scrape path. No document caps cardinality or separates bounded gauges from
per-entity detail.

Proposed fix F15 — add a cardinality section to the observability model: cap label
cardinality, define top-N/aggregation for per-host and per-task series, and move
unbounded per-entity detail behind on-demand status queries rather than always-on
exported metrics.

### R2-18 — Cross-lane graceful-shutdown ordering is undefined (F16)

Per-task pause is well specified (`detailed-http-first-slice.md:266-281`,
`split-download.md:243-251`) and libtorrent has its own shutdown
(`libtorrent-integration.md:103-115`), but there is no global lane-quiescence
order. A SIGTERM that closes the disk/journal lane before the last durable-piece
record is fsynced silently loses recovery state — undercutting the poweroff-recovery
guarantee during a *normal* shutdown, not just power loss. `detailed-runtime.md`
closes queues and quarantines leaked buffers but does not order the flush against
close, and does not address deadlock when the control lane must stop a disk lane
blocked on a full bounded channel.

Proposed fix F16 — specify the shutdown sequence in `detailed-runtime.md`:
network stop-reads -> drain in-flight disk writes -> flush and fsync journal ->
persist session -> save BT resume data -> exit. Define the graceful-timeout
behavior for an in-flight fsync and the deadlock-avoidance rule for closing a lane
blocked on a full channel (drain-then-close, not close-then-drain).

### R2-19 — Disk-full handling contradicts itself (F17)

`disk-adapter.md:301` and `retry-policy.md:253` say ENOSPC is terminal and never
retried, while `backpressure.md:113` lists "pause a task" as a disk-full response,
and the `disk-adapter.md` `Faulted` state says "fail or pause according to policy"
without defining that policy. Whether a disk-full task can be paused with durable
state intact and resumed after the user frees space (the obvious desktop behavior)
is unspecified, as is the fate of in-flight buffers and a half-written piece at
ENOSPC, and ENOSPC at preallocation time vs mid-transfer.

Proposed fix F17 — reconcile into one policy: on ENOSPC, pause the task with
durable state intact (do not mark terminal), discard in-flight buffers for the
incomplete piece so recovery resets it to pending, and resume on user action after
space is freed. Distinguish preallocation-time ENOSPC (fail the allocation cleanly)
from mid-transfer ENOSPC (pause). Update `disk-adapter.md`, `retry-policy.md`, and
`backpressure.md` to the single policy.

### R2-20 — Proxy CONNECT/SOCKS/auth flows are one-liners (F18)

`detailed-http-first-slice.md:189` handles `407` in one line and
`protocol-modernization.md:238-247` lists proxy interaction abstractly, but the
actual flows are undesigned: HTTP `CONNECT` tunnel establishment and failure
classification for HTTPS, proxy-auth challenge/retry loop, SOCKS5 auth,
FTP-over-HTTP-proxy, and sourcing credentials from `http_proxy`/`https_proxy`/
`no_proxy` with correct precedence. CONNECT failure vs origin failure need distinct
retry classification that the current error classes do not carry.

Proposed fix F18 — add a proxy-flow section (in `protocol-modernization.md`)
covering CONNECT tunnel setup/failure classification, proxy-auth loop, SOCKS5, env
var precedence, and a distinct `ProxyConnect` error class in the retry taxonomy.

## Downgraded And Rejected Findings

Recorded so they are not acted on as written. Verification refuted or substantially
overstated these.

- REFUTED — "registered buffers break the memory bound" (`buffer-pool.md`). The
  registered arena is a fixed, pre-sized budget bounded like every other class
  (`buffer-pool.md:156-157, 198, 40`); the watermark prevents *new* allocation and
  reads, and the doc never claims it reclaims registered memory. The only true
  kernel is that registered memory is non-reclaimable under pressure, which the doc
  already states. No fix needed.
- REFUTED — "`If-Range` used with weak validators" (`detailed-http-first-slice.md`).
  The resume block populates `If-Range` only "when a strong validator is available"
  (line 108-110), exactly the RFC 7233 requirement. The weak-validator path is
  handled via length comparison plus `StaleValidator` classification. At most a
  wording nit (state explicitly that `If-Range` is omitted when only a weak
  validator exists). No correctness fix needed.
- REFUTED — "generation is never defined." `detailed-core.md` defines a
  `Generation` counter as the staleness-fencing type. The residual is only that its
  journal persistence and storage-boundary enforcement could be spelled out more,
  which is folded into F5.
- Downgraded to low — Windows reserved-name list (`security-recovery.md:27`): the
  list is explicitly illustrative ("such as"), and `detailed-storage.md:53` refers
  generically to the full check. Keep as an implementation reminder (apply the full
  `COM1-9`/`LPT1-9` set and the reserved-with-extension case), not a spec gap.
- Downgraded to low — zero-copy "bypasses `write_block`" (`zero-copy.md`): the
  "all of these go through `write_block`" statement is scoped to the allowed-paths
  list, and true kernel zero-copy is gated on constraints that preserve placement,
  cancellation, and journaling (lines 6-8, 80-86). Residual is an underspecification
  of how a bytes-never-in-user-space transfer reconciles with the `BufferLease`
  signature; note it, do not treat as a contradiction.
- Downgraded to low — fast-durability "self-contradiction" (`detailed-storage.md`):
  the doc states the "must not mark unverified corrupt data complete" rule and a
  mitigation ("may redownload recent pieces after crash", line 293 "unknown bytes
  are never counted complete by length alone"). It is an underspecification of how
  fast-mode recovery avoids trusting an un-fsynced no-checksum piece, best resolved
  by the WAL-ordering note added under F5, not a flat contradiction.
- Downgraded to low — option-metadata "violation" (`configuration.md` New Options):
  the typed registry, not the prose, is the source of truth
  (`detailed-config.md:8-10, 52-61`) with a CI inventory check
  (`detailed-config.md:104-114`). Bare-name listing is the doc's consistent style
  with metadata deferred to the generated registry. Real residual: promote the
  security-critical new options into the registry table with defaults/scope/
  security_class before shipping. Count was ~73, not ~90.
- Downgraded to low — hook-scope "injection" (`configuration.md`/`security-recovery.md`):
  `scope` and `runtime_update` are deliberately separate axes
  (`configuration.md:45-50`); hooks are off by default and rejected from URL rules
  and non-local RPC. Residual: state explicitly whether post-startup input files
  may carry per-download exec hooks. Not a flat contradiction.
- Downgraded to low — error-taxonomy mismatch (`OptionRequiresNewGeneration` vs
  `ErrorKind` vs `OptionPatchRejected`): real inconsistency across three surfaces,
  but error codes are generated and CI-enforced (`detailed-config.md:96-102`), so it
  is a design-consistency defect, not a runtime bug. Reconcile the vocabulary in one
  pass when generating `error_codes.json`.

## Coverage Caveats

- The cross-cluster verification pass produced no output (one agent returned
  empty), so there is no independent cross-cluster consistency layer beyond the
  per-cluster adversarial checks folded in above.
- Load-bearing findings were adversarially re-verified against the text; most
  low-severity items were not. The planning-cluster findings (phase-ordering
  criteria that depend on later-phase machinery, "wired-vs-parsed-only has no CI
  detection mechanism," first-slice-vs-Phase-4 mismatch) are plausible but were not
  independently re-verified against the text and are not carried as confirmed here.

## Recommended Sequencing

Before the first implementation slice, resolve the seam contradictions and
silent-corruption gaps — cheap now, expensive later:

1. F1 — reconcile the `DiskBackend` trait (`BufferLease`, union of operations).
2. F2 — split the control queue for priority.
3. F5 — one normative journal spec (record enum, format, endianness, torn-write
   CRC).
4. F4 — cross-mirror validator for concurrent split.
5. F7 — `Accept-Encoding: identity` on range/resume.
6. F3 — quarantine budget and reclamation.
7. F6 — fold `disk-cache` into the budget.

The missing-doc gaps can be scheduled with their phases, with two exceptions that
must land before any remote-RPC exposure is enabled:

- F12 — SSRF guardrail mechanism.
- F8 — redirect policy, including cross-origin credential stripping.
