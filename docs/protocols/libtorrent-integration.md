# Libtorrent Integration

[Documentation](../README.md)

Status: native adapter implemented; engine integration and Phase-6 acceptance
are in progress. The gates below require complete CI and measurement evidence.

Decision: libtorrent runs outside the main control and HTTP network event loops.

The main downloader owns scheduling, configuration, RPC, output-root policy, and
final user-visible state. Libtorrent owns BitTorrent peer protocol mechanics
inside an isolated adapter lane.

Transfer and BT workers consume only their own allocation and cancellation
requests from the shared runtime mailbox. Both publish through the existing
scheduler. Native work advances through nonblocking command and persistence
completions; query projection uses immutable, identity-bound BT snapshots.

Recovery validates the combined transfer/BT queue before normalizing active
tasks to waiting or paused. BT tasks retain their GID and metadata identity;
they receive fresh scheduler IDs and never acquire transfer journals. A build
without BT support rejects a store containing BT tasks before admission.

Periodic checkpoints pause through the native disk barrier, persist the tracked
result, mark the resumed generation dirty, then resume. Pause, removal and
shutdown retain ownership until that same boundary and native removal complete.
Checkpoint failure preserves the previous blob, records `DirtyCheckpoint`, and
cannot acknowledge a drain while a native callback still owns the task.
Unaccepted native commands return their metadata and resume ownership to the
caller for bounded retry. Shutdown also awaits option acknowledgements,
background preparation and pending scheduler events before stopping the adapter.

## Phase 6 Gates

The pure `ariax-bt-metadata` parser is shared by admission and session recovery.
It has no native dependencies. `ariax-bt` owns safe mapping, resource accounting
and the adapter lane; only `ariax-bt-libtorrent-sys` crosses the native ABI.

Implementation starts after the passing remote matrix and native Linux
measurement campaign required by [continuous-integration.md](../development/continuous-integration.md). The accepted
milestone has six gates:

| Gate | Required Behavior |
| --- | --- |
| `P6-01` Native integration | Pinned libtorrent 2.1.1 and `cxx`/`cxx-build` 1.0.202, a narrow unsafe bridge, a safe Rust adapter, and separate native builds for Linux, macOS, Windows MSVC and Windows GNU. `full` and `compat` enable BT; other bundles exclude its native dependencies. |
| `P6-02` Safe admission | Bounded v1/v2/hybrid torrent and magnet admission, metadata-only downloads and following, a native metadata hold before storage initialization, stable validated file mappings, protected roots and destination/credential policy. |
| `P6-03` Scheduling and resources | Existing scheduler and immutable queries own lifecycle; bounded commands, events, completions and blobs retain reservations through cancellation. Native threads, memory, sockets, files and bandwidth consume process shares. |
| `P6-04` Persistence | Fresh SQLite/JSON v3 stores distinguish transfers from BT without requiring transfer journals for BT. Tracked periodic, pause, remove and shutdown checkpoints survive alert loss; failure preserves the last safe resume data with `DirtyCheckpoint`. |
| `P6-05` Public interfaces | Shared Rust, CLI and RPC torrent/magnet admission, peer/file/status queries, seeding events and exact supported option mappings with acknowledged live updates. |
| `P6-06` Acceptance | Real native transfers, security and crash tests, bounded parser fuzzing, FFI lifetime/exception coverage, the complete CI matrix, and native measurements with 1,000 BT peers and mixed HTTP/BT controls. |

The native dependency graph uses Boost 1.91.0 headers and OpenSSL 3.6.3,
verified from upstream source archives. Native libraries and their build
settings are specific to the target ABI; an installed system libtorrent is
not a substitute for the pinned patched source. Disable WebTorrent, I2P,
mutable torrents and dependency logging explicitly.

The magnet gate must expose held metadata through tracked state even when its
notification is dropped. Approval supplies the complete validated mapping and
selection only after their session-store transaction succeeds. Pausing in
response to `metadata_received_alert` is insufficient: upstream initializes
storage immediately after posting that alert. Rejection or cancellation keeps
storage uninitialized.

Admission rechecks output collisions against the current catalogs before
publication. Late metadata is checked again after its binding is durable and
before native approval. A background check whose catalog changed is repeated;
progress-only snapshot updates do not invalidate a mapping check. The durable
binding is visible to later admission checks before payload creation is allowed.

The native magnet decoder also receives the configured depth, token, byte,
piece and file-count limits. It bounds the file-tree walk before constructing
native file storage; Rust validates canonical metadata and the exact resulting
native layout before approval. Native build provenance covers every installed
Boost header as well as libtorrent, OpenSSL and the patch. Target builds use
separate source/build caches and serialize mutations within one target.

Hybrid torrents retain their v1 file order and explicit padding after their
real files and offsets are checked against the v2 tree. Pure v2 torrents use
libtorrent's implicit alignment padding, including the last piece. A hybrid
single-file torrent therefore does not acquire a synthetic trailing file.

The checkpoint hook must complete after the native disk release barrier and
must deliver success or failure through an owned tracked result. The upstream
`save_resume_data()` alert and synchronous `get_resume_data()` alone do not
establish that barrier. Serialization checks its byte limit before growing the
output; an oversized result is a failed checkpoint.

No migration from older development stores, historical JSON reader, downgrade
path or obsolete API alias is added. Existing aria2-facing behavior, torrent
protocol versions, feature bundles and MSRV remain requirements.

## Threading Model

```text
ControlPlane / RequestScheduler / RPC
        |
        | bounded commands
        v
BtAdapter
        |
        | FFI-safe calls
        v
libtorrent session thread(s)
        |
        | bounded events/snapshots
        v
StatusSnapshotStore / Scheduler
```

The libtorrent session is not polled by the main Tokio reactor. It may use its
own Asio event loop and disk threads internally. The adapter is the only bridge.

## Command Channel

Commands into BT lane:

- add torrent,
- add magnet,
- pause,
- resume,
- remove,
- change supported BT options,
- request status snapshot,
- request resume-data save,
- shutdown.

The channel is bounded. If it is full, control operations get backpressure or a
typed overload error; memory must not grow unbounded.

Baseline full-build bridge caps are 256 command items / 8 MiB encoded payload,
1024 coalescible event items / 16 MiB, and 64 reliable terminal/checkpoint items
/ 4 MiB. Raw torrent/resume blobs are not copied through the coalescible event
lane; they use one tracked sized handoff charged to the BT/session budget.
Reservations may shrink to the verified retained allocation after a native
handoff. Shrinking returns only unused credit; cancellation cannot release
bytes still owned by native work, a callback or an unread completion.

## Runtime BT Option Updates

BT settings have their own runtime-update class; they do not reuse HTTP
`active_restart` semantics.  Tearing down and re-adding a libtorrent session to
apply one setting would discard peer/swarm state and is therefore prohibited.

| Class | Adapter action | Public result |
| --- | --- | --- |
| `bt_live` | Send a versioned `settings_pack` patch over the bounded command channel; wait for the session-thread acknowledgement before publishing the new snapshot. | applied without task generation change |
| `bt_restart_required` | Do not mutate an active session. Require the caller to stop/pause and explicitly restart the BT task under its documented resume-data policy. | `OptionPatchRejected/requires_explicit_bt_restart` for an active task |
| `startup_only` (BT) | Accept only while constructing a new session. | `OptionPatchRejected/not_runtime_mutable` otherwise |

The option registry records the libtorrent version/range and the exact setting
mapping for every `bt_live` entry.  Unknown or unavailable settings fail before
they enter the adapter.  The configuration compatibility matrix labels these as
design/BT behavior rather than pretending that aria2 has an identical live-update
rule.

A fully drained paused task accepts a validated `bt_restart_required` patch
without restarting itself. The owner fences admission, rechecks collisions,
and commits options and the replacement mapping/endpoints atomically. It keeps
the torrent identity and protected root, retains lifecycle counters, and retires
the old resume blob and checkpoint sequence. An explicit `unpause` starts the
next generation and rechecks payload pieces. New output paths must be absent;
existing files are reusable only at paths already owned by the task. An active
task still receives `requires_explicit_bt_restart`, and startup settings remain
immutable. The shared native session is never rebuilt for a task option patch.

Paused-only catalogs and their option updates do not initialize libtorrent.
Startup policy is applied before any recovered task can enter the adapter;
restored task settings cannot exceed its discovery or peer permissions.

The registry exports the following exact mappings for libtorrent 2.1.1. Global
task-option updates change defaults for future admissions; they do not modify
existing tasks. The overall upload cap is a session setting and waits for its
native acknowledgement. Session encryption and destination policy are startup
authority; RPC task/global changes cannot relax them.

| Option | Owner And Mapping |
| --- | --- |
| `max-download-limit`, `max-upload-limit` | Task `torrent_handle::set_download_limit` / `set_upload_limit` |
| `max-overall-upload-limit` | Session `settings_pack::upload_rate_limit` |
| `bt-max-peers` | Task `torrent_handle::set_max_connections`, bounded by the process share |
| `enable-dht`, `enable-peer-exchange` | Task `disable_dht` / `disable_pex` flags, within startup session permissions |
| `bt-encryption` | Startup `settings_pack::in_enc_policy` and `out_enc_policy`: required = forced, preferred = enabled, disabled = disabled |
| `bt-listen-address` | Startup `settings_pack::listen_interfaces` with one numeric socket address |
| `bt-allow-private-destinations` | Startup destination policy; false installs the special-use IP filter before admitting tasks |
| `select-file`, `index-out`, `out` | Validated stable mapping supplied to `ariax_approve_metadata` before storage initialization |
| `bt-tracker`, `bt-exclude-tracker` | Validated `add_torrent_params::trackers`, without changing the info dictionary |
| `bt-metadata-only`, `bt-save-metadata` | Engine metadata hold and atomic metainfo publication |
| `seed-ratio`, `seed-time` | Engine seeding completion policy using persisted byte/time counters |
| `bt-resume-data-limit`, `bt-resume-timeout` | Engine tracked checkpoint byte/deadline bounds |
| `follow-torrent` | Engine verified transfer-to-torrent admission |

DHT uses libtorrent's pinned public bootstrap defaults when enabled. Tests may
disable discovery and explicitly connect approved local peers. Trackers, web
seeds and peers resolved or discovered later remain subject to the session IP
filter and SSRF policy.

### Torrent Following

With `follow-torrent=true`, HTTP metadata identified as
`application/x-bittorrent` or a `.torrent` URI is fetched under the existing
destination, redirect, checksum, bandwidth and metadata-memory policies. The
document is limited to 16 MiB and passes the same admission checks as an explicit
torrent. `false` downloads the document normally; `mem` follows without retaining
the document. Following does not bypass validation of the torrent's own endpoints.

The shared metadata handoff records the parent's generation, option snapshot and
document hash, and atomically persists its `metadata-expansion` relationship with
the child. A canceled or changed parent cannot publish children. Restart completes
an already committed parent without re-fetching or duplicating its child. The
child uses the engine's BT defaults plus applicable explicit selection/rate
settings. Parent/child relationships appear in `followedBy` on every interface.

## Event Channel

Events out of BT lane:

- metadata received,
- piece complete,
- state changed,
- stats update,
- file completed,
- error,
- seeding started/stopped,
- resume data ready.

Events are normalized before reaching RPC/status code. Raw libtorrent objects do
not leak across the adapter boundary.

Entering `Seeding` publishes reliable `aria2.onBtDownloadComplete` and
`ariax.onSeeding` notifications. The latter contains `gid` and the boolean
`seeding`; leaving seeding publishes `seeding: false`. These transitions are
observed even though aria2 reports both downloading and seeding as `active`.

Overflow policy (the channel is bounded, so full-channel behavior must be
defined per event class):

- Coalescible events (`stats update`, `piece complete` progress, `state
  changed` to a non-terminal state) may be coalesced or drop-oldest under
  pressure; only the latest value matters. A dropped stats update causes at most
  a stale status snapshot.
- Terminal `error`, `file completed`, and terminal `state changed` are sent on
  a reliable adapter sub-channel.  If that sub-channel reaches its deadline,
  the adapter marks the task unhealthy and forces a checkpoint/reconciliation;
  it never silently discards the terminal transition.

Resume-data durability does **not** depend on a `resume data ready` alert being
delivered.  Libtorrent can drop alerts in its own bounded alert queue before the
adapter sees them.  The adapter therefore requests resume data on an explicit
cadence, before pause/remove, and at shutdown; each request has a tracked
completion that the shutdown barrier awaits.  `resume data ready` is merely a
wakeup/diagnostic event. Configure `alert_queue_size` to 4096 by default
(bounded by the declared libtorrent memory share), record any upstream
alert-loss indication, and immediately schedule a status/resume-data
reconciliation when it occurs.

## Callback Rules

Libtorrent callbacks must not:

- call RPC handlers,
- mutate scheduler maps directly,
- block on control-plane locks,
- execute shell hooks,
- log secrets or raw credentials,
- panic or unwind across FFI.

They may:

- enqueue normalized events,
- update adapter-local atomics,
- request async resume-data persistence through a command.

## Disk Interaction

Initial plan:

- libtorrent uses its own disk subsystem for torrent payloads,
- downloader validates and sanitizes output paths before passing them in,
- downloader imports resume data and status snapshots,
- downloader controls final stopped result and RPC-visible lifecycle.

This means BT payload writes do not use the HTTP `StorageEngine` initially.
That is acceptable only because libtorrent owns BT piece verification and
resume semantics in the BT lane.

Per-file path validation (interim guarantee, required in the first full build):

- Sanitizing the save-root alone is not sufficient. A multi-file torrent encodes
  a relative path for every file in its metadata, and those paths are attacker-
  controlled (`..`, absolute components, symlink escape, Windows reserved names).
- At torrent-add, iterate the full file list and run each encoded relative path
  through `SafePathBuilder::build(SafePathInput)` (the same builder mandated in
  [security-recovery.md](../architecture/security-recovery.md) for torrent file paths). Reject the torrent if any path
  cannot resolve safely under the output root.
- For paths that are unsafe but reducible to a safe sanitized name, use
  libtorrent's per-file rename API to pin the sanitized name inside the output
  root before the session starts writing.
- This runs even though BT disk I/O is delegated to libtorrent, so malicious
  torrent metadata cannot directly choose an escaping path. It is an interim
  metadata-sanitization guarantee, not the capability-rooted local-attacker
  guarantee of the project `DiskBackend`: the first full build requires a save
  root that is not writable by an untrusted local principal while libtorrent is
  active. Supporting attacker-writable roots requires the deferred custom
  libtorrent storage backend.

### Symlink And Attribute Entries

BitTorrent v1/v2 metadata can mark a file entry as a symlink (BEP 47 `attr`
containing `l`, with a `symlink path` target) or as executable/hidden/padding.

- Symlink entries are rejected by default: the torrent fails at add with a
  typed metadata error naming the offending entry. There is no sanitized
  mapping, because a symlink whose target is chosen by the torrent author is a
  filesystem-escape primitive regardless of where the link itself is placed.
- A future opt-in (`bt-allow-symlinks`, default `false`, `unsafe_compat`
  class) may map symlink entries whose *target*, after `SafePathBuilder`
  validation of every component, resolves inside the same torrent's output
  root and refers to a file entry of the same torrent. Absolute targets,
  `..` escapes, targets crossing the root, and dangling targets remain
  rejected even then. Until that option exists, rejection is unconditional.
- Padding entries (`attr` `p`) are internal and never mapped to user-visible
  paths. Executable/hidden attributes are applied only where the platform
  supports them and never widen permissions beyond the persistence policy.
- The same policy applies when metadata arrives late (magnet): the check runs
  before libtorrent may create any file, at metadata-received, with the task
  failing rather than the entry being skipped silently.

### Deterministic Collision Handling

Sanitization and platform case rules can map distinct metadata paths to one
filesystem path. Before the session starts writing (and again at magnet
metadata-received), the adapter builds the complete sanitized path set and
checks it for collisions:

- exact duplicates after sanitization,
- case-folding duplicates on case-insensitive filesystems (checked by policy on
  every platform so a torrent created on Linux fails deterministically on
  Windows/macOS rather than corrupting one of the two files),
- Windows reserved names (`CON`, `NUL`, `COM1`…), trailing dots/spaces, and
  reserved characters, normalized by `SafePathBuilder`,
- a file path colliding with a directory prefix of another entry.

Collisions are resolved deterministically in file-index order: the
first-indexed entry keeps the sanitized name and later colliding entries get an
index-suffixed sanitized name through the per-file rename API; if renaming
cannot produce a safe unique name, the torrent is rejected. The mapping is
recorded in the task metadata so RPC `getFiles`, selected-file indexes, and
resume across restarts remain stable. Resolution is a pure function of the
metadata file list and platform policy — never of filesystem probe order.

### Selected-File Roots And Late Metadata

- `select-file`/`index-out` operate on metadata file indexes after the
  sanitized mapping is fixed; deselecting a colliding entry does not change the
  names assigned to other entries.
- A single-file torrent uses `out`/`dir` naming rules; a multi-file torrent
  root name is itself a validated component and cannot be `..`, absolute, or a
  reserved name.
- On magnet metadata arrival, if a previously persisted sanitized mapping
  exists (resume), the newly computed mapping must match it exactly; any
  difference (changed metadata, moved files) fails the task rather than
  silently re-mapping onto existing files. Metadata replacement for an active
  task is rejected outright.

Future option:

- custom libtorrent storage backend delegates file placement to
  `StorageEngine`.

That should be a later phase after the FFI boundary and correctness tests are
stable.

## Shutdown

The BT shutdown barrier is shared by ordinary shutdown, BT pause/remove, and
the global runtime shutdown sequence.  It must complete before the global
journal/session checkpoint is finalized:

1. scheduler quiesces new BT commands and asks the adapter to stop admitting
   peer work;
2. adapter requests fresh resume data explicitly and awaits its tracked result
   while continuing to drain/normalize relevant alerts;
3. adapter stages the sized opaque resume-data blob and final BT snapshot for
   the task checkpoint, then acknowledges the scheduler;
4. only then may the global journal/session checkpoint durably persist that
   staged BT data and the libtorrent session be stopped;
5. the adapter publishes the final snapshot after its ownership boundary is
   closed.

There is one bounded timeout for this barrier.  On timeout or a failed
resume-data request, mark the task's session-store checkpoint explicitly dirty
(`DirtyCheckpoint`, carrying the last known safe resume data) rather than
claiming a clean pause/shutdown.  This is a session-store state, not a control
journal record type; the project-owned non-BT transfer journal stays separate
from libtorrent resume semantics. Recovery
then asks libtorrent to validate or resume conservatively.  It retains durable
payload/resume data and does not manufacture a clean completion.

Libtorrent resume data is opaque bencode, not an HTTP-style piece record. It does
not fit the [detailed-storage.md](../storage/detailed-storage.md) record enum and is stored as a sized BT
resume-data blob in the session store ([session-persistence.md](../storage/session-persistence.md)), keyed by gid,
rather than as journal piece records. BT durability is owned by libtorrent's
own resume semantics.

Forced shutdown:

- control plane stops waiting after the barrier timeout and marks the
  session-store checkpoint dirty (`DirtyCheckpoint`),
- next startup treats the BT task as unclean and asks libtorrent to validate or
  resume from the last saved data.

The resume-data request cadence defaults to 60 seconds while payload/seeding
state is changing. The staged blob defaults to a 16 MiB cap with a 64 MiB hard
maximum, and the pause/remove/shutdown barrier defaults to 30 seconds with a
300-second hard maximum. An over-cap blob or expired barrier produces
`DirtyCheckpoint`; it never allocates an unbounded bencode value or claims a
clean checkpoint.

## Tests

Required adapter tests:

- `bt_live` setting changes reach only the session thread and do not recreate a
  session or task generation,
- `bt_restart_required` changes fail explicitly while active,
- an overflowing libtorrent alert queue cannot lose the checkpoint because the
  explicit resume-data request is awaited,
- pause/remove/shutdown all use the same BT barrier and produce either a fresh
  resume blob or `DirtyCheckpoint`,
- every torrent file path is validated through `SafePathBuilder::build` before
  libtorrent receives it,
- the initial adapter rejects or documents an output root writable by an
  untrusted local principal; a custom-storage test is required before claiming
  capability-rooted BT path containment,
- a symlink entry rejects the torrent at add and at magnet metadata-received
  with a typed error before any file is created,
- sanitized-name, case-folding, reserved-Windows-name, and file-vs-directory
  collisions resolve deterministically in file-index order on every platform,
  independent of filesystem state, and persist across restart,
- selected-file roots keep stable names when other entries are deselected,
- a resumed magnet whose recomputed sanitized mapping differs from the
  persisted mapping fails instead of re-mapping onto existing files,
- metadata replacement for an active task is rejected.

## Why Isolate It

Isolation keeps the main downloader predictable:

- HTTP/FTP/RPC latency is not tied to swarm work,
- libtorrent Asio internals do not dictate the main runtime,
- FFI and unsafe code are contained,
- BT feature completeness comes from a mature library,
- scheduler semantics remain aria2-like and testable.
