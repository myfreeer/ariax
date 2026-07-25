# Libtorrent Integration

Status: draft.

Decision: libtorrent runs outside the main control and HTTP network event loops.

The main downloader owns scheduling, configuration, RPC, output-root policy, and
final user-visible state. Libtorrent owns BitTorrent peer protocol mechanics
inside an isolated adapter lane.

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
wakeup/diagnostic event.  Configure `alert_queue_size` generously, record any
upstream alert-loss indication, and immediately schedule a status/resume-data
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
  `security-recovery.md` for torrent file paths). Reject the torrent if any path
  cannot resolve safely under the output root.
- For paths that are unsafe but reducible to a safe sanitized name, use
  libtorrent's per-file rename API to pin the sanitized name inside the output
  root before the session starts writing.
- This runs even though BT disk I/O is delegated to libtorrent, so the
  path-safety guarantee holds before the delegating storage backend exists.

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
resume-data request, persist an explicit `DirtyCheckpoint` record with the last
known safe resume data rather than claiming a clean pause/shutdown.  Recovery
then asks libtorrent to validate or resume conservatively.  It retains durable
payload/resume data and does not manufacture a clean completion.

Libtorrent resume data is opaque bencode, not an HTTP-style piece record. It does
not fit the `detailed-storage.md` record enum and is stored as a sized BT
resume-data blob in the session store (`session-persistence.md`), keyed by gid,
rather than as journal piece records. The journal record types remain
HTTP-oriented; BT durability is owned by libtorrent's own resume semantics.

Forced shutdown:

- control plane stops waiting after the barrier timeout and records
  `DirtyCheckpoint`,
- next startup treats the BT task as unclean and asks libtorrent to validate or
  resume from the last saved data.

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
  libtorrent receives it.

## Why Isolate It

Isolation keeps the main downloader predictable:

- HTTP/FTP/RPC latency is not tied to swarm work,
- libtorrent Asio internals do not dictate the main runtime,
- FFI and unsafe code are contained,
- BT feature completeness comes from a mature library,
- scheduler semantics remain aria2-like and testable.
