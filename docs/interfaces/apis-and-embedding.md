# APIs, Integrations, And Embedding

[Documentation](../README.md)

Status: Phase 4B implements the shared dispatcher, query and control methods,
durable option/source mutation, configuration and session operations, filtered
events, loopback HTTP/WebSocket, both stdio framings, combined transports, direct
CLI controls and typed Rust embedding. Repair and completion evidence is tracked in
[implementation-readiness.md](../project/implementation-readiness.md#phase-4-repair-gates).
Phase 5 adds Metalink admission, FTP/FTPS/SFTP sources, exact host-key approval
and self-contained session import through that same control plane; its
validation is tracked by the [shared transfer gates](../protocols/detailed-protocol-transfers.md).
Phase 6 adds torrent/magnet admission, peer queries and JSON v3 task kinds. The
C ABI remains later work. Optional legacy
HTTP Basic authentication supplements method tokens at the HTTP request and
WebSocket upgrade boundaries.

Decision: expose aria2-compatible RPC for ecosystem compatibility, and expose a
typed native library API for embedding. Add a stable C ABI only after the core
engine API is stable enough to freeze.

These APIs serve different users and should not be forced into one shape.

## API Surfaces

```text
CLI
  aria2c-like command line

aria2-compatible RPC
  JSON-RPC 2.0 over HTTP
  JSON-RPC over WebSocket events
  JSON-RPC over stdio for embedding/supervision
  optional XML-RPC compatibility

Native Rust library API
  typed async API for embedding
  no JSON stringly-typed option maps unless explicitly requested

C ABI
  optional stable ABI for C/C++/Go/Python/Node bindings
  opaque handles and callback/event queue
```

## aria2-Compatible RPC

Yes, the downloader should provide aria2-compatible RPC as a first-class
compatibility surface.

### Executable Phase 4 Checkpoint

The experimental `ariax` binary exposes one dispatcher through:

- HTTP/1.1 `POST /jsonrpc` on an explicitly IP-loopback listener, and
- a dedicated IP-loopback WebSocket listener, and
- stdio frames with exactly one `Content-Length` header, or bounded NDJSON.
  Startup selection also supports shared HTTP+stdio and WebSocket+stdio services.

All transports cap requests at 2 MiB and responses at 16 MiB. Batches and
`system.multicall` are capped at 256 members, list pages at 1,000 tasks, stdio
headers at 16 KiB, concurrent listener tasks at 64, and graceful network drain
at five seconds. Missing, duplicate, invalid, oversized, or truncated stdio
framing is rejected. Network listeners refuse non-loopback bind addresses. On
EOF by default, Ctrl-C, or RPC shutdown, transport/progress handles drain, live HTTP
workers are cancelled and joined, and process bootstrap closes journals and the
session owner.

Implemented aria2-compatible operations cover `addUri` for enabled protocols,
`addMetalink` for bounded base64 v3/v4 documents; status, queue,
URI/file/server, option/global-option, version/session/global-stat queries;
pause/resume/remove/bulk controls; result removal/purge; position/source
changes; shutdown; and `system.listMethods`, `system.listNotifications`, and
bounded non-transactional `system.multicall`. Namespaced extensions provide
subscriptions/polling, source replacement, config check/reload/dump, and bounded
JSON session export/import. They operate on the real scheduler and persisted
session rather than placeholder state. GID lookup accepts unique hexadecimal
prefixes from one through sixteen digits.

`aria2.addMetalink([base64, options?, position?])` returns the selected child
GIDs in document order. CLI `--add-metalink` and Rust `AddMetalink` use the same
atomic admission, selection filters and portable output-name checks. A disabled
protocol or Metalink feature rejects explicitly. `minimal` enables Metalink;
`standard` also enables FTP/FTPS and SFTP.

`aria2.addTorrent([base64, webSeeds?, options?, position?])` and magnets through
`aria2.addUri` share admission with Rust `AddTorrent` and `AddMagnet`. The typed
`BitTorrentOptions` builders cover rate/peer limits, discovery, selection,
metadata and seeding; `from_pairs` is the explicit registry-backed CLI adapter.
`EngineBuilder::bittorrent` configures session-wide listening, encryption,
discovery and private-destination policy before any imported task starts.
`Engine::peers` and `TaskStatus::bittorrent` expose typed peer and lifecycle
snapshots. CLI `--add-torrent` and `--add-magnet` use those same owners; a magnet
passed to `--add-uri` follows the same path. All three surfaces reject BT in
builds without the feature. Runtime task changes acknowledge native settings
before the new option snapshot is published.

`ARIAX_RPC_SECRET` enables the aria2 `token:<secret>` first-parameter policy on
HTTP, WebSocket, and stdio. Every multicall member authenticates independently.
The dispatcher accepts JSON-RPC notifications without generating a response.
The bounded broker publishes scheduler-observed aria2 start/pause/stop/
complete/error notifications plus coalesced namespaced status updates.
WebSocket and Content-Length/NDJSON stdio clients receive pushed events; HTTP and
other request/response clients can use explicit `ariax.subscribe`/
`ariax.pollEvents`. `--rpc-user` and `--rpc-passwd` (or `ARIAX_RPC_USER` and
`ARIAX_RPC_PASSWD`) enable HTTP Basic on HTTP requests and WebSocket upgrades.
Basic authentication does not apply to stdio or replace method tokens.

The typed Rust `Engine`/`EngineBuilder` provides task and queue status, URI/file/
server queries, global statistics, version/session information, typed HTTP
options and configuration updates, task/source/queue controls, session import/
export, filtered subscriptions, diagnostics and orderly shutdown over the same
process control plane. `rpc_json` is the explicit complete JSON compatibility
entry point. The direct CLI commands use that engine. A stable external-runtime
API and the C ABI remain later gates.

`tellStatus` retains aria2's closed status vocabulary and adds bounded HTTP
extension fields. Its optional `retryDiagnostic` object carries decimal source,
piece, attempt, deadline, and lease identities plus stable trigger, delay,
disposition, and next-action codes. It never emits URI text. Live decisions are
exact; a restart exposes only the journaled error class/reason/caps/wait and
sets `recovered=true`. The failed lease remains unknown after recovery; the
replacement lease is attached later if the resumed scheduler assigns one.

Reasons:

- existing dashboards, browser extensions, scripts, mobile clients, and
  automation tools know aria2 RPC,
- compatibility helps adoption,
- it provides a stable remote-control boundary,
- aria2-style option names and response shapes are already expected by users.

Rules:

- implemented RPC methods must drive the real engine,
- no synthetic placeholder status,
- unsupported methods return explicit compatibility errors,
- response shapes match aria2 for implemented fields,
- extension fields are opt-in or namespaced,
- compatibility tests compare against aria2 behavior where practical.

### Compatibility Wire Rules

The aria2-compatible surface is deliberately closed:

- Task `status` is only `active`, `waiting`, `paused`, `error`, `complete`, or
  `removed`.  The normative mapping from internal scheduler states, including
  `PausedRestarting -> waiting` with no pause notification, is in
  [detailed-core.md](../architecture/detailed-core.md); clients must never receive an internal state name.
- A GID on the wire is exactly 16 lowercase hexadecimal digits including
  leading zeroes.  RPC lookup accepts a unique one-to-sixteen digit hexadecimal
  prefix, but every result/event returns the full form.  Invalid, ambiguous,
  and missing prefixes have deterministic typed RPC errors.
- Implemented aria2 options and methods are listed in generated
  `runtime_compatibility.json` and `aria2_compat.json`.  Extension-only options
  are explicitly marked unavailable in aria2 rather than assigned invented
  compatibility behavior.
- Restart-only changes such as `split`, `max-connection-per-server`,
  `min-split-size`, and `lowest-speed-limit` are accepted as an active restart:
  clients observe a transient `waiting` state, no pause event, and eventual
  resume with the pending values.
- `PausedHostKey` projects to aria2 `paused`, but normal `unpause`/resume returns
  `HostKeyApprovalRequired` and does not grant trust. The namespaced extension
  `ariax.approveHostKey(gid, challengeId, fingerprintSha256)` (and the native
  equivalent) must match the current published challenge before task-scoped
  pinning/requeue. This is an intentional security divergence so generic
  “unpause all” automation cannot approve a new server identity.
- Slow-slot `demote` (`WaitingSlow`) projects to aria2 `waiting` with no pause
  event and readmits automatically; only the explicit slow-slot `pause` policy
  (`PausedSlow`) projects to `paused`. A task with some leases in retry wait and
  any lease transferring stays `active`.

### RPC Authentication

`rpc-secret` uses aria2's method-level token convention.  For every direct
method call, the first positional parameter is the exact string
`token:<secret>`; the dispatcher removes and validates it before method argument
validation.  This applies equally to HTTP, WebSocket, and stdio when a secret is
configured.  Tokens are never logged, echoed in errors, or retained in event
payloads.

`system.multicall` is the envelope exception to direct method
authentication. The dispatcher recognizes the method before applying the
direct-method token check, requires exactly one outer parameter containing the
member array, and authenticates every member's `params` array independently.
Each member must begin with `token:<secret>` when a secret is configured. The
outer envelope does not carry or consume a credential; an outer token is not a
substitute for member tokens. This is the aria2 wire shape and prevents one
authenticated envelope from granting a different method authorization scope.

For example, this request authenticates its member without an outer token:

```json
{"jsonrpc":"2.0","id":1,"method":"system.multicall","params":[[{"methodName":"aria2.getVersion","params":["token:<secret>"]}]]}
```

A missing or incorrect member token produces only that member's unauthorized
result and does not dispatch it. Other members continue in order. An empty or
invalid outer envelope, an over-limit envelope, or a rejected nested member
cannot establish event authorization; a valid authenticated sibling may do so.
The Phase-4B repair implements this member-token shape on HTTP, WebSocket,
and stdio, including mixed authorized and rejected members (`P4-01`).

`rpc-user`/`rpc-passwd` provide legacy HTTP Basic authentication only.  It is a
deprecated transport gate, not a replacement for `rpc-secret`.  If both are
configured, a network request must pass Basic authentication and each method
must carry a valid token; Basic authentication never overrides token policy.
New deployments should use a secret over TLS or a local stdio transport.

Configured method secrets must contain 1 through 4,096 UTF-8 bytes; an empty
configured secret is an error. Startup requires both Basic fields when either
is configured. Requiring the pair and a nonempty user is an intentional
fail-closed divergence from aria2's empty-field fallback. User names are
nonempty, at most 1,024 UTF-8 bytes, and cannot contain a colon or control
characters; passwords are at most 4,096 UTF-8 bytes and cannot contain control
characters. An explicitly empty password is permitted. CLI values override
their corresponding environment values. Invalid credentials are rejected
before directories or session files are created, with no credential text in
diagnostics. Registry metadata marks both fields sensitive and startup-only;
remote option updates and persistence omit them.

HTTP checks exactly one `Authorization` header before reading the JSON body.
WebSocket checks the same gate before creating an event context. Scheme names
are case-insensitive; the encoded credentials must match the canonical Basic
value. Missing, duplicate, malformed, and incorrect headers receive the same
bounded `401` and `WWW-Authenticate` challenge. An HTTP authentication failure
closes that connection so unread request bytes cannot become another request.
Successful Basic authentication alone never authenticates method-token events.
Cloned dispatchers share authentication-failure throttling across listeners.

Authentication failures use a generic unauthorized response and are rate
limited; they never reveal whether a user name, password, token, or GID was
valid.

### Query And Response Work Bounds

Request size alone does not bound response amplification or scheduler work.
The target registry defaults below match the checkpoint's fixed transport caps;
configurable registry entries for these limits remain pending:

```text
rpc-max-request-size=2MiB
rpc-max-response-size=16MiB
rpc-max-batch-calls=256
rpc-max-list-items=1000
```

`system.multicall` rejects more than the configured call count before dispatch.
`tellWaiting`/`tellStopped` and extension list methods clamp/reject a requested
page above `rpc-max-list-items`; clients paginate. `tellActive` is bounded by
task admission, but the same response-byte cap still applies. The scheduler
publishes immutable active/waiting/stopped membership indexes on membership
changes. A query clones one index root in O(1), then resolves task snapshots and
formats outside scheduler/storage actors through the RPC/CPU budget; it never
walks all tasks while holding an actor turn or scheduler lock. Membership is
consistent to the index version and each task snapshot exposes its own version,
rather than pretending a 10,000-task query is one atomic engine instant.

The serializer writes into a byte-counting bounded chunk sink instead of first
building a second unbounded JSON value tree. Exceeding the cap returns a typed
`ResponseTooLarge` error and releases all snapshot references; it never sends a
truncated JSON document.

Pending responses require both client credit and process-wide `rpc_budget`
credit before serialization, plus the resident-byte permit defined in
[detailed-runtime.md](../runtime/detailed-runtime.md). One client is one HTTP/WebSocket connection or one stdio
transport; all listeners share the process budget. One client cannot reserve
the whole process budget. Baseline permits one actively serializing/full-size
response per client, including a response blocked in the transport writer.
The permit follows every retained serialized chunk until transport release or
disconnect; constructing the HTTP response does not release its credit.

Each client has at most four outstanding requests and 8 MiB of combined request
and command state. This includes the executing request, queued requests, and
the reader's current body, including one waiting to enter a full channel.
Reserved bytes cover raw input and parsed state while both are retained.
HTTP pipelining, WebSocket messages, and stdio frames stop before reading or
allocating another body when credit is exhausted; a bounded busy response may
be used instead. Serialization cannot begin behind a blocked full-size reply.
Disconnect, cancellation, parse failure, and serialization failure release all
owned credit. A channel with four slots alone does not establish this bound.
An accepted deferred mutation retains its command lease until completion even
when the originating transport disconnects. Its reply bytes are released with
the transport; its still-owned command state is not prematurely refunded.

The bounded parser uses Serde's structured visitor interface to reserve owned
nodes and strings before allocation. Raw input, parser scratch, queued command
state, result workspace, and serialized bytes have separate lifetimes within
the shared accounting domain. Budget exhaustion produces a bounded busy error
or read backpressure; it does not execute a partially parsed request. Response
construction transfers permit ownership to the HTTP/WebSocket byte owner and
stdio writer, so a completed backend call cannot refund a blocked response.

Typed Rust calls share one client budget per engine handle. Their caller-owned
arguments enter request accounting before the facade constructs JSON command
state, and intermediate results retain their workspace until typed projection
finishes. Returned typed values belong to the embedding caller. Event polling
uses the same client budget and rejects an event larger than its projection
allowance before cloning it. The synchronous low-level control primitive has a
fallback direct-call client for typed input and scheduler work. Transports and
embedding adapters supply their own client for input parsing, projection, and
response ownership. Background owner progress has separate request slots.

The current implementation has shared profile/resident reservations, bounded
Serde request construction, four client request leases, one response lease,
and transport-owned serialized bytes. Event entries and connection caches are
also charged; deferred source mutations retain their command leases after
disconnect. Batch results serialize one member at a time, and retained
multicall members acquire additional credit.

Borrowed result preflight bounds URI, file, option, and session construction;
status/event lists bound each row and the accumulated result. Typed input
preparation reserves temporary copies, and pending option/source changes retain
their request leases. Native calls share those client and projection budgets.
Scheduler simulation and status-draft copies of existing tasks are forecast
before mutation and retained with the input lease while a driver chain remains
pending. Idle cleanup discards unused plans before refunding their input.
After admission, add, option, and source changes retain a bounded owner-held
continuation independently of the transport's reply wait. It keeps the catalog
replacement and request credit until the scheduler publishes the accepted
change. Disconnecting a caller cannot skip that publication or cancel the
durable mutation. One scheduler chain executes at a time; pending source and
option intents retain their existing client and profile reservations. Normal
shutdown drains accepted work within the supervisor's shutdown allowance before
taking ownership of the engine for teardown. Failure to drain leaves a dirty
shutdown that requires recovery.

Production RPC, CLI and typed Rust calls share one managed progress runtime.
Its urgent and bulk mailboxes apply profile capacities, bounded-burst fairness,
and coalescing that preserves intervening per-task actions. Bulk operations
capture task identities once and advance at most one target per owner turn;
later accepted per-task pause, resume or remove supersedes unfinished earlier
bulk work. The owner shares a 32-step, approximately 1 ms cooperative turn
budget across completion and mutation helpers. Persistence preludes use owned
nonblocking submissions and completion polling. Shutdown closes admission under
the mailbox lock and signals out of band. Accepted envelopes and pending
replies retain the owner even after the last caller/backend handle disappears.

Queries capture an immutable root through a pointer-only publication lock and
project outside the mutable owner. Applied identities bind status and membership
to the matching source, option and configuration snapshots; GID prefix lookup
uses the ordered index. Two shared query/configuration slots bound execution,
and query jobs retain snapshot and result-workspace credit through cancellation.
Live authenticated URI queries retain live URI text; exports use sanitized
persistence metadata. The compatibility `call_shared` adapter briefly acquires
the owner to attach the shared backend. Reusing a backend or typed engine handle
avoids that attachment lock; projection itself never holds it.

Admission/import preparation and configuration validation also execute outside
the owner. Atomic import revalidates current queue positions and fences mutation
publication while immutable queries remain available. Native Linux benchmark
acceptance stays deferred until CI is ready. See
[measurement evidence](../runtime/performance-profiles.md#native-windows-control-plane-evidence).

`system.multicall` is bounded but not transactional. Inner calls execute in
order and may have side effects before a later inner call fails or the combined
response exceeds the byte cap. The returned error includes the number of
completed inner calls without echoing sensitive parameters; clients requiring
atomic option mutation use the single atomic `changeOption` patch operation.

### Event Delivery And Slow Consumers

WebSocket and stdio notifications use the per-client bounded queues defined in
[messaging-model.md](../runtime/messaging-model.md).  A blocked client can never block the dispatcher,
scheduler, storage, or journal appender.

When a secret is configured, a WebSocket or event-enabled stdio connection is
unauthenticated at transport setup. It receives no pushed notification and owns
no broker subscription until the dispatcher validates a method token on that
connection. Install the subscriber after authentication and before dispatching
the authorized method so its events are observable. A later method error does
not undo valid authentication. In a batch or multicall only an authenticated
member can establish that authorization; parsing the envelope cannot.

Event authorization is local to the connection, never shared through the
process backend. Every subsequent method still requires its own token. Closing
the transport drops the subscriber; reconnecting must authenticate again. With
no secret configured, the connection is authorized immediately. Explicit
`ariax.subscribe` and `ariax.pollEvents` use normal method authentication.
The Phase-4B repair implements this lifecycle on WebSocket and Content-Length
stdio, including first-call events, method errors, and reconnect (`P4-02`).

- Replies, terminal errors, durability failures, and shutdown notices are
  lossless within the client deadline; failure to enqueue them disconnects the
  client with the stable `SlowConsumer` error.
- Status/stat/progress notifications coalesce to the latest value per task and
  event kind.  Other nonterminal informational events may be dropped with a
  visible loss counter.
- A reconnecting client obtains a fresh snapshot with normal query methods and
  then resubscribes; event streams are not a durable replay log.

### Control Mutation Recovery

`changeOption` follows each option's declared `runtime_update` and the accepted
patch-version rules in [detailed-config.md](detailed-config.md). A live-only patch must remain live.
For a restart-class patch, `OK` means the complete patch and restart intent are
durably accepted, not that cancellation or readmission has finished. The patch
identity and staged snapshot remain owned until the actual admission appends
and flushes its matching `GenerationStarted(reason=option_patch)`. Only then
does the current-generation SQLite mirror advance and the pending state clear.
Recovery reuses an existing staged snapshot and appends only the missing
promotion, as specified in [detailed-storage.md](../storage/detailed-storage.md); it must never append an
untagged replacement over an accepted patch. Phase 4B implements these `P4-04`
replay and live-rate repairs. `P4-07` also covers supported runtime options,
including real output-path and piece-geometry restarts and atomic rejection.

`changeUri` and `ariax.replaceSources` validate the complete proposed source set
and scheduler conflicts before mutation. An active replacement owns a bounded
internal quiescence operation, preserves the user's desired pause state, and
waits for cancellation drain before committing source rows. It cannot issue
`Resume` while that drain is pending. On commit, SQLite and the catalog use the
new set and the task is requeued automatically if the user still wants it to
run. Actual network admission remains subject to ordinary scheduler limits.

The asynchronous control entry point owns the pending reply while the control
owner continues polling completions and accepting independent commands. The
synchronous primitive rejects a replacement needing active quiescence before
mutation; CLI, transport, and typed Rust adapters use the asynchronous entry
point for that operation. Disconnecting a caller releases reply ownership but
does not cancel a mutation whose quiescence has already begun.

Keep the existing success shapes: `changeUri` returns the deletion/addition
counts and `ariax.replaceSources` returns the GID. A validation/conflict error
means no replacement was committed. A confirmed commit must not become an
ordinary rejection because readmission is delayed; later worker failures are
reported through task status. An uncertain persistence outcome fails closed
for recovery and is not described as a rollback. Paused/waiting tasks preserve
their desired state, and a racing explicit pause or remove takes precedence
over automatic readmission. The `P4-05` implementation and regression tests now
cover this contract, including retry-wait timer cancellation and a caller that
disconnects after quiescence starts.

### Compatibility Target

- JSON-RPC is required.
- WebSocket event publishing is required for modern integrations.
- JSON-RPC over stdio is supported for embedding and parent-process
  supervision.
- XML-RPC is optional/feature-gated because it adds parser/security surface and
  fewer new clients need it.

## RPC Over Stdio

The executable combined form is
`ariax --rpc-transport=http+stdio --rpc SESSION_DB CONTROL_DIR OUTPUT_ROOT LOOPBACK_ADDR`.
The `--rpc-http`, `--rpc-ws`, and `--rpc-stdio` forms remain aliases for one
transport. `--rpc-transport` also accepts `http`, `websocket`, `stdio`, and
`websocket+stdio`. All startup switches precede the command. Both stdio
framings use the same bounded reader, single writer, authentication context,
and optional event stream. The request limit may be lowered, never raised
above the process cap. CRLF is accepted only as a line ending in NDJSON;
embedded raw carriage returns remain invalid JSON rather than being erased.

The default EOF policy is `shutdown`. `close-transport` and `ignore` stop
reading the closed input without polling it again and leave other listeners
and the engine running until an RPC shutdown or Ctrl-C. A standalone engine
with no remaining listener can still be stopped by Ctrl-C. Transport failures
and progress-loop failures request an orderly process drain.

Decision: support a stdio RPC mode for embedding.

Use cases:

- desktop applications that spawn the downloader as a child process,
- language bindings that want process isolation without opening a TCP port,
- sandboxed environments where loopback listeners are inconvenient,
- tests that need deterministic startup and shutdown.

Transport:

```text
--rpc-transport=http|stdio|http+stdio
--rpc-stdio-framing=content-length|ndjson
```

Default:

```text
--rpc-transport=http
--rpc-stdio-framing=content-length
```

`content-length` framing is preferred because JSON-RPC messages may contain
newlines and large structured payloads:

```text
Content-Length: 123\r\n
\r\n
{...json-rpc message...}
```

`ndjson` may be provided for simple tools, but it must reject embedded raw
newlines unless escaped by JSON and must enforce request-size caps.

Rules:

- stdio uses the same RPC dispatcher as HTTP/WebSocket RPC,
- stdio commands drive the real `ControlPlane`,
- no separate coordinator or synthetic status exists,
- stdout is reserved for framed RPC responses/events,
- logs go to stderr or configured log files,
- binary torrent/metalink payloads are base64 or passed through documented
  file/URI APIs, not raw mixed bytes in the stream,
- request-size, auth policy, and unsafe-hook restrictions still apply,
- when `rpc-secret` is configured, stdio uses the same `token:<secret>` method
  parameter convention as network RPC; local process ownership does not bypass
  an explicitly configured secret.

Security:

- stdio transport is local to the parent process and does not listen on the
  network,
- it should not require `rpc-secret` by default,
- if `http+stdio` is enabled, HTTP RPC still follows normal bind/secret/CORS
  rules,
- inherited environment variables and current directory must not bypass output
  root or network policy.

Lifecycle:

- EOF on stdin requests graceful shutdown only when
  `--rpc-stdio-eof=shutdown`; otherwise it closes that transport and the engine
  continues if other transports are active,
- child process exit waits for configured durability/shutdown policy,
- parent can send a normal shutdown RPC for deterministic completion.

Config:

```text
--rpc-stdio-eof=shutdown|close-transport|ignore
--rpc-stdio-events=true|false
--rpc-stdio-max-request-size=SIZE
```

`rpc-stdio-events=true` streams WebSocket-equivalent JSON-RPC notifications over
stdio. Clients that only want request/response can disable events.

## RPC Compatibility Mode

Modes:

```text
--rpc-compat=aria2|extended|strict
```

`aria2`:

- default for network RPC,
- matches aria2 method names and response shapes for implemented behavior,
- extension metadata hidden unless requested.

`extended`:

- includes new diagnostics such as backend info, stall reason, profile,
  buffer-pool stats, and detailed retry state.

`strict`:

- rejects options/method fields not in the implemented compatibility matrix,
- useful for CI and third-party client testing.

The dispatcher applies compatibility projection to direct calls, batches, and
each multicall member. `aria2` hides additional status fields unless the caller
names them in its key selection, and hides extension-only option keys from
aria2 option queries; `ariax` methods remain explicit opt-ins. `extended`
includes all implemented fields. `strict` additionally rejects unknown request,
multicall-member, method, and status-selection fields before backend dispatch.
Disabled protocol methods return `ProtocolFeatureUnavailable` with a stable
feature name instead of fabricating a result.

`ariax.getDiagnostics` reports the actual profile, selected event/disk backend,
RPC/resident credits, task/worker counts, configuration generation, and event
subscriber count. It contains no paths, URI text, credentials, or command
arguments. `ariax.subscribe` accepts a third closed filter object with `methods`
and `gids` arrays (at most 64 methods and 256 full GIDs). An omitted or empty
array matches everything; both nonempty arrays must match. Global events
without a GID bypass only the GID filter. Filtering happens before enqueue and
budget charging. `ariax.setEventFilter` applies the same filter to the current
authenticated WebSocket/stdio push stream; HTTP callers use explicit
subscriptions. Changing a filter removes queued events it no longer matches.
Native subscriptions use the same typed filter and queue limits.

## Native Rust Library API

The Rust API is not just a wrapper around RPC.

It should expose typed operations:

```rust
let engine = Engine::builder()
    .profile(Profile::Auto)
    .output_root(root)
    .build()
    .await?;

let task = engine.add_uri(AddUri {
    uris,
    options: DownloadOptions { split: Some(8), ..Default::default() },
}).await?;

let mut events = engine.subscribe();
```

Benefits:

- type-safe options,
- structured errors,
- no JSON serialization overhead,
- direct event subscription,
- embedder-controlled runtime integration where possible,
- easier tests.

The native API still uses the same control plane and scheduler as RPC and CLI.
There must not be separate engines with different behavior.

`Engine::subscribe()` returns a bounded subscription.  Snapshot/progress events
may coalesce; a subscriber that cannot receive required terminal events is
closed with `SlowConsumer` and can recover by querying a snapshot and creating a
new subscription.  Library callbacks must not run on scheduler or storage
actor threads.

The typed facade accepts the complete implemented transfer option family on add
and change, typed retry/checksum/mirror policies, `DownloadOptions::from_pairs`,
Metalink byte input and exact host-key approval, future-task global defaults,
versioned configuration checks/reloads/dumps, source replacement, queue
position, bulk controls, diagnostics, and filtered subscriptions. Explicit
`Engine::rpc_json` is available for compatibility clients that choose JSON;
it uses the same dispatcher and the engine client's existing budget, and its
returned bytes retain response credit until released. The CLI `--rpc-call`
form accepts one complete JSON-RPC document after the session paths and prints
its bounded reply before orderly shutdown. It provides the full advertised
method catalog through the shared dispatcher.

## Runtime Ownership For Embedding

Embedders need two modes:

Owned runtime:

- downloader creates and owns worker pools,
- simplest for C API and CLI,
- default.

External runtime:

- Rust embedders can provide a Tokio handle or runtime integration,
- downloader still owns disk/cpu/bt lanes unless explicitly configured,
- blocking work remains off caller event-loop threads.

The API must document shutdown, cancellation, and thread ownership clearly.

## C ABI

Yes, a C API is useful, but it should be introduced after the Rust API reaches
stability.

Design:

- opaque handles: `ariax_engine_t`, `ariax_task_t`, `ariax_event_t`,
- no Rust types across ABI,
- no panics across ABI,
- explicit create/destroy,
- async operations return request ids or futures represented by handles,
- callbacks are optional; polling event queues must be supported,
- callback dispatch and polling queues are bounded and follow the same
  slow-consumer rule as RPC; a callback is never invoked while holding an
  engine lock,
- all strings are UTF-8 with explicit length,
- binary metadata is pointer+length and copied or lifetime-scoped explicitly,
- errors have stable numeric codes plus message strings,
- versioned ABI struct for forward compatibility.

Example shape:

```c
ariax_engine_t* ariax_engine_new(const ariax_engine_config_t* cfg,
                                 ariax_error_t* err);
ariax_request_id_t ariax_add_uri(ariax_engine_t* engine,
                                 const ariax_add_uri_t* req,
                                 ariax_error_t* err);
int ariax_poll_event(ariax_engine_t* engine,
                     ariax_event_t* out,
                     uint32_t timeout_ms,
                     ariax_error_t* err);
void ariax_engine_free(ariax_engine_t* engine);
```

C ABI feature:

```text
--features c-api
```

The C ABI should be fuzzed and sanitizer-tested because it is an unsafe
boundary.

## Language Bindings

Preferred binding strategy:

- Python/Node/Go can use aria2-compatible RPC immediately.
- High-performance local bindings can use the C ABI later.
- Rust users use the native crate API.

This avoids prematurely freezing multiple language-specific APIs.

## Security

RPC:

- loopback by default,
- secret required for non-loopback,
- CORS disabled by default,
- request size caps,
- unsafe hooks rejected unless explicitly enabled.

Library:

- embedder can disable RPC entirely,
- output root and network policy are still enforced,
- no shell hooks unless explicitly enabled.

C ABI:

- validate every pointer and length,
- never retain borrowed pointers beyond documented lifetime,
- catch panics at ABI boundary,
- provide deterministic cleanup.

## Versioning

API versioning:

- RPC compatibility matrix version,
- native Rust crate semver,
- C ABI version integer,
- feature list in `getVersion`/diagnostics.

Breaking changes:

- never in aria2-compatible fields without a major compatibility mode change,
- native Rust API follows semver,
- C ABI uses versioned structs and symbol/version checks.
