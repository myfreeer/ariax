# APIs, Integrations, And Embedding

Status: reviewed contract with a bounded Phase-3B RPC subset executable. The
full aria2-compatible control plane, authentication, events, native embedding
API, and C ABI remain pending.

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

### Executable Phase 3B Subset

The experimental `ariax` binary currently exposes one dispatcher through:

- HTTP/1.1 `POST /jsonrpc` on an explicitly IP-loopback listener, and
- stdio frames with exactly one `Content-Length` header.

Both transports cap requests at 2 MiB and responses at 8 MiB; stdio headers are
capped at 16 KiB, HTTP connection tasks at 64, and graceful HTTP drain at five
seconds. Missing, duplicate, invalid, oversized, or truncated stdio framing is
rejected. The listener refuses non-loopback bind addresses. On EOF or Ctrl-C,
the transport and progress handles are drained, live HTTP workers are cancelled
and joined, and the process bootstrap closes its journals/session owner.

Only `aria2.addUri`, `aria2.tellStatus`, `aria2.pause`, `aria2.remove`, and
`aria2.getGlobalStat` (plus their unprefixed aliases) are implemented. They
operate on the real scheduler and persisted session, not placeholder state.
`addUri` supports a URI array plus the reviewed HTTP option subset documented in
`detailed-http-first-slice.md`; task/source/options admission is atomic and
recovery restores the source-aware catalog. The three GID methods currently
require one full hexadecimal GID rather than the broader prefix lookup contract
below. Notifications, batches, WebSocket/NDJSON, method tokens/HTTP Basic RPC
authentication, list methods, runtime mutation, and non-loopback serving are
explicitly deferred to Phase 4.

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
  `detailed-core.md`; clients must never receive an internal state name.
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

`system.multicall` authenticates each inner method independently: every inner
`params` array must begin with `token:<secret>`.  An outer credential does not
authorize uncredentialed inner calls.  This prevents one authenticated envelope
from accidentally granting a different method authorization scope.

`rpc-user`/`rpc-passwd` provide legacy HTTP Basic authentication only.  It is a
deprecated transport gate, not a replacement for `rpc-secret`.  If both are
configured, a network request must pass Basic authentication and each method
must carry a valid token; Basic authentication never overrides token policy.
New deployments should use a secret over TLS or a local stdio transport.

Authentication failures use a generic unauthorized response and are rate
limited; they never reveal whether a user name, password, token, or GID was
valid.

### Query And Response Work Bounds

Request size alone does not bound response amplification or scheduler work.
Baseline registry defaults are:

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

Per-transport pending response bytes count against `rpc_budget`, and socket/
stdio backpressure stops further response work. One client cannot reserve the
whole process budget; per-client and global shares are enforced before
serialization. Baseline permits one actively serializing/full-size response per
client; later pipelined requests retain only their bounded parsed command state
until the prior response releases bytes. Each client has at most four accepted
requests / 8 MiB of request-plus-command state; further HTTP pipelining or stdio
frames receive backpressure/a typed busy error before parsing another body.

`system.multicall` is bounded but not transactional. Inner calls execute in
order and may have side effects before a later inner call fails or the combined
response exceeds the byte cap. The returned error includes the number of
completed inner calls without echoing sensitive parameters; clients requiring
atomic option mutation use the single atomic `changeOption` patch operation.

### Event Delivery And Slow Consumers

WebSocket and stdio notifications use the per-client bounded queues defined in
`messaging-model.md`.  A blocked client can never block the dispatcher,
scheduler, storage, or journal appender.

- Replies, terminal errors, durability failures, and shutdown notices are
  lossless within the client deadline; failure to enqueue them disconnects the
  client with the stable `SlowConsumer` error.
- Status/stat/progress notifications coalesce to the latest value per task and
  event kind.  Other nonterminal informational events may be dropped with a
  visible loss counter.
- A reconnecting client obtains a fresh snapshot with normal query methods and
  then resubscribes; event streams are not a durable replay log.

Compatibility target:

- JSON-RPC is required.
- WebSocket event publishing is required for modern integrations.
- JSON-RPC over stdio is supported for embedding and parent-process
  supervision.
- XML-RPC is optional/feature-gated because it adds parser/security surface and
  fewer new clients need it.

## RPC Over Stdio

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
