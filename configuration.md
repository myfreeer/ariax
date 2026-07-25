# Configuration And aria2 Compatibility

Status: draft.

Configurability is a product requirement. The implementation must be explicit
about which aria2 options are implemented, unsupported, unsafe-compat only, or
build-feature dependent.

## Principles

- One typed option registry is the source of truth for CLI, config file, input
  file, RPC, docs, and tests.
- Every option has an owner subsystem. Options without behavioral ownership are
  not shipped as implemented.
- Values are parsed once into typed values, then rendered back to aria2-style
  strings for RPC compatibility.
- Runtime option changes declare whether they apply live, apply to waiting
  downloads, require restart of the download task, or are startup-only.
- Unsupported options fail with a precise error. They are never silently
  accepted.
- Compatibility claims are generated from the matrix, not from README prose.

## Option Metadata

Each option definition includes:

```text
name
short_name
type
default
allowed_values or range
category
scope: startup | global | per_download | input_file | rpc_change
runtime_update: none | live | waiting_only | active_restart | new_generation |
  startup_only | unsafe_compat_only | bt_live | bt_restart_required |
owner
build_features
security_class: safe | sensitive | unsafe_exec | network_exposure
aria2_compat_status: implemented | partial | unsupported | unsafe_compat | feature_gated
aria2_available: true | false
aria2_runtime_update: live | active_restart | unavailable
compatibility_difference: none | required | intentional | unresolved
behavior_test
docs
```

Scope and runtime update are separate:

- `scope` says where the option may appear or be set.
- `runtime_update` says what happens when RPC or a library caller changes it
  after a task exists.

Runtime update meanings:

- `none`: no runtime mutation API accepts this option.
- `live`: active tasks update in place without cancelling workers.
- `waiting_only`: applies to waiting/reserved tasks and future task
  generations; active tasks keep their current snapshot.
- `active_restart`: active tasks are internally quiesced, workers are cancelled,
  durable state is saved, and the task automatically resumes as a new generation
  with the updated option. The compatibility wire reports `waiting` during the
  restart and emits no user-pause event.
- `new_generation`: change is accepted only by explicitly creating/restarting
  a task generation; it is not silently applied to the current active
  generation.
- `startup_only`: process-level option fixed at startup.
- `unsafe_compat_only`: accepted only when unsafe compatibility mode is enabled
  and caller policy allows it.
- `bt_live`: a supported libtorrent setting is applied by the isolated BT
  adapter on its session thread without recreating the torrent session.
- `bt_restart_required`: an active BT task must be explicitly stopped and
  restarted; it is never silently requeued as an HTTP-style new generation.

Examples:

```text
split:
  type: integer
  scope: global,per_download,input_file,rpc_change
  runtime_update: active_restart
  owner: segment_scheduler
  status: implemented

max-download-limit:
  type: size
  scope: global,per_download,input_file,rpc_change
  runtime_update: live
  owner: rate_limiter
  status: implemented

on-download-complete:
  type: command
  scope: startup,per_download,input_file
  runtime_update: unsafe_compat_only
  owner: event_hook_service
  security_class: unsafe_exec
  status: unsafe_compat
```

## Config Layering

Precedence, from low to high:

1. built-in defaults,
2. system config,
3. user config,
4. environment proxy variables where aria2 supports them,
5. command-line global options,
6. input-file per-download options,
7. RPC global changes for future downloads,
8. RPC per-download changes.

The resolved option set is immutable for a task generation. Live changes create
a new typed snapshot and the scheduler applies the declared behavior.

Every task stores:

- `initial_options`: the resolved options at add time,
- `generation_options`: immutable options for the current worker generation,
- `pending_options`: accepted changes waiting for the next generation,
- `live_overrides`: rate limits and other live values that are safe to update
  atomically.

`getOption` returns the effective per-download view, including pending changes
with their application state when extended diagnostics are requested.

## Config File Format

Decision: keep aria2-style flat config as the primary, stable, compatibility
format.

The default config file is a line-oriented text file:

```text
# aria2-compatible flat config
dir=${HOME}/Downloads
max-concurrent-downloads=5
split=8
retry-profile=conservative
```

Rules:

- one `name=value` option per line,
- option names are long option names without `--`,
- `#` at the beginning of a line starts a comment,
- unknown options are rejected or warned according to strictness mode,
- values are parsed through the same typed option registry as CLI/RPC,
- shell-style command execution or includes are not part of the format,
- environment expansion stays explicit and limited; `${HOME}` compatibility can
  be supported for the same path-like options as aria2.

This is intentionally not an nginx-style nested DSL. A downloader config is not
a web-server routing config, and a general nested language would increase
parser, documentation, reload, and security surface. Most users need stable
defaults, input-file scoped options, and RPC updates, not arbitrary nested
inheritance.

## URL Default Rules

Pattern-based defaults are useful, but they should be a small optional rule
layer rather than replacing the flat config.

Decision: support optional URL/default rules in a separate rules file or a
clearly prefixed section syntax. Do not make nested blocks the main config
format.

Preferred format: TOML rules file.

```toml
[[rule]]
name = "internal mirrors"
match.host_suffix = ".corp.example"
match.scheme = ["http", "https"]

[rule.options]
ca-store = "os+custom"
ca-certificate = "/etc/corp-ca.pem"
max-connection-per-server = 8

[[rule]]
name = "large linux ISOs"
match.url_glob = "https://*.kernel.org/*.iso"

[rule.options]
split = 16
retry-on-http-status-add = "429,500-504"
```

Flat config points to it:

```text
url-rules-file=/path/to/rules.toml
```

Matching rules:

- rules apply only when a new download is added,
- rules produce per-download default options before input-file or RPC
  per-download overrides,
- rule order is deterministic; later matching rules override earlier rule
  defaults unless `rule.stop=true` is set,
- allowed match keys are explicit: `scheme`, `host`, `host_suffix`,
  `port`, `path_glob`, `url_glob`, `protocol`, and optional Metalink
  attributes,
- no arbitrary code, regex backtracking hazards, shell expansion, network I/O,
  or filesystem probing during matching,
- only options with `scope` including `per_download` may appear in rule
  options,
- startup-only, process-wide, RPC listener, event backend, disk backend, and
  unsafe exec options are rejected in rules.

Precedence with rules:

1. built-in defaults,
2. system flat config,
3. user flat config,
4. URL default rules selected by the resolved config,
5. environment proxy variables where aria2 supports them,
6. command-line global options,
7. input-file per-download options,
8. RPC global changes for future downloads,
9. RPC per-download changes.

This is not overdesigned if kept to deterministic per-download defaults. It
would be overdesigned if it became a nested language for every subsystem,
included files recursively, or tried to reconfigure active downloads by URL
pattern.

New options:

```text
--url-rules-file=PATH
--url-rules-mode=off|toml
--url-rules-strict=true|false
```

Defaults:

```text
--url-rules-mode=off
--url-rules-strict=true
```

## Config Reload

Decision: config reload is useful, but it must be explicit and limited.

No automatic reload by default. File watchers are cross-platform, racy, and can
turn simple config edits into surprising live engine mutations.

Supported operations:

```text
ariax config check --config PATH
ariax config reload
ariax config dump --effective
ariax config dump --defaults
ariax config dump --format=flat|toml|json
```

RPC/library equivalents may exist:

```text
aria2.reloadConfig
aria2.getEffectiveConfig
```

Reload rules:

- reload reparses flat config and URL rules into a new validated global option
  snapshot,
- only options whose `runtime_update` allows `live` or `waiting_only` mutation
  are applied automatically,
- `active_restart`, `new_generation`, and `startup_only` differences are
  reported but not applied to active tasks unless the caller opts into a
  controlled restart,
- unsafe exec options remain rejected unless unsafe compatibility policy allows
  them,
- URL rules affect only downloads added after reload,
- reload is atomic: parse/validate everything first, then publish one snapshot,
- failed reload keeps the previous config active and reports all validation
  errors.

Optional restart mode:

```text
ariax config reload --restart-affected=none|waiting|active
```

Default:

```text
--restart-affected=none
```

`active` mode is explicit because it can cancel workers and create new
generations.

## Config Save And Dump

Decision: allow dump/export. Do not silently rewrite the user's hand-written
config.

Reasons:

- comments and ordering matter to users,
- secrets may be present,
- generated config can accidentally persist temporary RPC changes,
- rewriting a config file is risky across crashes and permissions.

Allowed:

```text
ariax config dump --effective --redact-secrets --format=flat > ariax.conf
ariax config dump --defaults --format=flat
ariax config dump --effective --format=json
ariax config export-url-rules --format=toml
```

Rules:

- default dump redacts secrets and unsafe exec command values,
- `--include-secrets` is local CLI only, never allowed over unauthenticated
  RPC, and cannot be combined with `--write`; it is an explicit stdout-only
  diagnostic action rather than a session/export persistence mechanism,
- dump output goes to stdout or an explicitly separate output path,
- replacing the active config file requires `--write PATH --atomic` and must
  write through temp-file, fsync, rename, and parent-directory fsync where
  supported,
- generated config includes a header identifying it as generated,
- save/dump of effective config includes source metadata only in JSON/TOML
  diagnostic formats, not aria2-compatible flat config.

No `saveConfig` RPC should overwrite the active config by default. If provided,
it must require local/admin policy and an explicit destination path.

The user-maintained configuration source may intentionally contain credentials,
but automatic session/journal persistence and aria2/JSON session exports always
follow `session-persistence.md`'s omission policy.  `--include-secrets` never
changes that policy.

## Input File Compatibility

The input file parser follows aria2 semantics:

- URI lines define one download; TAB separates mirror URIs for the same entity.
- Lines starting with `#` are comments.
- Indented `key=value` lines apply only to the preceding URI group.
- Option names in input files do not include the `--` prefix.
- Parameterized URI and force-sequential behavior are preserved when those
  options are implemented.

Parsing is a pure function with property tests. No files are opened and no
network requests are made during parse.

## RPC Option Compatibility

`changeOption`:

- Allows aria2 input-file options except startup-only and explicitly excluded
  options.
- Live options update atomics or rate limiters immediately.
- `waiting_only` options update waiting tasks and pending options for active
  tasks, but do not affect already-running workers until a generation change.
- `active_restart` options transition the task to `PausedRestarting`, save
  durable state, cancel workers, and requeue the task with a new generation.
  `PausedRestarting` is internal: `tellStatus` reports `waiting`, and the pause
  hook/event is not emitted.
- `new_generation` options require an explicit restart/requeue operation or a
  stopped/waiting task; the API returns `OptionPatchRejected` with reason
  `requires_new_generation` if the caller asks for immediate live mutation.
- Unsafe shell hook options are rejected unless unsafe compatibility mode was
  enabled at startup and the RPC caller is local/admin.

`changeGlobalOption`:

- Updates the global template for future downloads.
- Live global limits update scheduler budgets.
- Queue-size options request immediate queue maintenance.
- Startup-only transport options, such as the active RPC listen port, are
  rejected with `OptionPatchRejected/not_runtime_mutable`.

Runtime update examples:

```text
max-download-limit                 live
max-overall-download-limit         live
lowest-speed-limit                 active_restart (aria2 behavior)
retry-on                           live for future retry decisions
retry-on-http-status               live for future retry decisions
retry-after                        live for future retry decisions
slow-slot-policy                   live
split                              active_restart
max-connection-per-server          active_restart
min-split-size                     active_restart
dir,out                            waiting_only or new_generation
ca-certificate,ca-store            active_restart
check-certificate                  active_restart
event-backend,disk-io-backend      startup_only
rpc-listen-port,rpc-transport      startup_only
on-download-complete               unsafe_compat_only
bt-request-timeout                 bt_live
bt-listen-port                     bt_restart_required
```

The four live retry/slow-slot controls above are design extensions unavailable in
aria2; their live behavior is intentional and appears as such in the generated
runtime-compatibility matrix. `lowest-speed-limit` is an aria2 option and remains
restart-on-change. `split`, `max-connection-per-server`, and `min-split-size`
also remain `active_restart`, matching aria2.

All rejected option mutations use one public error envelope:

```text
OptionPatchRejected {
  reason: invalid_value | unsupported | not_runtime_mutable |
          requires_new_generation | requires_explicit_bt_restart |
          unsafe_compat_required,
  option,
  requested_value,
}
```

CLI, JSON-RPC, stdio, and the native API map this same vocabulary to their
transport-specific representation. No second per-option error scheme exists.

For BT options, `bt_live` maps to an acknowledged libtorrent `settings_pack`
update through the adapter. `bt_restart_required` never performs an automatic
generation requeue: an active task receives
`OptionPatchRejected/requires_explicit_bt_restart` until the caller explicitly
stops and restarts it. The authoritative adapter behavior is in
`libtorrent-integration.md`.

`getOption` and `getGlobalOption`:

- Return string values compatible with aria2 response shapes for implemented
  options.
- Include build-feature dependent options only if present in the build or
  configured as visible compatibility stubs.

## Compatibility Matrix Categories

The full matrix should be generated into a machine-readable file during
implementation. This draft lists the intended category-level ownership.

Basic:

- `dir`, `out`, `input-file`, `max-concurrent-downloads`,
  `check-integrity`, `continue`, `help`, `version`: implemented.

HTTP/FTP/SFTP:

- Proxy: `all-proxy`, protocol proxies, proxy user/password, `no-proxy`,
  `proxy-method`: implemented for supported protocols.
- Auth: `http-user`, `http-passwd`, `http-auth-challenge`, `netrc-path`,
  `no-netrc`, FTP credentials: implemented.
- FTP/SFTP: `ftp-user`, `ftp-passwd`, `ftp-pasv`, `ftp-reuse-connection`,
  `ftp-type`, `ssh-host-key-md`: implemented per `detailed-ftp-sftp.md`.
  `ftp-type=ascii` is rejected for split/resume (offset math is invalid in ASCII
  mode) and allowed only for whole-file sequential download.
- Transfer: `split`, `max-connection-per-server`, `min-split-size`,
  `max-tries`, `retry-wait`, `timeout`, `connect-timeout`,
  `lowest-speed-limit`, `max-file-not-found`, `max-resume-failure-tries`:
  implemented.
- Retry policy: aria2-compatible retry options are implemented, and extended
  retry controls are implemented through `retry-policy.md` metadata. Retry
  status-code sets, `Retry-After`, and stale connection/validator behavior must
  be explicit, bounded, and test-covered.
- HTTP headers/cookies/TLS: `header`, `user-agent`, `referer`,
  `load-cookies`, `save-cookies`, `check-certificate`, `ca-certificate`,
  `certificate`, `private-key`, `min-tls-version`: implemented or
  feature-gated by TLS provider.
- User `header` values are syntax/CRLF validated. Generated `Host`, framing,
  range, encoding, validator, integrity, proxy-auth, and authorization headers
  are reserved; a conflicting user entry is rejected rather than merged. The
  policy is re-applied after every redirect.
- `min-tls-version=TLSv1.1` is `unsafe_compat` only if a legacy TLS feature is
  compiled in. Safe builds accept `TLSv1.2` and `TLSv1.3`.
- Placement: `remote-time`, `reuse-uri`, `auto-file-renaming`,
  `allow-overwrite`, `always-resume`, `conditional-get`,
  `content-disposition-default-utf8`: implemented.
- `enable-http-pipelining`: unsupported unless the HTTP client exposes safe
  pipelining semantics. HTTP/2 multiplexing is not reported as this option.

BitTorrent:

- Full build uses libtorrent for DHT, PEX, magnet metadata, trackers, web seed,
  encryption, peer limits, seeding ratio/time, and resume data.
- `bt-*`, `dht-*`, `listen-port`, `peer-id-prefix`, `peer-agent`,
  `follow-torrent`, `torrent-file`, `select-file`, and `index-out` are
  feature-gated. Minimal builds reject them clearly.
- Options not expressible through libtorrent are marked partial or unsupported
  until implemented in the adapter.

Metalink:

- `follow-metalink`, `metalink-file`, `metalink-base-uri`,
  `metalink-language`, `metalink-location`, `metalink-os`,
  `metalink-version`, `metalink-preferred-protocol`,
  `metalink-enable-unique-protocol`, `select-file`: implemented.

RPC:

- `enable-rpc`, `rpc-listen-all`, `rpc-listen-port`,
  `rpc-listen-address`, `rpc-secret`, `rpc-user`, `rpc-passwd`,
  `rpc-max-request-size`, CORS options, WebSocket events: implemented.
- `rpc-secret` uses aria2's first-positional-parameter `token:<secret>` scheme;
  each inner request in `system.multicall` authenticates independently. Legacy
  HTTP Basic Auth is compatibility-only and never overrides the token policy.
- `rpc-secure`, `rpc-certificate`, `rpc-private-key`: implemented when TLS
  server feature is enabled.
- Remote RPC without a secret on non-loopback is rejected unless explicitly
  overridden by an insecure startup-only option.

Advanced:

- `event-backend=auto|tokio` selects the single Tokio/Mio network reactor.
  `event-poll` is accepted as a deprecated option-name alias; legacy values such
  as `epoll`/`kqueue` express the expected Mio platform selector and never create
  a separate raw reactor. `disk-io-backend=auto|uring|iocp|blocking` is selected
  independently (a test-only `sync` value exists for tests and tiny
  single-file tools; see `event-backends.md`).
- `file-allocation`, `no-file-allocation-limit`, `disk-cache`, `enable-mmap`,
  `max-mmap-limit`: implemented with platform capability checks.
- `max-overall-download-limit`, `max-download-limit`,
  `max-overall-upload-limit`, `max-upload-limit`: implemented live.
- `save-session`, `save-session-interval`, `auto-save-interval`,
  `force-save`, `remove-control-file`, `save-not-found`,
  `keep-unfinished-download-result`, `max-download-result`: implemented.
- `daemon`, `stop`, `stop-with-process`, logging options, console options:
  implemented.
- `on-*` shell hooks: unsafe compatibility only, disabled by default.

## New Options

New options are namespaced where possible and do not pretend to be aria2
options:

Runtime and backend:

- `event-backend`
- `event-backend-fallback`
- `disk-io-backend`
- `profile=auto|concurrency|throughput|latency|compact`
- `max-threads`
- `net-workers`
- `disk-workers`
- `cpu-workers`
- `bt-workers`
- `shared-worker-pool=true|false`
- `durability=fast|balanced|strict`
- `disk-queue-bytes`
- `disk-queue-ops`
- `adaptive-backpressure=true|false`

Buffer pool:

- `buffer-pool-prealloc`
- `buffer-pool-lock-memory`

Retry and scheduling:

- `retry-profile=aria2|conservative|aggressive|custom`
- `retry-on=reset,eof,timeout,hang,lowest-speed,stale-connection,dns-transient`
- `retry-on-http-status`
- `retry-on-http-status-add`
- `retry-on-http-status-remove`
- `retry-after=respect|ignore`
- `retry-after-max`
- `retry-after-min`
- `retry-backoff=fixed|exponential|exponential-jitter`
- `retry-max-wait`
- `retry-max-attempts`
- `retry-max-attempts-per-mirror`
- `retry-max-elapsed`
- `stale-validator-policy=fail|restart-if-safe|revalidate`
- `slow-slot-policy=off|demote|pause`
- `slow-slot-speed-limit`
- `slow-slot-grace-period`
- `slow-slot-min-active-time`
- `slow-slot-max-demotions`
- `slow-slot-readmit-after`
- `slow-slot-readmit-policy=front|original-position|back`
- `retry-wait-consumes-slot=true|false|auto`
- `endgame-max-duplicates` (bounded concurrent duplicate attempts in endgame
  mode; see `split-download.md`)

RPC and embedding:

- `rpc-insecure-listen`
- `rpc-compat=aria2|extended|strict`
- `rpc-transport=http|stdio|http+stdio`
- `rpc-stdio-framing=content-length|ndjson`
- `rpc-stdio-eof=shutdown|close-transport|ignore`
- `rpc-stdio-events=true|false`
- `rpc-stdio-max-request-size`

Protocol modernization:

- `http2=true|false|auto`
- `http2-max-concurrent-streams`
- `http3=false|true|auto`
- `quic-max-connections`
- `ech=false|true|auto`
- `ca-store=os|mozilla|custom|os+custom`
- `ca-directory`
- `dns-backend=system|cares|trust-dns|doh|dot`
- `doh-url`
- `dot-server`
- `dns-cache=true|false`
- `happy-eyeballs-timeout`

Transfer integrity and redirects:

- `verify-mirror-identity=off|strict` (default `off`, aria2-compatible: trust
  the mirror list for concurrent split; `strict` gates concurrent multi-mirror
  split on a shared content digest — see `split-download.md`)
- `max-redirects` (default 20; see `redirect-policy.md`)
- `allow-redirect-downgrade=true|false` (default `false`; permit `https -> http`
  redirects)

Session and control files:

- `session-store=hybrid|sqlite|control-files|memory`
- `session-db`
- `control-file-dir`
- `control-file-location=central|beside-output|both`
- `control-file-suffix`
- `save-session-format=aria2|json`

Config management:

- `url-rules-file`
- `url-rules-mode=off|toml`
- `url-rules-strict=true|false`

Metalink:

- `realtime-chunk-checksum=true|false`
- `metalink-chunk-alignment=auto|strict|relaxed`

Security policy:

- `allow-exec-hooks`
- `allowed-output-root`
- `network-allowlist`
- `network-denylist`
- `rpc-allow-private-address-downloads`

CLI-only config utility flags are not persistent downloader options and should
not appear in RPC `getGlobalOption`:

- `ariax config check --config PATH`
- `ariax config reload --restart-affected=none|waiting|active`
- `ariax config dump --effective|--defaults`
- `ariax config dump --format=flat|toml|json`
- `ariax config dump --redact-secrets|--include-secrets`
- `ariax config dump --write PATH --atomic`

## Documentation Rules

The manual is generated from option metadata:

- default value,
- valid values,
- scope,
- runtime mutability,
- security warning if sensitive,
- build feature if feature-gated.

CI fails if the registry, manual, CLI help, RPC option allowlists, and
compatibility matrix disagree.

An option cannot be marked `implemented` merely because it parses or names a
test. Its `behavior_test` must set a non-default value through every supported
surface and assert an observable effect in the owning subsystem. CI fails when
the test is missing, skipped for the enabled feature set, or observes only the
default behavior.
