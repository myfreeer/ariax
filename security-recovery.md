# Security And Recovery Design

Status: reviewed contract with implementation in progress. Portable path
normalization/rejection, persisted root bindings, and bounded control-journal
framing/replay plus scalar typed payload decoding are executable. Native
capability acquisition, safe descendant open, collection payloads, typed state
recovery, durable appending, and reconciliation remain pending.

This document turns the safety requirements into enforceable design rules.

## Safe Path Builder

All metadata-derived and path-bearing option inputs use the single
`SafePathBuilder::build(SafePathInput) -> Result<SafePathOutput, PathError>` API
owned by `detailed-storage.md`. This document adds policy constraints; it does
not define a second builder shape.

```text
SafePathBuilder::build(SafePathInput) -> Result<SafePathOutput, PathError>
```

Input decoding and component construction are deterministic:

- Decode an external byte/percent-encoded name exactly once. Reject invalid
  UTF-8, including overlong encodings and surrogate encodings, rather than using
  lossy replacement.
- Normalize every Unicode component to NFC before validation, collision checks,
  persistence, and display. Never normalize after a path has been opened.
- `out` and `index-out` may express relative subdirectories. After rejecting an
  absolute/drive/UNC prefix, split them on both `/` and `\\`, collapse repeated
  separators by ignoring empty interior components, and pass every resulting
  component separately through the builder. `.` and `..` are rejected, not
  collapsed. A wholly empty result is invalid.
- Metadata formats that already supply component arrays do not get a second
  separator interpretation: a separator inside one supplied component is an
  error.

Reject:

- absolute paths,
- Windows drive prefixes and UNC prefixes,
- `.` and `..`,
- path separators inside a component,
- empty components except where the format explicitly permits them and they are
  ignored,
- NUL and control characters,
- bidirectional override/isolate formatting characters that can spoof displayed
  path order,
- `:` in every component. The portable policy deliberately rejects this on all
  platforms so a session restored on Windows cannot reinterpret `file:stream`
  as an alternate data stream,
- reserved Windows names: `CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9`, and
  `LPT1`-`LPT9`, including when they carry an extension or stream suffix
  (`CON.txt`, `nul.log` are also reserved), and the superscript-digit forms
  `COM¹`/`COM²`/`COM³` and `LPT¹`/`LPT²`/`LPT³` that current Windows naming
  rules also reserve,
- trailing spaces or dots on Windows,
- symlink escapes when opening existing directories.

Windows path-length policy: the builder computes the full final path length and
uses the extended-length (`\\?\`) form when the classic `MAX_PATH` limit would
otherwise be exceeded and the platform supports it. Because the `\\?\` form
disables the Win32 normalization that the reject list above compensates for,
the builder's own component validation is mandatory before that form is used.
If the final path still exceeds the platform/filesystem limit, path
construction fails with a typed `PathError`, not a truncated or aliased name.

Reserved-name, trailing-dot/space, and collision checks run after NFC
normalization. On case-insensitive targets the builder uses the filesystem's
case-insensitive comparison key for collision detection. Visually confusable
Unicode characters are not treated as equivalent—there is no stable universal
homoglyph mapping—but security decisions never depend on rendered similarity,
and diagnostics escape non-ASCII/bidirectional-sensitive names unambiguously.

The builder retains a capability for the canonical output root and resolves or
creates every descendant relative to retained directory capabilities. The
display `PathBuf` is never reopened as authority. Linux uses `openat2` with
`RESOLVE_BENEATH`/no-symlink constraints when supported and a stepwise
`openat`/`O_NOFOLLOW` directory-fd walk otherwise. Other Unix targets use the
same held-directory-fd pattern. Windows opens each directory without
share-delete, rejects reparse points, retains the handle chain until the final
handle is acquired, and verifies volume/file identity. All platform-specific
unsafe/syscall code stays behind the project safe-open adapter.

This race-resistant open contract is required on supported production targets,
including the bounded blocking fallback. If the runtime probe cannot provide
it, metadata-derived output creation fails closed with `SafeOpenUnavailable`;
there is no silent best-effort check-then-open mode for an attacker-writable
root. `detailed-storage.md` owns the canonical capability-bearing API and
recovery reconstruction rule.

This builder is mandatory for:

- torrent file paths,
- Metalink names,
- Content-Disposition filenames,
- `out`,
- `index-out`,
- session restore,
- RPC upload metadata paths.

The initial libtorrent adapter is a documented boundary: libtorrent receives
sanitized relative paths rather than the project's open file capabilities. Its
output root therefore must not be writable by an untrusted local principal
while the session runs. Stronger local-attacker containment for BitTorrent
requires the deferred custom libtorrent storage backend; metadata sanitization
alone must not be described as providing that stronger guarantee.

## No RCE Policy

Default behavior:

- no shell command execution,
- no command templates,
- no metadata-controlled process launch,
- no RPC-enabled hook changes.

Safe event alternatives:

- structured WebSocket events,
- JSON event log,
- local plugin API with typed messages,
- webhook client with URL allowlist and no shell interpolation.

Unsafe compatibility:

- `--allow-exec-hooks=true` enables aria2-style local hooks.
- Hooks are startup-only unless a local admin RPC policy permits changes.
- Commands are executed without a shell when possible.
- Environment variables are allowlisted.
- Arguments are passed as argv fields, not concatenated command strings.
- Hook path must be absolute or under an allowlisted directory.
- Hook stdout/stderr size is capped.
- Hook timeout is enforced.

## RPC Security

Defaults:

- bind only loopback,
- require `rpc-secret` for non-loopback,
- cap request body by `rpc-max-request-size`,
- reject unauthenticated WebSocket event subscriptions,
- redact tokens, passwords, cookies, Authorization headers, and proxy
  credentials.

Remote RPC guardrails:

- Optional network allow/deny lists for downloads submitted over RPC:
  `network-allowlist` and `network-denylist`. When both are set, denylist wins
  (default-deny on conflict); an empty allowlist means "no restriction", a
  non-empty allowlist means "only these match". Defaults: both empty.
- Private-address blocking to reduce SSRF risk when RPC is exposed:
  `rpc-allow-private-address-downloads`, default `false`. When false, downloads
  whose resolved address is non-global or special-use are refused. Setting it
  true relaxes only RFC 1918 IPv4 and IPv6 unique-local destinations; loopback,
  link-local, unspecified, multicast, broadcast, documentation/benchmark,
  reserved ranges, and cloud-metadata endpoints still require an explicit
  startup administrator allowlist.
- CORS disabled by default; wildcard CORS requires explicit insecure opt-in.
- `changeGlobalOption` cannot mutate startup-only listener security.

## SSRF Guardrail

`README.md` promises that redirects, proxies, and DNS resolution obey SSRF
guardrails when RPC is remotely exposed. This section defines the mechanism.

The guardrail runs whenever a download target is submitted over a non-loopback
RPC listener (and always for redirect targets, see `redirect-policy.md`):

- Resolve the host, then evaluate every resolved address against a generated,
  pinned IANA special-purpose prefix table. Default remote-RPC policy permits
  only globally routable unicast and denies unspecified/current-network,
  loopback, RFC 1918/unique-local, carrier-grade NAT, link-local, protocol/
  benchmarking/documentation ranges, multicast, reserved/future-use, IPv4
  broadcast, IPv4-mapped forms of any denied IPv4 address, and known cloud
  metadata endpoints (including `169.254.169.254`) before connect. The table
  update is reviewed like a dependency update; code does not rely on an
  incomplete hand-written trio of private ranges.
- Canonicalize numeric hosts before classification, including unusual integer/
  legacy IPv4 spellings and IPv4-mapped IPv6. Policy is applied to the canonical
  address, so alternate textual forms do not bypass the deny set.
- DNS-rebinding protection: pin the address that passed the check and connect to
  that exact address, so a second resolution cannot swap in a blocked target
  between check and connect. A reconnect or DNS-cache refresh performs a new
  resolution/check and creates a new pin; it never reuses approval for a
  hostname with a different address. Happy Eyeballs may race connection
  attempts only among addresses that each individually passed the check; the
  winning connection's address is the pin.
- Composition: the guardrail applies after `network-allowlist`/`network-denylist`
  and before the connection is made. A user-configured mirror is not exempt when
  RPC is remotely exposed; an explicitly loopback-bound RPC instance may relax
  only RFC 1918/unique-local blocking via
  `rpc-allow-private-address-downloads=true`; other special-use ranges still
  require an explicit startup allowlist.
- Proxy interaction: the guardrail validates both the proxy endpoint and the
  final origin. For an untrusted remote-RPC task, locally resolve and validate
  the origin, pass its pinned numeric address to SOCKS5 or HTTP `CONNECT`, and
  retain the original hostname only for TLS SNI, certificate verification, and
  generated HTTP `Host`. `socks5h`, hostname-form `CONNECT`, or another
  proxy-resolved destination is refused unless a local administrator marked the
  proxy at startup as destination-enforcing with an allowlist at least as strict
  as this guardrail. A per-task option or RPC caller cannot grant that trust.
  See `protocol-modernization.md`.
- Redirect and proxy changes repeat the entire decision. Failure to express a
  pinned connect address separately from Host/SNI fails closed.

Output root guardrail:

- `allowed-output-root` may restrict all metadata-derived and user-provided
  output paths to one or more configured roots.
- `SafePathBuilder` runs after this policy; both checks must pass.

## HTTP Header Boundary

Generic custom headers cannot override fields that enforce framing, placement,
identity, or credential scope. Header names and values are syntax-validated and
reject CR/LF/NUL/control injection. Case-insensitive attempts to set `Host`,
`Content-Length`, `Transfer-Encoding`, `Range`, `If-Range`, `Accept-Encoding`,
`Authorization`, `Proxy-Authorization`, `Cookie`, integrity digests, or request-signature
fields reject the task. The HTTP builder generates those fields and rebuilds
them after each redirect; it never resolves conflicts using last-write-wins.
The canonical list and harmless-header behavior are in
`detailed-http-first-slice.md`.

## Correct Range And Placement Rules

HTTP range write acceptance:

- Request says `Range: bytes=A-B`.
- Response must be `206`.
- `Content-Range` must be `bytes A-B/T` or an equivalent valid form.
- Body length must equal `B - A + 1`.
- If total `T` is known, it must match the task total.
- If body length differs, no bytes are committed durable for that segment.
- After response-head validation, every attempt calls storage `BeginLease` with
  its `LeaseId` and writes only provisional blocks. Exact body length and all
  required validator/digest checks precede `CommitLease`; short/oversized body,
  redirect, cancellation, or validation failure calls `AbortLease`. These
  storage-owned operations, not file length or physical bytes, decide progress.
- An endgame overlap group cannot become committed or durable until all
  competing writes are fenced. If any losing attempt wrote or has uncertain
  cancellation, all touched pieces return to pending metadata and in-memory
  state; their physical bytes remain untrusted and are overwritten by the next
  lease rather than restored in place.

Sequential resume:

- Existing partial length is read without truncation.
- Resume request starts at existing durable length.
- `206` is required.
- `200` causes full restart only after the storage engine creates a new
  generation and explicitly truncates or renames according to overwrite policy.

Disk placement:

- Protocol workers cannot write files directly.
- Workers submit global offsets to the storage engine.
- Storage engine validates layout, selected files, generation, and current
  piece state.
- Disk ack includes exact offset and byte count written.

## Security Boundary Tests

- path decoding rejects invalid/overlong UTF-8, ADS colons, reserved names,
  traversal, bidi overrides, and NFC-equivalent collisions,
- `out`/`index-out` split relative subdirectories into validated components and
  reject absolute, drive, UNC, `.` and `..` forms,
- decimal/legacy IPv4 and IPv4-mapped IPv6 forms of private, loopback,
  link-local, special-use, and metadata addresses are denied,
- DNS rebinding on reconnect triggers a new resolution and policy decision,
- untrusted `socks5h` and hostname `CONNECT` are refused, while local resolution
  uses the pinned numeric connect address with the original Host/SNI,
- a redirect or proxy change re-runs both endpoint and final-origin checks,
- custom-header case changes, duplicates, and CR/LF injection cannot override a
  generated safety/credential field,
- a failed range attempt has one `AbortLease` and no committed progress.

## Control Journal

The normative journal format and record enum are defined in
`detailed-storage.md`. This section states only the security-relevant
properties; it does not restate the field layout, to avoid the divergence that
previously existed between the two documents.

Security-relevant properties:

- append-only or atomically replaced,
- a CRC covers the full record framing plus payload, and an explicit
  `max_record_len` bound is checked before any read, so a torn or garbage length
  field cannot drive an over-read,
- every record carries the task generation and a monotonically increasing
  per-task sequence,
- recovery scans until the last valid committed record; invalid tail bytes are
  ignored,
- a record is never trusted without a valid length, CRC, commit marker, and
  expected sequence,
- multiple records normally share one generation; only `GenerationStarted`
  advances the generation.

The record enum (including `GenerationStarted`, which advances the generation,
and `PieceStarted`, which marks a piece in-flight for recovery) is the union
enum in `detailed-storage.md`.

The primary persistence model is described in `session-persistence.md`: global
queue/session metadata in SQLite, crash-critical progress in per-task control
journals.

## Durable Completion

A piece is complete only after:

1. all bytes are written at correct offsets,
2. checksum is valid when available,
3. disk write has been acknowledged,
4. journal durable record has been saved.

A download is complete only after:

1. all selected pieces are complete,
2. final full-file checksum is valid if configured,
3. final filenames are atomically moved into place if temp naming is used,
4. final file metadata and parent directory are flushed according to durability
   mode,
5. stopped result is persisted.

## Crash Scenarios

Power loss during network read:

- no durable journal record exists,
- segment returns to pending.

Power loss after disk write before journal:

- data may exist on disk,
- piece is not trusted,
- piece is revalidated or redownloaded.

Power loss after journal before final rename:

- piece remains complete,
- startup resumes finalization through the `FinalizeIntent` redo rule in
  `detailed-storage.md`.

Power loss during final rename:

- the flushed `FinalizeIntent` plus temp/final presence, recorded length, and
  file-identity evidence select exactly one finalization outcome
  (`detailed-storage.md` Idempotent Rename Recovery),
- no existing unrelated file is overwritten; a foreign final-path object fails
  closed as a collision.

Power loss during journal checkpoint compaction:

- until the SQLite pointer is `installed`, the old segment set remains
  authoritative and the candidate checkpoint is discarded on any validation
  failure,
- after `installed`, the checkpoint set is authoritative and stale old
  segments are unreachable garbage,
- compaction never promotes provisional/in-flight state to durable.

Power loss during control save:

- torn tail is ignored,
- previous committed state remains valid.

## Secrets And Logs

Sensitive values are stored in `Secret<T>` wrappers:

- no `Debug` reveal,
- redacted serialization,
- optional zeroize on drop,
- never included in panic messages.

Logs must redact:

- RPC secrets,
- HTTP Authorization,
- cookies,
- proxy credentials,
- netrc credentials,
- SFTP passwords and keys,
- signed URLs if configured.

## Unsafe Code And FFI

Pure Rust crates forbid unsafe code.

Allowed unsafe crates:

- `platform-io`: system calls, IOCP, io_uring, openat wrappers.
- `bt-libtorrent`: C++ bridge.

Requirements:

- every unsafe block has an invariant comment,
- Miri for pure code,
- ASAN/UBSAN/TSAN for FFI builds,
- fuzz tests cross FFI boundaries with small corpus inputs,
- panics do not unwind across FFI.
