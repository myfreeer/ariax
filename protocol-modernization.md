# Protocol Modernization

Status: draft.

Modern protocols should be part of the roadmap, but not all of them belong in
the mandatory baseline. The downloader must keep the reliable aria2-style
HTTP/FTP/SFTP behavior while adding modern transports behind capability and
feature gates.

## Baseline

Required in the standard build:

- HTTP/1.1
- HTTP keep-alive
- HTTPS with TLS 1.2 and TLS 1.3
- certificate validation with secure defaults
- proxy and no-proxy behavior compatible with aria2
- Range requests with strict `206`/`Content-Range` validation
- identity-coded, known-length HTTP bodies for the immutable-layout baseline;
  fresh sequential, range, split, and resume requests send
  `Accept-Encoding: identity` (see below)
- IPv4/IPv6 with Happy Eyeballs
- FTP and SFTP transfer support (correctness contract in `detailed-ftp-sftp.md`)

FTP/SFTP are part of the `standard` build profile alongside HTTP(S); their
resume, range, and validator semantics differ from HTTP and are specified in
`detailed-ftp-sftp.md`.

Required if the chosen HTTP stack supports it well:

- HTTP/2 over TLS via ALPN
- HTTP/2 multiplexing with per-origin stream limits

Feature-gated or future:

- HTTP/3 over QUIC
- Encrypted ClientHello (ECH)
- DNS-over-HTTPS
- DNS-over-TLS
- Oblivious DoH

## HTTP/1.1 Keep-Alive

HTTP/1.1 keep-alive is core.

Rules:

- connection pool keyed by scheme, host, port, proxy, TLS identity, and auth
  context,
- idle timeout and max idle connections per host,
- do not reuse a connection after ambiguous protocol errors,
- drain or close bodies before reuse,
- preserve per-host connection budgets,
- support explicit disable for compatibility.

aria2 compatibility:

```text
--enable-http-keep-alive=true|false
```

Hyper ingress controls exposed by this implementation:

```text
--http-ingress-buffer-limit=SIZE
--http1-read-buffer-size=auto|SIZE
--http1-max-buffer-size=SIZE
--http1-max-headers=N
```

`http1-read-buffer-size=SIZE` selects Hyper's exact read-buffer mode and is
mutually exclusive with a non-default max-buffer override. The registry rejects
sizes below Hyper's supported minimum instead of allowing a builder panic.
`http1-max-headers` maps to Hyper's response-parser cap; the default is 100 and
the project hard maximum is 1024. Its resolved header-entry allowance plus the
read/max buffer is charged to `http_ingress_budget`, so raising the count can
reduce connection admission rather than grow memory silently.
`http-ingress-buffer-limit` is the project-wide budget for adapter-held body
frames plus the configured estimate of framework-owned HTTP ingress memory; it
is separate from `disk-cache`/`BufferPool`.

## HTTP/1.1 Pipelining

HTTP/1.1 pipelining should not be enabled by default.

Reasons:

- many servers/proxies behave poorly,
- head-of-line blocking,
- limited benefit compared to HTTP/2 multiplexing,
- higher correctness risk for range downloads.

aria2 has `--enable-http-pipelining`; this design should mark it unsupported or
compatibility-only unless the selected HTTP stack exposes safe behavior and
tests prove it.

## HTTP/2 Multiplexing

HTTP/2 is implemented through the selected Hyper client and remains configurable
per task/origin policy.

Benefits:

- fewer TCP/TLS handshakes,
- stream multiplexing,
- better behavior behind some CDNs,
- header compression.

Downloader rules:

- per-origin stream limit separate from connection limit,
- per-stream range validation remains mandatory,
- stream-level stalls do not freeze task speed stats,
- connection-level flow control is integrated with backpressure,
- one bad stream must not corrupt other streams,
- fall back to HTTP/1.1 if ALPN or server behavior fails.

Config:

```text
--http2=true|false|auto
--http2-max-concurrent-streams=N
--http2-initial-stream-window-size=SIZE|auto
--http2-initial-connection-window-size=SIZE|auto
--http2-max-frame-size=SIZE|auto
--http2-max-header-list-size=SIZE|auto
--http2-adaptive-window=true|false
```

The fixed window/frame settings are passed to Hyper/h2 and exposed in effective
configuration and diagnostics. `http2-adaptive-window=true` is mutually
exclusive with explicit initial-window overrides because adaptive mode ignores
those fixed windows. It is parsed and reported but feature-gated/rejected in the
first bounded-memory slice until a hard growth cap is proven. Registry validation
enforces the HTTP/2 frame/window/header ranges. Connection/stream admission uses
the resolved fixed windows plus measured stack overhead so a high stream count
cannot exceed `http-ingress-buffer-limit`.

HTTP/2 does not replace `split`; it changes how range workers map to streams
and connections. A single HTTP/2 connection may carry multiple range leases if
the server and flow control perform well.

## HTTP/3 / QUIC

HTTP/3 should be planned as feature-gated, not required in the first stable
baseline.

Reasons to include:

- modern CDN support,
- connection migration,
- no TCP head-of-line blocking,
- potentially better lossy-network behavior.

Reasons to gate:

- more complex UDP path and firewall behavior,
- proxy support is different,
- library maturity and binary size,
- QUIC congestion control and flow control need careful tuning,
- range correctness and retry semantics still need the same validation.

Config:

```text
--http3=false|true|auto
--quic-max-connections=N
```

`auto` may try HTTP/3 only when HTTPS origin advertises support and policy
allows UDP. It must fall back cleanly to HTTP/2 or HTTP/1.1.

## TLS

Baseline:

- TLS 1.2 and TLS 1.3,
- secure certificate validation by default,
- OS/native trust store by default,
- configurable CA bundle/path/directory in addition to or instead of OS trust,
- client certificate support,
- min TLS version option,
- ALPN for HTTP/2 and HTTP/3 where supported.

TLS 1.3 should be enabled by default through the TLS provider.

Config:

```text
--min-tls-version=TLSv1.2|TLSv1.3
--check-certificate=true|false
--ca-store=os|mozilla|custom|os+custom
--ca-certificate=PATH
--ca-directory=PATH
--certificate=PATH
--private-key=PATH
```

Default:

```text
--min-tls-version=TLSv1.2
--ca-store=os
```

aria2 compatibility boundary:

- aria2 exposes `TLSv1.1` for `--min-tls-version`.
- This design does not include TLS 1.1 in the safe default option set.
- TLS 1.1 may exist only as `unsafe_compat` behind an explicit legacy TLS
  feature, and never as the default.

Trust store rules:

- desktop builds default to the operating system trust store,
- custom CA files/directories are supported for private PKI,
- `os+custom` appends custom roots to the OS store,
- `custom` uses only user-provided roots,
- `mozilla` is optional for static/minimal builds where native store support is
  unavailable or intentionally disabled,
- invalid CA files fail at startup or task creation, not mid-transfer,
- active trust configuration is visible through diagnostics without exposing
  private keys.

Per-download overrides are allowed only if the option matrix marks them safe for
runtime restart. Changing CA settings for an active download requires a
controlled reconnect; existing TLS sessions are not silently reinterpreted.

## ECH

Encrypted ClientHello should be a future/feature-gated capability.

Reasons:

- depends on DNS HTTPS/SVCB records and TLS library support,
- deployment is still evolving,
- failure and fallback behavior must be careful to avoid surprising users.

Config:

```text
--ech=false|true|auto
```

`auto` should use ECH only when DNS and TLS backend support it and should
fallback without hard failure unless the user requires ECH.

## DNS

Baseline:

- system resolver,
- async resolver where supported,
- IPv4/IPv6 Happy Eyeballs,
- DNS cache with TTL handling,
- user-configured DNS servers where supported.

Resolver selection:

```text
--dns-backend=system|hickory|doh|dot
--doh-url=URL
--dot-server=HOST:PORT
--dns-cache=true|false
--happy-eyeballs-timeout=MS
```

`hickory` is the in-process async resolver selected in `library-choice.md`.
Configuration may accept `trust-dns` as a deprecated input alias for migration,
but dumps, diagnostics, and generated help emit `hickory`.

`cares` is not a baseline backend. If encountered as a reserved compatibility
value it rejects as unsupported rather than silently selecting Hickory; the
crate comparison and condition for reconsideration are in `library-choice.md`.

DoH/DoT are feature-gated because they add TLS/HTTP dependency paths and policy
questions.

Rules:

- resolver choice is part of the connection identity where needed,
- proxy rules decide whether DNS is local or proxy-resolved,
- DNS failures feed retry policy separately from transport failures,
- DNS privacy features do not bypass no-proxy or network allow/deny policy.

## Cookie Jar

`--load-cookies`/`--save-cookies` are listed as implemented, so the cookie jar
needs a defined contract:

- parse the Netscape/Mozilla cookie file format; reject malformed lines rather
  than misinterpreting them (this file is untrusted input and has a fuzz target),
- domain/path matching with public-suffix enforcement so a cookie cannot be set
  for a registrable-suffix domain,
- honor `Secure`, `HttpOnly`, and `SameSite`; drop `Secure` cookies on plaintext
  requests,
- expiry and session-cookie rules; session cookies are not persisted by
  `--save-cookies`,
- cookies are scoped by host and MUST NOT propagate to unrelated mirror hosts in
  a multi-URI/Metalink download (ties into the cross-mirror identity rule in
  `split-download.md`),
- cookie propagation across redirects follows `redirect-policy.md` (dropped when
  the redirect crosses to an unrelated origin).

## Cookie And DNS Cache Bounds

- The DNS cache has a size cap with LRU eviction, honors TTL including TTL=0, and
  performs negative/failure caching with a short bounded TTL so a resolver
  outage does not hammer the resolver. Resolver timeout feeds retry policy.
- Happy Eyeballs cancels and closes the losing A/AAAA connection racer so it does
  not leak against the file-descriptor budget.

## Proxy Interaction

HTTP/2, HTTP/3, ECH, DoH, and DoT must respect proxy configuration.

- HTTP CONNECT proxy works for HTTPS over TCP.
- HTTP/2 through proxies depends on client/proxy support.
- HTTP/3 through traditional HTTP proxies is not assumed.
- SOCKS remote DNS is available only when final-hop destination enforcement is
  trusted under the policy below.
- DoH/DoT must not bypass an explicit proxy policy; explicit configuration is
  still subject to the final-hop policy and remote-RPC trust boundary below.

Proxy flows (the actual connection establishment, not just the toggle):

- HTTPS through an HTTP proxy uses `CONNECT` to open a tunnel. A non-2xx
  `CONNECT` response is a distinct failure from an origin error and is classified
  as a `ProxyConnect` retry class, not an origin `5xx`/timeout.
- Proxy authentication (`407`) runs its own challenge/response loop against the
  proxy credentials, separate from origin `401` auth. A failed proxy-auth loop
  does not consume origin retry budget.
- SOCKS5 supports username/password auth and (for `socks5h`) remote DNS. SOCKS
  connect failures are also `ProxyConnect`.
- FTP-over-HTTP-proxy is supported for compatibility where the proxy allows it.
- Proxy credentials come from explicit options first, then `http_proxy` /
  `https_proxy` / `all_proxy` / `no_proxy` environment variables, with
  `no_proxy` taking precedence to bypass. Precedence is: per-download option >
  global option > environment variable.
- Proxy credentials are redacted from logs, status, and diagnostics.
- The `ProxyConnect` retry class is added to the retry taxonomy in
  `retry-policy.md`.

### Final-Hop Destination Policy

SSRF policy applies to the origin reached after the proxy, not merely to the
proxy socket. Each task resolves one of two modes before connecting:

- `LocalPinned` is the default and is mandatory for URLs submitted through an
  untrusted non-loopback RPC listener. Resolve locally, canonicalize literals
  (including integer/obscure IPv4 forms and IPv4-mapped IPv6), apply the network
  allow/deny and private/link-local/loopback/metadata rules to every result, pin
  an allowed numeric address for that connection attempt, and send that numeric
  address in SOCKS5 or the HTTP `CONNECT` authority. Preserve the original DNS
  hostname only for TLS SNI, certificate verification, and the generated HTTP
  `Host` field. If the client/proxy stack cannot separate connect address from
  Host/SNI, fail closed.
- `TrustedProxyEnforced` permits `socks5h` or hostname-form `CONNECT` only for a
  proxy that a local administrator marked at startup as destination-enforcing
  and bound to an administrator-controlled destination allowlist at least as
  strict as this process's SSRF policy. A per-download option or remote RPC
  caller cannot grant this trust.

The proxy endpoint itself is also resolved and checked according to whether it
is an administrator-configured trusted endpoint or an untrusted per-task proxy.
Every redirect, reconnect, DNS-cache expiry, and proxy change repeats the final
destination decision. A retry may select a newly resolved address only after
that address passes the policy; it never re-resolves behind an already approved
hostname tunnel. Diagnostics record the policy mode and canonical destination
class without credentials or sensitive URL data.

FTP-over-HTTP-proxy and optional protocols obey this same rule. A compatibility
feature that can express only a hostname to a non-enforcing proxy is unavailable
to untrusted remote-RPC tasks rather than exempt from SSRF checks.

## Request Header Ownership

The protocol stack validates custom HTTP fields before request construction.
Field names are case-insensitive tokens; CR, LF, NUL, forbidden controls, and
ambiguous singleton duplicates reject the task. `Host`, `Content-Length`,
`Transfer-Encoding`, `Range`, `If-Range`, `Accept-Encoding`, `Authorization`,
`Proxy-Authorization`, `Cookie`, `Content-Digest`, `Repr-Digest`, `Signature`, and
`Signature-Input` are generated/reserved and cannot be supplied through the
generic custom-header option.

The request builder is authoritative for reserved fields. Redirects rebuild
rather than clone the header map, then re-run origin credential, cookie, proxy,
range, and encoding policy. Non-reserved custom headers survive in configured
order only when their field definitions permit the requested duplicate/list
form. The detailed first-slice and redirect behavior is specified in
`detailed-http-first-slice.md` and `redirect-policy.md`.

## Content-Encoding And Range Correctness

`Content-Length` and `Content-Range` describe encoded bytes, while a decoded body
is in a different byte space. Byte-range resume over a content-coded response is
therefore a corruption path: `Range: bytes=<durable_length>-` is interpreted by
the server in encoded space, but `durable_length` is tracked in decoded space.

Rules:

- The immutable-layout baseline sends `Accept-Encoding: identity` on fresh
  sequential, range, split, and resume requests.
- A fixed-layout `200` requires a valid `Content-Length`; a fixed-layout `206`
  requires a validated known total in `Content-Range`. Chunked, missing-length,
  or non-identity responses are rejected before body polling. They are not an
  automatic sequential fallback.
- If a server ignores `identity` and returns a content-coded body to a range or
  resume request, reject it without writing at any offset.
- Decoded or unknown-length whole-entity `GET` is a separate, explicit
  `GrowingSequential` capability, not part of the first slice. It requires an
  append/growing storage layout, a configured maximum decoded size, distinct
  wire and decoded counters, and a durable final-extent commit before hashing or
  completion. It starts at `0` and cannot split or resume.
- Wire offsets and decoded-file offsets are different types/concepts. No range
  request is constructed from a decoded-file offset.

## Range And Multiplexing Semantics

Regardless of protocol version:

- every range stream is validated independently,
- `200 OK` to a range request is not accepted as a nonzero offset write,
- each response stream is one `TransferAttemptId`; a range attempt writes
  provisionally under one `LeaseId`, while a sequential stream advances through
  piece-aligned checkpoint leases on the same connection,
- short, oversized, redirect, cancellation, and validator failure issue
  `AbortLease` for the current incomplete lease, while only exact successful
  validation of a span may issue its `CommitLease`,
- per-stream retries map back to range leases,
- disk placement uses global offsets,
- stats sampler runs independent of packet/stream events.

## Compatibility Matrix

Protocol features should appear in the option matrix:

```text
HTTP/1.1 keep-alive: implemented
HTTP/1.1 pipelining: unsupported or compat-only
HTTP/2: implemented or feature_gated
HTTP/3: feature_gated
TLS 1.3: implemented through TLS provider
ECH: feature_gated
DoH/DoT: feature_gated
GrowingSequential HTTP bodies: feature_gated
```
