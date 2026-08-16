# Redirect Policy

Status: reviewed contract with the Phase-3B redirect state machine and policy
client integration executable. The implemented gate covers bounded hop/loop
handling, method rewriting, HTTPS downgrade rejection, per-hop destination and
proxy re-admission, request reconstruction, origin-scoped credentials/cookies,
and nonzero durable-prefix restart decisions. The bounded SHA-256 `Repr-Digest`
endgame profile applies only to explicitly submitted, independently probed
mirrors; it does not promote a redirect target. Cross-origin pool admission
therefore still requires the configured whole-entity checksum. Broader RFC
9530/Metalink redirect identity remains a later gate.

Several documents defer to a "redirect policy" (`detailed-http-first-slice.md`,
`retry-policy.md`, `library-choice.md`) but none defined it. This document owns
redirect handling: limits, validator/range interaction, credential scoping, and
the SSRF/mirror boundaries. Redirects are budgeted separately from retry.

## Limits

- `--max-redirects=N` (default bounded, e.g. 20). Exceeding it fails the request
  with a redirect-limit error, not a retry.
- Loop detection: a redirect chain that revisits an already-seen (method, URL)
  pair fails as a redirect loop.
- `3xx` without a `Location`, or with an unparseable/relative-to-nothing
  `Location`, is a protocol error, not a retryable status.

## Validator And Range Interaction

A redirect during a range or resume request can land on a different entity, so
byte offsets do not transfer implicitly.

- If a response attempt already has a begun storage lease, following a redirect
  first issues `AbortLease`. The target request gets a new `LeaseId`; no byte
  received under the old attempt can become committed after the redirect.
- The follow-up is rebuilt and sends generated `Range` and
  `Accept-Encoding: identity` as applicable. It must still establish the same
  known total and exact `Content-Range` before `BeginLease` is issued.
- A same-origin redirect may resend a strong `If-Range`. A cross-origin redirect
  never forwards the old `If-Range`, because ETags and HTTP dates cannot prove
  identity across origins.
- A nonzero resume may continue across origins only when a configured/shared
  whole-entity digest proves that the durable prefix and target belong to the
  same representation. Otherwise policy must fail or create a new generation
  and restart at offset `0`; it cannot adopt the target's new validator at the
  old offset.
- For a split lease under `--verify-mirror-identity=off`, a cross-origin target
  is an exclusive replacement for the source of that lease: cancel/abort any
  competing duplicate, do not add the target to the mirror pool, and do not race
  it against another origin for the same span.
- Under `--verify-mirror-identity=strict`, a redirect target may join the mirror
  pool only when the persisted whole-entity checksum already authorizes the
  cross-origin representation. The executable `Repr-Digest` exact-range gate
  applies only to explicitly submitted mirrors and cannot admit a redirect
  target as an ordinary or endgame peer.

A `200` response to any nonzero follow-up range never writes at that offset; it
enters the normal fail/new-generation restart policy.

## Custom Header Rebuild

Redirects do not forward the previously serialized request header map. The
client starts with the task's validated custom-header list, re-applies the
case-insensitive reserved-header policy in `detailed-http-first-slice.md`, and
then generates `Host`, framing, range, encoding, validator, authentication, and
proxy-authentication fields for the new hop.

A custom header containing invalid syntax, CR/LF, or a reserved field is rejected
at task validation, so it cannot survive by changing case or by appearing twice.
Harmless custom headers remain configured across redirects. Authorization,
cookies, signatures, and other origin-bound fields are never classified as
harmless custom headers and are re-derived under the rules below.

## Credential Scoping (Security)

Redirects are a classic secret-exfiltration path. Header propagation is scoped,
not blindly forwarded:

- Drop `Authorization` when the redirect changes origin (scheme, host, or port).
  Re-derive credentials for the new origin only from configured per-host
  credentials or `.netrc`, never by forwarding the prior header.
- Cookies follow the cookie jar's host scoping (`protocol-modernization.md`
  "Cookie Jar"); a cookie set for origin A is not sent to unrelated origin B.
- Proxy credentials are re-evaluated for the new target host and proxy.
- Refuse `https -> http` downgrade by default (`--allow-redirect-downgrade` to
  opt in); a downgrade silently drops TLS protection.

## SSRF And Network Policy

Redirect targets are attacker-influenced when RPC is remotely exposed, so they
flow through the same guardrail as the initial request:

- Resolve and re-check the redirect target against the SSRF guardrail in
  `security-recovery.md` (generated non-global/special-use and metadata-endpoint
  denial, resolve-and-pin against DNS rebinding).
- Redirect targets obey `network-allowlist`/`network-denylist` and no-proxy
  policy exactly as an initial URL does.
- If the redirect changes proxy selection, validate both the new proxy endpoint
  and the final target again. For an untrusted remote-RPC task, proxy-side DNS or
  a hostname-form `CONNECT` is refused unless an administrator configured that
  proxy as a trusted destination-enforcing hop. Otherwise the client locally
  resolves and validates the target, sends the pinned numeric address to
  SOCKS/`CONNECT`, and preserves the original hostname only in TLS SNI,
  certificate verification, and the generated HTTP `Host` field.

## Tests

- redirect chain exceeding `--max-redirects` fails cleanly,
- redirect loop detected and failed,
- cross-origin redirect drops `Authorization` and unrelated-host cookies,
- `https -> http` redirect refused by default, allowed with opt-in,
- ranged/resume request redirected cross-origin does not continue at the prior
  durable resume offset without a whole-entity digest (fails or restarts from
  `0` in a new generation),
- split redirect under identity mode `off` replaces only that lease's source and
  never joins/races the mirror pool,
- strict redirect admission without a persisted whole-entity checksum cannot
  promote a target even if it advertises `Repr-Digest`,
- redirect aborts any begun provisional lease before a new `LeaseId` is used,
- harmless custom headers are rebuilt while reserved, credential, and framing
  fields cannot be overridden or blindly forwarded,
- redirect target to a private/metadata address is refused under the SSRF
  guardrail,
- redirect through untrusted `socks5h` or hostname `CONNECT` is refused; the
  locally resolved pinned-address form retains the original Host/SNI.
