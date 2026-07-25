# Retry Policy

Status: draft.

Retry behavior must be configurable, bounded, observable, and safe. A retry
policy may decide whether to retry a failed transfer span, but it must not
weaken range validation, storage placement, checksum validation, or overwrite
rules.

## Goals

- Keep aria2-compatible options: `max-tries`, `retry-wait`,
  `max-file-not-found`, `max-resume-failure-tries`, `timeout`,
  `connect-timeout`, and `lowest-speed-limit`.
- Add explicit controls for reset, stale connection, hang, retryable HTTP
  status codes, `Retry-After`, maximum attempts, and maximum wait.
- Make every retry decision visible in task status and diagnostics.
- Avoid retry storms against busy servers.
- Avoid server-controlled unbounded sleeps.

## Error Classes

The retry engine classifies failures before applying policy:

```text
ConnectTimeout
ReadTimeout
NoProgressHang
LowestSpeed
ConnectionReset
UnexpectedEof
StaleConnection
StaleValidator
DnsTransient
HttpStatus(code)
InvalidRangeResponse
ShortBody
OversizedBody
ChecksumMismatch
DiskError
UserCancelled
ProxyConnect
ControlConnection
DataConnection
RangeUnsupported
AuthFailure
UnsupportedRepresentation
UnsafeDestination
InvalidRequestHeader
```

The following classes extend the taxonomy for proxy, non-HTTP, representation,
and request-policy failures:

- `ProxyConnect`: a proxy `CONNECT` tunnel or SOCKS handshake failed, distinct
  from an origin failure so the retry decision can target the proxy hop (see
  `protocol-modernization.md`).
- `ControlConnection`: an FTP/SFTP control/session connection failed or dropped
  (see `detailed-ftp-sftp.md`).
- `DataConnection`: an FTP data-channel setup failed or the transfer connection
  closed prematurely.
- `RangeUnsupported`: HTTP range/SFTP random access is unavailable, or FTP lacks
  `SIZE`/the `REST` needed to resume a nonzero durable prefix. FTP does not enter
  arbitrary split mode; the class selects fresh sequential fallback, another
  source, or fail/new-generation restart as applicable.
- `AuthFailure`: credentials were rejected (FTP/SFTP login, or an exhausted
  proxy/HTTP auth challenge). Not retried by default without changed credentials.
- `UnsupportedRepresentation`: a fixed-layout request received chunked,
  unknown-length, or content-decoded data. It may try another source, but cannot
  silently switch to a growing layout or write decoded data at a range offset.
- `UnsafeDestination`: the final origin cannot be locally resolved/pinned or
  trusted at the proxy hop under the SSRF policy. It is a terminal policy failure
  for that route, not a transient proxy error.
- `InvalidRequestHeader`: custom-header syntax or a reserved-header conflict was
  rejected before sending. It is terminal until configuration changes.

`StaleConnection` means a reused HTTP keep-alive connection, pooled TLS session,
or protocol connection was closed or reset before a valid response/body could
complete. This is retryable by default after any begun attempt receives
`AbortLease`; a retry cannot adopt its provisional bytes merely because disk
writes were acknowledged.

`StaleValidator` means the remote entity no longer matches the resume
validator, for example changed ETag, changed Last-Modified where it is the only
validator, or a `412`/`416` response that proves the local partial data no
longer matches the remote object. This is not a normal range retry. It may only
fail or restart from offset 0 according to explicit restart/overwrite policy.

`NoProgressHang` is detected by monotonic time, not by packet arrival. The
stats sampler and retry timers continue to advance even when a socket is silent.

## Policy Options

Compatibility options:

```text
--max-tries=N
--retry-wait=SEC
--max-file-not-found=N
--max-resume-failure-tries=N
--timeout=SEC
--connect-timeout=SEC
--lowest-speed-limit=SPEED
```

New options:

```text
--retry-profile=aria2|conservative|aggressive|custom
--retry-on=reset,eof,timeout,hang,lowest-speed,stale-connection,dns-transient
--retry-on-http-status=408,425,429,500-504
--retry-on-http-status-add=CODE[,CODE|RANGE...]
--retry-on-http-status-remove=CODE[,CODE|RANGE...]
--retry-after=respect|ignore
--retry-after-max=SEC
--retry-after-min=SEC
--retry-backoff=fixed|exponential|exponential-jitter
--retry-max-wait=SEC
--retry-max-attempts=N
--retry-max-attempts-per-mirror=N
--retry-max-elapsed=SEC
--stale-validator-policy=fail|restart-if-safe|revalidate
```

`--max-tries` remains the aria2-compatible high-level attempt cap. Internally it
maps into the retry budget for a URI/range generation. `--retry-max-attempts`
is the explicit new spelling for users who want retry policy without relying on
aria2 terminology. If both are set, the stricter cap wins unless the option
registry marks one as an explicit override.

`--retry-on` is a set, not a boolean. `custom` profile requires this set and the
status-code set to be known in the resolved option snapshot.

## Profiles

`aria2`:

- preserve aria2-compatible defaults where practical,
- `max-tries=5`,
- `retry-wait=0`,
- retry transient transport failures and timeout paths,
- retry `504`,
- retry `502`/`503` only when `retry-wait > 0`,
- use `max-file-not-found` for repeated `404`/not-found cases.

`conservative`:

- default for the new engine,
- retry reset, unexpected EOF, timeout, hang, lowest-speed,
  stale-connection, and transient DNS,
- retry HTTP `408`, `425`, `429`, `500`, `502`, `503`, and `504`,
- respect `Retry-After` with a bounded cap,
- use exponential jittered backoff,
- do not retry `400`, `401`, `403`, `404`, `405`, `409`, `410`, `412`, `416`,
  or invalid range responses as ordinary transient errors.

`aggressive`:

- larger caps and broader status-code set,
- useful for unreliable mirrors,
- still cannot retry disk errors, user cancellation, unsafe stale validators,
  invalid placement, or repeated checksum-corrupt mirrors as if they were
  transient network failures.

`custom`:

- user-provided trigger set, status-code set, wait policy, and caps.

## Retry-After

`Retry-After` handling applies only when the response status is retryable by
policy. It must not make a non-retryable status retryable by itself.

Rules:

- accept both delta-seconds and HTTP-date forms,
- reject negative, overflowing, unparsable, or absurd values,
- clamp to `retry-after-min` and `retry-after-max`,
- then clamp again to `retry-max-wait`,
- add jitter unless `retry-backoff=fixed` and `retry-after` is exact,
- expose the selected delay and source in status.

Defaults:

```text
--retry-after=respect
--retry-after-min=0
--retry-after-max=300
--retry-max-wait=300
```

If `--retry-after=ignore`, the header is parsed only for diagnostics and normal
backoff decides the delay.

## Attempt Accounting

Retry budget is tracked at several scopes:

- task generation,
- URI/mirror,
- range lease or failed byte span,
- error class,
- HTTP status code,
- stale-validator restart count.

Counters are explicit because different failures have different blast radius:

- a connection reset should not consume the checksum-corruption budget,
- a corrupt mirror should not exhaust every other mirror immediately,
- a task-level stale validator restart must be rare and user-visible,
- disk errors are not retried as network attempts.

The scheduler chooses the next action in this order:

1. reject non-retryable correctness failures,
2. check task/generation cancellation,
3. if the failed attempt reached `BeginLease`, issue/confirm `AbortLease` and
   return its complete span to pending,
4. check per-span and per-mirror caps,
5. check global task retry cap,
6. compute wait using `Retry-After` and backoff,
7. release transfer buffers,
8. schedule a timer in the control/scheduler lane with a fresh `LeaseId`.

Retry wait never sleeps a worker thread and never holds a transfer buffer.

Clock rule: live retry timers use the monotonic clock. The persisted
`RetryState` deadline (`next_retry_unix_ms`) is wall-clock only because
monotonic time does not survive restart; on recovery it is re-clamped to at
most `retry-max-wait` from now, so a wall-clock jump can neither skip a
mandatory wait entirely nor stall a task far beyond the configured bound.

## Status Codes

Status-code sets support individual codes and inclusive ranges:

```text
408,425,429,500-504
```

Validation rules:

- codes must be integers from `100` through `599`,
- malformed ranges are rejected at option-parse time,
- duplicate codes are normalized,
- redirects are handled by redirect policy, not retry status policy,
- auth failures are not retried unless the auth challenge flow has new
  credentials to try,
- `404` remains governed by `max-file-not-found` unless the user explicitly
  adds it to retry status codes.

For range downloads, an HTTP status retry decision still requires range
correctness:

- `200 OK` to a nonzero range request is not a retryable success,
- invalid `206`/`Content-Range` marks the mirror range-invalid,
- `416` can trigger stale-validator handling or resume failure handling, but it
  must not write bytes at guessed offsets.
- a short/oversized body aborts the complete provisional lease attempt; the
  first slice does not turn its prefix into committed progress or retry only a
  guessed suffix.

## Stale Validator Policy

Options:

```text
--stale-validator-policy=fail|restart-if-safe|revalidate
```

`fail`:

- default in strict/resume-sensitive contexts,
- stops the task with a clear stale validator error.

`restart-if-safe`:

- allowed only when overwrite/restart policy permits replacing local partial
  data,
- creates a new task generation from offset 0,
- never mixes old partial bytes with the new entity.

`revalidate`:

- attempts a fresh HEAD/conditional request before deciding,
- falls back to `fail` or `restart-if-safe` according to the resolved policy.

Stale validator handling is separate from stale connection retry.

## Safety Rules

### Shared Lease, Endgame, And Redirect Rule

- Every network attempt that may write has a unique `(generation, LeaseId)` and
  begins a storage lease before body bytes are accepted. A retry, redirect,
  cancellation, short/oversized body, or failed validator/digest check must
  complete `AbortLease` before a replacement attempt is scheduled. Only exact
  protocol validation followed by `CommitLease` can publish progress.
- Same-offset duplicate attempts are always provisional. Same-mirror endgame is
  allowed only under one strong per-origin validator. Cross-mirror endgame is
  allowed only when the exact range has a shared digest that each candidate can
  verify before commit; strict mode backed only by a whole-file checksum does not
  qualify. The first hash-valid eligible `CommitLease` wins and all other leases
  are aborted.
- Under mirror-identity `off`, a cross-origin redirect is scoped as an exclusive
  replacement for the redirected split lease and cannot silently join or race
  the mirror pool. Under `strict`, pool admission requires the complete shared
  identity gate. For RFC 9530, field kind, algorithm, digest value, covered
  representation, content coding, and covered range must match—algorithm alone
  is not evidence of identity.
- A cross-origin redirect cannot continue a nonzero durable resume prefix without
  a shared whole-entity digest; it must fail or restart at `0` in a new
  generation.

These rules are normative for `split-download.md` and `redirect-policy.md`.

Never retry by writing into the same durable state when:

- the task generation changed,
- the response range is invalid,
- a mirror repeatedly produces checksum failures,
- the remote validator changed and restart is not allowed,
- local disk writes failed with ENOSPC, permission denied, or data corruption,
- the user cancelled, removed, or paused the task.

Retrying may reuse only already committed spans whose offset, generation, and
validation state storage can prove. Provisional or aborted bytes are never
inferred from file contents. The first slice retries the complete failed lease;
any future partial-promotion optimization must be an explicit storage operation.

Fixed-layout HTTP retries require identity coding and a known length. They never
auto-enable `GrowingSequential`. FTP retries resume one sequential stream from
the contiguous committed durable prefix; they do not schedule arbitrary FTP
range leases or drain per-lease tails.

Only `CommitLease` bytes debit the user-configured rate buckets. Bytes from a
failed/aborted attempt are surfaced as discarded and excluded from those buckets;
they instead consume the finite discard guard. Protocols cancel promptly, and
discard/retry/endgame budgets prevent retry cycling from becoming an unbounded
raw-network bypass.

## Observability

Expose per task and per lease:

- retry trigger,
- HTTP status code where applicable,
- mirror/URI,
- current attempt and remaining attempts,
- retry wait deadline,
- whether `Retry-After` was used, ignored, clamped, or invalid,
- stale connection vs stale validator classification,
- next action: same mirror, different mirror, sequential fallback, restart, or
  terminal failure,
- prior lease disposition (`committed`/`aborted`) and the new `LeaseId` when a
  retry is scheduled,
- policy failures distinguish unsafe destination, invalid custom header, and
  unsupported fixed-layout representation from transient proxy/network errors.

RPC and CLI status must update during retry waits without waiting for another
network packet.
