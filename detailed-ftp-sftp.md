# Detailed FTP And SFTP Design

Status: draft.

The HTTP-centric resume/validation model (`EntityValidator` with
ETag/Last-Modified, the HTTP `StaleValidator` classification, and
`206`/`Content-Range` range acceptance) does not map to FTP or SFTP. FTP ships in
the `standard` build profile, so it needs its own correctness contract. This
document defines resume offsets, FTP's sequential connection/data-channel model,
validators, transfer type, TLS, and the SFTP read model.

## FTP Connection And Data-Channel Model

FTP is not one multiplexed connection. It has a control connection plus a
separate data connection per transfer.

- One control connection per server session carries commands/responses.
- One `RETR` opens one data connection (PASV/EPSV preferred; PORT/EPRT only
  where the network allows and policy permits active mode) and streams from its
  start offset to EOF.
- The standard implementation permits only one active `RETR` for a file from a
  given FTP source. FTP has no server-side end offset, so it is not assigned
  arbitrary/noncontiguous split leases and does not participate in endgame.
- One `REST`/`RETR` response attempt has one `LeaseId` covering
  `[durable_prefix, SIZE)`. It calls `BeginLease` before reading data, keeps all
  writes provisional, and calls `CommitLease` only after exact EOF at `SIZE` and
  validator/checksum success. Premature close, cancellation, or validation
  failure calls `AbortLease`; the next attempt starts from the previously
  committed durable prefix. This follows the storage-owned attempt contract in
  `detailed-storage.md` without defining another FTP journal format.
- Distinct FTP mirrors are failover sources for the same file, not concurrent
  streams into one output. Other files/tasks may of course transfer in parallel.
- The control connection may be reused across sequential transfers; each `RETR`
  data connection is closed at EOF or immediately on cancellation/failure and is
  counted against the per-host connection budget.

## Resume Offset (REST)

FTP has no `Range` header. Resume uses `REST`:

- `SIZE <path>` establishes total length (binary mode only; see Transfer Type).
- `durable_prefix` is the end of the largest contiguous sequence of committed
  storage spans starting at byte `0`; sparse later pieces never advance it.
- Resume sends `REST <durable_prefix>`, requires a successful restart reply, and
  then sends `RETR <path>`. Incoming byte `n` maps to
  `global_offset = durable_prefix + n` until the known `SIZE` is reached.
- The server streams from that offset to EOF. The client does not open another
  connection at a later lease boundary and does not read/discard a tail for an
  artificial range.
- `SIZE` is required for the immutable fixed layout. `REST` is additionally
  required when `durable_prefix > 0`. If `SIZE` is unavailable, the standard
  implementation fails that source; an unknown-length FTP download would require
  the same explicit future growing-layout capability as other sequential
  protocols. If `REST` is unavailable, a fresh transfer may start at `0`, while
  an existing partial task must fail or restart in a new generation according to
  resume/overwrite policy.
- EOF before `SIZE - durable_prefix` aborts the current provisional response
  attempt and is `UnexpectedEof`/`DataConnection`; bytes beyond the advertised
  `SIZE` are a protocol error, cause prompt close, and are discarded under the
  separate discard guard rather than user-rate accounting.

## Validators

FTP has no ETag. The only resume validators are:

- `SIZE`: total length. A changed `SIZE` invalidates resume (equivalent to a
  `StaleValidator` in the HTTP model).
- `MDTM`: last modification time. A changed `MDTM` invalidates resume.
- Optional configured checksum/digest (from Metalink or user), which is the only
  strong validator available for FTP.

Resume requires `SIZE` unchanged plus `MDTM` unchanged, or a configured
checksum. Without any of these, resume is unsafe: restart from 0 or require an
explicit unsafe override. SFTP uses file size plus mtime from `fstat` the same
way.

## Transfer Type

- All fixed-layout/resumable transfers MUST use binary mode (`TYPE I`). ASCII mode
  (`TYPE A`) performs line-ending translation, which changes byte counts and
  makes `SIZE`, `REST` offsets, and byte placement incorrect.
- `ftp-type=ascii` is therefore incompatible with the immutable fixed layout and
  resume. It is outside the standard implementation and would require an
  explicit growing sequential mode with separate wire/output counters.

## FTPS (TLS)

- Explicit FTPS (`AUTH TLS` on the control connection) is the default secure
  mode; implicit FTPS (TLS from connect on a dedicated port) is supported where
  configured.
- Both the control and each data connection are protected (`PBSZ 0` / `PROT P`).
  A data connection that is not TLS-wrapped when the control connection is
  encrypted is rejected, not silently sent in the clear.
- Certificate validation follows the same trust configuration as HTTPS
  (`protocol-modernization.md` TLS).

## SFTP Read Model

- SFTP runs over SSH; the library choice (libssh2 vs russh) is per
  `library-choice.md` and must not block network reactor threads.
- Reads are random-access (`SSH_FXP_READ` at explicit offsets), so a range lease
  maps to offset reads without a `REST` equivalent. Total length and mtime come
  from `fstat`.
- Read-ahead/window sizing follows the library's flow control; SFTP does not open
  a new connection per range (unlike FTP), so multiple leases share one SSH
  channel subject to window limits.

## Retry Classes

FTP/SFTP failures feed the retry engine with protocol-specific classes distinct
from HTTP status codes:

- control-connection failure (login, `PASV`/`EPSV` negotiation),
- data-connection failure or premature close,
- `SIZE` unsupported (fixed-layout source unsupported),
- `REST` unsupported for a nonzero durable prefix (resume-unsupported source),
- transient transfer errors (retryable per policy),
- authentication failure (terminal unless credentials change).

## Tests

- `REST`/`RETR` resume writes at the correct durable offset,
- a single `RETR` uses one provisional `LeaseId` for the exact remaining span,
- a failed stream aborts its attempt and retries from the prior committed
  contiguous durable prefix,
- changed `SIZE` or `MDTM` invalidates resume (restart or fail),
- `SIZE`-unsupported source fails fixed-layout transfer; `REST`-unsupported
  source can start fresh but cannot resume a partial generation,
- ASCII mode rejected for fixed-layout/resume,
- FTPS data connection without `PROT P` rejected when control is encrypted,
- data-channel premature-close retries the complete uncommitted remaining span,
- SFTP offset reads place bytes at correct global offsets,
- FTP never receives noncontiguous split leases or endgame duplicates,
- one sequential FTP data connection is counted against the per-host budget.
