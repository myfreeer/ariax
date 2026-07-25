# Detailed FTP And SFTP Design

Status: reviewed pre-implementation contract. Implementation pending.

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
- One `REST`/`RETR` data stream has one `TransferAttemptId` and advances through
  successive piece-aligned storage `LeaseId`s from `durable_prefix` to `SIZE`.
  The same data connection streams continuously from response reads into bounded
  buffers and offset writes; a lease boundary checkpoints metadata and opens the
  next storage lease without another FTP command or connection.
- Each exact non-final storage span commits after its disk acknowledgements. The
  final span additionally requires exact EOF at `SIZE`. Premature close or
  cancellation aborts only the current incomplete lease; earlier checkpoints
  remain resumable if `SIZE`/`MDTM` or the configured checksum still proves the
  representation. Representation validation failure invalidates the affected
  checkpoints through the normal restart/hash-failure policy.
- Client download-rate tokens gate the FTP data-channel `read`, not `write_at`.
  Once bytes are accepted and charged, storage writes them without another
  rate-limit wait. This follows the storage-owned attempt contract in
  `detailed-storage.md` without defining another FTP journal format.
- Distinct FTP mirrors are failover sources for the same file, not concurrent
  streams into one output. Other files/tasks may of course transfer in parallel.
- The control connection may be reused across sequential transfers; each `RETR`
  data connection is closed at EOF or immediately on cancellation/failure and is
  counted against the per-host connection budget.

### FTP Data-Endpoint Authorization

An FTP reply is not authority to connect to an arbitrary address. EPSV is
preferred because it supplies only a port; the client always combines that port
with the already resolved, policy-approved control-peer address. For legacy
PASV, `ftp-pasv-address=control-peer|server` defaults to `control-peer`, ignoring
the reply's advertised address while honoring its validated port. `server` is
an explicit local-admin compatibility mode: the advertised address must pass
the complete allow/deny, non-global/private-address, proxy, DNS-pinning, and
per-host budget policy before connect, and an untrusted remote RPC caller cannot
enable it. Hostnames are never accepted inside a numeric PASV tuple.

Active PORT/EPRT mode is off unless explicitly selected. The client creates one
bounded listener on its configured local interface, advertises only that local
address/ephemeral port, and accepts exactly one data connection before the
deadline. The peer address must equal the approved control peer (or the same
explicit administrator allowlist used for PASV); mismatches are closed before
any FTPS handshake and do not satisfy the transfer. The patched accept loop
continues until the approved peer or deadline, but fails after 32 rejected peers
to bound callback abuse. A task/RPC input cannot choose an arbitrary bind
address or third-party callback target. Active mode is direct-connection only;
proxy use is rejected because the callback cannot inherit the proxied control
path. Passive and active data sockets both consume the ordinary socket/host
budgets and repeat policy after a control reconnect or proxy change.

The adapter always obtains the control socket from the downloader-owned
connector and calls SuppaFTP `connect_with_stream`, then installs the
project-owned passive stream builder. It never calls the crate's address-taking
connect helpers, uses its default passive builder, or enables its NAT workaround.
The pinned patch exposes the local-bind and active-peer policy hook described
above; unpatched active mode's first-peer acceptance is forbidden.

These rules prevent PASV bounce/SSRF and active-mode callback abuse without
changing storage/retry semantics. A rejected data endpoint is
`UnsafeDestination`, not a transient `DataConnection` error for another blind
retry to the same endpoint.

## FTP Control-Reply Bounds

The selected patched SuppaFTP parser enforces limits before growing a line or
aggregate reply buffer. The fixed baseline limits are 64 KiB including CRLF for
one control line, 1 MiB for one complete single/multiline reply, and 4096 lines.
They apply to greeting, ordinary replies, errors, and FEAT continuation parsing.
Every retained byte takes `task_metadata_budget` plus the global resident permit;
diagnostics keep only the existing capped/redacted message form.

Crates.io SuppaFTP 10.0.1 and current upstream main use unbounded `read_until`
and FEAT vectors and are forbidden by the dependency-source assertion. The
pinned patch scans bounded input before append, treats a line/aggregate/count
overflow or malformed multiline terminator as a fatal source protocol error,
and closes the control connection. It never tries to resynchronize after an
over-cap reply. The downloader does not call unbounded directory-list
convenience APIs; adding directory downloads later requires a separate bounded
listing contract.

The same patch separates command serialization from diagnostics and removes all
raw FTP wire/path logging. It emits only safe verbs/status codes and bounded
length/count metadata; it never formats USER/PASS/ACCT arguments, SITE/custom
commands, remote paths, welcome/reply/FEAT text, or data listings. Logger-side
redaction and SuppaFTP's workspace-global `no-log` feature are not accepted as
substitutes. The project wrapper owns capped/redacted protocol diagnostics.

## Resume Offset (REST)

FTP has no `Range` header. Resume uses `REST`:

- `SIZE <path>` establishes total length (binary mode only; see Transfer Type).
- `durable_prefix` is the end of the largest contiguous sequence of committed
  storage spans starting at byte `0`; sparse later pieces never advance it.
- Resume sends `REST <durable_prefix>`, requires the intermediate `350` restart
  reply, and then sends `RETR <path>`. Incoming byte `n` maps to
  `global_offset = durable_prefix + n` until the known `SIZE` is reached.
- The server streams from that offset to EOF. The client does not open another
  connection at a later storage-lease boundary and does not read/discard a tail
  for an artificial network range.
- `SIZE` is required for the immutable fixed layout. `REST` is additionally
  required when `durable_prefix > 0`. If `SIZE` is unavailable, the standard
  implementation fails that source; an unknown-length FTP download would require
  the same explicit future growing-layout capability as other sequential
  protocols. If `REST` is unavailable, a fresh transfer may start at `0`, while
  an existing partial task must fail or restart in a new generation according to
  resume/overwrite policy.
- EOF before `SIZE - durable_prefix` aborts the current incomplete storage lease
  and is `UnexpectedEof`/`DataConnection`; previously committed checkpoints stay
  resumable under the validator rule. Bytes beyond advertised `SIZE` are a
  protocol error, cause prompt close, consume normal received-payload rate
  tokens, and also consume the separate discard guard.

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

- SFTP runs over SSH through russh plus the pinned patched russh-sftp selected
  in `library-choice.md`; the adapter must not block network reactor threads.
- Reads are random-access (`SSH_FXP_READ` at explicit offsets), so a range lease
  maps to offset reads without a `REST` equivalent. Total length and mtime come
  from `fstat`.
- Read-ahead/window sizing follows the library's flow control; SFTP does not open
  a new connection per range (unlike FTP), so multiple leases share one SSH
  channel subject to window limits.

## SFTP Host-Key Verification And Approval

Host-key verification happens after SSH key exchange and before any password,
private-key, agent, or keyboard-interactive authentication is attempted.

Resolution order:

1. If the task supplies an exact public-key pin or SHA-256 host-key fingerprint,
   the presented key must match it.
2. A matching entry in the configured private `known_hosts` file is accepted.
3. `ssh-host-key-md=TYPE=DIGEST` is supported for aria2 compatibility. SHA-1 and
   MD5 forms are legacy/unsafe-compat inputs with a warning; modern configuration
   emits SHA-256 fingerprints.
4. `sftp-check-host-key=false` is an explicit insecure bypass analogous to
   `check-certificate=false`. It is visible in status/diagnostics and cannot be
   enabled by an untrusted remote RPC policy.
5. Otherwise an unknown, well-formed host key transitions the task to
   `PausedHostKey` before credentials or file requests are sent.

An explicit pin or known-host entry that mismatches is terminal
`HostKeyMismatch`; it is not converted into an approval prompt.

The `known_hosts` reader uses the exact `russh::keys::ssh_key` parser pinned and
re-exported by the selected russh version, behind an owned matcher. It supports
canonical host and `[host]:port` patterns, negation, wildcards, and OpenSSH
hashed-host entries. `@revoked` is a terminal rejection.
`@cert-authority` is feature-gated until host-certificate principal/time/CA
validation has its own interoperability matrix; a CA key is never mistaken for
an ordinary exact host key. The file is streamed/capped at 8 MiB, 65,536
entries, and 64 KiB per line. A malformed matching entry fails closed; malformed
unrelated lines produce one bounded diagnostic and are skipped. Matching uses
the original canonical hostname/port, never a reverse-DNS name invented from
the selected address.

The paused challenge contains a challenge id, canonical host and port, key
algorithm, and SHA-256 fingerprint. Host/algorithm display strings are at most
253/64 bytes, the encoded public key blob is at most 16 KiB, and exactly one
current challenge is retained per task; an over-cap key/challenge is a terminal
protocol error rather than a persistence allocation. The handshake connection may be closed to
release resources. Normal scheduler/RPC `Resume`/aria2 `unpause` does **not**
approve trust; it leaves the task paused and returns
`HostKeyApprovalRequired`. Approval uses the extension/native
`ApproveHostKey(gid, challenge_id, fingerprint_sha256)` operation (CLI:
`ariax approve-host-key ...`). Both displayed values must match the current
challenge, after which the exact key is pinned in persistence-safe task
metadata and normal readmission starts a fresh generation. A raced/stale
challenge fails without sending credentials; if reconnect presents another
key, the task remains paused with a new challenge. Approval is task-scoped and
does not silently edit a global `known_hosts` file.

Interactive CLI mode prints host/port, algorithm, and SHA-256 fingerprint and
asks the user to allow or stop. Allow invokes the explicit challenge-bound
approval operation; stop removes the task with a host-key-rejected diagnostic.
Without an attached TTY, the CLI never auto-approves: it leaves the task paused
and prints the fingerprint plus the explicit approve/bypass/pin choices.

Approval state and the accepted task pin are not secrets and may be persisted.
Logs/events include the fingerprint but never authentication credentials or
private-key material.

## SFTP Authentication, Algorithms, And Session Policy

Authentication runs only after host-key resolution succeeds and tries, in
order, each configured method the server offers: explicit private key
(`sftp-private-key`, with `sftp-private-key-passphrase` or an interactive
prompt for encrypted keys), SSH agent when `sftp-use-agent=true`, then
password from task options/`.netrc` (FTP credential options apply). There is
no keyboard-interactive support in the first SFTP slice beyond single-prompt
password equivalence. A method the server rejects is not retried with the same
credentials; exhaustion is terminal `AuthFailure`. Passphrases and passwords
follow the `Secret<T>` and secrets-at-rest rules; decrypted key material lives
only for the handshake.

Algorithm policy follows russh defaults minus legacy algorithms: no SHA-1 KEX
(`diffie-hellman-group14-sha1` and older), no `ssh-rsa` (SHA-1 signature) host
keys or client keys, no CBC ciphers, no `hmac-md5`/`hmac-sha1-96`. The
accepted set is pinned in the registry (a diagnostic lists the negotiated
KEX/host-key/cipher/MAC per connection) so a russh default change cannot
silently widen it. An `unsafe_compat` build flag may re-enable legacy
algorithms explicitly; there is no runtime silent fallback.

Session behavior:

- `connect-timeout`/`timeout` apply to TCP+handshake and per-request
  inactivity; rekeying follows russh's RFC 4253 data/time limits,
- SFTP connections honor the same proxy option surface as other protocols
  where a tunnel applies (`all-proxy` CONNECT/SOCKS with the SSRF
  destination-pinning rules); SSH-level jump hosts are out of scope,
- server `limits@openssh.com`/version responses bound read-request size; the
  adapter clamps its offset-read size accordingly,
- remote paths are exchanged as bytes and interpreted as UTF-8 with an
  explicit failure (no lossy conversion) for display/layout mapping; the
  remote path is a user input, not attacker metadata, but still passes
  `SafePathBuilder` for any local output naming derived from it,
- the adapter never follows a remote symlink for layout decisions: `fstat` on
  the opened handle (not `stat` on the path) supplies size/mtime, so a
  symlinked remote file transfers as its target content without local path
  influence.

Bounded offset pipeline: the adapter keeps at most
`sftp-max-outstanding-reads` (default 8, hard maximum 64, further capped by the server window and
the separate `sftp_ingress_budget`) offset reads in flight per channel, sized by
the current `RatePermit`, server limit, remaining span, and a default 64 KiB
request cap (hard maximum 1 MiB and never larger than the negotiated packet
payload). The selected patched russh-sftp 2.3.0 returns each raw offset response
as an owned `Data.data: Vec<u8>` after first reading a capped packet buffer. The
unpatched crates.io 2.3.0 receive loop passes `u32::MAX` to its packet reader and
is forbidden. The pinned patch threads the configured 128 KiB default (hard
maximum 1 MiB, clamped down by the server extension) into every inbound read;
an over-cap prefix or malformed frame is fatal before payload allocation,
cancels the SFTP channel, and completes all outstanding requests with a typed
error rather than continuing on a desynchronized stream. Request admission
reserves the packet cap plus requested data length—the temporary decode
double-allocation worst case—in SFTP ingress before send, charges both until the
packet/vector are released, then copies the data into a reserved `BufferLease`
and releases the vector immediately.
There is no whole-file/high-level `read` path. Completions integrate with the
standard cancellation, generation, retry, and storage-lease contracts; a
cancelled request drains to its completion before either allocation is reused.

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
- a single `RETR` uses one `TransferAttemptId` and multiple piece-aligned
  `LeaseId`s without opening another data connection at lease boundaries,
- a failed stream aborts only its current incomplete lease and retries from the
  latest contiguous durable checkpoint,
- changed `SIZE` or `MDTM` invalidates resume (restart or fail),
- `SIZE`-unsupported source fails fixed-layout transfer; `REST`-unsupported
  source can start fresh but cannot resume a partial generation,
- ASCII mode rejected for fixed-layout/resume,
- FTPS data connection without `PROT P` rejected when control is encrypted,
- EPSV/PASV defaults connect only to the approved control peer; a PASV-advertised
  different host is refused unless explicit admin `server` mode passes the full
  destination policy,
- active mode accepts one timely connection only from the approved control peer
  and cannot be configured by untrusted RPC as a third-party callback,
- a mismatched active peer is closed before TLS and the patched loop can still
  accept the approved peer; deadline and 32-rejection caps are enforced, while
  unpatched first-peer acceptance and proxied active mode are rejected,
- every control connection uses the downloader connector plus
  `connect_with_stream`, and every passive data connection uses the owned
  endpoint-validating builder; default connect/build/NAT-workaround paths fail
  the dependency-use assertion,
- FTP greeting, ordinary, multiline, and FEAT replies reject a line above
  64 KiB, an aggregate above 1 MiB, or more than 4096 lines before proportional
  allocation; overflow closes the connection and the unpatched crate fails the
  Phase-0 source/provenance assertion,
- canary user/password/path/custom-command/reply secrets never appear through
  the patched dependency at any enabled log level; safe records contain only
  verb/status/length/count metadata,
- data-channel premature-close preserves earlier durable checkpoints and retries
  from the current pending span,
- SFTP offset reads place bytes at correct global offsets,
- SFTP offset admission accounts the packet-buffer plus returned-vector peak;
  an over-cap packet length is rejected before payload allocation and cannot
  exceed `sftp_ingress_budget`,
- the patched receive driver reads only the four-byte prefix before rejecting an
  over-cap frame, closes on malformed framing without resynchronization, and
  completes every pending request exactly once; an unpatched dependency fails
  the Phase-0 source/provenance assertion,
- explicit host-key/SHA-256 pin match succeeds and mismatch is terminal before
  authentication,
- an unknown key pauses before credentials, exposes a stable challenge, and
  generic `Resume` fails with `HostKeyApprovalRequired`,
- explicit approval pins only the exact current challenge/fingerprint and a
  stale challenge fails without authentication,
- a changed key on reconnect creates a new paused challenge rather than using
  the prior approval,
- `sftp-check-host-key=false` is explicit, observable, and rejected by untrusted
  remote RPC policy,
- interactive CLI allow/stop and non-TTY no-auto-approve behavior,
- FTP never receives noncontiguous split leases or endgame duplicates,
- one sequential FTP data connection is counted against the per-host budget.
