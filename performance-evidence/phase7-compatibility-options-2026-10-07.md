# Phase 7 Compatibility Options Follow-Up

The three authorized compatibility items are implemented: effective `getOption`
values, bounded live `max-concurrent-downloads` with startup `-j`, and public HTTP
header options. The [machine-readable evidence](phase7-compatibility-options-2026-10-07.json)
binds final compiled inputs, checks, retained binaries and package results.
This remains local evidence; Phase 7 and release approval stay open.

## Behavior

- `getOption` reports executable transfer defaults, including `ftp-pasv=true`
  when FTP is compiled. Secret/admin options and internal bindings are omitted.
  Query projection retains the response budget without expanding saved maps.
- `max-concurrent-downloads` applies to shared scheduler slots. Lowering the
  limit lets running work drain; increasing it admits waiting work. Zero or a
  value above bootstrap capacity rejects the whole configuration patch. Reload
  and startup `-j N` / `-jN` use the same policy.
- Repeated CLI `header`, RPC header arrays/newline strings, `user-agent` and
  `referer` reach probes, payload ranges and metadata fetches. `Host`,
  `Authorization`, `Proxy-Authorization` and `Cookie` override generated fields.
  Other reserved fields, including `Range`, framing, encoding and digest fields,
  reject with a warning that names the field without printing its value.
- Origin-bound custom headers are stripped across origins, including range
  requests made after a redirecting probe. Proxy authorization goes only to the
  selected HTTP proxy, including CONNECT; Host changes do not alter DNS or TLS
  identity. Printable ASCII values and case-insensitive duplicate rejection keep
  parsing deterministic.
- Header/referer values remain volatile and redacted. Recovery retains a
  non-secret requirement; URI replacement alone cannot bypass it. Waiting or
  paused tasks can resupply headers through `changeOption`, preserving pause
  intent and existing progress. A missing signed URI still requires replacement.

The registry now contains 112 options: 58 shared upstream names and 54 Ariax
extensions, against 207 pinned handlers. Name coverage does not establish
semantic parity. Broader choices remain in the
[compatibility decision document](../docs/project/aria2-compatibility.md).

## Local Validation

Linux/WSL and native Windows-GNU each pass 40 focused full-engine tests, 11
minimal-engine tests, 16 CLI tests, 21 configuration tests, and two focused
scheduler/driver tests. Coverage includes actual HTTP/proxy wire values,
redirected ranges, background workers, secret redaction, restart and JSON import,
header resupply, signed-URI rejection, effective defaults, idle-only limit
updates, draining and atomic configuration rejection.

Focused Clippy covers affected libraries, binaries and tests on both hosts.
The initial broader invocation reached an unrelated diagnostics-disabled
benchmark dead-code warning; the completed focused invocation excludes benches.
Generated contracts, formatting, documentation, 16 protocol feature selections,
40 release-tool tests and four publication tests are checked. Final CLI smoke
checks exercise startup `-j`, live mutation, header rejection warnings, effective
options, restart and header resupply through stdio RPC. Package checks use a
reduced environment and verify loaded modules and database reopening. Linux
functional checks pass with the existing `/usr/local/lib/libnanosleep.so` host
preload present; fresh-OS module-closure acceptance is not claimed.

The retained full CLIs use automatic OpenSSL TLS/RSA selection. Windows imports
only Windows-provided DLLs; no separate C++, GCC, thread or OpenSSL DLL is needed.
Native dependencies are reused. These builds do not claim a new independent
reproducibility campaign. Temporary test, Clippy and package outputs are removed;
one final build directory per host remains.

## Remaining Acceptance

Unreproduced Windows `changeUri`/`addUri` and Linux mixed-burst latency failures
remain deferred. Real power-loss testing remains deferred to a VM campaign.
Supporting native Linux kernel/io_uring, fresh/minimum OS, macOS/MSVC and complete
release acceptance remain external gates. Existing aria2 partial-file/control-file
migration is excluded. The previous fully instrumented local TSan result remains
historical evidence; this change introduces no new unsafe adapter or sanitizer
CI/std-build requirement. No push, CI dispatch, tag or release is performed.
