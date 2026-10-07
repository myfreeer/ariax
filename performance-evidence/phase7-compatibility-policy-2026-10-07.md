# Phase 7 Compatibility And Crypto Policy Follow-Up

This local follow-up implements the first straightforward aria2 compatibility
slice and the requested OpenSSL reuse policy. Ariax remains unreleased and
separate from aria2. The historical latency failures and real power-loss
recovery campaign are explicitly deferred; native platform acceptance remains
open. Detailed behavior and next decisions are in
[Aria2 compatibility progress](../docs/project/aria2-compatibility.md).

## Changes

Direct add commands normalize long separated/inline values, registry short
aliases and bare booleans before typed admission. Startup supports `-i` session
input and separated `--profile`. Alias collisions, invalid bounds, unavailable
features and unsupported path/continuation overrides retain rejection. Twenty
upstream protocol names are correctly classified; generated contracts now
reject false upstream/extension labels. Regeneration also refreshes the existing
journal `recovery_memory_limit` error, which was missing from the checked-in
inventory after the earlier replay-budget repair.

Engine BT and CLI full/compat automatically reuse OpenSSL for rustls TLS and
SSH RSA. Either legacy crypto selector chooses the combined backend without
implicitly enabling SFTP. Non-OpenSSL builds retain ring TLS and the documented
RustCrypto RSA limitation; RUSTSEC-2023-0071 remains visible. Other SSH algorithms,
key formats and certificate processing still use their existing dependencies.

The package helper resolves the actual backend for each bundle. Historical
removed artifacts keep their recorded backend; current full packages use the
OpenSSL identity. Sixteen refreshed dependency closures contain no new notice
packages. Current Windows binaries retain only Windows-provided DLL imports.

## Validation

The [machine-readable evidence](phase7-compatibility-policy-2026-10-07.json)
records test totals, exact compiled-source hashes, feature selections, final
binary hashes/imports, path audits, CLI persistence/import checks and package
smoke results. Both final builds request only `--features full`, proving that
OpenSSL no longer needs a second selector. These builds reuse the reviewed
native dependencies and do not claim a new independent reproducibility result.

TLS coverage checks the actual selected provider, classical groups, TLS version
policy, trusted HTTPS and hostname rejection. CLI success/rejection checks run
with full features and without them; configuration tests and focused Clippy
cover the changed frontend. Real CLI checks reopen stored options, import a
paused aria2 session through `-i`, and verify rejection leaves no session state.
The CLI smoke fixture initially expected an explicit default `ftp-pasv=true`
in `getOption`; existing persistence omits that default. The corrected fixture
asserts changed values and keeps default omission visible as a semantic
compatibility difference rather than changing the storage contract here.

Package smoke uses reduced developer environment, verifies the file inventory,
queries live RPC features, checks unknown-method rejection and EOF shutdown,
and reopens SQLite. Linux's existing system preload is recorded; this does not
establish fresh/minimum OS acceptance. No CI dispatch or additional TSan campaign
was needed for this feature-selection and argument-parsing change.

## Retention And Remaining Work

Keep only the final Linux and Windows build sets, their reusable toolchains and
dependencies, and final useful reports. Temporary test targets, staging copies
and superseded production compile units are removed after validation.

Real power-loss recovery is deferred to a VM campaign with an explicit
storage/cache model. A VM result does not establish physical-device durability.
Deferred timing failures remain unresolved. Other-platform/minimum-OS acceptance,
the complete release matrix and the compatibility decisions above still gate
full Phase-7 closure and any aria2c replacement artifact.
