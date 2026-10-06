# Phase 7 OpenSSL, Static Runtimes And Local TSan

Status: the authorized local goal is complete, including artifact cleanup and
fully instrumented local TSan. This is not release approval.
The [machine-readable report](phase7-openssl-static-tsan-2026-10-07.json) records
build identities, checks and platform limits.

## Crypto Selection

`--no-default-features --features full,crypto-openssl` selects OpenSSL for the
rustls TLS provider and SSH RSA authentication/exchange signatures. Separate
`tls-openssl` and `sftp-openssl-rsa` features permit independent selection; the
RSA feature does not enable SFTP by itself. OpenSSL takes precedence when
features unify, with no fallback after failure. Default backends remain supported.
TLS retains trust, hostname, version and classical-group policy. RSA retains
SHA-256/SHA-512 PKCS#1 signatures and existing public-key admission bounds.

This consolidates TLS and RSA onto the existing OpenSSL installation. Ring is
still used for SSH AEAD, and RustCrypto remains for key formats, admission,
certificate verification and other SSH algorithms. `RUSTSEC-2023-0071` is not
ignored or declared resolved. The [six-package advisory delta review](phase7-openssl-advisories-2026-10-07.json)
uses the recorded RustSec snapshot; it is not a complete new cargo-audit run.

## Local Validation

- Four default bundles and OpenSSL minimal/standard/full passed on both hosts:
  14 CLI builds with path/import audits and reduced-PATH CLI checks.
- Three RSA success/rejection tests and eleven TLS tests passed on each host;
  SFTP/FTPS security aggregates passed on each host, and default TLS passed Linux.
- Full OpenSSL CLI checks passed Rust 1.88 MSRV on both hosts. Focused CLI,
  engine and BT Clippy passed all targets/features on both hosts.
- Both final full OpenSSL packages passed RPC/features, unknown-method rejection,
  shutdown and database reopening. Windows loads only application/system modules.
  Linux retains its existing system preload and is not fresh-OS acceptance.
- The updated inventory covers 16 selections, 320 packages and 240 unique
  notice texts. All 157 Python helper tests, workspace formatting, vendor
  provenance, documentation, manifest and publication checks pass.

Windows statically links GCC, C++, threading and OpenSSL runtimes. The CLI has
no `libstdc++-6.dll`, `libgcc_s_seh-1.dll`, `libwinpthread-1.dll` or OpenSSL DLL
dependency. OpenSSL minimal/standard Linux builds require glibc 2.38; default
minimal/standard require 2.34. Full/compat retain glibc 2.38 and GLIBCXX 3.4.30.

## Local TSan

The official 2026-10-06 nightly and matching rust-src were checksum-verified and
installed in isolation after normal validation and cleanup. Rust 1.101.0-nightly
uses LLVM 23.1.3; this run explicitly selects Clang 19.1.7's external TSan
runtime. std and libtest were rebuilt with instrumentation. Symbol inspection
confirms their TSan calls and one runtime initialization definition per inspected
executable. See the [reproduction configuration](../docs/development/local-hardening.md#fully-instrumented-local-rust-tsan).

Clean Rust and C++ controls pass; both deliberate races produce data-race
reports and exit 66. The empty instrumented libtest harness passes. All 73
runtime tests, 36 journal-appender/replay tests and two original FFI integration
tests pass without sanitizer reports: 111 project tests in total. The storage
suite's ignored crash helper is explicitly spawned by its passing recovery test.
The FFI tests cover rejection/redaction/callback ownership and v1/v2/hybrid
transfers, storage ownership and checkpointing without alert delivery.

OpenSSL and libtorrent were rebuilt with Clang TSan, and their static archives
contain instrumentation calls. No compiler source build, CI std rebuild,
suppression or system preload change was introduced. System libc/libstdc++ and
the existing host preload are not instrumented; this is focused WSL1 evidence,
not a completely instrumented OS or validation of every compiler/runtime pair.

## Retention And Remaining Acceptance

Only the final full OpenSSL Linux and Windows build sets remain under their
respective build roots, alongside toolchains, dependency caches and useful
reports. Superseded variants and staged package copies are removed; their
validation hashes remain in the catalog and evidence. No saved binary archive
or filesystem compression is used. More than 20 GB of obsolete normal build
outputs were pruned (logical bytes, possibly counting hard links). Sanitizer
targets, native build trees and temporary campaign helpers were removed after
their final reports; the isolated official nightly and source archive cache remain.

Historical Windows `changeUri`/`addUri` and native Linux mixed-burst latency
failures remain explicitly deferred. Native Linux kernel/io_uring, other
platforms and fresh/minimum OS, hardware power loss and complete release-matrix
acceptance remain external gates. Phase 7 remains active.
