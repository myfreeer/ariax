# Phase 6 Local OpenSSH Interoperability

Acceptance update: the [Phase 6 closure](phase6-acceptance-2026-10-05.md)
accepts the milestone using verified production/build source equivalence and
separate validation of benchmark changes. It supersedes the provisional open
acceptance and identical-commit requirement below. The original run outcomes,
failures, source identities and platform limits remain unchanged.

On October 5, 2026, both the WSL Linux and native Windows-GNU clients pass
`openssh_public_key_offsets_and_final_attributes_interoperate` at
`624af250863193b6c7e438d02c55e55499f634a7`. Each connects to an isolated
OpenSSH 10.5p1 server on Windows/MSYS2 using OpenSSL 3.6.3. This supplies fresh
live OpenSSH interoperability evidence for both clients.

## Focused Results

| Client | Compile Seconds | Fixture Command Seconds | Test Seconds | Result |
| --- | ---: | ---: | ---: | --- |
| Linux under WSL 1 | 127.968 | 0.473 | 0.18 | 1 passed, 0 failed, 0 ignored |
| Native Windows-GNU | 213.735 | 0.741 | 0.24 | 1 passed, 0 failed, 0 ignored |

Compilation uses standalone Rust 1.97.1 (`8bab26f4f`, LLVM 22.1.6) and the
existing offline dependency cache. Only the `openssh_interop` target with the
`sftp` feature is built; no workspace matrix or native BT provisioning runs.
The two builds overlap, so their durations must not be added as wall time.
The fixture timings are functional-test observations, not performance evidence.

Each unchanged test checks public-key authentication to the pinned ED25519
host, four three-byte offset reads with SHA-512 chunk verification, final
attributes and exact `abcdefghijkl` payload bytes. It requires twelve durable
bytes, the expected host-key algorithm, and zero retained protocol-metadata
and SFTP-ingress reservations. The ignored test is selected explicitly through
the fixture runner. Both server logs show successful public-key authentication,
the forced `internal-sftp` session and orderly disconnection.

## Isolation And Cleanup

Each server listens only on an allocated loopback port, uses a copied fixture
host key and explicit authorized-key file, disables password authentication
and forces the SFTP subsystem. The startup readiness connection closes before
authentication; its pre-authentication log entries are expected fixture probes.
The tests use an empty private known-hosts file and an explicit fixture host
pin. Existing user SSH files and services are untouched.

Build outputs, logs and server payload fixtures remain on the selected temporary
disk. Its WSL DrvFs mount reports `0777` even after `chmod 0600`, so the user
explicitly permits a small disk-backed WSL directory for the Linux client's
test key and known-hosts file. Both files have verified `0600` permissions.
The Windows client uses current-user ownership and protected private ACLs;
native key-file verification succeeds. WSL's mode view of those Windows files
does not represent their native ACL protection.

After each test, the fixture sends termination to the actual MSYS2 daemon PID.
Both daemons are confirmed absent, both listener ports are closed, and all
fixture/payload and private credential directories are removed. The extra WSL
temporary parent is also removed. Retained evidence contains logs and hashes,
not credential copies.

## Provenance And Reproduction

The [machine-readable record](phase6-openssh-local-2026-10-05.json) preserves
all 490 verified source-file hashes, build and test commands, binary hashes,
toolchain/server identities, normalized test/server logs and explicit cleanup
results. All six build/test/server log hashes verify. The focused `sftp` builds
emit existing dead-code warnings for `peer`/`proxied` fields and
`acquire_storage_read`; there are no compiler errors. These production sources
are unchanged from the full functional CI baseline. The original logs and
local launcher remain in the external evidence snapshot; portable paths and
the fixture username are normalized, while hashes refer to unchanged originals.

| Binary | SHA-256 |
| --- | --- |
| WSL Linux test | `f60f782a1327a512c9043e69860bfb9d8c4edda7f9bdbe094266839c95bde3a8` |
| Windows-GNU test | `e30665e77ec3d191483d3195f024c2087146865a9c59fec1d75378ea515db01c` |
| MSYS2 OpenSSH server | `131162c7f78827b6f905a78c2739d3e5a295d6825b58e0f0c3c1194a42911c8b` |

The Linux client runs on WSL 1 kernel `4.4.0-17763-Microsoft`; the native
Windows host is Windows 10 build `17763.316`. Windows compilation and execution
use MSYS2 MINGW64 with its matching GCC runtime first and explicit paths to the
repository's standalone Rust distribution. Linux and Windows use separate
target directories.

The compile command is `cargo test --locked --offline -p ariax-engine
--features sftp --test openssh_interop --no-run --message-format=json`.
The local launcher adapts `scripts/run-openssh-interop.py`'s existing isolated
fixture flow for the selected temporary locations and Windows wrapper, adds
port/PID/permission reporting, and verifies cleanup. It does not modify the
repository's test, server payload or production code.

## Acceptance Scope

This closes the fresh local live-OpenSSH evidence gap. The successful
[six-scenario benchmark campaign](phase6-benchmarks-rerun-2026-10-05.md) remains
at `9107ecb`; only documentation/evidence changed before this `624af25` run.
The [full functional matrix](phase6-full-ci-2026-10-05.md) remains at `a6f9b0d`.
Final `P6-06` evidence still needs to converge on one candidate source.

The server platform here is Windows/MSYS2 and the Linux client runs under
WSL 1. This positive fixture does not replace adversarial trust/framing tests
or rejection coverage. Separate kernel/backend, custom BT storage and
release-platform requirements remain tracked. No benchmark, CI dispatch or
push is performed by this local validation.
