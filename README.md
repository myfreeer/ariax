# Ariax

Ariax is an experimental Rust downloader with aria2-style configuration, queue
control and JSON-RPC. It combines resumable, multi-source transfers with bounded
memory, responsive controls and crash-aware persistence.

[Documentation](docs/README.md) ·
[Implementation status](docs/project/implementation-readiness.md) ·
[CI](https://github.com/myfreeer/ariax/actions/workflows/ci.yml)

## Current Status

HTTP(S), Metalink, FTP/FTPS and SFTP are implemented through the shared scheduler,
CLI, Rust API and RPC control plane. The
[September 22 CI baseline](performance-evidence/ci-baseline-2026-09-22.md) passes
on Linux, macOS, Windows MSVC and Windows GNU, including feature and MSRV checks
and native Linux performance measurements.

Phase 6 BitTorrent integration is in progress. Ariax remains unreleased;
the [implementation plan](docs/project/implementation-plan.md) tracks remaining
protocol, native backend and hardening gates. HTTP/2, growing/chunked HTTP
transfers, XML-RPC, the C ABI and complete aria2 compatibility are not yet
available. Internal persistence formats may change before release.

## Capabilities

- **Transfers:** known-length HTTP(S), parallel ranges and mirrors, redirects,
  proxies, verified TLS, bounded retries and resumable progress.
- **Additional protocols:** Metalink v3/v4, FTP/FTPS and SFTP, with shared
  scheduling, verification and persistence.
- **Control:** direct CLI commands, a typed Rust API, and JSON-RPC over loopback
  HTTP, WebSocket or stdio. Pause, resume, queue changes and status queries use
  the same engine.
- **Resource limits:** process, connection, buffer, queue and bandwidth budgets;
  profiles for concurrency, throughput, latency and compact operation.
- **Integrity and recovery:** validated paths and byte placement, streamed
  checksums, journaled transfer progress and a SQLite session index.

Supported behavior is recorded in the
[configuration contract](docs/interfaces/configuration.md) and
[generated compatibility tables](generated/README.md). An option's presence in
the inventory does not imply that its behavior is implemented.

## Run The CLI

See the [development guide](docs/development/README.md) for the pinned toolchain
and a focused source build. The examples below assume `ariax` is on `PATH`.
Run `ariax --help` for the current experimental command syntax.

Create private state and output directories, then download a file. This is a
POSIX-shell example; replace the example URL with a file you want to download.
The output root must be an absolute path.

```sh
umask 077
mkdir -p state/control downloads
ariax --add-uri "$PWD/state/session.sqlite" "$PWD/state/control" \
  "$PWD/downloads" https://example.org/file.bin
```

The add command waits for the download and saves session state before exiting.
For a long-running service, start loopback RPC using the same directories:

```sh
ariax --rpc-http "$PWD/state/session.sqlite" "$PWD/state/control" \
  "$PWD/downloads" 127.0.0.1:6800
```

Configure `ARIAX_RPC_SECRET` when method-token authentication is needed. See
[RPC authentication and transports](docs/interfaces/apis-and-embedding.md#rpc-authentication)
for client conventions and the
[configuration guide](docs/interfaces/configuration.md) for options and profiles.
One process owns a session store at a time; use RPC to control a running service.

## Feature Bundles

Choose a CLI bundle with `--no-default-features --features <bundle>`.

| Bundle | Scope |
| --- | --- |
| Default | HTTP(S) and the shared control interfaces |
| `minimal` | Default capabilities plus Metalink |
| `standard` | `minimal` plus FTP/FTPS and SFTP |
| `full` | `standard` plus the Phase 6 BitTorrent integration under development |
| `compat` | `full`; additional compatibility behavior remains subject to its gates |

The completed CI baseline predates BitTorrent. Full/compat BT behavior requires
the [six Phase 6 gates](docs/protocols/libtorrent-integration.md#phase-6-gates).
Shell hooks remain unsupported. Runtime `--profile` settings are separate from
these compile-time feature bundles.

## Repository Map

| Location | Contents |
| --- | --- |
| [bin/ariax](bin/ariax) | Experimental CLI and RPC transports |
| [crates](crates) | Core, configuration, runtime, storage, engine and adapters |
| [docs](docs/README.md) | Architecture, subsystem contracts, roadmap and reviews |
| [tools](tools) / [scripts](scripts) | Contract generation and validation tools |
| [compat](compat/README.md) / [generated](generated/README.md) | Pinned reference inputs and generated contracts |
| [fuzz](fuzz/README.md) | Bounded parser and recovery fuzz targets |
| [performance-evidence](performance-evidence) | Retained benchmark and CI evidence |
| [vendor](vendor/README.md) | Audited, pinned protocol dependency patches |

## Development

Rust 2024, Rust **1.97.1** for development, and MSRV **1.88** are required.
Full builds, platform tests, strict Clippy, feature validation and benchmarks run
in CI. Local work should use documentation checks and focused debugging.

Start with the [development guide](docs/development/README.md), then read the
[architecture overview](docs/architecture/overview.md) and the contract for the
subsystem you are changing. The [documentation index](docs/README.md) separates
current contracts, implementation evidence and historical reviews.
