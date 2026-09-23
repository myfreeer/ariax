# Development Guide

[Documentation](../README.md)

## Before Editing

Read the [readiness gates](../project/implementation-readiness.md) and the
[owning subsystem contract](../README.md#source-of-truth). Update that contract
when behavior changes. Preserve the shared scheduler, resource ownership,
security boundaries and the supported aria2-facing behavior.

## Validation Workflow

Push changes or open a pull request to run [CI](continuous-integration.md).
Preflight checks documentation, formatting, generated contracts and dependency
policies before the platform, feature and MSRV matrix. Successful `main` pushes
then run native Linux benchmarks. The workflow's `CI Required` job is the
aggregate result.

Keep local validation focused. Documentation changes need no Rust build:

```sh
git diff --check
python3 -B scripts/check_docs.py
```

For code changes, add behavior tests that cover success and rejection paths.
Use the affected test or parser for minimal local debugging when needed; leave
full workspace builds, native dependency builds and platform validation to CI.
Avoid creating separate build directories for repeated copies of the same
check. Generated build outputs under `toolchains/` and `target/` are disposable;
installed toolchains, local configuration and history backups are separate and
should be retained during cleanup.

## Toolchain And Focused Builds

The workspace uses Rust 2024. [rust-toolchain.toml](../../rust-toolchain.toml)
pins Rust **1.97.1**, rustfmt and Clippy. CI also checks MSRV **1.88** against the
locked dependencies. Select the pinned toolchain for this checkout without
changing the machine's global default.

When a local CLI binary is needed, build only the desired package and feature
bundle. For the implemented HTTP(S), Metalink, FTP/FTPS and SFTP protocols:

```sh
cargo build --locked --package ariax-cli --no-default-features \
  --features standard --profile release-cli
```

The executable is `target/release-cli/ariax` (`ariax.exe` on Windows), unless
`CARGO_TARGET_DIR` selects another location. Use its full path or add its
directory to `PATH` for the [CLI examples](../../README.md#run-the-cli).
The unnamed default and `minimal` bundles provide smaller protocol subsets;
see [feature bundles](../../README.md#feature-bundles).

Phase 6 adds a pinned native libtorrent graph for `full` and `compat`. Native
libraries must match the Rust target ABI. Linux, macOS, Windows MSVC and Windows
GNU installations are separate; never mix WSL, MSVC and MinGW tools or outputs.
Native provisioning and full validation belong in CI. See the
[BitTorrent contract](../protocols/libtorrent-integration.md#phase-6-gates) for
the dependency versions and outstanding acceptance gates.

Release CLI artifacts use `release-cli` with `panic=abort`. Embedding crates
retain the consumer's unwind strategy; the planned C ABI has its own
`release-capi` profile. The
[artifact and ABI matrix](../architecture/overview.md#language-and-library-choice)
defines the remaining packaging requirements.

## Generated Contracts

[compat/README.md](../../compat/README.md) describes the pinned aria2 reference.
[generated/README.md](../../generated/README.md) explains the generated option,
compatibility, runtime and persistence contracts. Change their source registries
and generators, then regenerate only when needed:

```sh
cargo xtask generate /path/to/pinned-aria2-checkout
```

CI independently runs the generator in check mode. Documentation URLs in option
metadata are relative to the repository root; normal Markdown links are
relative to the document containing them.

## Documentation Layout

Keep the root README focused on what Ariax does, its current status and how to
start. Put exact contracts in the appropriate subsystem directory and add them
to the [documentation index](../README.md). Keep one H1 per document, use
kebab-case filenames and Title Case headings, and prefer relative links over
bare filenames. Historical reviews stay under `docs/reviews/` and must remain
clearly marked as non-normative.

The documentation checker validates local links and anchors, the option
registry's documentation URLs, index coverage and the single-H1 rule. It skips
fenced examples and third-party vendored READMEs. It performs no downloads or
compilation.

## Evidence And Releases

Record the exact source commit, toolchain, platform and results for validation
claims. Preserve raw measurements and hashes in `performance-evidence/`.
Historical evidence describes its recorded commit and must not be rewritten to
imply validation of later changes. Native backend, security and release gates
remain separate from the general CI baseline; release tags require the complete
required matrix to pass.
