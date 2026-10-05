# Release Packaging And Licenses

[Documentation](../README.md) · [Readiness](../project/implementation-readiness.md)

## Project License

Ariax-owned code and documentation use the [MIT license](../../LICENSE).
Workspace packages inherit `license = "MIT"`; publication remains disabled.
Third-party source, data, native libraries and their notices keep their own
licenses. The project license does not relicense those materials.

The locked CLI dependency review supports MIT for Ariax-owned code. Cargo
dependencies have permissive license choices, including Apache-2.0, BSD,
ISC, Unicode-3.0 and Zlib obligations. These are compatible with MIT-owned
application code when their separate terms are retained. An `OR` permits a
choice; an `AND` requires both licenses. MIT does not replace Apache patent,
notice or modification obligations.

## Inventory Scope

Resolve `ariax-cli` separately with `--no-default-features --features BUNDLE`
for `minimal`, `standard`, `full` and `compat`, on each release target.
Use locked Cargo metadata and `cargo tree --edges normal,build`; omit test
dependencies. A target-filtered package closure includes build tools and
procedural macros and is conservative, not an exact map of linked bytes.
Workspace-wide feature resolution alone is insufficient for bundle notices.

For each package, retain its version, declared license expression, source or
vendor provenance, archive checksum, and original notice texts. Supplement
missing published license files only from the package's pinned upstream
revision, recording URL and digest. Preserve unresolved omissions explicitly.
Inspect nested native sources and embedded data independently of Cargo metadata.

The [October 5 inventory](../../performance-evidence/phase7-release-license-inventory-2026-10-05.json)
and [notice draft](../../performance-evidence/phase7-third-party-notices-2026-10-05.txt)
cover Linux and Windows-GNU CLI bundles. They retain original texts and hashes,
including conservative source-only notices. Their explicit open items must be
resolved against actual release archives before using them as final notices.

## Embedded And Native Materials

Every current bundle embeds the [public-suffix snapshot](../../assets/public-suffix-list.dat)
under MPL-2.0. Include that exact source file, its
[provenance](../../assets/public-suffix-list.toml), the MPL-2.0 license and an
explicit source-availability notice in the distribution. Keep modifications
to MPL-covered files under their required terms; separate Ariax code remains
MIT. IANA classifier data retains its
[source and terms reference](../../compat/iana-special-purpose.pin).

`ring` includes several upstream crypto notices; bundled SQLite includes a
public-domain dedication distinct from its MIT Rust wrapper. Preserve nested
notices for these and the compression code. Patched FTP/SFTP libraries retain
their original licenses and [patch provenance](../../vendor/README.md).

`full` and `compat` also include libtorrent (BSD-3-Clause), Boost (BSL-1.0)
and OpenSSL (Apache-2.0), pinned in
[native sources](../../native/libtorrent/sources.json). Preserve libtorrent's
full `LICENSE` as well as `COPYING`: its platform-specific Apple route header
has APSL/BSD notices, and other embedded code has additional terms. Determine
which files are used for each platform before approving that platform's
package. Source archives need review even when a file is absent from the binary.

Rust runtime and compiler-builtins, system libraries, Windows redistributable
DLLs, and any supplied tools/data require a final artifact-based inventory.
Do not copy all DLLs from a toolchain directory into a release. Record actual
imports, selected runtime files, their versions, licenses and redistribution
conditions. Cargo package metadata does not cover these materials.

## Release Checklist

- Freeze the candidate commit, target triples, bundle flags, toolchain and
  native source/build manifests. Regenerate inventories for that exact set.
- Complete dependency advisory review and the remaining platform, sanitizer,
  kernel, recovery and performance gates in the readiness document.
- Close missing notice and source-availability items; review nested native
  code, IANA data terms, and the scope of extracted aria2 compatibility data.
- Assemble each archive with the binary, project `LICENSE`, complete applicable
  third-party notices, required source/data and redistribution information,
  release notes, source/build provenance and file checksums.
- Verify actual binary imports and packaged runtime dependencies on a clean
  native host. Verify notices and source links against that archive's contents.
- Retain reproducibility results, install/run/uninstall checks and limitations
  for each declared platform and feature bundle.
- Approve the complete candidate test matrix before tagging. The existing
  disabled tag gate stays in place until the release gates are satisfied.

Preparing a notice draft or choosing MIT does not approve a release. Benchmarks
remain manually triggered; this local review does not dispatch them.
