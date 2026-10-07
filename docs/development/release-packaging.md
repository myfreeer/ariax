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

The [current inventory](../../performance-evidence/phase7-release-license-inventory-2026-10-07.json)
and [notice collection](../../performance-evidence/phase7-third-party-notices-2026-10-07.txt)
cover 16 Linux/Windows-GNU selections and 320 packages. The
[license review](../../distribution/license-review.json) records the winapi,
IANA and aria2 source-artifact dispositions. Notice bytes remain checked against
their pinned archives or upstream provenance; Git attributes preserve them.

The catalog currently retains one full/OpenSSL binary per host. The
[pre-CI local validation](../../performance-evidence/phase7-pre-ci-local-2026-10-07.md)
owns their current hashes, imports, focused checks and package smoke evidence.
Its [dependency analysis](../../performance-evidence/phase7-binary-size-2026-10-07.md)
distinguishes build dependencies from linked code. Earlier independently matching
builds and removed variants retain their dated records; they cannot be staged
or used as reproducibility evidence for a changed current binary.

Linux GNU release-helper builds using OpenSSL pack relative relocations with
`-Wl,-z,pack-relative-relocs`. These artifacts already require glibc 2.38;
the relocation format requires glibc 2.36 and does not raise that floor.
The option is applied to the final CLI link per bundle. Smaller ring-only
bundles retain their glibc 2.34 policy, and Windows receives no ELF link flags.

## Embedded And Native Materials

Every current bundle embeds the [public-suffix snapshot](../../assets/public-suffix-list.dat)
under MPL-2.0. Include that exact source file, its
[provenance](../../assets/public-suffix-list.toml), the MPL-2.0 license and an
explicit source-availability notice in the distribution. Keep modifications
to MPL-covered files under their required terms; separate Ariax code remains
MIT. IANA classifier data retains its
[source and terms reference](../../compat/iana-special-purpose.pin).
The [IANA/IETF registry statement](https://www.iana.org/help/licensing-terms)
dedicates applicable protocol-registry rights under CC0-1.0. Both special-purpose
address registries are linked from the protocol registry index. This specific
dedication governs the data; the generic ICANN website terms are not its license.
Retain the data provenance and CC0 text with the package.

The generated aria2 inventories have a separate source-distribution scope.
`generated/aria2_options.json` includes extracted C++ constructor expressions
and directives. Preserve upstream GPL-2.0-or-later notices for the extracted
material in the option, RPC and compatibility inventories; Ariax additions to
those artifacts remain compatible with that distribution. These files are not
compiled or embedded into the CLI and are excluded from binary package layouts.
The source-distribution notice identifies upstream authors, the exact commit,
extraction/modification method and GPL text. Do not describe a repository source
archive as containing only MIT materials.

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

## Package Manifests

The [package catalog](../../distribution/package-manifests.json) uses an explicit
allowlist for each target and CLI bundle. The
[distribution guide](../../distribution/README.md) documents validation and
staging through `scripts/release_manifest.py`.
The catalog names its notice inventory explicitly and binds its hash, current
dependency lock and notice collection to the packaged `license-inventory.json`
and `THIRD-PARTY-NOTICES.txt`. Updating a dated inventory cannot leave a package
pointing at the older collection. Historical catalogs without the explicit
inventory field retain their original October 5 interpretation.
The layout includes `LICENSE`, third-party notices, runtime notices, the exact
MPL-covered source and provenance, source-availability information, release
limitations, file hashes and the binary's build identity. It does not copy the
repository tree or toolchain directory wholesale.

Retained full/OpenSSL entries are unapproved drafts, bound to exact binary
hashes and validation evidence. Linux records its interpreter, required shared
objects and maximum glibc symbol requirement. Windows records case-insensitive
system DLL imports and rejects additional runtime DLLs. Unknown imports or
missing required files reject preparation.

Candidate Rust and native C/C++ builds must remap repository, dependency-cache
and output paths and pass binary inspection. Reproducibility does not establish
path portability; native reuse must satisfy its recorded provenance checks.

For full/compat, `bt_native.py --release-paths --work-dir ABSOLUTE_DIRECTORY`
builds a separate installation. `--source-cache` reuses an existing verified
source cache in place and rejects missing or modified trees without repairing
them. Remap the native work and source-cache roots and the installed headers
consumed by the bridge. Keep compiler arguments and the fixed source-date epoch
in the native manifest. OpenSSL receives remaps through a relative response
file so its embedded compiler description does not expose build paths.
Its compiled defaults use `/etc/ssl` on Linux and
`C:/Program Files/Common Files/SSL` on Windows. Disabled engine/module paths
use fixed system installation locations, independent of the build directory.
Do not rewrite compiled binaries to remove paths.

Full/compat Linux drafts require glibc 2.38 and GLIBCXX 3.4.30; libstdc++ is a
system prerequisite, not a bundled copy of the host runtime. Current Windows
builds must statically link the MinGW C++, GCC exception and pthread runtimes
and import only Windows-provided DLLs. Historical Windows drafts carried
`libstdc++-6.dll`, `libgcc_s_seh-1.dll` and
`libwinpthread-1.dll` beside the executable. Current package validation rejects
all additional runtime DLLs. Keep the GCC runtime exception and winpthread
notices for their statically linked code. Reduced-environment checks must
observe only the executable and Windows system modules.
Independent CLI comparisons use fresh target/temp directories. Record whether
the native installation is shared or rebuilt independently. A second native
installation must match the compiler, normalized build inputs and every consumed
header/archive hash. An independently rebuilt native installation strengthens
the comparison; sharing one does not establish native-library reproducibility.

The CLI and retained native installation have separately recorded source-date
epochs. Reusing a verified dependency does not require rebuilding it after each
CLI commit; independent comparisons must match both recorded build identities.
`scripts/release_build.py --crypto-backend openssl` selects the OpenSSL TLS/RSA
features and requires `--native-dir` even for minimal/standard. Minimal enables
only the TLS provider because it has no SFTP. Full/compat automatically use
OpenSSL for TLS and SSH RSA even with `--crypto-backend default`; smaller bundles
without an OpenSSL selector retain ring TLS/RustCrypto RSA. Either legacy
selector selects the common crypto backend wherever those protocols are enabled.
The RustCrypto-only RSA limitation is documented in the
[SFTP contract](../protocols/detailed-ftp-sftp.md); the advisory is not suppressed.

Package records marked `validated-removed` preserve completed validation after
cleanup and are excluded from staging. Only `draft-retained` entries identify
available binaries. OpenSSL entries, including automatic full/compat builds, use the `-openssl` package suffix;
their manifests record the selected crypto backend and measured imports.

The maintained [local build helper](local-hardening.md#local-release-preparation)
selects the pinned native toolchain and prepares these remaps for all four
bundles. Its records include source hashes, encoded Rust arguments, quoted
C/C++ flags, actual imports and CLI startup checks. A single rebuilt artifact
must not inherit a prior artifact's independent-reproducibility result.
Historical [path/reproducibility work](../../performance-evidence/phase7-release-blockers-2026-10-06.md)
and [static-runtime validation](../../performance-evidence/phase7-openssl-static-tsan-2026-10-07.md)
retain the earlier artifact identities. Current paths and results are in the
[pre-CI local record](../../performance-evidence/phase7-pre-ci-local-2026-10-07.md).
Fresh/minimum-OS operation, the complete candidate matrix, macOS/MSVC packaging
and independent reproduction of the latest binaries remain separate gates.

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

## Dependency Policy

The repository-root `deny.toml` defines the workspace license, advisory, source
and banned-backend policy. Validate it with pinned `cargo-deny` 0.19.0 using the
repository Cargo/Rustc, a fresh advisory database, and `--locked`; this command
resolves metadata but does not build native libraries. The policy checks all
workspace features and does not exempt unpublished crates or silently ignore
advisories. Duplicate versions warn; registry wildcard requirements, unreviewed registry
or Git sources, and the forbidden Cargo TLS/legacy-crypto backends fail.
Unpublished local path dependencies are exempt from the wildcard check.

`python3 scripts/verify-protocol-features.py` additionally checks each public
bundle's precise feature/provider contract. Native OpenSSL/Boost/libtorrent
sources and data licenses require their separate pinned-source review; Cargo
metadata cannot establish their advisory status. Supplement RustSec results
with the retained OSV/GitHub advisory matches, including identifiers absent
from RustSec. The dated exact-source inventory records target/bundle graphs
and hashes; it does not substitute for audited rebuilt release artifacts.

Keep unresolved findings visible in release evidence. An applicability note is
not an advisory suppression or approval to release. Any future exception must
identify the advisory, exact package/version, affected call path, justification
and revalidation trigger in both the policy and its review record.
