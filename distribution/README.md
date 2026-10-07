# Distribution Materials

[Release packaging guide](../docs/development/release-packaging.md) ·
[License review](license-review.json)

These materials prepare CLI package manifests. They do not authorize a release.
The project [MIT license](../LICENSE) applies to Ariax-owned code; files under
`licenses/` preserve original third-party terms and bytes.

`package-manifests.json` records the four default bundles for Linux and
Windows-GNU and OpenSSL package selections. Current full/compat automatically
select OpenSSL for TLS and SSH RSA; older removed records keep their original
backend identity. `draft-retained` entries
identify available binaries; `validated-removed` entries preserve validation
records after cleanup and cannot be staged. Keep only the final selected binary
in each platform's build directory. macOS and MSVC need separate manifests.

Windows packages import only Windows-provided DLLs. The C++, GCC exception,
thread and optional OpenSSL runtimes are linked statically; the runtime file
catalog is empty and their license notices remain required.

Run `python3 -B scripts/release_manifest.py` to validate the catalog. To stage
available drafts, supply `--artifacts ARTIFACT_ROOT --output NEW_DIR` with the
catalog's retained binary paths. Preparation verifies hashes before copying,
rejects unsafe or duplicate paths and unknown runtime dependencies, and writes
per-package manifests and checksums. It never builds, dispatches CI or publishes.
Temporary staged packages can be deleted after their smoke checks.

The [current pre-CI validation record](../performance-evidence/phase7-pre-ci-local-2026-10-07.md)
owns the retained executable identities and their local validation. Earlier
independent builds and archive checks are historical and linked from the
[packaging guide](../docs/development/release-packaging.md). Removed variants
retain evidence but no package copies. Current-host smoke is not clean/minimum-OS
acceptance, and the latest binaries need their own reproducibility evidence.

The [current inventory](../performance-evidence/phase7-release-license-inventory-2026-10-07.json)
covers 16 selections and 320 packages. Review notices against actual candidate
archives; conservative normal/build closures can include code absent from the
final executable. Ariax remains standalone with best-effort aria2 compatibility.

The [aria2 source notice](aria2-source-notice.txt) and its GPL text belong to
source distributions. Extracted inventory JSON files and those source-only
notices are excluded from the binary package catalog. The exact MPL-covered
public-suffix data and its license are included in every binary package.
