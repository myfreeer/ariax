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

The following results describe the earlier source and artifacts; superseded
binaries and archive copies were removed during the requested cleanup.

The four minimal/standard drafts pass their path and runtime inspections and match
[independent local rebuilds](../performance-evidence/phase7-release-verification-2026-10-05.md).
Compilation uses separate target/temp directories with the same installed
toolchain and source cache. Reduced-environment checks pass on both current
hosts; Linux also loads its configured system preload. These checks do not
establish fresh-OS or minimum-OS acceptance. Earlier binaries and failed path
audits remain in the prior evidence and Git history.

The [full/compat follow-up](../performance-evidence/phase7-release-blockers-2026-10-06.md)
adds four archives with verified extracted inventories, notices, independent
native/CLI build matches and reduced-environment package operation on both
hosts. All eight entries remain drafts; their distinct source identities are
retained. Temporary combined inputs and duplicate staging copies were removed
after verification. Superseded archives, extracted packages and independent
binaries have also been removed; the reports retain their validation results.

The conservative dependency notice collection includes normal/build dependency
closures across all eight bundles. It can contain unused source notices and
historical open items; `license-review.json` records this slice's resolutions.
Final release notices should be checked against the actual candidate archives.

The [aria2 source notice](aria2-source-notice.txt) and its GPL text belong to
source distributions. Extracted inventory JSON files and those source-only
notices are excluded from the binary package catalog. The exact MPL-covered
public-suffix data and its license are included in every binary package.
