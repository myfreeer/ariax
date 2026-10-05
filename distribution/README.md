# Distribution Materials

[Release packaging guide](../docs/development/release-packaging.md) ·
[License review](license-review.json)

These materials prepare CLI package manifests. They do not authorize a release.
The project [MIT license](../LICENSE) applies to Ariax-owned code; files under
`licenses/` preserve original third-party terms and bytes.

`package-manifests.json` describes all four bundles for Linux and Windows-GNU.
The retained minimal/standard artifacts have concrete draft file inventories.
Full/compat entries remain planned until their binary identities and runtime
imports are verified. macOS and MSVC need separate manifests.

Run `python3 -B scripts/release_manifest.py` to validate the catalog. To stage
the four retained drafts, supply `--artifacts ARTIFACT_ROOT --output NEW_DIR`.
The artifact root contains the retained `release-linux-first` and
`release-windows-first` directories. Preparation verifies hashes before copying,
rejects unsafe or duplicate paths and unknown runtime dependencies, and writes
per-package manifests and checksums. It never builds, executes the binaries,
dispatches CI or publishes a package.

The conservative dependency notice collection includes normal/build dependency
closures across all eight bundles. It can contain unused source notices and
historical open items; `license-review.json` records this slice's resolutions.
Final release notices should be checked against the actual candidate archives.

The [aria2 source notice](aria2-source-notice.txt) and its GPL text belong to
source distributions. Extracted inventory JSON files and those source-only
notices are excluded from the binary package catalog. The exact MPL-covered
public-suffix data and its license are included in every binary package.
