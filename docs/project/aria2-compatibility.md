# Aria2 Compatibility Progress And Decisions

Ariax remains a standalone command. Aria2 compatibility is best-effort within
the documented behavior, resource and security contracts. The release does not
provide an `aria2c` replacement or alias, and complete aria2 parity is not a
P7 acceptance requirement. The first compatibility pass fills spelling gaps
through existing executable handlers.
The pinned reference is `9e7273583f83e881e3ec067b523ba88724088d2f`.

## Implemented First Pass

- Direct URI, Metalink, torrent and magnet commands accept `--name value` in
  addition to `--name=value` for their supported options.
- Registry short aliases work with separate or attached values: `-o`, `-s`,
  `-x`, `-k`, `-t`; `-p` selects FTP passive mode and `-n` disables netrc.
- Boolean flags imply `true`; `=false` is explicit. `--` preserves subsequent
  literal arguments. Duplicate aliases/long names reject together, with bounds,
  feature availability and output-root authority enforced by the typed API.
- Startup session import accepts `-i FILE` / `-iFILE` within RPC service startup.
  `--profile VALUE` works alongside `--profile=VALUE`.
- Twenty protocol options previously mislabeled as Ariax extensions now record
  their upstream presence and runtime-update class. Generation rejects both
  missing upstream claims and false extension labels.

The [configuration contract](../interfaces/configuration.md#compatibility-matrix-categories)
explains admission and syntax. The [generated inventory](../../generated/aria2_compat.json)
contains 112 registry entries: 58 shared upstream names and 54 extensions,
against 207 pinned handlers. The 149 names absent from the registry are an audit
input, not a count of proven missing implementations; some have standalone CLI owners.
Status and executable behavior remain separate from name coverage. All 36
pinned RPC method names and six notifications are present in the public method
and event inventories, which does not prove complete semantic parity.

## Implemented Follow-Up

- `getOption` includes executable transfer defaults such as `ftp-pasv=true`,
  filtered by compiled protocol availability. Secrets and internal bindings are
  omitted; persisted snapshots remain sparse.
- `max-concurrent-downloads` and startup `-j N` / `-jN` control the shared active
  task limit within bootstrap capacity. Decreases let occupied slots drain;
  increases admit queued work. Zero, excessive limits and invalid mixed patches
  reject before publication. Omitting the setting uses bootstrap capacity.
- Per-download `header`, `user-agent` and `referer` reach probes, range requests
  and metadata fetches. Repeated CLI headers and RPC header arrays are accepted.
  `Host`, `Authorization`, `Proxy-Authorization` and `Cookie` may override
  generated fields; other reserved fields reject with value-free warnings.
  Custom headers stay bound to their original origin; proxy authorization goes
  only to the selected proxy, including CONNECT. All three options support
  waiting-task changes; `user-agent` also supports inherited global defaults.

Header and referer values are deliberately volatile. Recovered tasks requiring
them remain blocked, and URI replacement cannot silently remove that requirement.
Waiting or paused tasks accept resupplied headers through `changeOption`, keeping
their progress. Persistent secret storage and global secret-bearing templates
remain separate compatibility decisions.

## Resolved Scope

The standalone artifact decision is settled. Missing aria2 options and frontend
conventions remain documented compatibility opportunities, rather than release
blockers merely because aria2 supports them. Implemented and advertised behavior
still requires success, rejection, persistence and feature-availability coverage.

Existing aria2 partial downloads and `.aria2` control-file migration are excluded
from compatibility scope and P7 acceptance. No importer for that progress is
required. Ariax's own resumable downloads and crash recovery remain in scope.
The supported aria2 text input-file importer continues to admit task
descriptions; it does not import existing partial-file progress.

## Remaining Tradeoffs

| Area | Existing Boundary And Required Choice | Recommended Next Step |
| --- | --- | --- |
| Bare URI invocation and aria2 configuration | Current commands explicitly name the session store, control directory and output root. Implicit paths and auto-loaded config change ownership and startup precedence. | Consider an opt-in Ariax frontend only with documented paths and precedence; no aria2c alias is planned. |
| `-d`, `-c` and integrity | `-d` cannot replace the authorized output root. Ariax recovery uses its own journals and v3 sessions; direct `-c` remains unsupported. | Define path containment, integrity checks and any continuation spelling for Ariax-owned progress. Existing aria2 partial-file support is excluded. |
| HTTP/2, unknown-length bodies and XML-RPC | These require protocol, storage or parser work beyond argument spelling. | Prioritize concrete client workloads and retain explicit unsupported responses until each path has acceptance evidence. |
| Hooks and security differences | Shell execution, weakened host-key checks, unsafe paths or relaxed SSRF rules would change existing security contracts. | Preserve those boundaries; compatibility must not silently weaken them. |

These opportunities do not reopen the standalone artifact decision.
Release/platform acceptance, the deferred
latency failures and VM power-loss campaign remain tracked in
[implementation readiness](implementation-readiness.md#phase-7-local-validation).
