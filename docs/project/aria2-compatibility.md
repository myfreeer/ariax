# Aria2 Compatibility Progress And Decisions

The first compatibility pass fills spelling gaps through existing executable
handlers. Ariax remains a separate experimental command while behavior and
migration differences are evaluated. This is not an aria2c replacement approval.
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
contains 108 registry entries: 54 shared upstream names and 54 extensions,
against 207 pinned handlers. The 153 names absent from the registry are an audit
input, not 153 proven missing implementations; some have standalone CLI owners.
Status and executable behavior remain separate from name coverage. All 36
pinned RPC method names and six notifications are present in the public method
and event inventories, which does not prove complete semantic parity.

## Remaining Tradeoffs

| Area | Existing Boundary And Required Choice | Recommended Next Step |
| --- | --- | --- |
| Bare URI invocation and aria2 configuration | Current commands explicitly name the session store, control directory and output root. Implicit paths and auto-loaded config change ownership and startup precedence. | Design an opt-in frontend with documented paths and precedence before an aria2c alias. |
| `-d`, `-c`, integrity and aria2 partial files | `-d` cannot replace the authorized output root. Existing Ariax recovery uses its own journals and v3 sessions; a parsed `-c` would not import aria2 progress. | Specify path containment and an explicit, verified import contract; keep existing files unchanged on rejection. |
| `max-concurrent-downloads` / `-j` | Active-task limits share bounded scheduler and process resources; startup, global mutation, decreases and queued work need consistent meaning. | Map the limit to existing scheduler capacity with defined live-decrease behavior and admission tests. |
| Custom headers, user agent and referer | The HTTP layer has bounded custom-header support, but public per-task options require persistence, reserved-header rejection, redaction and redirect credential rules. | Add typed options through admission, storage and redirects as one change; reject unsupported names rather than passing arbitrary data through. |
| Effective option queries | `getOption` currently reports sanitized persisted options; some default transfer values are omitted. Persisted storage need not duplicate every default. | Define a separate effective-option response projection, with protocol/feature filtering and the existing response-size budget. |
| HTTP/2, unknown-length bodies and XML-RPC | These require protocol, storage or parser work beyond argument spelling. | Prioritize concrete client workloads and retain explicit unsupported responses until each path has acceptance evidence. |
| Hooks and security differences | Shell execution, weakened host-key checks, unsafe paths or relaxed SSRF rules would change existing security contracts. | Preserve those boundaries; compatibility must not silently weaken them. |

The next decision is the desired frontend and workload scope, after reviewing
these implemented improvements. Release/platform acceptance, the deferred
latency failures and VM power-loss campaign remain tracked in
[implementation readiness](implementation-readiness.md#phase-6-acceptance-and-phase-7-handoff).
