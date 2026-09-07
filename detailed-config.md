# Detailed Config And Option Registry Design

Status: first-slice implementation in progress. The typed registry, bounded
value parser, aria2-style flat parser, and generated option/runtime contracts
are implemented. Phase 4 supplies bounded config check/reload/dump for its
limited executable option set; URL rules and full runtime application remain
pending. Phase 4B repairs production-policy retry admission and recovery
(`P4-03`); active option recovery remains under `P4-04` in
`implementation-readiness.md`.

This document expands `configuration.md` into concrete artifacts, parsers, and
runtime update mechanics.

## Registry Structure

The option registry is data, not hand-coded parser branches.

```rust
pub struct OptionDef {
    pub name: &'static str,
    pub short: Option<char>,
    pub value_type: ValueType,
    pub default: DefaultValue,
    pub category: Category,
    pub scope: ScopeSet,
    pub runtime_update: RuntimeUpdate,
    pub owner: Owner,
    pub build_features: FeatureSet,
    pub security: SecurityClass,
    pub compat: CompatStatus,
    pub aria2_available: bool,
    pub aria2_runtime_update: Aria2RuntimeUpdate,
    pub compatibility_difference: CompatibilityDifference,
    pub docs: DocRef,
    pub behavior_tests: &'static [&'static str],
}
```

Enums:

```rust
pub enum RuntimeUpdate {
    None,
    Live,
    WaitingOnly,
    ActiveRestart,
    NewGeneration,
    StartupOnly,
    UnsafeCompatOnly,
    BtLive,
    BtRestartRequired,
}

pub enum CompatStatus {
    Implemented,
    Partial,
    Unsupported,
    UnsafeCompat,
    FeatureGated,
}
```

The registry generates:

- CLI parser metadata,
- flat config parser allowlist,
- input-file per-download allowlist,
- RPC `changeOption` and `changeGlobalOption` allowlists,
- help/manual tables,
- compatibility matrix,
- default config dump,
- design-option inventory check.

## Value Types

Supported first-slice value types:

```rust
Bool
Integer { min, max }
SizeBytes { min, max }
DurationSeconds { min, max }
Enum { values }
String { max_len }
Path { expansion, must_exist }
Uri
StringList { separator }
HeaderList
StatusCodeSet
OptionMap
SecretString
```

Rules:

- parse once into typed values,
- preserve enough original string form for aria2-compatible RPC rendering,
- reject unknown enum values with a precise error,
- normalize sizes and durations internally,
- secrets use redacted debug/serialization.

## Artifacts

Generated files in an implementation should look like:

```text
generated/options.json
generated/aria2_compat.json
generated/runtime_updates.json
generated/runtime_compatibility.json
generated/rpc_methods.json
generated/design_option_inventory.json
generated/error_codes.json
```

`design_option_inventory.json` is produced by scanning the repository's
Markdown design documents for
documented option-looking strings, with a small ignore list for CLI-only flags
such as `ariax config dump --effective`. The literal inventory pattern example
is written here as `--<option-name>` so it is not mistaken for a real option.

CI rule:

```text
documented option - ignored cli flag - registry option = empty
registry implemented option - passing behavioral fingerprints = empty
```

`runtime_compatibility.json` records whether aria2 implements the option,
aria2's active-change behavior, this design's behavior, and whether a difference
is required or intentional. Design-only retry/slow-slot options are marked
`aria2_available=false`; they are not assigned a fictional aria2 restart rule.

An implemented option's behavioral fingerprint sets a non-default value through
each supported surface and asserts an observable change in its owning subsystem.
A test-id string, parser round-trip, or registry snapshot does not satisfy this
gate. Feature-enabled CI fails if the fingerprint is missing, skipped, or observes
only default behavior.

## Flat Config Parser

Input:

```text
name=value
# comment
```

Parser stages:

1. read UTF-8 text under the document/line caps in `configuration.md`,
2. split lines,
3. trim trailing CR,
4. ignore empty/comment lines,
5. split on first `=`,
6. look up registry entry,
7. check scope includes `startup`, `global`, or `per_download`,
8. parse typed value,
9. store source span and source file.

`per_download` options in flat config become defaults in the global download
template for future tasks. They are not process-global runtime settings and do
not mutate already-created tasks unless a later reload/change operation applies
the option's declared `runtime_update` behavior.

No includes, shell execution, command substitution, or arbitrary environment
expansion. `${HOME}` expansion is allowed only for registry-marked path
options.

Unknown option behavior:

- strict mode: error,
- compatibility mode: warn and ignore only if marked known-unsupported,
- never silently accept.

## URL Rules Parser

TOML schema:

```toml
[[rule]]
name = "rule name"
stop = false

[rule.match]
scheme = ["https"]
host = "example.org"
host_suffix = ".example.org"
port = 443
path_glob = "/releases/*.iso"
url_glob = "https://*.example.org/*.iso"
protocol = ["http", "https"]

[rule.options]
split = 8
max-connection-per-server = 4
```

Validation:

- at least one match key is required,
- only documented match keys are allowed,
- glob syntax is linear-time and size-capped,
- document, rule-count, and glob-byte caps are the exact registry/hard values in
  `configuration.md`; emitted option maps reserve `task_metadata_budget`,
- no regex in first slice,
- options must have `per_download` scope,
- `startup_only`, `unsafe_compat_only`, backend, RPC listener, and shell hook
  options are rejected,
- rule file is parsed completely before publication.

Rule application:

```text
built-in defaults
  -> global config
  -> URL rules in file order
  -> environment proxy defaults
  -> CLI global options
  -> input-file per-download options
  -> RPC global template
  -> RPC per-download patch
```

Rules apply at `AddUri` time only. They never mutate active tasks by URL after
creation.

## Runtime Update Application

`OptionPatch`:

```rust
pub struct OptionPatch {
    pub id: OptionPatchId,
    pub values: BTreeMap<OptionId, OptionValue>,
    pub source: OptionSource,
}
```

Apply algorithm:

1. parse all values,
2. reject unknown/unsupported/feature-disabled options,
3. reject scope violations,
4. classify by `runtime_update`,
5. if any option requires explicit restart and caller did not request it,
   return a grouped error,
6. build one side-effect-free patch plan containing the next live, current,
   pending, and restart-intent snapshots; if any member cannot be planned,
   reject the whole patch,
7. durably accept the patch/restart intent when persistence is required; a
   failure before this point leaves every old value visible and active,
8. publish the planned values as one scheduler-owned patch version (including
   live atomics/handles) and acknowledge the patch,
9. for active restart, enter `PausedRestarting`, cancel/drain workers, retain the
   already accepted pending snapshot, and requeue. The subsequent admission is
   the sole generation increment point defined by `detailed-core.md`.

The restart intent carries one `OptionPatchId` and one canonical staged
snapshot. The accepted snapshot and patch identity outlive the RPC call and
remain owned through cancellation drain and actual generation admission.
`drive_engine` becoming idle does not prove that either barrier has finished.
The current-generation SQLite mirror advances only after the matching
generation record is flushed; pending state then clears. Recovery retains the
complete pending options when only their staged journal prefix exists and
repairs the SQLite mirror from journal authority.

Queue-full rejection before owner acceptance can retry the same owned command.
An uncertain accepted append, flush, or mirror failure instead faults the
driver under `session-persistence.md`; it must not retry a speculative append.
Recovery replays the prefix and appends only the missing generation promotion.
Mismatched hashes, patch identities, and invalid option maps remain errors.
New patches use distinct identities; only a newly accepted complete patch may
supersede a pending snapshot. A live-only patch does not restart a task. A
mixed patch publishes one accepted version and quiesces the task once if any
member requires an authorized restart or new generation.

An active restart is internal quiescence, not a user pause: the wire snapshot is
`waiting`, the pause hook/event is not emitted, pending options are applied, and
the task resumes automatically.

Patch atomicity is by accepted version, including a patch that mixes `live`,
`waiting_only`, and `active_restart` members. No observer may see only a subset
of one accepted patch, and a rejected patch has no live side effects. The
acknowledgement means the complete patch version and any required restart intent
are accepted durably; it does not wait for network/disk cancellation to finish.
If quiescence or later option application fails after acknowledgement, the task
follows the `PausedRestarting -> Error` row with the patch id in diagnostics; it
does not silently roll back only the live members and create a mixed version.

The checkpoint classifies every accepted active patch as `ActiveRestart`,
promotes its SQLite options, and clears pending patch metadata when the drive
loop returns. A later admission can then append an untagged snapshot over the
staged patch, producing `InvalidStagedSnapshotReplacement` at restart. `P4-04`
requires the sequence above for rate, split, output, and mixed patches,
including restart after each persistence boundary.

All rejected mutations use the same grouped public error; transport adapters do
not invent per-option error types:

```json
{
  "code": "OptionPatchRejected",
  "rejected": [
    {"name": "event-backend", "reason": "not_runtime_mutable"},
    {"name": "dir", "reason": "requires_new_generation"}
  ]
}
```

Allowed reasons are `invalid_value`, `unsupported`, `not_runtime_mutable`,
`requires_new_generation`, `requires_explicit_bt_restart`, and
`unsafe_compat_required`. `active_restart` and `bt_live` are accepted
operations, not rejection reasons.

## Config Reload

Reload command flow:

```text
read files -> parse flat config -> parse URL rules -> validate registry/scope
  -> compute diff -> classify runtime updates -> publish global snapshot
  -> optionally restart affected waiting/active tasks
```

Default restart mode is `none`.

Atomicity:

- parse and validate into a new `ConfigSnapshot`,
- do not mutate existing config until validation succeeds,
- failed reload returns diagnostics and keeps current snapshot,
- successful reload increments `config_generation`.

Active tasks keep their `generation_options` unless the reload explicitly
performs allowed restart behavior.

## Config Dump

Dump modes:

```text
defaults
effective
task-effective <gid>
url-rules
```

Formats:

```text
flat
json
toml
```

Rules:

- secrets redacted by default,
- source metadata emitted only in JSON/TOML diagnostics,
- flat dump emits aria2-compatible `name=value` where possible,
- unsupported/feature-gated values are annotated in JSON/TOML, not flat config,
- writing to a path uses temp file, fsync, rename, parent fsync where supported.

## Tests

Required tests:

- all registry options parse default values,
- every implemented option has one owner and a passing non-default behavioral
  fingerprint for each enabled surface,
- flat config unknown option strict vs compat behavior,
- `${HOME}` expansion only on allowed path options,
- URL rules reject startup-only and unsafe options,
- URL rules apply in deterministic order,
- reload failure keeps old snapshot,
- reload live change updates global budget,
- reload active-restart option reports pending restart by default,
- RPC active restart reports `waiting`, emits no pause event, and resumes with
  the pending option,
- delayed cancellation keeps the accepted patch pending beyond the RPC reply;
  replay before/after staging, generation flush, and SQLite promotion retains
  the exact options and a valid journal prefix,
- uncertain append/flush/mirror failures stop the driver for recovery; queue
  backpressure before acceptance retries without a duplicate staged snapshot,
- live-only rate changes avoid a restart, mixed patches publish one complete
  accepted version, and restart-class changes promote exactly once,
- runtime compatibility generation distinguishes aria2 options from extensions,
- every rejection surface maps to the one `OptionPatchRejected` vocabulary,
- dump redacts secrets,
- design-option inventory catches undocumented registry drift.
