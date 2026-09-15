# Ariax Fuzz Targets

This is a standalone `cargo-fuzz` package. It is intentionally excluded from
the normal workspace so production, MSRV, and native-target builds do not link
the libFuzzer runtime.

The targets exercise bounded untrusted-input surfaces in the implemented HTTP
slice and journal replay path:

- `http_response_validator`: status, range, length, encoding, ETag, date, and
  bounded SHA-256 `Repr-Digest` response-head validation;
- `http_request_headers`: custom-header admission and generated request
  header ownership;
- `http_retry_specs`: retry profiles, trigger sets, status ranges, and policy
  parsers;
- `http_discard_budget`: hierarchical accounting, caps, overflow buckets, and
  no-refund invariants;
- `journal_replay`: bounded segment/header/record replay and valid-prefix
  accounting;
- `rpc_json`: bounded JSON-RPC request, notification, and batch dispatch,
  canonical/duplicate Basic authorization headers, response validity, and exact
  budget refunds. Its paused Tokio clock exercises authentication throttling
  without real-time sleeps in the fuzz loop.
- `session_document`: strict JSON migration documents and aria2 input-file
  syntax, including bounded metadata comments and exact safe projections.
- `url_rules`: bounded TOML rule parsing, safe option admission, canonical
  round trips, and bounded URL matching without network access.
- `metalink`: capped v3/v4 pull parsing, namespaces/entities, safe paths,
  selection and verification-manifest round trips.
- `verification_manifest`: binary digest-table decoding, exact geometry,
  canonical encoding and fingerprint stability.

From the repository root, after installing `cargo-fuzz`, run for example:

```text
cargo fuzz run --manifest-path fuzz/Cargo.toml http_response_validator
cargo fuzz run --manifest-path fuzz/Cargo.toml journal_replay
cargo fuzz run --manifest-path fuzz/Cargo.toml rpc_json
cargo fuzz run --manifest-path fuzz/Cargo.toml session_document
cargo fuzz run --manifest-path fuzz/Cargo.toml url_rules
```

CI should use a fixed corpus/time budget and retain only minimized inputs. The
targets cap input-derived collections before constructing headers, scopes, or
replay segments; they do not authorize network or filesystem access.

Local Phase 5 smoke runs use at most 1,000 inputs per process and small inputs,
with a cooldown between runs. Compile all fuzz binaries before collecting
timing or benchmark evidence.
