#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
verifier="$repo_root/scripts/verify-rusqlite-features.sh"

valid_tree='ariax-storage v0.1.0|'
valid_tree+=$'\nrusqlite v0.40.1|backup,bundled,cache,hashlink,limits,modern_sqlite'
valid_tree+=$'\nlibsqlite3-sys v0.38.1|bundled,bundled_bindings,cc,default,min_sqlite_version_3_34_1,pkg-config,vcpkg'
valid_tree+=$'\nrusqlite v0.40.1|backup,bundled,cache,hashlink,limits,modern_sqlite (*)'

expect_accept() {
    local name=$1
    local fixture=$2

    if ! printf '%s\n' "$fixture" | "$verifier" --stdin >/dev/null; then
        printf 'expected fixture to pass: %s\n' "$name" >&2
        exit 1
    fi
}

expect_reject() {
    local name=$1
    local fixture=$2

    if printf '%s\n' "$fixture" | "$verifier" --stdin >/dev/null 2>&1; then
        printf 'expected fixture to fail: %s\n' "$name" >&2
        exit 1
    fi
}

expect_reject_with_message() {
    local name=$1
    local fixture=$2
    local expected_message=$3
    local output

    if output=$(printf '%s\n' "$fixture" | "$verifier" --stdin 2>&1); then
        printf 'expected fixture to fail: %s\n' "$name" >&2
        exit 1
    fi
    if [[ $output != *"$expected_message"* ]]; then
        printf 'fixture failed for the wrong reason: %s\n' "$name" >&2
        printf 'expected diagnostic containing: %s\n' "$expected_message" >&2
        printf 'actual diagnostic: %s\n' "$output" >&2
        exit 1
    fi
}

expect_accept valid "$valid_tree"
expect_reject_with_message \
    extra-rusqlite-feature \
    "${valid_tree//limits,modern_sqlite/limits,modern_sqlite,serialize}" \
    'unexpected resolved feature set for rusqlite'
expect_reject_with_message \
    missing-rusqlite-feature \
    "${valid_tree//,limits,modern_sqlite/,modern_sqlite}" \
    'unexpected resolved feature set for rusqlite'
expect_reject_with_message \
    disagreeing-rusqlite-instances \
    "${valid_tree/limits,modern_sqlite/limits,modern_sqlite,serialize}" \
    'resolved rusqlite instances disagree about enabled features'
expect_reject extra-libsqlite-feature "${valid_tree/pkg-config,vcpkg/pkg-config,unlock_notify,vcpkg}"
expect_reject duplicate-rusqlite-version "${valid_tree}"$'\nrusqlite v0.39.0|backup,bundled,cache,hashlink,limits,modern_sqlite'
expect_reject missing-libsqlite-package "$(printf '%s\n' "$valid_tree" | sed '/^libsqlite3-sys /d')"

cargo() {
    local -a expected=(
        +1.97.1
        tree
        --locked
        --workspace
        --all-features
        --target
        all
        -e
        normal,build,dev
        --prefix
        none
        --format
        '{p}|{f}'
    )
    local actual index

    if [[ $# -ne ${#expected[@]} ]]; then
        printf 'fake cargo expected %d arguments, received %d\n' \
            "${#expected[@]}" "$#" >&2
        return 97
    fi
    for index in "${!expected[@]}"; do
        actual="${@:$((index + 1)):1}"
        if [[ $actual != "${expected[index]}" ]]; then
            printf 'fake cargo argument %d mismatch: expected %s, received %s\n' \
                "$index" "${expected[index]}" "$actual" >&2
            return 97
        fi
    done

    printf '%s\n' \
        'ariax-storage v0.1.0|' \
        'rusqlite v0.40.1|backup,bundled,cache,hashlink,limits,modern_sqlite,serialize' \
        'libsqlite3-sys v0.38.1|bundled,bundled_bindings,cc,default,min_sqlite_version_3_34_1,pkg-config,vcpkg'
}
export -f cargo

if output=$("$verifier" --rustup 1.97.1 2>&1); then
    printf '%s\n' 'expected the dev-edge feature fixture to fail' >&2
    exit 1
fi
if [[ $output != *'unexpected resolved feature set for rusqlite'* ]]; then
    printf '%s\n' 'dev-edge fixture failed before the resolved feature check' >&2
    printf 'actual diagnostic: %s\n' "$output" >&2
    exit 1
fi

printf '%s\n' 'verified rusqlite feature graph rejection cases and dev-edge invocation'
