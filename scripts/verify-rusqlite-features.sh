#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf 'usage: %s [--cargo PATH | --rustup VERSION | --stdin | --tree-file PATH]\n' "$0" >&2
    exit 2
}

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)

tree_args=(
    tree
    --locked
    --workspace
    --all-features
    --target all
    # Include dev edges because workspace tests and `--all-targets` builds can
    # unify additional rusqlite features through dev-dependencies.
    -e normal,build,dev
    --prefix none
    --format '{p}|{f}'
)

case ${1-} in
    '')
        tree_output=$("$repo_root/scripts/cargo-local.sh" linux "${tree_args[@]}")
        ;;
    --rustup)
        [[ $# -eq 2 && $2 =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || usage
        tree_output=$(cargo "+$2" "${tree_args[@]}")
        ;;
    --cargo)
        [[ $# -eq 2 && -x $2 ]] || usage
        tree_output=$("$2" "${tree_args[@]}")
        ;;
    --stdin)
        [[ $# -eq 1 ]] || usage
        tree_output=$(</dev/stdin)
        ;;
    --tree-file)
        [[ $# -eq 2 && -r $2 ]] || usage
        tree_output=$(<"$2")
        ;;
    *)
        usage
        ;;
esac

normalize_features() {
    local csv=$1
    local -a features=()
    local IFS=,

    read -r -a features <<< "$csv"
    printf '%s\n' "${features[@]}" | LC_ALL=C sort -u | paste -sd, -
}

verify_package_features() {
    local package=$1
    local expected=$2
    local row identity raw_features actual
    local -A identities=()
    local -A resolved_sets=()

    while IFS= read -r row; do
        [[ $row == "$package v"*'|'* ]] || continue
        if [[ $row == *' (*)' ]]; then
            row=${row:0:${#row}-4}
        fi
        identity=${row%%|*}
        raw_features=${row#*|}
        actual=$(normalize_features "$raw_features")
        identities["$identity"]=1
        resolved_sets["$actual"]=1
    done <<< "$tree_output"

    if [[ ${#identities[@]} -ne 1 ]]; then
        printf 'expected exactly one resolved %s package, found %d\n' \
            "$package" "${#identities[@]}" >&2
        printf 'resolved package identities: %s\n' "${!identities[*]:-(none)}" >&2
        return 1
    fi

    if [[ ${#resolved_sets[@]} -ne 1 ]]; then
        printf 'resolved %s instances disagree about enabled features: %s\n' \
            "$package" "${!resolved_sets[*]}" >&2
        return 1
    fi

    actual=${!resolved_sets[*]}
    if [[ $actual != "$expected" ]]; then
        printf 'unexpected resolved feature set for %s\n' "$package" >&2
        printf 'expected: %s\n' "$expected" >&2
        printf 'actual:   %s\n' "$actual" >&2
        return 1
    fi
}

# Cargo reports the complete feature closure in `{f}`, not only the four
# workspace-selected rusqlite roots. `cache` necessarily enables `hashlink`,
# while `bundled` necessarily enables `modern_sqlite`; the latter enables the
# bundled/bindings closure in libsqlite3-sys. Freeze both resolved sets so a
# new direct dependency cannot silently unify an unrelated SQLite feature.
expected_rusqlite='backup,bundled,cache,hashlink,limits,modern_sqlite'
expected_libsqlite3_sys='bundled,bundled_bindings,cc,default,min_sqlite_version_3_34_1,pkg-config,vcpkg'

verify_package_features rusqlite "$expected_rusqlite"
verify_package_features libsqlite3-sys "$expected_libsqlite3_sys"

printf '%s\n' \
    'verified rusqlite roots backup+bundled+cache+limits and their exact resolved SQLite feature closure'
