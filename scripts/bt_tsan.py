#!/usr/bin/env python3
"""Build or reuse a direct native TSan driver without Rust libtest transport."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import tomllib

import bt_native
import local_hardening as evidence

ROOT = Path(__file__).resolve().parents[1]
CRATE = Path("crates/ariax-bt-libtorrent-sys")
TEST = CRATE / "tests/native.rs"
NAME = "ariax-local-tsan-driver"
CASES = (
    "native_rejection_is_bounded_redacted_and_keeps_callbacks_owned_on_drop",
    "native_v1_v2_hybrid_transfers_hold_storage_and_checkpoint_without_alert_delivery",
)
NATIVE_INPUTS = {
    "sourcesSha256": "native/libtorrent/sources.json",
    "patchSha256": "native/libtorrent/ariax.patch",
    "opensslPatchSha256": "native/libtorrent/openssl.patch",
    "builderSha256": "scripts/bt_native.py",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def driver_source(source):
    gate = '#![cfg(feature = "native")]\n'
    require(source.startswith(gate), "native test feature gate changed")
    found = re.findall(r"^#\[test\]\nfn ([a-z_0-9]+)\(\) \{", source, re.MULTILINE)
    require(len(found) == len(CASES) and set(found) == set(CASES)
            and source.count("#[test]") == len(CASES), "native test inventory changed")
    require("#[ignore" not in source and "fn main(" not in source, "unexpected test entry point")
    source = source.removeprefix(gate).replace("#[test]\n", "")
    # Keep this adaptation byte-identical to the reviewed initial direct driver.
    source += '\nfn main() {\n let case = std::env::args().nth(1).expect("case name");\n match case.as_str() {\n'
    source += ''.join('  "' + case + '" => ' + case + '(),\n' for case in CASES)
    return source + '  _ => panic!("unknown case"),\n }\n println!("case {}: passed", case);\n}\n'


def manifest(root):
    return ('[package]\nname = "' + NAME + '"\nversion = "0.0.0"\nedition = "2024"\n'
            'publish = false\n[workspace]\n[dependencies]\nariax-bt-libtorrent-sys = { path = '
            + json.dumps(str(root / CRATE)) + ', features = ["native"] }\n')


def source_hashes(root):
    paths = {Path("Cargo.toml"), Path("Cargo.lock"), Path("rust-toolchain.toml")}
    paths.update(Path(name) for name in NATIVE_INPUTS.values())
    for directory in (root / CRATE, root / ".cargo"):
        paths.update(path.relative_to(root) for path in directory.rglob("*") if path.is_file())
    return {path.as_posix(): evidence.digest(root / path) for path in sorted(paths)}


def verify_lock(root, project):
    def packages(path):
        return tomllib.loads(path.read_text(encoding="utf-8"))["package"]

    def identity(package):
        return tuple(package.get(key) for key in ("name", "version", "source", "checksum"))

    baseline = {identity(package) for package in packages(root / "Cargo.lock")}
    actual = packages(project / "Cargo.lock")
    driver = [p for p in actual if p["name"] == NAME]
    require(len(driver) == 1 and identity(driver[0]) == (NAME, "0.0.0", None, None),
            "unexpected driver package identity")
    require(all(identity(p) in baseline for p in actual if p["name"] != NAME),
            "driver dependencies differ from repository lock")


def verify_project(root, project, record):
    generated = driver_source((root / TEST).read_text(encoding="utf-8"))
    require(record["originalSourceSha256"] == evidence.digest(root / TEST), "stale native test source")
    require(record["driverSourceSha256"] == hashlib.sha256(generated.encode()).hexdigest(),
            "stale generated driver identity")
    require((project / "src/main.rs").read_bytes() == generated.encode(), "modified generated driver")
    require(tomllib.loads((project / "Cargo.toml").read_text()) == tomllib.loads(manifest(root)),
            "modified driver manifest")
    fixtures = root / CRATE / "tests/fixtures"
    expected = {p.relative_to(fixtures).as_posix(): evidence.digest(p)
                for p in fixtures.rglob("*") if p.is_file()}
    copied = project / "src/fixtures"
    actual = {p.relative_to(copied).as_posix(): evidence.digest(p)
              for p in copied.rglob("*") if p.is_file()}
    require(actual == expected, "modified driver fixtures")
    require(evidence.digest(project / "Cargo.lock") == record["lockSha256"], "modified driver lock")
    verify_lock(root, project)


def verify_reuse(root, record_path, snapshot=None):
    prior = read(record_path)
    require(prior.get("passed") and prior.get("build", {}).get("passed"), "prior driver build did not pass")
    current = source_hashes(root)
    recorded = prior.get("sourceHashes")
    if recorded is None:
        require(snapshot is not None, "legacy reuse requires a source snapshot")
        recorded = {}
        for path in snapshot if isinstance(snapshot, list) else [snapshot]:
            recorded.update(read(path)["files"])
    relevant = {name: value for name, value in recorded.items()
                if name in current or name.startswith((CRATE.as_posix() + "/", ".cargo/"))}
    require(relevant == current, "stale driver build sources")
    project = Path(prior.get("project", record_path.parent / "project"))
    verify_project(root, project, prior)
    tests = prior["tests"]
    require(tests and set(row["test"] for row in tests) == set(CASES)
            and all(row["passed"] and row["caseReportVerified"] for row in tests), "incomplete prior driver checks")
    binary = Path(tests[0]["command"][0])
    digest = evidence.digest(binary)
    require(all(row["binarySha256"] == digest and row["command"] == [str(binary), row["test"]] for row in tests),
            "stale or inconsistent driver binary")
    flags = prior["build"]["environment"]
    require(flags.get("ARIAX_BT_SANITIZER") == "thread"
            and "-fsanitize=thread" in flags.get("RUSTFLAGS", ""), "prior build lacks native TSan")
    return prior, project, binary


def build(root, output, cargo, env):
    project = output / "project"
    (project / "src").mkdir(parents=True)
    (project / "src/main.rs").write_text(driver_source((root / TEST).read_text()), encoding="utf-8")
    shutil.copytree(root / CRATE / "tests/fixtures", project / "src/fixtures")
    (project / "Cargo.toml").write_text(manifest(root), encoding="utf-8")
    shutil.copyfile(root / "Cargo.lock", project / "Cargo.lock")
    lock = evidence.run([cargo, "update", "--offline", "--workspace", "--manifest-path", project / "Cargo.toml"],
                        output / "lock", timeout=60, env=env, cwd=root)
    require(lock["passed"], "driver lock preparation failed")
    verify_lock(root, project)  # Fail before compiling a newly selected cached dependency.
    result = evidence.run([cargo, "build", "--locked", "--offline", "--manifest-path", project / "Cargo.toml"],
                          output / "build", timeout=180, env=env, cwd=root)
    require(result["passed"], "driver build failed or exceeded its short build budget")
    return result, project, Path(env["CARGO_TARGET_DIR"]) / "debug" / NAME


def run_cases(binary, output, repetitions, env, root):
    results = []
    for case in CASES:
        for repeat in range(repetitions):
            directory = output / f"case-{len(results) + 1:02d}"
            result = evidence.run([binary, case], directory, timeout=60, env=env, cwd=root)
            result["caseReportVerified"] = (directory / "stdout.log").read_text().strip() == f"case {case}: passed"
            result["sanitizerReportObserved"] = "ThreadSanitizer" in (directory / "stderr.log").read_text(errors="replace")
            result["remainingFixtures"] = sorted(p.name for p in (output / "tmp").glob("ariax-bt-*"))
            result["passed"] = (result["passed"] and result["caseReportVerified"]
                                and not result["sanitizerReportObserved"] and not result["remainingFixtures"])
            results.append({"test": case, "repetition": repeat + 1, **result})
            evidence.save(output / "tests.json", results)
            if not result["passed"]:
                return results
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("build", "reuse"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-dir", type=Path, required=True)
    parser.add_argument("--cargo", type=Path)
    parser.add_argument("--record", type=Path)
    parser.add_argument("--source-snapshot", type=Path, action="append",
                        help="legacy snapshots, oldest first; repeat for a later partial snapshot")
    parser.add_argument("--repetitions", type=int, choices=range(1, 6), default=1)
    args = parser.parse_args()
    if args.mode == "build" and not args.cargo:
        parser.error("build requires --cargo and the pinned toolchain environment")
    if args.mode == "reuse" and not args.record:
        parser.error("reuse requires --record")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {"passed": False, "scope": "Native C/C++ and FFI only; no Rust race coverage", "mode": args.mode,
              "runnerSha256": evidence.digest(Path(__file__))}
    try:
        require(sys.platform == "linux" and platform.machine() == "x86_64", "native TSan requires x86-64 Linux")
        native = args.native_dir.resolve()
        expected = {key: evidence.digest(ROOT / path) for key, path in NATIVE_INPUTS.items()}
        expected["sanitizer"] = "thread"
        bt_native.verify_manifest(native, "x86_64-unknown-linux-gnu", expected)
        report["nativeManifestSha256"] = evidence.digest(native / "ariax-native.json")
        report["sourceHashes"] = source_hashes(ROOT)
        temporary = output / "tmp"
        temporary.mkdir()
        env = dict(os.environ, TSAN_OPTIONS="halt_on_error=1:exitcode=66", ARIAX_BT_SANITIZER="thread",
                   ARIAX_BT_NATIVE_DIR=str(native), TMPDIR=str(temporary), TEMP=str(temporary), TMP=str(temporary),
                   CARGO_BUILD_JOBS="2", CARGO_INCREMENTAL="0", CARGO_NET_OFFLINE="true")
        if args.mode == "build":
            require(Path(env.get("CARGO_TARGET_DIR", "")).is_absolute(), "select an absolute CARGO_TARGET_DIR")
            require("-fsanitize=thread" in env.get("RUSTFLAGS", ""), "select the Clang TSan linker in RUSTFLAGS")
            built, project, binary = build(ROOT, output, args.cargo.resolve(), env)
        else:
            prior, project, binary = verify_reuse(ROOT, args.record.resolve(), args.source_snapshot)
            built = prior["build"]
            recorded_native = built["environment"].get("ARIAX_BT_NATIVE_DIR")
            if recorded_native is not None:
                require(Path(recorded_native) == native, "different native installation")
            if "nativeManifestSha256" in prior:
                require(prior["nativeManifestSha256"] == report["nativeManifestSha256"], "changed native installation")
            report["reusedRecordSha256"] = evidence.digest(args.record)
            if args.source_snapshot:
                report["reuseSourceSnapshotSha256"] = [evidence.digest(path) for path in args.source_snapshot]
        report.update(build=built, project=str(project), originalSourceSha256=evidence.digest(ROOT / TEST),
                      driverSourceSha256=evidence.digest(project / "src/main.rs"),
                      lockSha256=evidence.digest(project / "Cargo.lock"), dependenciesMatchRepositoryLock=True)
        report["tests"] = run_cases(binary, output, args.repetitions, env, ROOT)
        report["passed"] = (len(report["tests"]) == len(CASES) * args.repetitions
                            and all(row["passed"] for row in report["tests"]))
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = str(error) or type(error).__name__
    evidence.save(output / "result.json", report)
    print(json.dumps({"passed": report["passed"], "error": report.get("error"),
                      "executions": len(report.get("tests", []))}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
