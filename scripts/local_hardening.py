#!/usr/bin/env python3
"""Record bounded local commands and parser fuzz campaigns; never dispatch CI."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def test_counts(output, expected):
    rows = re.findall(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", output)
    passed = sum(int(row[1]) for row in rows)
    return bool(rows) and passed == expected and expected > 0 and all(
        row[0] == "ok" and int(row[2]) == 0 and int(row[3]) == 0 for row in rows)


def run(command, directory, *, timeout, expected_tests=None, env=None, cwd=None):
    """Each directory is immutable: even incomplete/failed attempts are retained."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    command = [str(value) for value in command]
    record = {"command": command, "passed": False, "timeoutSeconds": timeout,
              "expectedTests": expected_tests, "exitCode": None, "cleanup": "not-started"}
    selected_env = os.environ if env is None else env
    record["environment"] = {key: selected_env[key] for key in (
        "RUSTC", "RUSTFLAGS", "CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "RUST_TEST_THREADS",
        "ARIAX_BT_SANITIZER", "ASAN_OPTIONS", "UBSAN_OPTIONS", "TSAN_OPTIONS",
        "TMPDIR", "TEMP") if key in selected_env}
    executable = Path(command[0])
    if executable.is_file():
        record["binarySha256"] = digest(executable)
    process = None
    started = time.monotonic()
    try:
        with (directory / "stdout.log").open("wb") as out, (directory / "stderr.log").open("wb") as err:
            options = {"start_new_session": True} if os.name != "nt" else {
                "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
            process = subprocess.Popen(command, cwd=cwd, env=env, stdout=out, stderr=err, **options)
            process.wait(timeout=timeout)
            record["exitCode"] = process.returncode
            record["processSeconds"] = time.monotonic() - started
        if process.returncode != 0:
            raise RuntimeError(f"command exited {process.returncode}")
        if expected_tests is not None and not test_counts(
                (directory / "stdout.log").read_text(encoding="utf-8", errors="replace"), expected_tests):
            raise RuntimeError("missing, failed, ignored, or unexpected test count")
        record["passed"] = True
    except (Exception, KeyboardInterrupt) as error:
        record["error"] = str(error) or type(error).__name__
    finally:
        try:
            if process is not None:
                if os.name != "nt":
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    record["cleanup"] = "owned-process-group-terminated"
                elif process.poll() is None:
                    subprocess.run(["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                                   check=True, capture_output=True, timeout=10)
                    record["cleanup"] = "owned-process-tree-terminated"
                else:
                    record["cleanup"] = "parent-exited; fixture-child-cleanup-requires-verification"
                process.wait(timeout=10)
                record["exitCode"] = process.returncode
        except Exception as error:
            record.update(passed=False, cleanup="failed", cleanupError=str(error))
        record["elapsedSeconds"] = time.monotonic() - started
        for name in ("stdout.log", "stderr.log"):
            if (directory / name).is_file():
                record[name + "Sha256"] = digest(directory / name)
        save(directory / "result.json", record)
    return record


def fuzz_counts(output, requested):
    done = re.search(r"Done (\d+) runs in ", output)
    final = re.search(r"#(\d+)\s+DONE\s+cov:\s*(\d+)", output)
    initialized = re.search(r"#(\d+)\s+INITED\s+cov:", output)
    if not done or not final or not initialized or int(done[1]) != requested or int(final[1]) != requested:
        raise ValueError("missing coverage or incomplete fuzz execution count")
    initialization = int(initialized[1])
    if not 0 < initialization < requested:
        raise ValueError("fuzz burst performed no mutations")
    return {"executions": requested, "initializationExecutions": initialization,
            "mutationExecutions": requested - initialization, "coverage": int(final[2])}


def fuzz(binary, seeds, directory, *, batches=1, seconds=900):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    pool = directory / "corpus"
    pool.mkdir()
    for path in sorted(Path(seeds).glob("*")):
        if path.is_file() and 0 < path.stat().st_size <= 8192:
            shutil.copyfile(path, pool / digest(path))
    record = {"binarySha256": digest(binary), "seedSha256": sorted(p.name for p in pool.iterdir()),
              "requiredExecutions": batches * 512, "acceptedExecutions": 0,
              "initializationExecutions": 0, "mutationExecutions": 0,
              "attempts": [], "passed": False, "budgetSeconds": seconds}
    started, burst, slow = time.monotonic(), 64, 0
    env = dict(os.environ, ASAN_OPTIONS="detect_leaks=1:halt_on_error=1")
    try:
        while record["acceptedExecutions"] < record["requiredExecutions"]:
            if time.monotonic() - started >= seconds:
                raise RuntimeError("target wall budget exhausted")
            number = len(record["attempts"]) + 1
            work = directory / f"input-{number:04d}"
            work.mkdir()
            # Rotate at most eight seeds per process so initialization cannot
            # consume its mutation budget as the retained corpus grows.
            files = sorted(pool.iterdir())
            for index in range(min(8, len(files))):
                path = files[((number - 1) * 8 + index) % len(files)]
                shutil.copyfile(path, work / path.name)
            count = min(burst, record["requiredExecutions"] - record["acceptedExecutions"])
            artifacts = directory / f"artifacts-{number:04d}"
            artifacts.mkdir()
            command = [str(binary), str(work), f"-runs={count}", "-max_len=8192", "-timeout=1",
                       "-rss_limit_mb=512", "-print_funcs=0", f"-seed={20261005 + number}",
                       "-artifact_prefix=" + str(artifacts) + os.sep]
            attempt = run(command, directory / f"attempt-{number:04d}", timeout=5, env=env)
            record["attempts"].append(attempt)
            if not attempt["passed"]:
                raise RuntimeError("fuzz process failed; inspect retained attempt")
            output = (directory / f"attempt-{number:04d}" / "stderr.log").read_text(encoding="utf-8", errors="replace")
            counts = fuzz_counts(output, count)
            attempt.update(counts, accepted=attempt["processSeconds"] <= 0.5)
            if any(artifacts.iterdir()):
                raise RuntimeError("fuzzer retained a failure input")
            if attempt["accepted"]:
                record["acceptedExecutions"] += count
                record["initializationExecutions"] += counts["initializationExecutions"]
                record["mutationExecutions"] += counts["mutationExecutions"]
            else:
                slow += 1
                burst = max(16, burst // 2)
                if slow > 3:
                    raise RuntimeError("more than three over-limit fuzz processes")
            for path in work.iterdir():
                if path.is_file():
                    shutil.copyfile(path, pool / digest(path))
            shutil.rmtree(work)
            save(directory / "result.json", record)
            time.sleep(0.25)
        record["passed"] = True
    except (Exception, KeyboardInterrupt) as error:
        record["error"] = str(error) or type(error).__name__
    finally:
        record["elapsedSeconds"] = time.monotonic() - started
        record["corpusSha256"] = sorted(p.name for p in pool.iterdir())
        save(directory / "result.json", record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    command = sub.add_parser("run")
    command.add_argument("--output", type=Path, required=True)
    command.add_argument("--timeout", type=float, required=True)
    command.add_argument("--expected-tests", type=int)
    command.add_argument("command", nargs=argparse.REMAINDER)
    campaign = sub.add_parser("fuzz")
    campaign.add_argument("--output", type=Path, required=True)
    campaign.add_argument("--binary", type=Path, required=True)
    campaign.add_argument("--seeds", type=Path, required=True)
    campaign.add_argument("--batches", type=int, choices=(1, 20), default=1)
    args = parser.parse_args()
    if args.mode == "run":
        values = args.command[1:] if args.command[:1] == ["--"] else args.command
        if not values or args.timeout <= 0 or (args.expected_tests is not None and args.expected_tests <= 0):
            parser.error("a command, positive timeout and positive test count are required")
        result = run(values, args.output, timeout=args.timeout, expected_tests=args.expected_tests)
    else:
        result = fuzz(args.binary.resolve(), args.seeds, args.output, batches=args.batches)
    print(json.dumps({key: value for key, value in result.items() if key not in ("attempts", "corpusSha256")}))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
