#!/usr/bin/env python3
"""Run checked CI commands and collect bounded native benchmark evidence."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
RUST_VERSION = "1.97.1"
ACTIONLINT_VERSION = "1.7.12"
ACTIONLINT_SHA256 = "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8"
CHECKS = ("linux", "macos", "windows-msvc", "windows-gnu", "msrv-linux",
          "msrv-windows-gnu", "feature-minimal", "feature-standard", "feature-full",
          "feature-compat")
SCENARIOS = ("http", "websocket", "content-length", "ndjson", "administrative")
COMPILERS = {"rustc", "cargo", "clippy-driver", "gcc", "g++", "cc", "c++", "cc1",
             "cc1plus", "ld", "lld", "rust-lld", "collect2", "make", "ninja", "cmake"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def aggregate_success(event, ref, preflight, validation, benchmarks):
    expected_benchmark = "success" if event == "push" and ref == "refs/heads/main" else "skipped"
    return (event in {"push", "pull_request"} and preflight == "success"
            and validation == "success" and benchmarks == expected_benchmark)


def resolve_toolchain():
    configured = os.environ.get("ARIAX_TOOLCHAIN_ROOT")
    if configured:
        return Path(configured)
    version = os.environ.get("ARIAX_RUST_TOOLCHAIN", RUST_VERSION)
    cargo = subprocess.check_output(
        ["rustup", "which", "--toolchain", version, "cargo"], text=True).strip()
    return Path(cargo).parent.parent


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def checked_command(command, log_path, *, env=None, cwd=ROOT, capture=False):
    """Preserve the child's exit status even when its output is streamed."""
    command = [str(arg) for arg in command]
    print("RUN " + json.dumps(command), flush=True)
    lines = []
    with Path(log_path).open("w", encoding="utf-8") as log:
        with subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                              errors="replace") as process:
            for line in process.stdout:
                log.write(line)
                log.flush()
                print(line, end="", flush=True)
                if capture:
                    lines.append(line)
            code = process.wait()
    if code:
        raise subprocess.CalledProcessError(code, command)
    return "".join(lines)


class Runner:
    def __init__(self, name):
        self.name = name
        self.directory = ROOT / "toolchains/ci-reports" / name
        self.directory.mkdir(parents=True, exist_ok=True)
        self.target = ROOT / "toolchains/ci-target" / name
        self.tools = resolve_toolchain() / "bin"
        self.suffix = ".exe" if os.name == "nt" else ""
        self.env = os.environ.copy()
        self.env.update({
            "CARGO": str(self.tool("cargo")), "RUSTC": str(self.tool("rustc")),
            "RUSTDOC": str(self.tool("rustdoc")), "RUSTFMT": str(self.tool("rustfmt")),
            "CLIPPY_DRIVER": str(self.tool("clippy-driver")),
            "CARGO_TARGET_DIR": str(self.target), "PYTHONDONTWRITEBYTECODE": "1",
            "RUST_TEST_THREADS": "2",
            "RUST_TEST_NOCAPTURE": "1",
        })
        if os.name != "nt":
            # macOS's system temporary directory can traverse /var -> /private/var.
            # Select its real parent before fixtures create private descendants;
            # production persistence paths must continue to reject symlinks.
            self.env["TMPDIR"] = str(Path(tempfile.gettempdir()).resolve(strict=True))
        if os.name == "nt":
            self.env["ARIAX_REQUIRE_WINDOWS_REPARSE_TEST"] = "1"
        if "windows-gnu" in name:
            require(os.name == "nt" and self.env.get("MSYSTEM") == "MINGW64",
                    "Windows GNU validation requires native MSYS2 MINGW64")
            self.env.update({"CC": "gcc.exe", "CXX": "g++.exe", "AR": "ar.exe"})
        self.commands = []
        self.started = time.monotonic()

    def tool(self, name):
        return self.tools / (name + self.suffix)

    def run(self, command, *, capture=False, env=None):
        index = len(self.commands) + 1
        log = self.directory / f"{index:02d}.log"
        record = {"command": [str(arg) for arg in command], "log": log.name, "passed": False}
        self.commands.append(record)
        try:
            output = checked_command(command, log, env=env or self.env, capture=capture)
        except subprocess.CalledProcessError as error:
            record["exitCode"] = error.returncode
            raise
        record.update(passed=True, exitCode=0, sha256=sha256(log))
        return output

    def cargo(self, *arguments, capture=False, env=None):
        operation = arguments[0]
        command = self.tool("cargo-" + operation) if operation in {"fmt", "clippy"} else self.tool("cargo")
        selected = (env or self.env).copy()
        if operation == "clippy":
            selected["CARGO_TARGET_DIR"] = str(self.target.with_name(self.name + "-clippy"))
        return self.run([command, *arguments], capture=capture, env=selected)

    def finish(self, passed, error=None):
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        report = {"check": self.name, "sourceCommit": commit, "passed": passed,
                  "host": platform.platform(), "machine": platform.machine(),
                  "elapsedSeconds": round(time.monotonic() - self.started, 3),
                  "commands": self.commands, "error": error}
        (self.directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")


def actionlint():
    directory = ROOT / "toolchains/ci-tools" / ("actionlint-" + ACTIONLINT_VERSION)
    archive = directory / "archive.tar.gz"
    directory.mkdir(parents=True, exist_ok=True)
    if not archive.exists() or sha256(archive) != ACTIONLINT_SHA256:
        url = (f"https://github.com/rhysd/actionlint/releases/download/v{ACTIONLINT_VERSION}/"
               f"actionlint_{ACTIONLINT_VERSION}_linux_amd64.tar.gz")
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read(16 * 1024 * 1024 + 1)
        require(len(content) <= 16 * 1024 * 1024, "actionlint archive exceeds its cap")
        require(hashlib.sha256(content).hexdigest() == ACTIONLINT_SHA256, "actionlint checksum mismatch")
        archive.write_bytes(content)
    # Extract only the expected regular executable, never archive-selected paths.
    with tarfile.open(fileobj=io.BytesIO(archive.read_bytes()), mode="r:gz") as bundle:
        member = bundle.getmember("actionlint")
        require(member.isfile() and member.size <= 32 * 1024 * 1024, "invalid actionlint executable")
        executable = directory / "actionlint"
        executable.write_bytes(bundle.extractfile(member).read())
    executable.chmod(0o755)
    return executable


def preflight(runner):
    runner.run([sys.executable, "-B", "-m", "unittest", "discover", "-s", "scripts", "-p", "test_*.py"])
    runner.run([actionlint(), "-shellcheck=", "-pyflakes="])
    runner.run([sys.executable, "-B", "scripts/publication.py"])
    runner.run(["git", "show", "--format=", "--check", "HEAD"])
    runner.cargo("fmt", "--all", "--", "--check")
    runner.run(["bash", "scripts/test-verify-rusqlite-features.sh"])
    runner.run(["bash", "scripts/verify-rusqlite-features.sh", "--cargo", runner.tool("cargo")])
    runner.run([sys.executable, "scripts/verify-protocol-vendors.py"])
    runner.run([sys.executable, "scripts/verify-protocol-features.py", "--cargo", runner.tool("cargo")])
    pin = dict(line.split("=", 1) for line in (ROOT / "compat/aria2-reference.pin").read_text().splitlines())
    require(re.fullmatch(r"[0-9a-f]{40}", pin["commit"]), "invalid aria2 reference pin")
    reference = ROOT / "toolchains/ci-reference"
    runner.run(["git", "init", str(reference)])
    runner.run(["git", "-C", str(reference), "fetch", "--depth=1", pin["repository"], pin["commit"]])
    runner.run(["git", "-C", str(reference), "checkout", "--detach", "FETCH_HEAD"])
    runner.cargo("xtask", "verify-aria2", str(reference))
    runner.cargo("xtask", "verify-contracts", str(reference))
    environment = runner.env.copy()
    environment["LIBSQLITE3_FLAGS"] = "-DARIAX_UNEXPECTED_AMBIENT_SQLITE_FLAG=1"
    runner.cargo("test", "--locked", "-p", "ariax-xtask",
                 "session_contracts::tests::cargo_build_uses_repository_sqlite_flags",
                 "--", "--exact", env=environment)


def validate(runner):
    runner.run([runner.tool("rustc"), "--version", "--verbose"])
    if runner.name.startswith("msrv-"):
        runner.cargo("check", "--locked", "--workspace", "--all-targets", "--all-features")
    elif runner.name.startswith("feature-"):
        feature = runner.name.removeprefix("feature-")
        arguments = ("--locked", "-p", "ariax-cli", "--no-default-features", "--features", feature)
        runner.cargo("test", *arguments)
        runner.cargo("build", *arguments, "--profile", "release-cli")
    else:
        runner.cargo("build", "--locked", "--workspace")
        runner.cargo("test", "--locked", "--workspace")
        runner.cargo("test", "--locked", "--workspace", "--all-features")
        runner.cargo("clippy", "--locked", "--workspace", "--all-targets", "--all-features", "--", "-D", "warnings")
        if runner.name == "linux":
            runner.cargo("build", "--locked", "-p", "ariax-core", "--profile", "release-capi")


def integer(value, name, *, minimum=0, maximum=None):
    require(type(value) is int and value >= minimum, "invalid benchmark field: " + name)
    require(maximum is None or value <= maximum, "benchmark limit exceeded: " + name)
    return value


def validate_latencies(values):
    require(isinstance(values, dict) and bool(values), "invalid operation latency report")
    for name, item in values.items():
        integer(item.get("calls"), name + ".calls", minimum=1)
        integer(item.get("p99Us"), name + ".p99Us", maximum=50_000)


def validate_benchmark(report, scenario, metalink=True):
    require(isinstance(report, dict) and report.get("scenario") == scenario, "wrong benchmark scenario")
    require(report.get("os") == "linux", "native Linux evidence requires a Linux report")
    require(report.get("complete") is True and report.get("passed") is True, "incomplete or failed benchmark")
    integer(report.get("elapsedScenarioMs"), "elapsedScenarioMs", maximum=90_000)
    integer(report.get("controlRuntime", {}).get("maxSteps"), "controlRuntime.maxSteps", maximum=32)
    if scenario == "administrative":
        require(report.get("activeRanges") == 0 and report.get("resultSetupRemovals") == 128,
                "administrative scenario cardinality mismatch")
        operations = report.get("operations")
        require(isinstance(operations, list) and len(operations) == 6, "incomplete administrative operations")
        expected = {"ariax.importSession": 128, "aria2.unpauseAll": 128, "aria2.pauseAll": 127,
                    "ariax.exportSession": 128, "aria2.saveSession": 128, "aria2.purgeDownloadResult": 128}
        require({operation.get("method") for operation in operations} == expected.keys(), "wrong administrative operation set")
        for operation in operations:
            require(operation.get("completed") is True
                    and operation.get("targets") == expected[operation["method"]],
                    "incomplete administrative operation")
            # The later per-task mutation wins over the bulk unpause snapshot.
            result_count = {"aria2.unpauseAll": ("resumedTasks", 127),
                            "aria2.pauseAll": ("pausedTasks", 127),
                            "aria2.purgeDownloadResult": ("deletedTasks", 128)}.get(operation["method"])
            if result_count:
                field, count = result_count
                require(operation.get(field) == count, "wrong administrative result cardinality")
            integer(operation.get("maxQueryBurstUs"), "maxQueryBurstUs", maximum=500_000)
            integer(operation.get("cooldownMs"), "cooldownMs", minimum=250)
            if operation.get("urgentUs") is not None:
                integer(operation["urgentUs"], "urgentUs", maximum=50_000)
            validate_latencies(operation.get("queries"))
        integer(report.get("shutdownAcknowledgementUs"), "shutdownAcknowledgementUs", maximum=50_000)
        return
    require(scenario in SCENARIOS, "unknown benchmark scenario")
    require(report.get("rangeAdmission") == ("metalink" if metalink else "addUri"), "wrong admission fixture")
    require(report.get("ranges") == 1_000 and report.get("samples") == 20_000
            and report.get("verificationCalls") == 1_000, "incomplete transport measurements")
    require(report.get("renewedBarrierAfterWarmup") is True and report.get("perStatusRangeCheck") is True,
            "missing renewed active-range evidence")
    require(report.get("controlCalls") == 1_000
            and report.get("shutdownDrainBoundary") == "engine process exit",
            "missing control mutation or shutdown evidence")
    integer(report.get("shutdownAcknowledgementUs"), "shutdownAcknowledgementUs", maximum=50_000)
    integer(report.get("maxBurstCalls"), "maxBurstCalls", minimum=1, maximum=1_000)
    integer(report.get("maxBurstMs"), "maxBurstMs", maximum=500)
    integer(report.get("cooldownMs"), "cooldownMs", minimum=250)
    integer(report.get("p99Us"), "p99Us", maximum=50_000)
    require(bool(report.get("operations")), "missing per-operation latency evidence")
    validate_latencies(report["operations"])
    counts = {"tellStatus": 12_000, "tellWaiting": 4_000, "getFiles": 1_000,
              "getUris": 1_000, "getOption": 1_000,
              **dict.fromkeys(("addUri", "changeOption", "changePosition", "changeUri",
                               "pause", "remove", "removeDownloadResult", "unpause"), 125)}
    require({name: value["calls"] for name, value in report["operations"].items()} == counts,
            "incomplete per-operation measurements")
    for peak, cap in (("maxRpcBytes", "rpcLimit"), ("maxResidentBytes", "residentLimit"),
                      ("maxSampledRssBytes", "rssLimit")):
        integer(report.get(peak), peak, maximum=integer(report.get(cap), cap, minimum=1))


def compiler_processes():
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            name = (path / "comm").read_text().strip()
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        if name in COMPILERS:
            result.append({"pid": int(path.name), "name": name})
    return result


def measure_scenario(runner, binary, scenario, metalink):
    directory = runner.directory / scenario
    directory.mkdir(exist_ok=False)
    command = [str(binary), "--administrative" if scenario == "administrative" else "--scenario=" + scenario]
    env = runner.env.copy()
    env.update(ARIAX_RUN_ACTIVE_RPC_BENCH="1", ARIAX_BENCH_METALINK="1" if metalink else "0")
    observed = []
    start = time.monotonic()
    record = {"scenario": scenario, "command": command, "passed": False,
              "binarySha256": sha256(binary), "compilerProcessesObserved": observed}
    process = None
    try:
        require(not compiler_processes(), "compiler activity before benchmark")
        with (directory / "stdout.jsonl").open("wb") as output, (directory / "stderr.log").open("wb") as errors:
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output, stderr=errors,
                                       start_new_session=True)
            while process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    found = compiler_processes()
                    if found:
                        observed.append({"elapsedSeconds": time.monotonic() - start, "processes": found})
                    require(not found, "compiler activity during benchmark")
                    require(time.monotonic() - start <= 110, "benchmark outer deadline expired")
            record["exitCode"] = process.returncode
        require(process.returncode == 0, "benchmark process failed")
        reports = [json.loads(line) for line in (directory / "stdout.jsonl").read_text().splitlines() if line.strip()]
        require(len(reports) == 1, "benchmark must emit exactly one complete report")
        record["report"] = reports[0]
        validate_benchmark(reports[0], scenario, metalink)
        record["passed"] = True
    except BaseException as error:
        record["error"] = str(error) or type(error).__name__
        raise
    finally:
        # The session/group is created solely for this scenario and its children.
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
        record["elapsedWallSeconds"] = round(time.monotonic() - start, 3)
        for name in ("stdout.jsonl", "stderr.log"):
            path = directory / name
            if path.exists():
                record[name + "Sha256"] = sha256(path)
        (directory / "run.json").write_text(json.dumps(record, indent=2) + "\n")
        print(json.dumps({key: record[key] for key in ("scenario", "passed", "elapsedWallSeconds")}), flush=True)


def benchmark(runner):
    require(sys.platform == "linux", "native Linux benchmark runner required")
    require("microsoft" not in platform.release().lower(), "WSL does not establish native Linux acceptance")
    build = runner.cargo("bench", "--locked", "-p", "ariax-engine", "--all-features",
                         "--bench", "rpc_active_profile", "--no-run", "--message-format=json", capture=True)
    executables = set()
    for line in build.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("reason") == "compiler-artifact" and item.get("target", {}).get("name") == "rpc_active_profile":
            if item.get("executable"):
                executables.add(item["executable"])
    require(len(executables) == 1, "cannot identify the compiled benchmark binary")
    binary = Path(executables.pop())
    runner.run([runner.tool("rustc"), "--version", "--verbose"])
    files = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).split(b"\0")
    source_hashes = {os.fsdecode(path): sha256(ROOT / os.fsdecode(path)) for path in files if path}
    manifest = {"sourceCommit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "sourceFileSha256": source_hashes, "binarySha256": sha256(binary),
                "host": platform.platform(), "machine": platform.machine(), "logicalCpus": os.cpu_count()}
    (runner.directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    metalink = os.environ.get("ARIAX_BENCH_METALINK", "1") == "1"
    for scenario in SCENARIOS:
        time.sleep(2)
        measure_scenario(runner, binary, scenario, metalink)


def interrupted(_signum, _frame):
    raise KeyboardInterrupt("CI cancelled")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("resolve-toolchain", "preflight", "benchmark", "gate"):
        commands.add_parser(name)
    commands.add_parser("validate").add_argument("--check", choices=CHECKS, required=True)
    args = parser.parse_args()
    if args.command == "resolve-toolchain":
        print("ARIAX_TOOLCHAIN_ROOT=" + str(resolve_toolchain()))
        return 0
    if args.command == "gate":
        passed = aggregate_success(os.environ.get("GITHUB_EVENT_NAME"), os.environ.get("GITHUB_REF"),
                                   os.environ.get("ARIAX_CI_PREFLIGHT"), os.environ.get("ARIAX_CI_VALIDATION"),
                                   os.environ.get("ARIAX_CI_BENCHMARKS"))
        print("All required CI stages passed." if passed else "A required CI stage failed, was cancelled, or did not run.")
        return 0 if passed else 1
    signal.signal(signal.SIGTERM, interrupted)
    name = args.check if args.command == "validate" else "benchmarks" if args.command == "benchmark" else args.command
    runner = Runner(name)
    try:
        {"preflight": preflight, "validate": validate, "benchmark": benchmark}[args.command](runner)
    except BaseException as error:
        runner.finish(False, str(error) or type(error).__name__)
        raise
    runner.finish(True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
