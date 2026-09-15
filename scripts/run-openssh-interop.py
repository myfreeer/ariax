#!/usr/bin/env python3
"""Run one bounded test against an isolated OpenSSH process; never edit a service."""
import argparse
import csv
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]


def converted(path, mode):
    if mode == "native":
        return str(path)
    windows = subprocess.check_output(["wslpath", "-m", str(path)], text=True).strip()
    if mode == "windows":
        return windows
    return "/" + windows[0].lower() + windows[2:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sshd", required=True, type=Path)
    parser.add_argument("--test-binary", required=True, type=Path)
    parser.add_argument("--path-mode", choices=("native", "windows", "msys"), default="native")
    parser.add_argument("--user", required=True)
    parser.add_argument("--log", required=True, type=Path)
    args = parser.parse_args()
    assert all(c not in args.user for c in "\r\n \t")
    (ROOT / "toolchains").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="openssh-fixture-", dir=ROOT / "toolchains") as temporary, tempfile.TemporaryDirectory(prefix="ariax-ssh-client-") as private:
        directory = Path(temporary)
        fixture = ROOT / "crates/ariax-engine/tests/fixtures/sftp-test-host"
        key = directory / "host-key"
        authorized = directory / "authorized-keys"
        shutil.copyfile(fixture, key)
        shutil.copyfile(fixture.with_suffix(".pub"), authorized)
        key.chmod(0o600)
        payload = directory / "payload"
        payload.write_bytes(b"abcdefghijkl")
        output = directory / "output"
        output.mkdir()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        path = lambda p: converted(p, args.path_mode)
        config = directory / "sshd_config"
        config.write_text(f"ListenAddress 127.0.0.1\nPort {port}\nHostKey {path(key)}\n"
            f"PidFile {path(directory / 'pid')}\nAuthorizedKeysFile {path(authorized)}\n"
            f"AllowUsers {args.user}\nStrictModes no\nPasswordAuthentication no\nPubkeyAuthentication yes\n"
            "UseDNS no\nPermitRootLogin yes\nSubsystem sftp internal-sftp\nForceCommand internal-sftp\nLogLevel VERBOSE\n")
        command = [str(args.sshd.resolve()), "-D", "-e", "-f", path(config)]
        if args.path_mode == "msys":
            # Native descendants need MSYS paths for their runtime DLLs.
            command = [str(args.sshd.resolve().parent / "env.exe"), "MSYSTEM=MINGW64",
                "PATH=/usr/bin:/bin", path(args.sshd.resolve()), "-D", "-e", "-f", path(config)]
        args.log.parent.mkdir(parents=True, exist_ok=True)
        with args.log.open("wb") as log:
            server = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 5
                while True:
                    if server.poll() is not None:
                        raise RuntimeError(f"sshd exited; see {args.log}")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                            break
                    except OSError:
                        assert time.monotonic() < deadline, "sshd startup deadline"
                        time.sleep(0.02)
                env = os.environ.copy()
                env["ARIAX_OPENSSH_URI"] = f"sftp://{quote(args.user,safe='')}@127.0.0.1:{port}/{quote(path(payload).lstrip('/'),safe='/:')}"
                test = args.test_binary.resolve()
                windows_test = test.suffix == ".exe" and os.name != "nt"
                client_key = directory / "client-key" if windows_test else Path(private) / "client-key"
                shutil.copyfile(fixture, client_key)
                known_hosts = client_key.with_name("known-hosts")
                known_hosts.write_bytes(b"")
                if windows_test:
                    identity = subprocess.check_output([
                        str(Path(os.environ["ARIAX_WINDOWS_SYSTEM32"]) / "whoami.exe"), "/user", "/fo", "csv", "/nh"
                    ], text=True)
                    client_sid = next(csv.reader(identity.splitlines()))[1]
                    assert client_sid.startswith("S-1-") and all(c in "S-0123456789" for c in client_sid)
                for client_file in (client_key, known_hosts):
                    if windows_test:
                        acl_command = [str(Path(os.environ["ARIAX_WINDOWS_SYSTEM32"]) / "icacls.exe"), converted(client_file, "windows")]
                        subprocess.run(acl_command + ["/reset"], check=True, stdout=subprocess.DEVNULL)
                        subprocess.run(acl_command + ["/setowner", f"*{client_sid}"],
                            check=True, stdout=subprocess.DEVNULL)
                        subprocess.run(acl_command + ["/inheritance:r", "/grant:r",
                            f"*{client_sid}:(F)", "*S-1-5-18:(F)", "*S-1-5-32-544:(F)"],
                            check=True, stdout=subprocess.DEVNULL)
                    else:
                        client_file.chmod(0o600)
                env["ARIAX_OPENSSH_KEY"] = converted(client_key, "windows") if windows_test else str(client_key)
                env["ARIAX_OPENSSH_KNOWN_HOSTS"] = converted(known_hosts, "windows") if windows_test else str(known_hosts)
                env["ARIAX_OPENSSH_OUTPUT"] = converted(output, "windows") if windows_test else str(output)
                # WSL does not forward arbitrary Linux environment variables to
                # native children. Set fixture authority inside the native env.
                command = ([str(ROOT / "scripts/with-windows-gnu-env.sh"), "/usr/bin/env",
                    *[f"{name}={env[name]}" for name in ("ARIAX_OPENSSH_URI", "ARIAX_OPENSSH_KEY", "ARIAX_OPENSSH_KNOWN_HOSTS", "ARIAX_OPENSSH_OUTPUT")],
                    converted(test, "windows")]
                    if windows_test else [str(test)])
                subprocess.run(command + ["--ignored", "--exact", "openssh_public_key_offsets_and_final_attributes_interoperate", "--nocapture"],
                    env=env, check=True, timeout=20)
            finally:
                pid_file = directory / "pid"
                if args.path_mode == "msys" and pid_file.is_file():
                    daemon_pid = pid_file.read_text().strip()
                    assert daemon_pid.isdigit() and int(daemon_pid) > 1, "invalid fixture daemon PID"
                    subprocess.run([str(args.sshd.resolve().parent / "kill.exe"), "-TERM", daemon_pid],
                        check=True, timeout=5)
                    try:
                        server.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        subprocess.run([str(args.sshd.resolve().parent / "kill.exe"), "-KILL", daemon_pid],
                            check=False, timeout=5)
                if server.poll() is None:
                    server.terminate()
                    try:
                        server.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait(timeout=5)


if __name__ == "__main__":
    main()
