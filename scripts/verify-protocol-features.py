#!/usr/bin/env python3
"""Check the resolved protocol and crypto graph of every public feature bundle."""
import argparse
import copy
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "russh": ("0.62.4", {"flate2", "ring", "rsa"}),
    "russh-sftp": ("2.3.0", set()),
    "suppaftp": ("10.0.1", {"async-secure", "tokio", "tokio-rustls-ring"}),
}


def verify(graph, bundle):
    assert "quick-xml" in graph, "Metalink missing from bundle"
    for name in graph:
        assert name not in {"aws-lc-rs", "aws-lc-sys", "openssl", "openssl-sys", "native-tls", "des", "dsa"}, f"unapproved dependency: {name}"
    assert len(graph.get("rustls", {})) == 1
    tls = next(iter(graph["rustls"].values()))
    assert "ring" in tls and tls <= {"log", "logging", "ring", "std", "tls12"}, "unexpected TLS provider/features"
    if bundle == "minimal":
        assert not ({"russh", "russh-sftp", "suppaftp", "ssh-key"} & graph.keys()), "protocol dependency in minimal"
    else:
        for name, (version, features) in EXPECTED.items():
            assert graph.get(name) == {version: features}, f"unexpected {name} graph: {graph.get(name)}"
        assert set(graph.get("ssh-key", {})) == {"0.7.0-rc.11"}, "ssh-key must match russh's exact re-export"
        assert not (graph["ssh-key"]["0.7.0-rc.11"] & {"dsa", "des", "3des"}), "legacy SSH feature"


def self_test():
    base = {"quick-xml": {"0.41.0": set()}, "rustls": {"0.23.42": {"ring", "std", "tls12"}}}
    verify(base, "minimal")
    standard = copy.deepcopy(base)
    standard.update({n: {v: f.copy()} for n, (v, f) in EXPECTED.items()})
    standard["ssh-key"] = {"0.7.0-rc.11": {"ed25519"}}
    verify(standard, "standard")
    for change, bundle in [(lambda g: g.update({"des": {"1": set()}}), "standard"),
                           (lambda g: g["rustls"]["0.23.42"].add("aws_lc_rs"), "standard"),
                           (lambda g: g["russh"]["0.62.4"].add("default"), "standard"),
                           (lambda g: None, "minimal")]:
        bad = copy.deepcopy(standard)
        change(bad)
        try:
            verify(bad, bundle)
        except AssertionError:
            continue
        raise AssertionError("graph verifier accepted a forbidden closure")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustup")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    self_test()
    if not args.self_test:
        cargo = ["cargo", f"+{args.rustup}"] if args.rustup else [str(ROOT / "scripts/cargo-local.sh"), "linux"]
        for bundle in ("minimal", "standard", "full", "compat"):
            output = subprocess.check_output(cargo + ["tree", "--locked", "-p", "ariax-cli", "--no-default-features", "--features", bundle,
                "--target", "all", "-e", "normal,build", "--prefix", "none", "--format", "{p}|{f}"], cwd=ROOT, text=True)
            graph = {}
            for line in output.splitlines():
                match = re.fullmatch(r"(\S+) v(\S+)(?: \([^|]+\))?\|([^ ]*)(?: \(\*\))?", line)
                assert match, f"unrecognized Cargo graph row: {line}"
                name, version, features = match.groups()
                graph.setdefault(name, {}).setdefault(version, set()).update(filter(None, features.split(",")))
            verify(graph, bundle)
            print(f"Verified {bundle}: bounded protocol forks and one ring TLS provider.")
