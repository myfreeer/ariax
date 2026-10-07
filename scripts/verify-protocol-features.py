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


def verify(graph, bundle, *, openssl=False):
    openssl = openssl or bundle in {"full", "compat"}
    assert "quick-xml" in graph, "Metalink missing from bundle"
    native = {"ariax-bt", "ariax-bt-libtorrent-sys", "cxx", "cxx-build"}
    if bundle in {"full", "compat"}:
        assert native <= graph.keys(), "BitTorrent missing from full/compat"
        assert graph.get("cxx", {}).keys() == {"1.0.202"}, "wrong cxx version"
        assert graph.get("cxx-build", {}).keys() == {"1.0.202"}, "wrong cxx-build version"
        assert "native" in graph["ariax-bt-libtorrent-sys"]["0.1.0"], "native BT feature missing"
    else:
        assert not native & graph.keys(), "BitTorrent dependency outside full/compat"
    for name in graph:
        assert name not in {"aws-lc-rs", "aws-lc-sys", "native-tls", "des", "dsa"}, f"unapproved dependency: {name}"
    if openssl:
        assert set(graph.get("openssl", {})) == {"0.10.81"}, "unexpected OpenSSL binding"
        assert set(graph.get("openssl-sys", {})) == {"0.9.117"}, "unexpected OpenSSL sys binding"
        assert "openssl-src" not in graph, "use the reviewed native OpenSSL installation"
        assert not (graph["openssl"]["0.10.81"] & {"vendored"}), "unreviewed OpenSSL build"
    else:
        assert not ({"openssl", "openssl-sys"} & graph.keys()), "unselected OpenSSL backend"
    if openssl:
        assert graph.get("rustls-openssl") == {"0.4.2": {"tls12"}}, "unexpected TLS provider features"
    else:
        assert "rustls-openssl" not in graph, "unselected OpenSSL TLS provider"
    assert len(graph.get("rustls", {})) == 1
    tls = next(iter(graph["rustls"].values()))
    assert "ring" in tls and tls <= {"log", "logging", "ring", "std", "tls12"}, "unexpected TLS provider/features"
    if bundle == "minimal":
        assert not ({"russh", "russh-sftp", "suppaftp", "ssh-key"} & graph.keys()), "protocol dependency in minimal"
    else:
        for name, (version, features) in EXPECTED.items():
            selected = features | ({"openssl-rsa"} if name == "russh" and openssl else set())
            assert graph.get(name) == {version: selected}, f"unexpected {name} graph: {graph.get(name)}"
        assert set(graph.get("ssh-key", {})) == {"0.7.0-rc.11"}, "ssh-key must match russh's exact re-export"
        assert not (graph["ssh-key"]["0.7.0-rc.11"] & {"dsa", "des", "3des"}), "legacy SSH feature"


def self_test():
    base = {"quick-xml": {"0.41.0": set()}, "rustls": {"0.23.45": {"ring", "std", "tls12"}}}
    verify(base, "minimal")
    standard = copy.deepcopy(base)
    standard.update({n: {v: f.copy()} for n, (v, f) in EXPECTED.items()})
    standard["ssh-key"] = {"0.7.0-rc.11": {"ed25519"}}
    verify(standard, "standard")
    selected = copy.deepcopy(standard)
    selected["russh"]["0.62.4"].add("openssl-rsa")
    selected.update({"openssl": {"0.10.81": {"default"}}, "openssl-sys": {"0.9.117": set()},
                     "rustls-openssl": {"0.4.2": {"tls12"}}})
    verify(selected, "standard", openssl=True)
    for bad in (selected, dict(selected, **{"openssl-src": {"3": set()}})):
        try:
            verify(bad, "standard")
        except AssertionError:
            pass
        else:
            raise AssertionError("accepted an unselected OpenSSL backend")

    full = copy.deepcopy(selected)
    full.update({"ariax-bt": {"0.1.0": {"libtorrent"}},
                 "ariax-bt-libtorrent-sys": {"0.1.0": {"native"}},
                 "cxx": {"1.0.202": {"std"}}, "cxx-build": {"1.0.202": set()}})
    verify(full, "full")
    verify(full, "compat")
    for change in (lambda g: g.pop("rustls-openssl"),
                   lambda g: g["russh"]["0.62.4"].remove("openssl-rsa"),
                   lambda g: g.pop("openssl")):
        bad = copy.deepcopy(full)
        change(bad)
        try:
            verify(bad, "full")
        except AssertionError:
            pass
        else:
            raise AssertionError("accepted split crypto backends despite linked OpenSSL")
    for bundle in ("minimal", "standard"):
        try:
            verify(full, bundle)
        except AssertionError:
            pass
        else:
            raise AssertionError("native BT leaked into a smaller bundle")
    for change, bundle in [(lambda g: g.update({"des": {"1": set()}}), "standard"),
                           (lambda g: g["rustls"]["0.23.45"].add("aws_lc_rs"), "standard"),
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
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--rustup")
    selection.add_argument("--cargo", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    self_test()
    if not args.self_test:
        cargo = ([str(args.cargo)] if args.cargo else ["cargo", f"+{args.rustup}"] if args.rustup
                 else [str(ROOT / "scripts/cargo-local.sh"), "linux"])
        variants = [(bundle, extra) for bundle in ("minimal", "standard", "full", "compat")
                    for extra in ("", "sftp-openssl-rsa", "tls-openssl", "crypto-openssl")]
        for bundle, extra in variants:
            features = bundle + ("," + extra if extra else "")
            output = subprocess.check_output(cargo + ["tree", "--color", "never", "--locked", "-p", "ariax-cli", "--no-default-features", "--features", features,
                "--target", "all", "-e", "normal,build", "--prefix", "none", "--format", "{p}|{f}"], cwd=ROOT, text=True)
            graph = {}
            for line in output.splitlines():
                match = re.fullmatch(r"(\S+) v(\S+)(?: \([^|]+\))?\|([^ ]*)(?: \(\*\))?", line)
                assert match, f"unrecognized Cargo graph row: {line}"
                name, version, resolved_features = match.groups()
                graph.setdefault(name, {}).setdefault(version, set()).update(filter(None, resolved_features.split(",")))
            verify(graph, bundle, openssl=bool(extra))
            print(f"Verified {features}: bounded protocol forks and unified OpenSSL selection.")
