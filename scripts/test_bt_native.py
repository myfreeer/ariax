import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import bt_native


class NativeBuildTests(unittest.TestCase):
    def test_archive_extracts_regular_sources_and_rejects_escaping_or_link_entries(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for index, (name, kind) in enumerate((("source/file", tarfile.REGTYPE),
                                                 ("../escape", tarfile.REGTYPE),
                                                 ("source/link", tarfile.SYMTYPE),
                                                 ("source/device", tarfile.CHRTYPE))):
                archive = root / f"{index}.tar.gz"
                with tarfile.open(archive, "w:gz") as output:
                    entry = tarfile.TarInfo(name)
                    entry.type = kind
                    entry.linkname = "../../escape"
                    entry.size = 4 if kind == tarfile.REGTYPE else 0
                    output.addfile(entry, io.BytesIO(b"data") if entry.size else None)
                destination = root / f"out-{index}"
                if index == 0:
                    tree = bt_native.extract(archive, destination)
                    self.assertEqual((tree / "file").read_bytes(), b"data")
                else:
                    with self.assertRaises(ValueError):
                        bt_native.extract(archive, destination)
                    self.assertFalse(destination.exists())
            self.assertFalse((root / "escape").exists())

    def test_exact_patch_validates_all_files_before_any_write(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "a").write_text("one\ntwo\n")
            (root / "b").write_text("three\n")
            patch = root / "change.patch"
            prefix = "--- a/a\n+++ b/a\n@@ -1,2 +1,2 @@\n one\n-two\n+second\n"
            patch.write_text(prefix + "--- a/b\n+++ b/b\n@@ -1 +1 @@\n-wrong\n+third\n")
            with self.assertRaisesRegex(ValueError, "does not match"):
                bt_native.apply_patch(root, patch)
            self.assertEqual((root / "a").read_text(), "one\ntwo\n")
            patch.write_text(prefix + "--- a/b\n+++ b/b\n@@ -1 +1 @@\n-three\n+third\n")
            bt_native.apply_patch(root, patch)
            self.assertEqual((root / "a").read_text(), "one\nsecond\n")
            self.assertEqual((root / "b").read_text(), "third\n")

    def test_manifest_rejects_tampering_and_target_or_source_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / "library.a"
            library.write_bytes(b"native")
            inputs = {"patchSha256": "exact"}
            (root / "ariax-native.json").write_text(json.dumps({
                "target": "x86_64-unknown-linux-gnu", "inputs": inputs,
                "files": {"library.a": bt_native.digest(library)}}))
            bt_native.verify_manifest(root, "x86_64-unknown-linux-gnu", inputs)
            for target, expected in (("x86_64-pc-windows-gnu", inputs),
                                     ("x86_64-unknown-linux-gnu", {"patchSha256": "stale"})):
                with self.assertRaises(ValueError):
                    bt_native.verify_manifest(root, target, expected)
            library.write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                bt_native.verify_manifest(root, "x86_64-unknown-linux-gnu", inputs)

    def test_native_targets_never_mix_linux_and_windows(self):
        self.assertEqual(bt_native.native_target("x86_64-unknown-linux-gnu", "Linux", "x86_64"), "linux-x86_64")
        self.assertEqual(bt_native.native_target("aarch64-apple-darwin", "Darwin", "arm64"), "darwin64-arm64-cc")
        with self.assertRaises(ValueError):
            bt_native.native_target("x86_64-pc-windows-msvc", "Linux", "x86_64")
        with self.assertRaises(ValueError):
            bt_native.native_target("aarch64-unknown-linux-gnu", "Linux", "x86_64")

    def test_sanitizer_builds_cannot_reuse_normal_or_other_platform_installations(self):
        target = "x86_64-unknown-linux-gnu"
        self.assertNotEqual(bt_native.work_directory(target, "none"),
                            bt_native.work_directory(target, "address"))
        for target, mode in ((target, "unknown"), ("x86_64-pc-windows-gnu", "address")):
            with self.assertRaises(ValueError):
                bt_native.work_directory(target, mode)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "ariax-native.json").write_text(json.dumps({
                "target": "x86_64-unknown-linux-gnu", "inputs": {"sanitizer": "address"}, "files": {}}))
            with self.assertRaisesRegex(ValueError, "stale or wrong-ABI"):
                bt_native.verify_manifest(root, "x86_64-unknown-linux-gnu", {"sanitizer": "none"})

    def test_instrumented_build_installs_the_same_configuration_it_compiles(self):
        # Run the real orchestration against fake tool outputs, without fetching
        # sources or compiling native dependencies in the Python regression.
        for sanitizer, configuration in (("none", "Release"), ("address", "RelWithDebInfo")):
            with self.subTest(sanitizer=sanitizer), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                spec = root / "sources.json"
                spec.write_text(json.dumps({
                    **{name: {"version": "test", "sha256": "fixed"} for name in ("boost", "openssl", "libtorrent")},
                    "settings": {"CMAKE_BUILD_TYPE": "Release"}}))
                patch = root / "ariax.patch"
                patch.write_text("")
                sources = {}
                for name, license_file in (("boost", "LICENSE_1_0.txt"), ("openssl", "LICENSE.txt"), ("libtorrent", "LICENSE")):
                    sources[name] = root / name
                    sources[name].mkdir()
                    (sources[name] / license_file).write_text("test license")
                (sources["boost"] / "boost").mkdir()
                (sources["boost"] / "boost/version.hpp").write_text("test header")
                commands = []

                def run(command, **kwargs):
                    commands.append(command)
                    if "install_dev" in command:
                        prefix = Path(kwargs["cwd"]).parent / "openssl-install"
                        (prefix / "include").mkdir(parents=True)
                        (prefix / "lib").mkdir()
                        (prefix / "lib/libcrypto.a").write_bytes(b"test library")
                    if command[:2] == ["cmake", "--install"]:
                        prefix = Path(command[2]).parent / "install"
                        (prefix / "include").mkdir(parents=True)
                        (prefix / "lib").mkdir()
                    return SimpleNamespace(returncode=0, stdout="test compiler", stderr="")

                with mock.patch.multiple(bt_native, ROOT=root, SPEC=spec, PATCH=patch), \
                        mock.patch.object(bt_native, "native_target", return_value="linux-x86_64"), \
                        mock.patch.object(bt_native, "source_tree", side_effect=lambda name, *_: sources[name]), \
                        mock.patch.object(bt_native.subprocess, "run", side_effect=run):
                    args = SimpleNamespace(target="x86_64-unknown-linux-gnu", sanitizer=sanitizer,
                                           verify=False, dependencies_only=False, archive_dir=None, jobs=2)
                    bt_native.build(args)
                for verb in ("--build", "--install"):
                    command = next(command for command in commands if command[:2] == ["cmake", verb])
                    self.assertEqual(command[command.index("--config") + 1], configuration)
                configure = next(command for command in commands if command[:2] == ["cmake", "-S"])
                self.assertIn("-DCMAKE_BUILD_TYPE=" + configuration, configure)
                self.assertEqual(any("fsanitize=address,undefined" in arg for arg in configure), sanitizer == "address")


if __name__ == "__main__":
    unittest.main()
