import io
import errno
import hashlib
import http.client
import http.server
import json
from pathlib import Path
import tarfile
import tempfile
import socket
import ssl
import threading
import unittest
import urllib.error
from types import SimpleNamespace
from unittest import mock

import bt_native


class NativeDownloadTests(unittest.TestCase):
    def test_release_paths_require_reviewed_target_epoch_and_map_both_roots(self):
        work = Path(tempfile.gettempdir()).resolve() / 'native'
        cache = work.parent / 'cache'
        config = bt_native.release_configuration("x86_64-unknown-linux-gnu", work, cache, "123")
        self.assertEqual(config["opensslDirectories"]["OPENSSLDIR"], "/etc/ssl")
        self.assertEqual(len(config["flags"]), 2)
        for target, epoch in (("x86_64-pc-windows-msvc", "123"),
                              ("x86_64-unknown-linux-gnu", None), ("x86_64-unknown-linux-gnu", "now")):
            with self.assertRaises(ValueError):
                bt_native.release_configuration(target, work, cache, epoch)

    def test_retained_source_cache_is_read_only_and_rejects_changed_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            cache = Path(temporary)
            directory = cache / "sources/boost-0123456789abcdef"
            source = directory / "boost"
            source.mkdir(parents=True)
            header = source / "header.h"; header.write_text("original")
            spec = {"sha256": "0123456789abcdef" * 4}
            marker = directory / ".complete.json"
            marker.write_text(json.dumps({"root": "boost", "sha256": spec["sha256"],
                                          "files": bt_native.inventory(source)}))
            self.assertEqual(bt_native.retained_source_tree("boost", spec, cache), source)
            header.write_text("changed")
            with self.assertRaisesRegex(ValueError, "source drift"):
                bt_native.retained_source_tree("boost", spec, cache)
            self.assertEqual(header.read_text(), "changed")
            record = json.loads(marker.read_text()); record["root"] = "../escape"
            marker.write_text(json.dumps(record))
            with self.assertRaisesRegex(ValueError, "unsafe"):
                bt_native.retained_source_tree("boost", spec, cache)

    def test_custom_output_cannot_bypass_sanitizer_target_validation_or_modify_source_cache(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary).resolve()
            args = SimpleNamespace(target="x86_64-pc-windows-gnu", sanitizer="address", work_dir=work)
            with mock.patch.object(bt_native, "native_target", return_value="mingw64"):
                with self.assertRaisesRegex(ValueError, "Linux target"):
                    bt_native.build(args)
            args = SimpleNamespace(target="x86_64-unknown-linux-gnu", sanitizer="none", work_dir=work,
                                   source_cache=work / 'cache')
            with mock.patch.object(bt_native, "native_target", return_value="linux-x86_64"):
                with self.assertRaisesRegex(ValueError, "overlaps"):
                    bt_native.build(args)

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.body = b"verified native archive"
        self.spec = {"archive": "native.tar.gz", "url": "https://example.test/native.tar.gz",
                     "sha256": hashlib.sha256(self.body).hexdigest()}
        self.path = self.root / self.spec["archive"]
        self.partial = self.root / "native.tar.gz.part"

    def response(self, body=None, length=None):
        response = io.BytesIO(self.body if body is None else body)
        response.headers = {} if length is None else {"Content-Length": str(length)}
        return response

    def test_transient_open_failures_retry_then_reuse_only_verified_cache(self):
        failures = [ConnectionResetError(54, "Connection reset by peer"),
                    urllib.error.HTTPError(self.spec["url"], 503, "Unavailable", {}, None)]
        with mock.patch.object(bt_native.urllib.request, "urlopen",
                               side_effect=[*failures, self.response()]) as request, \
                mock.patch.object(bt_native.time, "sleep") as sleep:
            self.assertEqual(bt_native.fetch(self.spec, self.root, None), self.path)
            self.assertEqual(self.path.read_bytes(), self.body)
            bt_native.fetch(self.spec, self.root, None)
            self.assertEqual(request.call_count, 3, "verified cache must avoid networking")
            self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])
            self.assertTrue(all(call.kwargs["timeout"] == 60 for call in request.call_args_list))
        self.assertFalse(self.partial.exists())

    def test_interrupted_body_discards_partial_before_starting_fresh(self):
        for error in (ConnectionResetError(54, "reset"), http.client.IncompleteRead(b"prefix", 4)):
            with self.subTest(error=error):
                response = self.response()
                response.read = mock.Mock(side_effect=[b"partial content", error])
                attempts = 0

                def open_response(*args, **kwargs):
                    nonlocal attempts
                    self.assertFalse(self.path.exists())
                    self.assertFalse(self.partial.exists())
                    attempts += 1
                    return response if attempts == 1 else self.response()

                with mock.patch.object(bt_native.urllib.request, "urlopen", side_effect=open_response), \
                        mock.patch.object(bt_native.time, "sleep"):
                    bt_native.fetch(self.spec, self.root, None)
                self.assertEqual(attempts, 2)
                self.assertEqual(self.path.read_bytes(), self.body)
                self.path.unlink()

    def test_short_http_body_retries_before_hash_validation(self):
        with mock.patch.object(bt_native.urllib.request, "urlopen", side_effect=[
                self.response(b"short", len(self.body)), self.response(length=len(self.body))]) as request, \
                mock.patch.object(bt_native.time, "sleep"):
            bt_native.fetch(self.spec, self.root, None)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(self.path.read_bytes(), self.body)

    def test_real_http_transfer_recovers_from_truncation_and_service_unavailability(self):
        body = self.body
        attempts = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                attempts.append(self.path)
                self.send_response(503 if len(attempts) == 2 else 200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body[:5] if len(attempts) == 1 else body)
                self.wfile.flush()
                self.close_connection = True

        with http.server.HTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
            worker.start()
            try:
                spec = dict(self.spec, url=f"http://127.0.0.1:{server.server_port}/native.tar.gz")
                with mock.patch.object(bt_native.time, "sleep"):
                    bt_native.fetch(spec, self.root, None)
            finally:
                server.shutdown()
                worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(attempts, ["/native.tar.gz"] * 3)
        self.assertEqual(self.path.read_bytes(), body)
        self.assertFalse(self.partial.exists())

    def test_exhausted_transient_download_fails_without_publishing_or_leaving_partial(self):
        with mock.patch.object(bt_native.urllib.request, "urlopen",
                               side_effect=urllib.error.URLError(TimeoutError("timed out"))) as request, \
                mock.patch.object(bt_native.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.URLError):
                bt_native.fetch(self.spec, self.root, None)
        self.assertEqual(request.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])
        self.assertFalse(self.path.exists())
        self.assertFalse(self.partial.exists())

    def test_integrity_and_size_failures_do_not_retry_or_publish(self):
        for body, limit, message in ((b"tampered", 1024, "hash mismatch"),
                                     (self.body, 1, "exceeds limit")):
            with self.subTest(message=message), \
                    mock.patch.object(bt_native.urllib.request, "urlopen", return_value=self.response(body)) as request, \
                    mock.patch.object(bt_native, "DOWNLOAD_LIMIT", limit), \
                    mock.patch.object(bt_native.time, "sleep") as sleep:
                with self.assertRaisesRegex(ValueError, message):
                    bt_native.fetch(self.spec, self.root, None)
                request.assert_called_once()
                sleep.assert_not_called()
                self.assertFalse(self.path.exists())
                self.assertFalse(self.partial.exists())

    def test_permanent_http_tls_and_local_errors_are_not_retried(self):
        errors = [urllib.error.HTTPError(self.spec["url"], code, "Rejected", {}, None)
                  for code in (403, 404)]
        errors += [urllib.error.URLError(ssl.SSLCertVerificationError(1, "untrusted certificate")),
                   OSError(errno.ENOSPC, "disk full")]
        for error in errors:
            with self.subTest(error=error), \
                    mock.patch.object(bt_native.urllib.request, "urlopen", side_effect=error) as request, \
                    mock.patch.object(bt_native.time, "sleep") as sleep:
                with self.assertRaises(type(error)):
                    bt_native.fetch(self.spec, self.root, None)
                request.assert_called_once()
                sleep.assert_not_called()
                self.assertFalse(self.path.exists())
                self.assertFalse(self.partial.exists())

    def test_retry_classification_keeps_permanent_dns_and_http_failures_fatal(self):
        for code in (408, 429, 500, 502, 503, 504):
            with urllib.error.HTTPError(self.spec["url"], code, "retry", {}, None) as error:
                self.assertTrue(bt_native.transient_download_error(error))
        self.assertTrue(bt_native.transient_download_error(
            urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, "temporary DNS"))))
        self.assertFalse(bt_native.transient_download_error(
            urllib.error.URLError(socket.gaierror(socket.EAI_NONAME, "unknown host"))))


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

    def test_exact_patch_preserves_utf8_and_lf_under_a_legacy_locale(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.cc"
            source.write_bytes("// caf\u00e9 \u2014 \U0001f512\nold\n".encode("utf-8"))
            patch = root / "change.patch"
            patch.write_bytes(("--- a/source.cc\n+++ b/source.cc\n@@ -1,2 +1,2 @@\n"
                               " // caf\u00e9 \u2014 \U0001f512\n-old\n+new \U0001f512\n").encode("utf-8"))
            original_open = Path.open

            def legacy_open(path, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
                if "b" not in mode:
                    if encoding in (None, "locale"):
                        encoding = "gbk"
                    if newline is None and "w" in mode:
                        newline = "\r\n"
                return original_open(path, mode, buffering, encoding, errors, newline)

            with mock.patch.object(Path, "open", legacy_open):
                bt_native.apply_patch(root, patch)
                # Reapplying a valid patch still rejects changed source exactly.
                with self.assertRaisesRegex(ValueError, "does not match"):
                    bt_native.apply_patch(root, patch)
            self.assertEqual(source.read_bytes(), "// caf\u00e9 \u2014 \U0001f512\nnew \U0001f512\n".encode("utf-8"))

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
        self.assertEqual(len({bt_native.work_directory(target, mode) for mode in ("none", "address", "thread")}), 3)
        for target, mode in ((target, "unknown"), ("x86_64-pc-windows-gnu", "address"), ("x86_64-pc-windows-gnu", "thread")):
            with self.assertRaises(ValueError):
                bt_native.work_directory(target, mode)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "ariax-native.json").write_text(json.dumps({
                "target": "x86_64-unknown-linux-gnu", "inputs": {"sanitizer": "address"}, "files": {}}))
            with self.assertRaisesRegex(ValueError, "stale or wrong-ABI"):
                bt_native.verify_manifest(root, "x86_64-unknown-linux-gnu", {"sanitizer": "none"})
            with self.assertRaisesRegex(ValueError, "stale or wrong-ABI"):
                bt_native.verify_manifest(root, "x86_64-unknown-linux-gnu", {"sanitizer": "thread"})

    def test_instrumented_build_installs_the_same_configuration_it_compiles(self):
        # Run the real orchestration against fake tool outputs, without fetching
        # sources or compiling native dependencies in the Python regression.
        for sanitizer, configuration, release in (("none", "Release", False), ("address", "RelWithDebInfo", False),
                                                   ("thread", "RelWithDebInfo", False), ("none", "Release", True)):
            with self.subTest(sanitizer=sanitizer, release=release), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                spec = root / "sources.json"
                spec.write_text(json.dumps({
                    **{name: {"version": "test", "sha256": "fixed"} for name in ("boost", "openssl", "libtorrent")},
                    "settings": {"CMAKE_BUILD_TYPE": "Release"}}))
                patch = root / "ariax.patch"
                patch.write_text("")
                openssl_patch = root / "openssl.patch"
                openssl_patch.write_text("--- a/callback.c\n+++ b/callback.c\n@@ -1 +1 @@\n-old\n+adapter\n")
                sources = {}
                for name, license_file in (("boost", "LICENSE_1_0.txt"), ("openssl", "LICENSE.txt"), ("libtorrent", "LICENSE")):
                    sources[name] = root / name
                    sources[name].mkdir()
                    (sources[name] / license_file).write_text("test license")
                (sources["boost"] / "boost").mkdir()
                (sources["boost"] / "boost/version.hpp").write_text("test header")
                (sources["openssl"] / "callback.c").write_text("old\n")
                commands = []

                def run(command, **kwargs):
                    commands.append(command)
                    if "install_dev" in command:
                        prefix = Path(kwargs["cwd"]).parent / "openssl-install"
                        (prefix / "include").mkdir(parents=True, exist_ok=True)
                        (prefix / "lib").mkdir(exist_ok=True)
                        (prefix / "lib/libcrypto.a").write_bytes(b"test library")
                    if command[:2] == ["cmake", "--install"]:
                        prefix = Path(command[2]).parent / "install"
                        (prefix / "include").mkdir(parents=True, exist_ok=True)
                        (prefix / "lib").mkdir(exist_ok=True)
                    return SimpleNamespace(returncode=0, stdout="test compiler", stderr="")

                with mock.patch.multiple(bt_native, ROOT=root, SPEC=spec, PATCH=patch,
                                         OPENSSL_PATCH=openssl_patch), \
                        mock.patch.dict(bt_native.os.environ, SOURCE_DATE_EPOCH="123"), \
                        mock.patch.object(bt_native, "native_target", return_value="linux-x86_64"), \
                        mock.patch.object(bt_native, "source_tree", side_effect=lambda name, *_: sources[name]), \
                        mock.patch.object(bt_native.subprocess, "run", side_effect=run):
                    args = SimpleNamespace(target="x86_64-unknown-linux-gnu", sanitizer=sanitizer,
                                           verify=False, dependencies_only=False, archive_dir=None, jobs=2,
                                           release_paths=release)
                    bt_native.build(args)
                    work = bt_native.work_directory(args.target, sanitizer)
                    self.assertEqual((work / "openssl-source/callback.c").read_text(), "adapter\n")
                    count = len(commands)
                    bt_native.build(args)
                    self.assertEqual(len(commands), count, "unchanged native installation is reused")
                    openssl_patch.write_text(openssl_patch.read_text().replace("+adapter", "+updated adapter"))
                    args.verify = True
                    with self.assertRaisesRegex(ValueError, "stale or wrong-ABI"):
                        bt_native.build(args)
                    self.assertEqual(len(commands), count, "verification must never rebuild")
                    args.verify = False
                    bt_native.build(args)
                    self.assertGreater(len(commands), count)
                    self.assertEqual((work / "openssl-source/callback.c").read_text(), "updated adapter\n")
                for verb in ("--build", "--install"):
                    command = next(command for command in commands if command[:2] == ["cmake", verb])
                    self.assertEqual(command[command.index("--config") + 1], configuration)
                configure = next(command for command in commands if command[:2] == ["cmake", "-S"])
                self.assertIn("-DCMAKE_BUILD_TYPE=" + configuration, configure)
                self.assertEqual(any("fsanitize=address,undefined" in arg for arg in configure), sanitizer == "address")
                self.assertEqual(any("fsanitize=thread" in arg for arg in configure), sanitizer == "thread")
                openssl = next(command for command in commands if command[:2] == ["perl", "Configure"])
                self.assertEqual("-fsanitize=thread" in openssl, sanitizer == "thread")
                if release:
                    self.assertTrue(any("@ariax-remap.rsp" in arg for arg in openssl))
                    build = next(command for command in commands if "build_libs" in command)
                    self.assertIn("OPENSSLDIR=/etc/ssl", build)
                    self.assertTrue(any("-ffile-prefix-map=" in arg for arg in configure))
                    self.assertIn("/ariax-native-work", (work / "openssl-source/ariax-remap.rsp").read_text())


if __name__ == "__main__":
    unittest.main()
