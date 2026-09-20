"""Portable native-tool discovery, precedence, and rejection behavior."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import local_paths


class LocalPathTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for name, value in (("ROOT", self.root), ("CONFIGURATION", self.root / "local-paths.json")):
            context = patch.object(local_paths, name, value)
            context.start()
            self.addCleanup(context.stop)
        context = patch.dict(os.environ, {}, clear=True)
        context.start()
        self.addCleanup(context.stop)

    def tool(self, name):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture")
        path.chmod(0o755)
        return path

    def test_relative_configuration_and_environment_override(self):
        configured = self.tool("configured/icacls.exe")
        override = self.tool("override/icacls.exe")
        local_paths.CONFIGURATION.write_text(json.dumps({"windows_system32": "configured"}))
        self.assertEqual(local_paths.windows_tool("icacls.exe"), configured)
        with patch.dict(os.environ, ARIAX_WINDOWS_SYSTEM32=str(override.parent)):
            self.assertEqual(local_paths.windows_tool("icacls.exe"), override)

    def test_msys_discovery_requires_matching_bash_and_compiler(self):
        launcher = self.tool("installation/usr/bin/env.exe")
        with patch("local_paths.shutil.which", return_value=str(launcher)):
            with self.assertRaises(RuntimeError):
                local_paths.msys2_env()
            self.tool("installation/usr/bin/bash.exe")
            self.tool("installation/mingw64/bin/gcc.exe")
            self.assertEqual(local_paths.msys2_env(), launcher)

    def test_invalid_explicit_override_does_not_fall_back_to_path(self):
        tool = self.tool("path/whoami.exe")
        with patch("local_paths.shutil.which", return_value=str(tool)):
            self.assertEqual(local_paths.windows_tool("whoami.exe"), tool)
            for value in ("", "missing", "bad\npath"):
                with self.subTest(value=value), patch.dict(os.environ, ARIAX_WINDOWS_SYSTEM32=value):
                    with self.assertRaises(RuntimeError):
                        local_paths.windows_tool("whoami.exe")

    def test_missing_configuration_and_tools_are_rejected(self):
        with patch("local_paths.shutil.which", return_value=None):
            for resolve in (local_paths.msys2_env, lambda: local_paths.windows_tool("icacls.exe")):
                with self.assertRaises(RuntimeError):
                    resolve()
        local_paths.CONFIGURATION.write_text("[]")
        with self.assertRaises(RuntimeError):
            local_paths.windows_tool("whoami.exe")
        with self.assertRaises(RuntimeError):
            local_paths.windows_tool("../arbitrary.exe")


if __name__ == "__main__":
    unittest.main()
