"""A clean current tree cannot hide workstation material in earlier commits."""
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import publication


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "--quiet")

    def git(self, *arguments):
        return subprocess.check_output(["git", "-c", "user.name=Fixture", "-c",
                                       "user.email=fixture@example.invalid", *arguments],
                                      cwd=self.root, stderr=subprocess.STDOUT)

    def commit(self, message="Fixture"):
        self.git("add", ".")
        self.git("-c", "commit.gpgsign=false", "commit", "--quiet", "-m", message)

    def test_portable_code_and_system_paths_pass(self):
        (self.root / "script").write_text('/usr/bin/env bash\n${REPO_ROOT}/scripts/test\n')
        self.commit()
        self.assertEqual(publication.audit(self.root), [])
        self.assertEqual(publication.audit(self.root, history=True), [])

    def test_removed_workstation_path_still_fails_history_audit(self):
        path = self.root / "script"
        path.write_text("/mnt/" + "q/private-checkout/tool")
        self.commit()
        self.assertTrue(publication.audit(self.root))
        path.write_text("${REPO_ROOT}/tool")
        self.commit()
        self.assertEqual(publication.audit(self.root), [])
        self.assertTrue(publication.audit(self.root, history=True))

    def test_commit_messages_and_local_only_files_are_checked(self):
        directory = self.root / "toolchains"
        directory.mkdir()
        (directory / "local.json").write_text("{}")
        self.commit("Build from " + "Q:/" + "Users/developer/checkouts")
        self.assertTrue(publication.audit(self.root))
        issues = publication.audit(self.root, history=True)
        self.assertTrue(any("workstation path" in issue for issue in issues))
        self.assertTrue(any("local-only file" in issue for issue in issues))

    def test_environment_file_is_rejected_without_opening_its_contents(self):
        (self.root / ".env").write_text("fixture only")
        self.commit()
        with patch.object(Path, "read_bytes", side_effect=AssertionError("must not open environment file")):
            self.assertEqual(publication.audit(self.root), [".env: local-only file"])


if __name__ == "__main__":
    unittest.main()
