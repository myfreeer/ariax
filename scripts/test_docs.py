"""Behavior checks for the build-free documentation validator."""
from pathlib import Path
import tempfile
import unittest

import check_docs


class DocumentationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def document(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8')
        return path

    def test_relative_links_references_fragments_and_examples(self):
        source = self.document('README.md', '# Project\n'
                               '[file](docs/a%20b.md#option_name--rpc)\n'
                               '[duplicate](docs/a%20b.md#option_name--rpc-1)\n'
                               '[directory](docs)\n[remote](https://example.test/missing)\n'
                               '[reference][ref]\n[ref]: docs/a%20b.md#named\n'
                               '`[inline example](missing.md)`\n'
                               '```markdown\n# Example\n[example](missing.md)\n```\n'
                               '~~~text\n# Another Example\n~~~\n')
        target = self.document('docs/a b.md', '# Target\n## `option_name` / RPC\n'
                               '## `option_name` / RPC\n<a id="named"></a>\n')
        self.assertEqual(check_docs.check_documents(self.root, [source, target]), [])

    def test_rejects_missing_files_anchors_and_extra_titles(self):
        source = self.document('README.md', '# Project\n# Extra\n'
                               '[missing](gone.md)\n[anchor](#gone)\n'
                               '[outside](../outside.md)\n')
        errors = check_docs.check_documents(self.root, [source])
        self.assertEqual(len(errors), 4)
        self.assertTrue(any('expected one H1, found 2' in e for e in errors))
        self.assertTrue(any('missing target: gone.md' in e for e in errors))
        self.assertTrue(any('missing anchor: #gone' in e for e in errors))
        self.assertTrue(any('link leaves repository' in e for e in errors))

    def test_new_subsystem_document_requires_an_index_entry(self):
        index = self.document('docs/README.md', '# Docs\n')
        contract = self.document('docs/storage/contract.md', '# Contract\n')
        documents = [index, contract]
        self.assertEqual(len(check_docs.check_index(self.root, documents)), 1)
        index.write_text('# Docs\n[Contract](storage/contract.md)\n', encoding='utf-8')
        self.assertEqual(check_docs.check_index(self.root, documents), [])

    def test_option_links_resolve_from_repository_root(self):
        self.document('crates/ariax-config/src/registry.rs',
                      'docs: "docs/configuration.md#options",\n')
        generated = self.document('generated/options.json',
                                  '{"options":[{"name":"split",'
                                  '"docs":"docs/configuration.md#options"}]}\n')
        compat = self.document('generated/aria2_compat.json',
                               '{"reviewed":[{"name":"split",'
                               '"docs":"docs/configuration.md#options"}]}\n')
        self.document('docs/configuration.md', '# Configuration\n## Options\n')
        self.assertEqual(check_docs.check_option_links(self.root), [])
        generated.write_text('{"options":[{"name":"split",'
                             '"docs":"configuration.md#options"}]}\n', encoding='utf-8')
        errors = check_docs.check_option_links(self.root)
        self.assertEqual(len(errors), 1)
        self.assertIn('generated/options.json (split): missing target', errors[0])
        compat.write_text('{"reviewed":[{"name":"split",'
                          '"docs":"docs/configuration.md#gone"}]}\n', encoding='utf-8')
        errors = check_docs.check_option_links(self.root)
        self.assertEqual(len(errors), 2)
        self.assertIn('generated/aria2_compat.json (split): missing anchor', errors[1])


if __name__ == '__main__':
    unittest.main()
