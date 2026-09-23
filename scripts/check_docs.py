#!/usr/bin/env python3
"""Check first-party Markdown, navigation and option documentation without a build."""
import html
import json
from pathlib import Path
import re
import subprocess
import unicodedata
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
INLINE_LINK = re.compile(r'!?\[[^\]\n]+\]\(\s*(?:<([^>\n]+)>|([^\s)]+))(?:\s+"[^"\n]*")?\s*\)')
REFERENCE = re.compile(r'^ {0,3}\[[^]\n]+\]:\s*(?:<([^>\n]+)>|(\S+))', re.MULTILINE)


def prose(content):
    """Mask fenced examples while preserving line positions for diagnostics."""
    result = []
    fence = None
    for line in content.splitlines(keepends=True):
        marker = re.match(r'^ {0,3}(`{3,}|~{3,})(.*)$', line)
        if marker and fence is None:
            fence = marker.group(1)
            result.append('\n')
        elif (marker and fence and marker.group(1)[0] == fence[0]
              and len(marker.group(1)) >= len(fence) and not marker.group(2).strip()):
            fence = None
            result.append('\n')
        else:
            result.append('\n' if fence else line)
    return re.sub(r'<!--.*?-->', lambda m: '\n' * m.group().count('\n'),
                  ''.join(result), flags=re.DOTALL)


def anchors(content):
    visible = prose(content)
    used = set()
    for heading in re.finditer(r'^ {0,3}#{1,6}\s+(.+?)\s*#*$', visible, re.MULTILINE):
        title = re.sub(r'\[([^]]+)\]\([^)]*\)', r'\1', heading.group(1))
        title = html.unescape(re.sub(r'<[^>]*>', '', title)).lower()
        slug = ''.join(c for c in title if c in '-_' or c.isspace()
                       or unicodedata.category(c)[0] in 'LNM').replace(' ', '-')
        candidate, count = slug, 0
        while candidate in used:
            count += 1
            candidate = f'{slug}-{count}'
        used.add(candidate)
    used.update(re.findall(r'<a\s+(?:id|name)=["\']([^"\']+)', visible))
    return used


def links(content):
    visible = re.sub(r'(`+).*?\1', '', prose(content))
    for pattern in (INLINE_LINK, REFERENCE):
        for match in pattern.finditer(visible):
            yield visible.count('\n', 0, match.start()) + 1, match.group(1) or match.group(2)


def local_target(root, source, url):
    parsed = urlsplit(url)
    if parsed.scheme or parsed.netloc:
        return None
    path = unquote(parsed.path)
    if path.startswith('/'):
        target = root / path.lstrip('/')
    else:
        target = source.parent / path if path else source
    return target.resolve(), unquote(parsed.fragment)


def link_error(root, source, url, cache):
    resolved = local_target(root, source, url)
    if resolved is None:
        return None
    target, fragment = resolved
    if not target.is_relative_to(root):
        return f'link leaves repository: {url}'
    if not target.exists():
        return f'missing target: {url}'
    if fragment and target.suffix.lower() == '.md':
        if target not in cache:
            cache[target] = anchors(target.read_text(encoding='utf-8'))
        if fragment not in cache[target]:
            return f'missing anchor: {url}'
    return None


def check_documents(root, documents):
    issues, cache = [], {}
    for source in documents:
        content = source.read_text(encoding='utf-8')
        name = source.relative_to(root)
        count = len(re.findall(r'^#\s+\S', prose(content), re.MULTILINE))
        if count != 1:
            issues.append(f'{name}: expected one H1, found {count}')
        for line, url in links(content):
            error = link_error(root, source, url, cache)
            if error:
                issues.append(f'{name}:{line}: {error}')
    return issues


def check_index(root, documents):
    index = root / 'docs/README.md'
    linked = {resolved[0] for _, url in links(index.read_text(encoding='utf-8'))
              if (resolved := local_target(root, index, url)) is not None}
    return [f'docs/README.md: missing index entry for {source.relative_to(root)}'
            for source in documents
            if source.is_relative_to(root / 'docs') and source != index and source not in linked]


def check_option_links(root):
    registry = root / 'crates/ariax-config/src/registry.rs'
    urls = re.findall(r'\bdocs:\s*"([^"]+\.md[^"]*)"', registry.read_text(encoding='utf-8'))
    sources = [(str(registry.relative_to(root)), url) for url in urls]
    for filename, field in [('options.json', 'options'), ('aria2_compat.json', 'reviewed')]:
        generated = root / 'generated' / filename
        contracts = json.loads(generated.read_text(encoding='utf-8'))
        sources += [(f'generated/{filename} ({option["name"]})', option['docs'])
                    for option in contracts[field]]
    issues, cache = [], {}
    for source, url in sources:
        # Registry metadata uses repository-root paths, unlike Markdown links.
        error = link_error(root, root / 'README.md', url, cache)
        if error:
            issues.append(f'{source}: {error}')
    return issues


def main():
    names = subprocess.check_output(
        ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z', '--', '*.md'],
        cwd=ROOT).decode().split('\0')
    documents = sorted({ROOT / name for name in names if name and (ROOT / name).is_file()
                        and (not name.startswith('vendor/') or name == 'vendor/README.md')})
    issues = check_documents(ROOT, documents) + check_index(ROOT, documents) + check_option_links(ROOT)
    if issues:
        print('\n'.join(issues))
        return 1
    print(f'Documentation checks passed: {len(documents)} files, local links, anchors, '
          'index coverage and option URLs.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
