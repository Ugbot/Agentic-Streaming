"""Executable checks for the Markdown under docs/ and the READMEs that link into it.

    python -m pytest docs/tools/test_docs.py

* every relative link resolves to a file in the repository, and a `#fragment` on a Markdown target
  names a heading of that file (GitHub heading slugs);
* every `<!-- snippet: path -->` fenced block on docs/python.md is byte-identical to the file under
  docs/ it names, so the documented code is the code that was run;
* every `<!-- matrix: binding -->` block on docs/runtimes/*.md matches what
  docs/tools/matrix_excerpt.py derives from the generated docs/capabilities.md.
"""
from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import pytest
from matrix_excerpt import BLOCK, RUNTIME_PAGES, load_matrix, render

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
EXTRA_PAGES = [
    REPO / "README.md",
    REPO / "agentic-pekko" / "README.md",
    REPO / "agentic-clj" / "README.md",
    REPO / "ports" / "jagentic-core" / "README.md",
    REPO / "ports" / "pyagentic" / "README.md",
    REPO / "pyflink" / "README.md",
    REPO / "python" / "README.md",
]
PAGES = sorted(DOCS.rglob("*.md")) + [p for p in EXTRA_PAGES if p.exists()]

INLINE_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
FENCE = re.compile(r"^(```|~~~)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
SNIPPET = re.compile(r"<!-- snippet: (\S+) -->\n```python\n(.*?)\n```", re.S)


def _outside_fences(text: str) -> Iterator[tuple[int, str]]:
    fenced = False
    for number, line in enumerate(text.splitlines(), start=1):
        if FENCE.match(line.strip()):
            fenced = not fenced
            continue
        if not fenced:
            yield number, line


def _slug(heading: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def _anchors(path: Path) -> set:
    anchors = set()
    seen: dict = {}
    for _, line in _outside_fences(path.read_text(encoding="utf-8")):
        m = HEADING.match(line)
        if not m:
            continue
        slug = _slug(m.group(2))
        n = seen.get(slug, 0)
        seen[slug] = n + 1
        anchors.add(slug if n == 0 else f"{slug}-{n}")
    return anchors


def _links(page: Path) -> Iterator[tuple[int, str]]:
    for number, line in _outside_fences(page.read_text(encoding="utf-8")):
        for target in INLINE_LINK.findall(line):
            if target.startswith(("http://", "https://", "mailto:", "#")) or "://" in target:
                continue
            yield number, target


def _broken_links(page: Path) -> list[str]:
    problems = []
    for number, target in _links(page):
        path_part, _, fragment = target.partition("#")
        resolved = (page.parent / path_part).resolve()
        where = f"{page.relative_to(REPO)}:{number}: {target}"
        if not resolved.exists():
            shown = resolved.relative_to(REPO) if resolved.is_relative_to(REPO) else resolved
            problems.append(f"{where} -> {shown} does not exist")
            continue
        if fragment and resolved.suffix == ".md" and fragment not in _anchors(resolved):
            problems.append(f"{where}: no heading #{fragment} in {resolved.relative_to(REPO)}")
    return problems


@pytest.mark.parametrize("page", PAGES, ids=lambda p: str(p.relative_to(REPO)))
def test_relative_links_resolve(page: Path) -> None:
    assert _broken_links(page) == []


def test_python_snippets_match_files() -> None:
    page = DOCS / "python.md"
    text = page.read_text(encoding="utf-8")
    blocks = SNIPPET.findall(text)
    assert blocks, "docs/python.md has no <!-- snippet: ... --> blocks"
    mismatched = []
    for rel, body in blocks:
        source = (DOCS / rel).read_text(encoding="utf-8").rstrip("\n")
        if source != body:
            mismatched.append(rel)
    assert mismatched == []
    documented = {rel for rel, _ in blocks}
    on_disk = {str(p.relative_to(DOCS)) for p in (DOCS / "snippets" / "python").glob("*.py")}
    assert on_disk == documented, "every snippet file must appear on docs/python.md and vice versa"


def test_runtime_pages_exist() -> None:
    expected = {"common-primitives", "flink", "pekko", "clojure", "python", "python-facade", "pyflink", "experimental"}
    assert expected <= {p.stem for p in RUNTIME_PAGES}


@pytest.mark.parametrize("page", RUNTIME_PAGES, ids=lambda p: p.name)
def test_matrix_blocks_are_derived_from_capabilities(page: Path) -> None:
    matrix = load_matrix()
    text = page.read_text(encoding="utf-8")
    for m in BLOCK.finditer(text):
        bindings = [b.strip() for b in m.group(1).split(",")]
        assert m.group(2) == render(matrix, bindings), f"{page.name}: run python docs/tools/matrix_excerpt.py --write"


def test_no_dashes_in_new_prose() -> None:
    """The runtime pages and the Python page use plain hyphens only."""
    offenders = []
    for page in [*RUNTIME_PAGES, DOCS / "python.md", DOCS / "pyflink.md"]:
        for number, line in _outside_fences(page.read_text(encoding="utf-8")):
            if "\u2013" in line or "\u2014" in line:
                offenders.append(f"{page.relative_to(REPO)}:{number}")
    assert offenders == []
