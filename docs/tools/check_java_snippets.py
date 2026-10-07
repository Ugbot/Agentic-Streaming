"""Compile the fenced Java blocks under docs/ that are presented as complete compilation units.

    python docs/tools/check_java_snippets.py              # compile, exit 1 on any failure
    python docs/tools/check_java_snippets.py --list       # show which blocks are selected and why
    python docs/tools/check_java_snippets.py --classpath-file target/example-classpath.txt

Selection rules for a ```java fence:

* the info string carries the word ``complete`` (```java complete): always compiled;
* the info string carries the word ``fragment`` (```java fragment): never compiled, even when the
  body looks like a compilation unit (used for listings that depend on classes outside the default
  build, for example the flink-agents plugin);
* otherwise the block is compiled when its first non-blank line starts with ``package`` or
  ``import`` and it declares a top-level ``class``, ``interface``, ``enum`` or ``record``, which is
  how a reader recognises a full source file rather than an excerpt.

The selected blocks of one page are compiled together in a single ``javac -proc:none`` call, so a
later listing may use a class defined earlier on the same page (as a reader copying the guide
would), against ``target/classes`` plus the
test-scope dependency classpath of the root module (``target/example-classpath.txt``, the same
file examples-bin/_common.sh produces). When that file is absent the script creates it with
``./mvnw -q compile dependency:build-classpath -Dmdep.includeScope=test``; that needs
``./mvnw -f ports/jagentic-core/pom.xml install -DskipTests`` to have run once.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
DEFAULT_CLASSPATH_FILE = REPO / "target" / "example-classpath.txt"
CLASSES_DIR = REPO / "target" / "classes"

FENCE = re.compile(r"^```java(?P<info>[^\n]*)\n(?P<body>.*?)^```", re.S | re.M)
TYPE_NAME = re.compile(r"^\s*public\s+(?:(?:final|abstract|static|sealed|non-sealed)\s+)*(?:class|interface|enum|record)\s+(\w+)", re.M)
ANY_TYPE = re.compile(r"^\s*(?:(?:public|final|abstract|static|sealed|non-sealed)\s+)*(?:class|interface|enum|record)\s+(\w+)", re.M)
FIRST_LINE_UNIT = re.compile(r"^\s*(package|import)\s")


@dataclass
class Snippet:
    page: Path
    line: int
    info: str
    body: str

    @property
    def where(self) -> str:
        return f"{self.page.relative_to(REPO)}:{self.line}"

    @property
    def reason(self) -> str | None:
        """Why the block is compiled, or None when it is a fragment."""
        tokens = self.info.split()
        if "fragment" in tokens:
            return None
        if "complete" in tokens:
            return "marked complete"
        first = next((l for l in self.body.splitlines() if l.strip()), "")
        if FIRST_LINE_UNIT.match(first) and ANY_TYPE.search(self.body):
            return "package/import plus a type declaration"
        return None

    def file_name(self) -> str:
        m = TYPE_NAME.search(self.body) or ANY_TYPE.search(self.body)
        return f"{m.group(1)}.java" if m else "Snippet.java"


def snippets(pages: list[Path] | None = None) -> list[Snippet]:
    found = []
    for page in pages or sorted(DOCS.rglob("*.md")):
        text = page.read_text(encoding="utf-8")
        for m in FENCE.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            found.append(Snippet(page, line, m.group("info"), m.group("body")))
    return found


def selected(pages: list[Path] | None = None) -> list[Snippet]:
    return [s for s in snippets(pages) if s.reason]


def ensure_classpath(classpath_file: Path) -> str:
    mvnw = REPO / "mvnw"
    if not classpath_file.exists() or not CLASSES_DIR.is_dir():
        classpath_file.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            str(mvnw), "-q", "compile", "dependency:build-classpath",
            "-Dmdep.includeScope=test", f"-Dmdep.outputFile={classpath_file}",
        ]
        print("resolving the root module classpath: " + " ".join(cmd), file=sys.stderr)
        result = subprocess.run(cmd, cwd=REPO, text=True, capture_output=True)
        if result.returncode != 0:
            raise SystemExit(
                "could not resolve the module classpath (run ./mvnw -f ports/jagentic-core/pom.xml install -DskipTests first):\n"
                + result.stdout + result.stderr
            )
    deps = classpath_file.read_text(encoding="utf-8").strip()
    return os.pathsep.join([str(CLASSES_DIR), deps])


def compile_page(page_snippets: list[Snippet], classpath: str, javac: str) -> str | None:
    """Compile every selected block of one page in a single javac call; None on success."""
    with tempfile.TemporaryDirectory(prefix="docs-java-") as tmp:
        sources = []
        for index, snippet in enumerate(page_snippets):
            src_dir = Path(tmp) / f"block{index}"
            src_dir.mkdir()
            src = src_dir / snippet.file_name()
            src.write_text(snippet.body, encoding="utf-8")
            sources.append((str(src), snippet.where))
        out = Path(tmp) / "out"
        out.mkdir()
        cmd = [javac, "-proc:none", "-Xlint:none", "-encoding", "UTF-8", "-d", str(out), "-cp", classpath]
        cmd += [path for path, _ in sources]
        result = subprocess.run(cmd, cwd=tmp, text=True, capture_output=True)
        if result.returncode == 0:
            return None
        text = result.stderr
        for path, where in sources:
            text = text.replace(path, where)
        return text.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--list", action="store_true", help="list the selected blocks without compiling")
    parser.add_argument("--classpath-file", type=Path, default=DEFAULT_CLASSPATH_FILE)
    parser.add_argument("pages", nargs="*", type=Path, help="restrict to these Markdown files")
    args = parser.parse_args(argv)

    pages = [p.resolve() for p in args.pages] or None
    chosen = selected(pages)
    if args.list:
        for s in chosen:
            print(f"{s.where}: {s.reason} -> {s.file_name()}")
        print(f"{len(chosen)} of {len(snippets(pages))} java blocks selected")
        return 0

    javac = shutil.which("javac")
    if javac is None:
        raise SystemExit("javac not found on PATH; a JDK 21 is required")
    classpath = ensure_classpath(args.classpath_file)

    by_page: dict[Path, list[Snippet]] = {}
    for s in chosen:
        by_page.setdefault(s.page, []).append(s)

    failures = []
    failed_blocks = 0
    for page, page_snippets in by_page.items():
        error = compile_page(page_snippets, classpath, javac)
        rel = page.relative_to(REPO)
        if error:
            failed_blocks += len(page_snippets)
            failures.append(f"{rel} ({len(page_snippets)} complete blocks) failed to compile:\n{error}")
            print(f"FAIL {rel}: {', '.join(str(s.line) for s in page_snippets)}")
        else:
            print(f"ok   {rel}: {', '.join(str(s.line) for s in page_snippets)}")
    for f in failures:
        print("\n" + f)
    print(f"\n{len(chosen) - failed_blocks} of {len(chosen)} complete java snippets compiled ({len(by_page)} pages)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
