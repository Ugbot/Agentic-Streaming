"""Derive the AGENTIC_FLINK_* environment variables from ConfigKeys.java and check docs/ against them.

    python docs/tools/check_env_vars.py            # check, exit 1 on drift
    python docs/tools/check_env_vars.py --list     # print the derived table
    python docs/tools/check_env_vars.py --write    # rewrite the generated block in docs/configuration.md

AgenticFlinkConfig resolves a key such as ``ollama.base.url`` from the environment variable
``AGENTIC_FLINK_OLLAMA_BASE_URL`` (upper case, dots to underscores, ``AGENTIC_FLINK_`` prefix).
Two kinds of ``AGENTIC_FLINK_*`` names are legitimate in the documentation:

* application keys: the variable maps back to a dotted key that appears as a string literal in a
  Java source file of the repository (every ``ConfigKeys`` constant, plus keys some modules read
  with a literal, for example ``a2a.auth.dev.mode`` in the A2A gateway);
* runtime and tooling variables read directly by scripts or code, for example ``AGENTIC_FLINK_JAR``
  in examples-bin/_common.sh; the literal name must appear in a non-documentation source file.

Anything else is drift and fails the check. The block between ``<!-- env-vars: ConfigKeys -->``
and ``<!-- /env-vars -->`` in docs/configuration.md is generated from ``ConfigKeys.java`` and must
match this script's output.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
CONFIG_KEYS = REPO / "src" / "main" / "java" / "org" / "agentic" / "flink" / "config" / "ConfigKeys.java"
CONFIGURATION_PAGE = DOCS / "configuration.md"
ENV_PREFIX = "AGENTIC_FLINK_"

CONSTANT = re.compile(r'public static final String (?P<name>[A-Z0-9_]+) = "(?P<value>[a-z0-9.]+)";')
ENV_MENTION = re.compile(r"AGENTIC_FLINK_[A-Z0-9][A-Z0-9_]*")
STRING_LITERAL = re.compile(r'"([a-z0-9]+(?:[._][a-z0-9]+)+)"')
BLOCK = re.compile(r"<!-- env-vars: ConfigKeys -->\n(.*?)<!-- /env-vars -->", re.S)

SOURCE_SUFFIXES = {".java", ".sh", ".py", ".yml", ".yaml", ".clj", ".edn", ".toml", ".properties", ".xml", ".kt", ".scala"}
SOURCE_SKIP_DIRS = {".git", "target", "node_modules", ".venv", "venv", "__pycache__", ".mvn"}


def env_name(key: str) -> str:
    return ENV_PREFIX + key.upper().replace(".", "_")


def config_keys() -> list[tuple[str, str]]:
    """(dotted key, environment variable) for every non-DEFAULT string constant in ConfigKeys."""
    rows = []
    for m in CONSTANT.finditer(CONFIG_KEYS.read_text(encoding="utf-8")):
        if m.group("name").startswith("DEFAULT_"):
            continue
        rows.append((m.group("value"), env_name(m.group("value"))))
    return rows


def render_table(rows: list[tuple[str, str]]) -> str:
    lines = ["| Config Key | Environment Variable |", "|---|---|"]
    lines += [f"| `{key}` | `{env}` |" for key, env in rows]
    return "\n".join(lines) + "\n"


def _source_files() -> list[Path]:
    files = []
    for path in REPO.rglob("*"):
        if not path.is_file() or path.suffix not in SOURCE_SUFFIXES:
            continue
        rel = path.relative_to(REPO)
        if rel.parts[0] == "docs" or any(part in SOURCE_SKIP_DIRS for part in rel.parts):
            continue
        files.append(path)
    return files


def known_variables() -> tuple[set[str], set[str]]:
    """Return (application variables derived from Java string literals, literal runtime variables)."""
    application: set[str] = set()
    literal: set[str] = set()
    for path in _source_files():
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        literal.update(ENV_MENTION.findall(text))
        if path.suffix == ".java":
            for key in STRING_LITERAL.findall(text):
                application.add(env_name(key))
    return application, literal


def doc_mentions() -> dict[str, list[str]]:
    mentions: dict[str, list[str]] = {}
    for page in sorted(DOCS.rglob("*.md")):
        for number, line in enumerate(page.read_text(encoding="utf-8").splitlines(), start=1):
            for name in ENV_MENTION.findall(line):
                mentions.setdefault(name, []).append(f"{page.relative_to(REPO)}:{number}")
    return mentions


def drift() -> list[str]:
    application, literal = known_variables()
    derived = {env for _, env in config_keys()}
    problems = []
    for name, where in sorted(doc_mentions().items()):
        if name in derived or name in application or name in literal:
            continue
        problems.append(f"{name} is not derived from any config key or read by any source file: {', '.join(where)}")
    return problems


def generated_block_matches() -> str | None:
    text = CONFIGURATION_PAGE.read_text(encoding="utf-8")
    m = BLOCK.search(text)
    if not m:
        return f"{CONFIGURATION_PAGE.relative_to(REPO)} has no <!-- env-vars: ConfigKeys --> block"
    expected = render_table(config_keys())
    if m.group(1) != expected:
        return f"{CONFIGURATION_PAGE.relative_to(REPO)}: generated table is stale; run python docs/tools/check_env_vars.py --write"
    return None


def write_block() -> None:
    text = CONFIGURATION_PAGE.read_text(encoding="utf-8")
    if not BLOCK.search(text):
        raise SystemExit(f"{CONFIGURATION_PAGE} has no <!-- env-vars: ConfigKeys --> block to rewrite")
    table = render_table(config_keys())
    updated = BLOCK.sub(lambda _: f"<!-- env-vars: ConfigKeys -->\n{table}<!-- /env-vars -->", text)
    CONFIGURATION_PAGE.write_text(updated, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--list", action="store_true", help="print the table derived from ConfigKeys.java")
    parser.add_argument("--write", action="store_true", help="rewrite the generated block in docs/configuration.md")
    args = parser.parse_args(argv)

    if args.list:
        sys.stdout.write(render_table(config_keys()))
        return 0
    if args.write:
        write_block()
        print(f"updated {CONFIGURATION_PAGE.relative_to(REPO)}")
        return 0

    problems = drift()
    stale = generated_block_matches()
    if stale:
        problems.append(stale)
    for problem in problems:
        print(problem)
    if not problems:
        derived = config_keys()
        mentioned = doc_mentions()
        print(f"{len(derived)} config keys derived from ConfigKeys.java; {len(mentioned)} AGENTIC_FLINK_* names in docs/, all resolved")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
