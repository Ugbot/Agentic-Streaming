"""Fail unless the release tag and python/pyproject.toml agree on the package version.

    python tools/check_release_version.py v1.0.0a2            # prints "1.0.0a2" on success
    python tools/check_release_version.py v1.0.0a2 dist/*.whl # also checks the built artifacts

The tag is `v<version>` or `<version>` (PEP 440). Used by .github/workflows/publish-pypi.yml
so the published version is the one the tag names, never a stale static number.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


class VersionMismatch(SystemExit):
    def __init__(self, message: str):
        super().__init__(f"release version check failed: {message}")


def version_from_tag(tag: str) -> str:
    name = tag.strip()
    if name.startswith("refs/tags/"):
        name = name[len("refs/tags/"):]
    if not name:
        raise VersionMismatch("empty tag; pass the release tag (for example v1.0.0a2)")
    return name[1:] if name.startswith("v") else name


def version_from_pyproject(path: Path = PYPROJECT) -> str:
    with path.open("rb") as fh:
        return str(tomllib.load(fh)["project"]["version"])


def version_from_artifact(path: Path) -> str:
    name = path.name
    m = re.match(r"^agentic[_-]flink-([^-]+?)(?:-py3|\.tar\.gz$)", name)
    if not m:
        raise VersionMismatch(f"cannot read a version from artifact name {name!r}")
    return m.group(1)


def check(tag: str, artifacts: list[Path] = (), pyproject: Path = PYPROJECT) -> str:
    tagged = version_from_tag(tag)
    declared = version_from_pyproject(pyproject)
    if tagged != declared:
        raise VersionMismatch(
            f"tag {tag!r} names version {tagged!r} but {pyproject} declares {declared!r}; "
            "bump pyproject.toml (or retag) so they agree")
    for artifact in artifacts:
        built = version_from_artifact(artifact)
        if built != tagged:
            raise VersionMismatch(f"artifact {artifact.name} is version {built!r}, expected {tagged!r}")
    return tagged


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    print(check(argv[1], [Path(a) for a in argv[2:]]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
