"""Fail unless the release tag, the setuptools-scm version of every Python project and the built
artifacts all name the same version.

    python tools/check_release_version.py v1.0.0rc1                # tag -> "1.0.0rc1", checks the checkout
    python tools/check_release_version.py v1.0.0rc1 dist/*         # also checks artifact file names
    python tools/check_release_version.py v1.0.0rc1 --complete dist/*   # every project: sdist and wheel

The tag is `v<version>`, `<version>` or `refs/tags/v<version>`, and <version> must be a
canonical PEP 440 public version (1.0.0, 1.0.0a1, 1.0.0rc1, 1.0.0.post1), because that is the
form setuptools-scm and the wheel file names use, so the three can be compared literally.

The version of the four Python projects (ports/pyagentic, python, pyflink, ports/agentic-pipeline)
is not written in their pyproject.toml; setuptools-scm reads it from the tag that the checkout
sits on. This script runs setuptools-scm in each project directory and fails when any of them
resolves to something other than the tagged version, for example because the checkout has
commits after the tag or uncommitted changes. Used by .github/workflows/publish-pypi.yml and
publish-testpypi.yml; see docs/release.md.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Distribution name -> project directory, relative to the repository root.
PROJECTS: dict[str, Path] = {
    "pyagentic": Path("ports/pyagentic"),
    "agentic-flink": Path("python"),
    "agentic-pyflink": Path("pyflink"),
    "agentic-pipeline": Path("ports/agentic-pipeline"),
}

# PEP 440, canonical (normalized) public version: no local part, no leading "v", lower case.
CANONICAL_VERSION = re.compile(
    r"^([1-9][0-9]*!)?(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*"
    r"((a|b|rc)(0|[1-9][0-9]*))?(\.post(0|[1-9][0-9]*))?(\.dev(0|[1-9][0-9]*))?$"
)


class VersionMismatch(SystemExit):
    def __init__(self, message: str):
        super().__init__(f"release version check failed: {message}")


def normalize_name(name: str) -> str:
    """PEP 503 normalization, the form used in sdist and wheel file names (with '_')."""
    return re.sub(r"[-_.]+", "-", name).lower()


def version_from_tag(tag: str) -> str:
    name = tag.strip()
    if name.startswith("refs/tags/"):
        name = name[len("refs/tags/"):]
    if not name:
        raise VersionMismatch("empty tag; pass the release tag (for example v1.0.0rc1)")
    version = name[1:] if name.startswith("v") else name
    if not CANONICAL_VERSION.match(version):
        raise VersionMismatch(
            f"tag {tag!r} does not name a canonical PEP 440 version; use v<major>.<minor>.<patch> with an "
            "optional a<N>, b<N>, rc<N>, .post<N> or .dev<N> suffix, all lower case (for example v1.0.0rc1)")
    return version


def version_from_scm(project_dir: Path, python: str = sys.executable) -> str:
    """The version setuptools-scm computes for `project_dir` (its pyproject.toml, its git root)."""
    proc = subprocess.run(
        [python, "-m", "setuptools_scm"], cwd=project_dir, capture_output=True, text=True)
    if proc.returncode != 0:
        raise VersionMismatch(
            f"setuptools-scm failed in {project_dir}: {proc.stderr.strip() or proc.stdout.strip()}")
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        raise VersionMismatch(f"setuptools-scm printed no version in {project_dir}")
    return lines[-1]


def version_from_artifact(path: Path) -> tuple[str, str]:
    """(normalized distribution name, version) from a wheel or sdist file name."""
    name = path.name
    if name.endswith(".whl"):
        parts = name[: -len(".whl")].split("-")
        if len(parts) < 5:
            raise VersionMismatch(f"cannot read a version from wheel name {name!r}")
        return normalize_name(parts[0]), parts[1]
    if name.endswith(".tar.gz"):
        stem = name[: -len(".tar.gz")]
        if "-" not in stem:
            raise VersionMismatch(f"cannot read a version from sdist name {name!r}")
        dist, version = stem.rsplit("-", 1)
        return normalize_name(dist), version
    raise VersionMismatch(f"{name!r} is neither a wheel (.whl) nor an sdist (.tar.gz)")


def check(tag: str, artifacts: list[Path] = (), *, complete: bool = False,
          projects: dict[str, Path] | None = None, repo_root: Path = REPO_ROOT,
          scm: bool = True) -> str:
    """Return the tagged version; raise VersionMismatch on the first disagreement.

    `projects` maps distribution names to project directories relative to `repo_root`. With
    `scm` the checkout of each project must resolve to the tagged version; with `complete`
    the artifacts must contain one sdist and one wheel for every project.
    """
    projects = PROJECTS if projects is None else projects
    tagged = version_from_tag(tag)
    if scm:
        for dist, rel in projects.items():
            resolved = version_from_scm(repo_root / rel)
            if resolved != tagged:
                raise VersionMismatch(
                    f"tag {tag!r} names version {tagged!r} but setuptools-scm resolves {rel} to "
                    f"{resolved!r}; check out the tag exactly (no later commits, no uncommitted changes) "
                    "or retag")
    known = {normalize_name(dist) for dist in projects}
    seen: dict[tuple[str, str], Path] = {}
    for artifact in artifacts:
        dist, built = version_from_artifact(artifact)
        if dist not in known:
            raise VersionMismatch(
                f"artifact {artifact.name} belongs to distribution {dist!r}, which is not one of "
                f"{sorted(known)}")
        if built != tagged:
            raise VersionMismatch(f"artifact {artifact.name} is version {built!r}, expected {tagged!r}")
        kind = "wheel" if artifact.name.endswith(".whl") else "sdist"
        seen[(dist, kind)] = artifact
    if complete:
        missing = [f"{dist} {kind}" for dist in sorted(known) for kind in ("sdist", "wheel")
                   if (dist, kind) not in seen]
        if missing:
            raise VersionMismatch("missing artifacts: " + ", ".join(missing))
    return tagged


def main(argv: list[str]) -> int:
    args = list(argv[1:])
    complete = "--complete" in args
    no_scm = "--no-scm" in args
    args = [a for a in args if a not in ("--complete", "--no-scm")]
    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    print(check(args[0], [Path(a) for a in args[1:]], complete=complete, scm=not no_scm))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
