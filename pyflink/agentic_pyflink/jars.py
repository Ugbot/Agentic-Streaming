"""Locate the two Java artifacts a PyFlink job needs on its classpath.

* ``agentic-flink-<version>-uber.jar`` -- the Flink framework with the canonical core
  (``jagentic-core``) and its dependencies shaded in; built by ``./mvnw install -DskipTests``
  at the repository root.
* ``agentic-pyflink-<version>.jar`` -- the JSON-line bridge (``org.agentic.pyflink.PyFlinkJob``)
  under ``pyflink/java``; built by ``./mvnw -f pyflink/java/pom.xml package``.

Resolution order: the ``AGENTIC_FLINK_UBER_JAR`` / ``AGENTIC_PYFLINK_JAR`` environment variables,
then the Maven ``target/`` directories of a source checkout found by walking up from this file.
Missing artifacts raise :class:`JarNotFoundError` naming the build command; nothing is guessed.
"""

from __future__ import annotations

import os
from pathlib import Path

UBER_JAR_ENV = "AGENTIC_FLINK_UBER_JAR"
BRIDGE_JAR_ENV = "AGENTIC_PYFLINK_JAR"

_UBER_GLOB = "agentic-flink-*-uber.jar"
_BRIDGE_GLOB = "agentic-pyflink-*.jar"


class JarNotFoundError(FileNotFoundError):
    """A required Java artifact is missing; the message says how to build it."""


def repo_root(start: Path | None = None) -> Path | None:
    """The repository checkout containing this package, or ``None`` when installed elsewhere."""
    here = (start or Path(__file__)).resolve()
    for candidate in [here, *here.parents]:
        if (candidate / "pyflink" / "java" / "pom.xml").is_file() and (candidate / "pom.xml").is_file():
            return candidate
    return None


def _from_env(var: str) -> Path | None:
    value = os.environ.get(var)
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_file():
        raise JarNotFoundError(f"{var}={value} does not point at a file")
    return path


def _newest(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime) if directory.is_dir() else []
    return matches[-1] if matches else None


def uber_jar() -> Path:
    found = _from_env(UBER_JAR_ENV)
    if found is None:
        root = repo_root()
        if root is not None:
            found = _newest(root / "target", _UBER_GLOB)
    if found is None:
        raise JarNotFoundError(
            "agentic-flink uber jar not found. Build it with\n"
            "  ./mvnw -f ports/jagentic-core/pom.xml install -DskipTests && ./mvnw install -DskipTests\n"
            f"at the repository root, or point {UBER_JAR_ENV} at an existing jar."
        )
    return found


def bridge_jar() -> Path:
    found = _from_env(BRIDGE_JAR_ENV)
    if found is None:
        root = repo_root()
        if root is not None:
            found = _newest(root / "pyflink" / "java" / "target", _BRIDGE_GLOB)
    if found is None:
        raise JarNotFoundError(
            "agentic-pyflink bridge jar not found. Build it with\n"
            "  ./mvnw -f pyflink/java/pom.xml package\n"
            f"after the framework jar, or point {BRIDGE_JAR_ENV} at an existing jar."
        )
    return found


def classpath_jars(extra: list[str] | None = None) -> list[Path]:
    """All jars a job needs, framework first, then the bridge, then any user-supplied extras."""
    jars = [uber_jar(), bridge_jar()]
    for item in extra or []:
        path = Path(item).expanduser()
        if not path.is_file():
            raise JarNotFoundError(f"extra jar {item} does not exist")
        jars.append(path)
    return jars


def as_urls(jars: list[Path]) -> list[str]:
    return [p.resolve().as_uri() for p in jars]
