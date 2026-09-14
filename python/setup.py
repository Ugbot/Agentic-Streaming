"""Build hook: bundle the shaded framework jar into the wheel and sdist.

`python -m build` (and `pip wheel`, `pip install python/`) must yield an artifact that starts
the JVM without a source checkout, so before setuptools collects package data this hook makes
sure `agentic_flink/jars/agentic-flink-<version>-uber.jar` exists. Resolution order:

1. a jar already under `agentic_flink/jars/` (a previous build, or an sdist that ships it);
2. `AGENTIC_FLINK_JAR` (developer override: an already built uber jar anywhere on disk);
3. `<repo>/target/agentic-flink-*-uber.jar` from a previous Maven build of the checkout;
4. otherwise run the Maven wrapper of the checkout:
   `./mvnw -f ports/jagentic-core/pom.xml install -DskipTests && ./mvnw -DskipTests package`.

Editable installs (`pip install -e python`) skip the hook because `agentic_flink._classpath`
finds the jar under `<repo>/target/` directly. Set `AGENTIC_FLINK_SKIP_JAR=1` to build a wheel
without the jar on purpose (the resulting wheel raises MissingJarError until
`AGENTIC_FLINK_JAR` is set). Every other miss is a hard build failure.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

HERE = Path(__file__).resolve().parent
JARS_DIR = HERE / "agentic_flink" / "jars"
REPO_ROOT = HERE.parent
MVNW = REPO_ROOT / ("mvnw.cmd" if os.name == "nt" else "mvnw")
CORE_POM = REPO_ROOT / "ports" / "jagentic-core" / "pom.xml"
SKIP_ENV = "AGENTIC_FLINK_SKIP_JAR"
OVERRIDE_ENV = "AGENTIC_FLINK_JAR"


class JarBuildError(RuntimeError):
    """The shaded framework jar could not be produced; the message says what to do."""


def _uber_jars(directory: Path) -> list[Path]:
    return sorted(Path(p) for p in glob.glob(str(directory / "agentic-flink-*-uber.jar")))


def _copy_into_jars(jar: Path) -> Path:
    JARS_DIR.mkdir(parents=True, exist_ok=True)
    target = JARS_DIR / jar.name
    if target.resolve() != jar.resolve():
        shutil.copy2(jar, target)
    return target


def _run_maven(args: list[str]) -> None:
    cmd = [str(MVNW), *args]
    print(f"[agentic-flink] {' '.join(cmd)}", file=sys.stderr, flush=True)
    subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)


def ensure_framework_jar() -> Path:
    """Return the bundled uber jar path, producing it if needed (see module docs)."""
    present = _uber_jars(JARS_DIR)
    if present:
        return present[-1]

    override = os.environ.get(OVERRIDE_ENV)
    if override:
        jar = Path(override).expanduser().resolve()
        if not jar.is_file():
            raise JarBuildError(f"{OVERRIDE_ENV}={override} does not point at a file")
        return _copy_into_jars(jar)

    built = _uber_jars(REPO_ROOT / "target")
    if built:
        return _copy_into_jars(built[-1])

    if not MVNW.is_file() or not CORE_POM.is_file():
        raise JarBuildError(
            "no agentic-flink-*-uber.jar under agentic_flink/jars/ and this is not a repository "
            f"checkout (no {MVNW} / {CORE_POM}). Set {OVERRIDE_ENV}=/path/to/agentic-flink-<v>-uber.jar, "
            f"or build from a checkout of https://github.com/Ugbot/Agentic-Streaming, or set {SKIP_ENV}=1 "
            "to knowingly build a wheel without the jar."
        )
    try:
        _run_maven(["-q", "-f", str(CORE_POM), "install", "-DskipTests"])
        _run_maven(["-q", "-DskipTests", "package"])
    except (OSError, subprocess.CalledProcessError) as exc:
        raise JarBuildError(f"Maven build of the framework jar failed: {exc}") from exc
    built = _uber_jars(REPO_ROOT / "target")
    if not built:
        raise JarBuildError(f"Maven succeeded but produced no agentic-flink-*-uber.jar under {REPO_ROOT / 'target'}")
    return _copy_into_jars(built[-1])


def _bundle_jar_unless_skipped() -> None:
    if os.environ.get(SKIP_ENV) == "1":
        print(f"[agentic-flink] {SKIP_ENV}=1: building without the framework jar", file=sys.stderr, flush=True)
        return
    jar = ensure_framework_jar()
    print(f"[agentic-flink] bundling {jar.name} ({jar.stat().st_size // (1 << 20)} MiB)", file=sys.stderr, flush=True)


class build_py(_build_py):
    def run(self) -> None:
        if not getattr(self, "editable_mode", False):
            _bundle_jar_unless_skipped()
            self.data_files = self._get_data_files()
        super().run()


class sdist(_sdist):
    def run(self) -> None:
        _bundle_jar_unless_skipped()
        super().run()


if __name__ == "__main__":
    setup(cmdclass={"build_py": build_py, "sdist": sdist})
