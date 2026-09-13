"""Classpath discovery for the JVM-backed runtimes.

Two groups of jars are needed:

* **framework** — the shaded ``agentic-flink-*-uber.jar`` (bundles the ``jagentic-core``
  canonical core, the Flink adapter and their libraries). Found via, in order:
  ``AGENTIC_FLINK_JAR``; the explicit path handed to :func:`agentic_flink.start_jvm`;
  a sibling Maven build (``<repo>/target/agentic-flink-*.jar``, for ``pip install -e``);
  package data under ``agentic_flink/jars/`` (a wheel that ships the jar).
* **flink** — the Flink distribution jars, which the shaded jar deliberately excludes
  (``provided`` scope). Found via ``AGENTIC_FLINK_CLASSPATH`` (path-separated list of jars
  or directories); ``$FLINK_HOME/lib``; the ``apache-flink`` wheel's ``pyflink/lib``
  (``pip install "agentic-flink[flink]"``); the ``python/tests/.cp`` Maven classpath of a
  source checkout.

Every miss raises :class:`MissingJarError` with the exact step that fixes it.
"""

from __future__ import annotations

import glob
import os
from importlib import util as importlib_util
from pathlib import Path
from typing import Iterable, List, Optional

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parents[1]
BUNDLED_JARS = PACKAGE_DIR / "jars"
DEV_CLASSPATH_FILE = PACKAGE_DIR.parent / "tests" / ".cp"

FLINK_EXTRA = 'pip install "agentic-flink[flink]"'


class MissingJarError(FileNotFoundError):
    """A required jar could not be found; the message says how to provide it."""


def _split_path_list(value: str) -> List[str]:
    return [p for p in value.split(os.pathsep) if p]


def _jars_in(directory: Path) -> List[str]:
    return sorted(str(p) for p in directory.glob("*.jar"))


def _expand(entries: Iterable[str]) -> List[str]:
    out: List[str] = []
    for entry in entries:
        p = Path(entry).expanduser()
        if p.is_dir():
            out.extend(_jars_in(p) or [str(p.resolve())])
        else:
            out.append(str(p.resolve()))
    return out


def framework_jar(explicit: str | Path | None = None) -> Path:
    """Locate the shaded framework jar (see module docs for the search order)."""
    env = os.environ.get("AGENTIC_FLINK_JAR")
    if env:
        p = Path(env).expanduser().resolve()
        if not p.exists():
            raise MissingJarError(f"AGENTIC_FLINK_JAR points at {p}, which does not exist")
        return p

    if explicit is not None:
        p = Path(explicit).expanduser().resolve()
        if not p.exists():
            raise MissingJarError(f"jar_path {p} does not exist")
        return p

    candidates = sorted(glob.glob(str(REPO_ROOT / "target" / "agentic-flink-*.jar")))
    candidates = [c for c in candidates if "original-" not in Path(c).name]
    if candidates:
        uber = [c for c in candidates if Path(c).name.endswith("-uber.jar")]
        return Path((uber or candidates)[-1]).resolve()

    if BUNDLED_JARS.is_dir():
        bundled: List[Path] = sorted(BUNDLED_JARS.glob("agentic-flink-*.jar"))
        if bundled:
            bundled_uber = [c for c in bundled if c.name.endswith("-uber.jar")]
            return (bundled_uber or bundled)[-1].resolve()

    raise MissingJarError(
        "agentic-flink shaded jar not found. Either set AGENTIC_FLINK_JAR=/path/to/agentic-flink-<version>-uber.jar, "
        "pass jar_path=... to agentic_flink.start_jvm(), install a wheel that bundles it under "
        f"agentic_flink/jars/ (looked in {BUNDLED_JARS}), or build it from a source checkout with "
        "`mvn -f ports/jagentic-core/pom.xml install -DskipTests && mvn -DskipTests package` "
        f"(looked in {REPO_ROOT / 'target'})."
    )


def bundled_jars() -> List[str]:
    """Every jar shipped as package data (empty when installed from source without jars)."""
    return _jars_in(BUNDLED_JARS) if BUNDLED_JARS.is_dir() else []


def _pyflink_lib() -> Optional[Path]:
    spec = importlib_util.find_spec("pyflink")
    if spec is None or not spec.submodule_search_locations:
        return None
    lib = Path(list(spec.submodule_search_locations)[0]) / "lib"
    return lib if lib.is_dir() and _jars_in(lib) else None


def dev_classpath() -> List[str]:
    """The Maven-materialized classpath of a source checkout (``python/tests/.cp``), if any."""
    if DEV_CLASSPATH_FILE.exists():
        entries = [p for p in DEV_CLASSPATH_FILE.read_text().strip().split(os.pathsep) if p]
        return [p for p in entries if Path(p).exists()]
    return []


def env_classpath() -> List[str]:
    """Extra jars/directories from ``AGENTIC_FLINK_CLASSPATH`` (always added to the JVM)."""
    env = os.environ.get("AGENTIC_FLINK_CLASSPATH")
    if not env:
        return []
    jars = _expand(_split_path_list(env))
    missing = [j for j in jars if not Path(j).exists()]
    if missing:
        raise MissingJarError(f"AGENTIC_FLINK_CLASSPATH names paths that do not exist: {missing}")
    return jars


def flink_jars() -> List[str]:
    """Locate the Flink distribution jars needed to run a job in-process."""
    env = env_classpath()
    if env:
        return env

    flink_home = os.environ.get("FLINK_HOME")
    if flink_home:
        lib = Path(flink_home).expanduser() / "lib"
        jars = _jars_in(lib) if lib.is_dir() else []
        if not jars:
            raise MissingJarError(f"FLINK_HOME={flink_home} has no jars under {lib}")
        return jars

    pyflink_lib = _pyflink_lib()
    if pyflink_lib is not None:
        return _jars_in(pyflink_lib)

    dev = dev_classpath()
    if dev:
        return dev

    raise MissingJarError(
        "Flink distribution jars not found. The shaded agentic-flink jar keeps Flink itself out "
        f"(provided scope). Install them with `{FLINK_EXTRA}` (uses the apache-flink wheel's "
        "pyflink/lib), point FLINK_HOME at a Flink 2.x distribution, or set AGENTIC_FLINK_CLASSPATH "
        "to a path-separated list of jars/directories."
    )


PEKKO_CLASSPATH_FILE = PACKAGE_DIR.parent / "tests" / ".cp-pekko"


def pekko_jars() -> List[str]:
    """Locate the agentic-pekko jar and its dependencies.

    ``AGENTIC_PEKKO_CLASSPATH`` (path-separated jars/directories) wins; otherwise a source
    checkout's ``agentic-pekko/target/agentic-pekko-*.jar`` plus the Maven classpath file
    ``python/tests/.cp-pekko`` (``mvn -f agentic-pekko/pom.xml dependency:build-classpath
    -Dmdep.outputFile=python/tests/.cp-pekko``)."""
    env = os.environ.get("AGENTIC_PEKKO_CLASSPATH")
    if env:
        jars = _expand(_split_path_list(env))
        missing = [j for j in jars if not Path(j).exists()]
        if missing:
            raise MissingJarError(f"AGENTIC_PEKKO_CLASSPATH names paths that do not exist: {missing}")
        return jars
    module_jars = sorted(glob.glob(str(REPO_ROOT / "agentic-pekko" / "target" / "agentic-pekko-*.jar")))
    if module_jars and PEKKO_CLASSPATH_FILE.exists():
        deps = [p for p in PEKKO_CLASSPATH_FILE.read_text().strip().split(os.pathsep) if p and Path(p).exists()]
        return module_jars[-1:] + deps
    raise MissingJarError(
        "agentic-pekko jars not found. Build them with `mvn -f agentic-pekko/pom.xml package -DskipTests` "
        "and `mvn -f agentic-pekko/pom.xml dependency:build-classpath -Dmdep.outputFile=python/tests/.cp-pekko`, "
        "or set AGENTIC_PEKKO_CLASSPATH to a path-separated list of the jar and its dependencies."
    )


def has_flink_classes() -> bool:
    """Whether the running JVM can see Flink's streaming API."""
    import jpype

    if not jpype.isJVMStarted():
        return False
    try:
        jpype.JClass("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")
    except Exception:
        return False
    return True


def has_class(fqn: str) -> bool:
    import jpype

    if not jpype.isJVMStarted():
        return False
    try:
        jpype.JClass(fqn)
    except Exception:
        return False
    return True
