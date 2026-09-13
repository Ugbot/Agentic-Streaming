"""JVM bootstrap for agentic-flink.

This module owns the lifecycle of the JPype-managed JVM. Call :func:`start_jvm`
once at process start; every other module's first :func:`jclass` call uses it.

Discovery order for the framework jar:

1. The ``AGENTIC_FLINK_JAR`` environment variable (a path to a jar).
2. The ``jar_path`` keyword argument to :func:`start_jvm`.
3. A sibling Maven build — ``../target/agentic-flink-*.jar`` relative to this
   package (covers ``pip install -e .`` from a checked-out repo).
4. Bundled package data under ``agentic_flink/jars/`` (when installed from a
   wheel that ships the jar).

The JVM started here runs in **thread mode**: same process, same address
space, Python and Java threads share the same runtime. Java calls cross the
JNI boundary without serialization.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import jpype
import jpype.imports  # noqa: F401  -- side-effect: enables ``from java...`` imports

from ._classpath import MissingJarError, bundled_jars, env_classpath, framework_jar

_DEFAULT_JVM_ARGS = (
    "-Xms256m",
    "-Xmx2g",
    "-XX:+UseG1GC",
    # Apache Arrow (used by Fluss's columnar log format) needs nio internals on JDK 17+.
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
)


class JvmNotStartedError(RuntimeError):
    """Raised when a wrapper module is used before :func:`start_jvm` has been called."""


def start_jvm(
    *,
    jar_path: str | Path | None = None,
    extra_jars: Iterable[str | Path] = (),
    jvm_args: Iterable[str] = (),
    convert_strings: bool = True,
    prepend_jars: Iterable[str | Path] = (),
) -> None:
    """Start the JVM and load the framework jar onto its classpath.

    Idempotent — a second call verifies the JVM is up and returns without
    starting a new one.

    Args:
        jar_path: explicit path to ``agentic-flink-*.jar``. Wins over auto-discovery
            but loses to the ``AGENTIC_FLINK_JAR`` environment variable.
        extra_jars: additional jars to add to the classpath (e.g. user-supplied
            DJL native binaries, Kafka connector, Jedis).
        jvm_args: extra JVM arguments. Sensible defaults are applied first.
        convert_strings: forwarded to :func:`jpype.startJVM`. Default :data:`True`
            so Java :class:`String` round-trips as Python :class:`str` without
            explicit wrappers.
        prepend_jars: jars placed *before* the framework jar, so their classes win when
            the shaded jar bundles an older copy (the Pekko runtime needs its own Jackson).

    Raises:
        MissingJarError: (a ``FileNotFoundError``) if no framework jar is found in any
            of the search locations; the message names the fix.
    """
    if jpype.isJVMStarted():
        return

    resolved = _resolve_jar(jar_path)
    classpath = [str(Path(j).resolve()) for j in prepend_jars]
    classpath.append(str(resolved))
    classpath += [j for j in bundled_jars() + env_classpath() if j not in classpath]
    classpath += [str(Path(j).resolve()) for j in extra_jars]
    args = list(_DEFAULT_JVM_ARGS) + list(jvm_args)

    jpype.startJVM(
        *args,
        classpath=classpath,
        convertStrings=convert_strings,
    )


def shutdown_jvm() -> None:
    """Shut down the JVM. Mostly useful for tests; once a JVM is shut down it
    cannot be restarted inside the same Python process (JPype limitation)."""
    if jpype.isJVMStarted():
        jpype.shutdownJVM()


def is_started() -> bool:
    """Whether the JVM is currently running."""
    return bool(jpype.isJVMStarted())


def jclass(fqn: str):
    """Resolve a Java class by fully qualified name.

    Lazy — call from inside a function body, not at module import time, so
    importing wrapper modules doesn't require the JVM.

    Raises:
        JvmNotStartedError: if :func:`start_jvm` hasn't been called.
    """
    if not jpype.isJVMStarted():
        raise JvmNotStartedError(
            "call agentic_flink.start_jvm() before resolving Java classes"
        )
    return jpype.JClass(fqn)


def _resolve_jar(explicit: str | Path | None) -> Path:
    """Find the framework jar. Raises :class:`MissingJarError` if none exists."""
    return framework_jar(explicit)


__all__ = [
    "JvmNotStartedError",
    "MissingJarError",
    "is_started",
    "jclass",
    "shutdown_jvm",
    "start_jvm",
]
