"""Unified runtime selector for the Python notebooks.

The framework can execute work in three modes. A notebook picks one via the
``AGENTIC_FLINK_MODE`` environment variable (or by passing ``mode=`` to
:func:`bootstrap`). The same notebook code works against any of them.

==========  ==============================================================
Mode        What happens
==========  ==============================================================
``inproc``  JPype boots a JVM **inside the notebook's Python process**.
            No Flink job submission — operators are called directly via
            :func:`jclass`. Same as notebooks 07/08 historically.
``session`` REST submission to a long-running Flink session cluster at
            ``FLINK_REST_URL`` (default ``http://localhost:8081``). The
            notebook's Python process stays out of the JVM entirely.
``embedded``JPype boots a JVM **plus** a transient Flink MiniCluster in
            this process. ``submit_level()`` runs the same
            ``SessionJobLauncher`` entry point against a
            ``LocalStreamEnvironment``. Useful for offline testing of the
            full job graph without docker.
==========  ==============================================================

Notebook bootstrap collapses to three lines regardless of mode::

    import agentic_flink as af
    rt = af.bootstrap()
    print(rt.info)
"""

from __future__ import annotations

import abc
import os
import pathlib
import subprocess
import sys
import threading
from typing import Any, Iterable, Optional

from . import _jvm

# Modes recognised by ``AGENTIC_FLINK_MODE``.
_MODES = ("inproc", "session", "embedded")

# Per-mode runtime dep lists. Used by bootstrap() to fail fast with a clear
# message when the kernel's interpreter is missing what we need.
_MODE_DEPS = {
    "inproc": ("jpype",),
    "session": ("requests",),
    "embedded": ("jpype",),
}


class RuntimeModeError(ValueError):
    """Raised when ``AGENTIC_FLINK_MODE`` (or an explicit ``mode=``) is unknown."""


class Runtime(abc.ABC):
    """Abstract execution target. Subclasses are :class:`InProcRuntime`,
    :class:`SessionRuntime`, and :class:`EmbeddedClusterRuntime`."""

    mode: str = "abstract"

    def __init__(self) -> None:
        self._started = False

    # ---- lifecycle ---------------------------------------------------------

    @abc.abstractmethod
    def start(self) -> None:
        """Bring the runtime up. Idempotent."""

    def close(self) -> None:
        """Tear down. Default no-op; subclasses override if they own resources."""

    # ---- capabilities (subclasses opt in) ---------------------------------

    def jclass(self, fqn: str):
        """Resolve a Java class. Only valid for in-JVM modes."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot resolve Java classes (mode={self.mode!r}). "
            "Use AGENTIC_FLINK_MODE=inproc or =embedded if you need in-JVM access."
        )

    def submit_level(self, level: str, **kwargs) -> Any:
        """Submit one ``SessionJobLauncher`` level. Only valid for session/embedded modes."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot submit jobs (mode={self.mode!r}). "
            "Use AGENTIC_FLINK_MODE=session or =embedded if you need to submit jobs."
        )

    # ---- info / repr -------------------------------------------------------

    @property
    def info(self) -> dict[str, Any]:
        """Printable summary — what mode, what endpoint, whether the LLM tier is live."""
        key = os.environ.get("ANTHROPIC_API_KEY") or ""
        return {
            "mode": self.mode,
            "started": self._started,
            "llm_tier": "Claude" if key.startswith("sk-ant-") else "disabled",
        }

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.info}>"


# ---------------------------------------------------------------------------
# InProcRuntime — JPype only, no Flink job submission.
# ---------------------------------------------------------------------------


class InProcRuntime(Runtime):
    """In-process JVM via JPype. No Flink job submission; operators are called
    directly through :meth:`jclass`. Same shape as notebooks 07 and 08."""

    mode = "inproc"

    def __init__(
        self,
        *,
        jar_path: str | pathlib.Path | None = None,
        extra_jars: Iterable[str | pathlib.Path] = (),
        jvm_args: Iterable[str] = (),
    ) -> None:
        super().__init__()
        self._jar_path = jar_path
        self._extra_jars = list(extra_jars)
        self._jvm_args = list(jvm_args)

    def start(self) -> None:
        if self._started:
            return
        _jvm.start_jvm(
            jar_path=self._jar_path,
            extra_jars=self._extra_jars,
            jvm_args=self._jvm_args,
        )
        self._started = True

    def jclass(self, fqn: str):
        if not self._started:
            raise _jvm.JvmNotStartedError(
                "InProcRuntime.start() not called yet (use af.bootstrap())"
            )
        return _jvm.jclass(fqn)

    @property
    def info(self) -> dict[str, Any]:
        base = super().info
        base.update({"jar": os.environ.get("AGENTIC_FLINK_JAR")})
        return base


# ---------------------------------------------------------------------------
# SessionRuntime — REST submission to a long-running Flink session cluster.
# ---------------------------------------------------------------------------


class SessionRuntime(Runtime):
    """Talks to a Flink session cluster over REST. The cluster can be on
    localhost (brought up by `examples-bin/run-session-cluster.sh`) or on
    any reachable host (set ``FLINK_REST_URL``)."""

    mode = "session"

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        anthropic_key: Optional[str] = None,
        jar_path: str | pathlib.Path | None = None,
        auto_upload: bool = True,
    ) -> None:
        super().__init__()
        # Resolve order: explicit arg > env var > sensible default.
        self._base_url = base_url or os.environ.get(
            "FLINK_REST_URL", "http://localhost:8081"
        )
        self._anthropic_key = anthropic_key or os.environ.get("ANTHROPIC_API_KEY")
        self._jar_path = jar_path
        self._auto_upload = auto_upload
        self._client = None  # type: ignore[assignment]
        self._jar_id: Optional[str] = None

    def start(self) -> None:
        if self._started:
            return
        # Imported here so import-time users don't pay for `requests` if they
        # never go session-mode.
        from .session import SessionClient

        self._client = SessionClient(self._base_url)
        # Don't block on cluster readiness here — surface clearer errors if the
        # user calls submit_level() against an unreachable cluster.
        if self._auto_upload:
            self._ensure_jar_uploaded()
        self._started = True

    def _ensure_jar_uploaded(self) -> None:
        existing = self._client.latest_jar("agentic-flink")
        if existing is not None:
            self._jar_id = existing
            return
        # Find a local jar to upload.
        if self._jar_path is None:
            here = pathlib.Path(__file__).resolve()
            candidates = sorted(here.parents[2].glob("target/agentic-flink-*.jar"))
            candidates = [c for c in candidates if "original" not in c.name and "sources" not in c.name]
            if not candidates:
                raise FileNotFoundError(
                    "no agentic-flink jar found to upload. Build with `mvn -DskipTests package` "
                    "or pass jar_path= to session_cluster()."
                )
            self._jar_path = candidates[-1]
        self._jar_id = self._client.upload_jar(self._jar_path)

    @property
    def session_client(self):
        """The underlying :class:`SessionClient` — exposed for level-09 cells."""
        if not self._started:
            raise RuntimeError("SessionRuntime.start() not called yet (use af.bootstrap())")
        return self._client

    @property
    def jar_id(self) -> Optional[str]:
        return self._jar_id

    def submit_level(self, level: str, **kwargs) -> Any:
        if not self._started:
            raise RuntimeError("SessionRuntime.start() not called yet (use af.bootstrap())")
        if self._jar_id is None:
            self._ensure_jar_uploaded()
        # Default anthropic_key from env if caller didn't pass one.
        if "anthropic_key" not in kwargs and self._anthropic_key:
            kwargs["anthropic_key"] = self._anthropic_key
        return self._client.submit_level(self._jar_id, level, **kwargs)

    def wait_ready(self, *, timeout_s: float = 60.0) -> dict:
        if not self._started:
            self.start()
        return self._client.wait_ready(timeout_s=timeout_s)

    @property
    def info(self) -> dict[str, Any]:
        base = super().info
        base.update({"endpoint": self._base_url, "jar_id": self._jar_id})
        return base


# ---------------------------------------------------------------------------
# EmbeddedClusterRuntime — JPype + in-process Flink MiniCluster.
# ---------------------------------------------------------------------------


class EmbeddedClusterRuntime(Runtime):
    """JPype JVM plus an in-process Flink MiniCluster. ``submit_level()`` runs
    the same ``SessionJobLauncher.main(args)`` entry point in a daemon thread —
    ``StreamExecutionEnvironment.getExecutionEnvironment()`` returns a
    ``LocalStreamEnvironment`` and ``env.execute()`` spins up a transient
    MiniCluster.

    Caveats:

    - There's no central JobManager REST API in this mode. ``jobs()`` /
      ``cancel()`` are best-effort, tracked per-thread.
    - All ZeroMQ / Fluss sinks and sources still work — the running job in the
      MiniCluster speaks the same protocols as in the real cluster, so a
      notebook can observe its output the same way.
    - Useful for offline testing without docker; not a substitute for the
      real session cluster when running multiple long-lived jobs concurrently.
    """

    mode = "embedded"

    def __init__(
        self,
        *,
        jar_path: str | pathlib.Path | None = None,
        extra_jars: Iterable[str | pathlib.Path] = (),
        jvm_args: Iterable[str] = (),
        anthropic_key: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._jar_path = jar_path
        self._extra_jars = list(extra_jars)
        self._jvm_args = list(jvm_args)
        self._anthropic_key = anthropic_key or os.environ.get("ANTHROPIC_API_KEY")
        # Background threads running submit_level().
        self._handles: dict[str, "EmbeddedJobHandle"] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        if self._started:
            return
        _jvm.start_jvm(
            jar_path=self._jar_path,
            extra_jars=self._extra_jars,
            jvm_args=self._jvm_args,
        )
        self._started = True

    def jclass(self, fqn: str):
        if not self._started:
            raise _jvm.JvmNotStartedError(
                "EmbeddedClusterRuntime.start() not called yet (use af.bootstrap())"
            )
        return _jvm.jclass(fqn)

    def submit_level(self, level: str, *, name: Optional[str] = None, **kwargs) -> "EmbeddedJobHandle":
        if not self._started:
            raise RuntimeError("EmbeddedClusterRuntime.start() not called yet")

        # Default anthropic_key from env when caller didn't pass one.
        if "anthropic_key" not in kwargs and self._anthropic_key:
            kwargs["anthropic_key"] = self._anthropic_key

        args = _build_launcher_args(level, name=name, **kwargs)
        Launcher = _jvm.jclass("org.agentic.flink.session.SessionJobLauncher")
        # Convert Python list to Java String[].
        import jpype

        jargs = jpype.JArray(jpype.JString)(args)
        handle_name = name or f"agentic-embedded-{level}"

        # SessionJobLauncher.main(String[]) blocks on env.execute() — run it in
        # a daemon thread so the notebook stays interactive.
        exc_box: dict[str, BaseException] = {}

        def _runner():
            try:
                Launcher.main(jargs)
            except BaseException as e:  # noqa: BLE001 — JPype exceptions surface here
                exc_box["err"] = e

        thread = threading.Thread(
            target=_runner, name=f"embedded-{handle_name}", daemon=True
        )
        handle = EmbeddedJobHandle(
            job_name=handle_name, level=level, thread=thread, exc_box=exc_box, args=args
        )
        with self._lock:
            self._handles[handle_name] = handle
        thread.start()
        return handle

    def jobs(self) -> list["EmbeddedJobHandle"]:
        """Best-effort list of submitted jobs — alive threads only."""
        with self._lock:
            return [h for h in self._handles.values() if h.is_alive()]

    def close(self) -> None:
        # Threads are daemon — they die with the process. Nothing to do.
        pass

    @property
    def info(self) -> dict[str, Any]:
        base = super().info
        base.update({"jar": os.environ.get("AGENTIC_FLINK_JAR"), "running_jobs": len(self.jobs())})
        return base


class EmbeddedJobHandle:
    """Handle returned by :meth:`EmbeddedClusterRuntime.submit_level`. Mirrors
    the shape of :class:`agentic_flink.session.JobHandle` so notebook code that
    works in session mode is portable."""

    __slots__ = ("job_name", "level", "_thread", "_exc_box", "args")

    def __init__(
        self,
        *,
        job_name: str,
        level: str,
        thread: threading.Thread,
        exc_box: dict,
        args: list[str],
    ):
        self.job_name = job_name
        self.level = level
        self._thread = thread
        self._exc_box = exc_box
        self.args = args

    @property
    def job_id(self) -> str:
        return self._thread.name

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def status(self) -> dict[str, Any]:
        if self._exc_box.get("err") is not None:
            return {"state": "FAILED", "error": repr(self._exc_box["err"])}
        return {"state": "RUNNING" if self.is_alive() else "FINISHED"}

    def cancel(self) -> None:
        # JPype + Flink MiniCluster: can't preempt the running env.execute() from
        # outside cleanly. Document as a best-effort no-op; relying on daemon
        # thread teardown when the process exits.
        pass


# ---------------------------------------------------------------------------
# Factories — short callable surface for notebook code.
# ---------------------------------------------------------------------------


def inproc(**kwargs) -> InProcRuntime:
    """Build (but don't start) an :class:`InProcRuntime`."""
    return InProcRuntime(**kwargs)


def session_cluster(base_url: Optional[str] = None, **kwargs) -> SessionRuntime:
    """Build (but don't start) a :class:`SessionRuntime`. ``base_url`` defaults
    from ``FLINK_REST_URL`` env var, then ``http://localhost:8081``.

    Named ``session_cluster`` (rather than ``session``) to avoid clashing with
    the ``agentic_flink.session`` submodule that exposes :class:`SessionClient`.
    """
    return SessionRuntime(base_url, **kwargs)


def embedded(**kwargs) -> EmbeddedClusterRuntime:
    """Build (but don't start) an :class:`EmbeddedClusterRuntime`."""
    return EmbeddedClusterRuntime(**kwargs)


def from_env(mode: Optional[str] = None) -> Runtime:
    """Pick a runtime from ``AGENTIC_FLINK_MODE`` (or the explicit ``mode`` arg).

    Valid modes: ``inproc`` (default), ``session``, ``embedded``."""
    chosen = (mode or os.environ.get("AGENTIC_FLINK_MODE") or "inproc").lower()
    if chosen not in _MODES:
        raise RuntimeModeError(
            f"unknown AGENTIC_FLINK_MODE {chosen!r}; expected one of {_MODES}"
        )
    if chosen == "inproc":
        return inproc()
    if chosen == "session":
        return session_cluster()
    if chosen == "embedded":
        return embedded()
    raise AssertionError("unreachable")  # pragma: no cover


# ---------------------------------------------------------------------------
# Bootstrap helper — what every notebook calls at cell 1.
# ---------------------------------------------------------------------------


def bootstrap(*, mode: Optional[str] = None, dotenv_path: Optional[str | pathlib.Path] = None) -> Runtime:
    """One-call notebook helper.

    1. Resolves ``repo_root`` (parent of ``notebooks/`` if cwd is a notebook dir).
    2. Inserts ``repo_root/python`` on ``sys.path`` so editable-source imports work.
    3. ``load_dotenv(repo_root / '.env', override=True)`` if python-dotenv is
       installed and the file exists.
    4. Locates the framework jar (sets ``AGENTIC_FLINK_JAR`` if missing).
    5. For in-JVM modes (inproc / embedded), builds and caches
       ``runtime-classpath.txt`` so JPype has all of Flink + LangChain4j + ...
       on the classpath without manual ``--classpath`` gymnastics.
    6. Sanity-checks the kernel's interpreter has the deps the chosen mode
       needs, with a clear error message if it doesn't.
    7. Returns a *started* :class:`Runtime`.
    """
    repo_root = _detect_repo_root()
    _put_python_on_path(repo_root)
    _load_dotenv_if_present(repo_root, dotenv_path)
    chosen = (mode or os.environ.get("AGENTIC_FLINK_MODE") or "inproc").lower()
    _check_deps(chosen)
    jar = _locate_jar(repo_root)
    if jar is not None:
        os.environ.setdefault("AGENTIC_FLINK_JAR", str(jar))

    if chosen in ("inproc", "embedded"):
        extra_jars = _ensure_classpath(repo_root)
        if chosen == "inproc":
            rt: Runtime = inproc(extra_jars=extra_jars)
        else:
            rt = embedded(extra_jars=extra_jars)
    elif chosen == "session":
        rt = session_cluster()
    else:
        raise RuntimeModeError(
            f"unknown AGENTIC_FLINK_MODE {chosen!r}; expected one of {_MODES}"
        )

    rt.start()
    return rt


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _detect_repo_root() -> pathlib.Path:
    cwd = pathlib.Path.cwd()
    # If we're in `notebooks/` (or `python/`), the project root is one up.
    if cwd.name in ("notebooks", "python"):
        return cwd.parent
    # Walk up until we find a pom.xml — handles weird cwds (e.g. PyCharm).
    for p in (cwd, *cwd.parents):
        if (p / "pom.xml").exists():
            return p
    return cwd


def _put_python_on_path(repo_root: pathlib.Path) -> None:
    py_src = str(repo_root / "python")
    if py_src not in sys.path:
        sys.path.insert(0, py_src)


def _load_dotenv_if_present(
    repo_root: pathlib.Path, explicit: Optional[str | pathlib.Path]
) -> None:
    target = pathlib.Path(explicit) if explicit else repo_root / ".env"
    if not target.exists():
        return
    try:
        from dotenv import load_dotenv  # type: ignore[import-not-found]
    except ImportError:
        return  # nothing to do — env vars must come from the shell
    load_dotenv(target, override=True)


def _check_deps(mode: str) -> None:
    deps = _MODE_DEPS.get(mode, ())
    missing = []
    for m in deps:
        try:
            __import__(m)
        except ModuleNotFoundError:
            missing.append(m)
    if not missing:
        return
    pip_targets = " ".join("pyzmq" if m == "zmq" else m for m in missing)
    raise SystemExit(
        f"agentic_flink: missing modules {missing} in this kernel "
        f"({sys.executable}).\n"
        f"  pip install {pip_targets}"
    )


def _locate_jar(repo_root: pathlib.Path) -> Optional[pathlib.Path]:
    env = os.environ.get("AGENTIC_FLINK_JAR")
    if env:
        p = pathlib.Path(env).expanduser()
        return p if p.exists() else None
    candidates = sorted(repo_root.glob("target/agentic-flink-*.jar"))
    candidates = [
        c for c in candidates
        if "original" not in c.name and "sources" not in c.name
    ]
    return candidates[-1] if candidates else None


def _ensure_classpath(repo_root: pathlib.Path) -> list[str]:
    """Build (or reuse) target/runtime-classpath.txt; return its entries."""
    cp = repo_root / "target" / "runtime-classpath.txt"
    if not cp.exists():
        # Run mvn dependency:build-classpath — this is the same incantation
        # notebooks 07/08 used inline. Quiet output to keep the cell tidy.
        subprocess.run(
            [
                "mvn",
                "-q",
                "dependency:build-classpath",
                f"-Dmdep.outputFile={cp}",
                "-Dmdep.includeScope=runtime",
            ],
            cwd=repo_root,
            check=True,
        )
    return [j for j in cp.read_text().strip().split(":") if j]


def _build_launcher_args(
    level: str,
    *,
    in_endpoint: Optional[str] = None,
    out_endpoint: Optional[str] = None,
    control_endpoint: Optional[str] = None,
    debug_sink_endpoint: Optional[str] = None,
    alerts_pub_endpoint: Optional[str] = None,
    anthropic_key: Optional[str] = None,
    products: Optional[Iterable[str]] = None,
    window_ms: Optional[int] = None,
    top_n: Optional[int] = None,
    name: Optional[str] = None,
    extra_args: Optional[dict[str, str]] = None,
) -> list[str]:
    """Mirrors :meth:`session.SessionClient.submit_level`'s arg shaping so
    embedded and session modes accept identical kwargs."""
    args: list[str] = ["--level", level]
    if in_endpoint:
        args += ["--in", in_endpoint]
    if out_endpoint:
        args += ["--out", out_endpoint]
    if control_endpoint:
        args += ["--control", control_endpoint]
    if debug_sink_endpoint:
        args += ["--debug-sink", debug_sink_endpoint]
    if alerts_pub_endpoint:
        args += ["--alerts-pub", alerts_pub_endpoint]
    if anthropic_key:
        args += ["--anthropic-key", anthropic_key]
    if products:
        args += ["--products", ",".join(products)]
    if window_ms is not None:
        args += ["--window-ms", str(window_ms)]
    if top_n is not None:
        args += ["--top-n", str(top_n)]
    if name:
        args += ["--name", name]
    if extra_args:
        for k, v in extra_args.items():
            args += [f"--{k}", str(v)]
    return args


__all__ = [
    "Runtime",
    "InProcRuntime",
    "SessionRuntime",
    "EmbeddedClusterRuntime",
    "EmbeddedJobHandle",
    "RuntimeModeError",
    "bootstrap",
    "from_env",
    "inproc",
    "session_cluster",
    "embedded",
]
