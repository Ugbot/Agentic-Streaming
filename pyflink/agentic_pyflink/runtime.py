"""``FlinkRuntime``: the Flink adapter of the canonical core, driven from Python via PyFlink.

The job graph is ``source -> json->event -> keyBy(conversation_id) -> WorkflowTurnFunction ->
result->json -> sink``; everything between the connectors is the Java adapter from
``org.agentic.flink`` (PR #21) running the canonical core unchanged. Python contributes the
portable workflow document, the Flink configuration, and the connectors.

Two ways to run:

* :meth:`FlinkRuntime.run` -- bounded: a list of turns goes in, the normalized results come back.
  The job ends when the turns are drained. This is what the CLI and the simple tests use.
* :meth:`FlinkRuntime.deploy` + :meth:`FlinkRuntime.submit` -- streaming: the job stays up, turns
  are handed to it one (or one batch) at a time, and each call blocks until the normalized result
  of every turn in the call is visible. :meth:`FlinkRuntime.restart` stops the job with a
  savepoint and restores a fresh job from it, which is how the conformance fixtures exercise
  ``restart_runtime``. The streaming path needs a :class:`FileSource` and a :class:`FileSink`
  whose directories this process can read and write (a local spool in ``local`` mode).

Workflow ``timers`` read processing time from the operator's wall clock unless the runtime is
constructed with ``clock="manual"``: then they read a ``ManualProcessingClock`` registered in the
gateway JVM under an id this runtime owns, which starts at zero and moves only through
:meth:`FlinkRuntime.advance_time` (the fixtures' ``advance_time_ms``). Local mode runs the cluster
inside the gateway JVM, so the operator and this process see the same reading, and the reading
outlives a :meth:`FlinkRuntime.restart`.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
import warnings
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pyflink.common import Configuration
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import add_jars_to_context_class_loader

from ._contract import Runtime, required_capabilities
from .capabilities import CAPABILITIES, check_requirements
from .config import FlinkConfig
from .connectors import (
    CollectionSource,
    CollectSink,
    FileSink,
    FileSource,
    Sink,
    Source,
    committed_part_files,
    encode_turn,
)
from .jars import as_urls, classpath_jars
from .workflow import Workflow, as_workflow, workflow_json

BRIDGE_CLASS = "org.agentic.pyflink.PyFlinkJob"
MANUAL_CLOCK_CLASS = "org.agentic.flink.runtime.ManualProcessingClock"
SAVEPOINT_PATH_KEY = "execution.state-recovery.path"
CLOCKS = ("system", "manual")

Result = dict[str, Any]


class RuntimeStateError(RuntimeError):
    """A method was called in a state where it has no meaning (e.g. ``submit`` before ``deploy``)."""


class ResultTimeoutError(TimeoutError):
    """No normalized result arrived within the deadline; the message says what was awaited."""


class FlinkRuntime(Runtime):
    """Runs portable workflow documents on Flink from Python. See the module docstring.

    Registered as ``pyflink`` in the ``agentic.runtimes`` entry-point group (``flink-jvm`` is the
    JPype facade in ``agentic-flink``); implements the shared ``agentic.runtime.Runtime`` ABC.
    """

    name = "pyflink"

    def __init__(
        self,
        config: FlinkConfig | None = None,
        *,
        source: Source | None = None,
        sink: Sink | None = None,
        result_timeout: float = 60.0,
        clock: str = "system",
        **overrides: Any,
    ) -> None:
        if clock not in CLOCKS:
            raise ValueError(f"clock must be one of {CLOCKS}, got {clock!r}")
        base = config or FlinkConfig()
        self.config = FlinkConfig(**{**base.__dict__, **overrides}) if overrides else base
        self._source = source
        self._sink = sink
        self.result_timeout = result_timeout
        self.clock_kind = clock
        self._clock_id: str | None = f"pyflink-{uuid.uuid4()}" if clock == "manual" else None
        self._clock = None
        self._spec: Workflow | None = None
        self._job_client = None
        self._env: StreamExecutionEnvironment | None = None
        self._savepoint: str | None = None
        self._spool: Path | None = None
        self._consumed: dict[str, int] = {}
        self._pending: list[Result] = []
        self._closed = False

    # ------------------------------------------------------------------------------ contract

    def capabilities(self) -> dict[str, str]:
        return dict(CAPABILITIES)

    def deploy(self, spec: Any) -> FlinkRuntime:
        """Validate ``spec`` against this runtime and start the streaming job.

        Raises :class:`CapabilityError` listing every capability the document needs that this
        runtime declares ``unsupported``; warns for ``not_tested`` ones. The Java validator
        rejects structurally invalid documents when the graph is built.
        """
        self._ensure_open()
        if self._job_client is not None:
            raise RuntimeStateError("a job is already deployed; call close() or restart() first")
        workflow = as_workflow(spec)
        check_requirements(required_capabilities(workflow), self.capabilities())
        if self._source is None or self._sink is None:
            spool = Path(tempfile.mkdtemp(prefix="agentic-pyflink-"))
            self._spool = spool
            self._source = self._source or FileSource(str(spool / "turns"))
            self._sink = self._sink or FileSink(str(spool / "results"))
        if self._source.bounded:
            raise RuntimeStateError(
                "deploy() needs an unbounded source (FileSource/KafkaSource); use run() for a list of turns"
            )
        if isinstance(self._sink, CollectSink):
            raise RuntimeStateError("deploy() cannot collect results in-process; use FileSink/KafkaSink or run()")
        self._spec = workflow
        self._start()
        return self

    def submit(self, event: Mapping[str, Any]) -> Result:
        """Deliver one turn to the running job and return its normalized result."""
        return self.submit_all([event])[0]

    def submit_all(self, events: Sequence[Mapping[str, Any]]) -> list[Result]:
        """Deliver ``events`` back to back (one file, in order) and return their results in order.

        Results are matched to turns by ``(conversation_id, turn_id)`` in arrival order, so a
        redelivered ``turn_id`` gets the second result the job produced for it.
        """
        self._ensure_open()
        if self._job_client is None or self._spec is None:
            raise RuntimeStateError("no job deployed; call deploy(spec) first")
        if not isinstance(self._source, FileSource):
            raise RuntimeStateError(
                f"submit() delivers turns through a FileSource; this runtime uses {type(self._source).__name__}. "
                "Produce turns to that connector directly."
            )
        if not isinstance(self._sink, FileSink):
            raise RuntimeStateError(
                f"submit() reads results from a FileSink; this runtime uses {type(self._sink).__name__}."
            )
        docs = [encode_turn(e) for e in events]
        keys = [(json.loads(d)["conversation_id"], json.loads(d)["turn_id"]) for d in docs]
        self._write_turn_file(docs)
        return [self._await(key) for key in keys]

    def restart(self) -> None:
        """Stop the job with a savepoint and start a fresh job restored from it."""
        self._ensure_open()
        if self._job_client is None or self._spec is None:
            raise RuntimeStateError("no job deployed; call deploy(spec) first")
        target = self._savepoint_dir()
        # PyFlink's JobClient.stop_with_savepoint wrapper predates Flink 2.x's three-argument signature.
        format_type = _java_class("org.apache.flink.core.execution.SavepointFormatType").CANONICAL
        self._savepoint = self._job_client._j_job_client.stopWithSavepoint(False, target, format_type).get()
        self._job_client = None
        self._env = None
        self._start()

    def advance_time(self, ms: int) -> int:
        """Move the manual processing clock forward by ``ms`` milliseconds and return its new reading.

        Logical time never moves backwards, so a negative ``ms`` raises :class:`ValueError`. Only
        a runtime constructed with ``clock="manual"`` can be advanced; the default reads wall time.
        """
        self._ensure_open()
        if int(ms) < 0:
            raise ValueError(f"logical time never moves backwards: advance by {ms}")
        return int(self._manual_clock().advance(int(ms)))

    def now_ms(self) -> int:
        """The manual processing clock's current reading in milliseconds."""
        self._ensure_open()
        return int(self._manual_clock().nowMs())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        client, self._job_client = self._job_client, None
        if client is not None:
            try:
                client.cancel().result()
            except Exception as e:  # the job may already have finished or failed
                warnings.warn(f"cancelling the Flink job failed: {e}", stacklevel=2)
        clock, self._clock = self._clock, None
        if clock is not None:
            clock.release()
        self._env = None

    def __enter__(self) -> FlinkRuntime:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------------------- bounded

    def run(self, spec: Any, turns: Sequence[Mapping[str, Any]]) -> list[Result]:
        """Run ``turns`` through ``spec`` as one bounded job and return the normalized results.

        Results are returned in per-conversation order (Flink keys the stream by conversation
        id); across conversations the order is whatever the sink delivered.
        """
        self._ensure_open()
        workflow = as_workflow(spec)
        check_requirements(required_capabilities(workflow), self.capabilities())
        if not turns:
            raise ValueError("run() needs at least one turn")
        env = self._environment()
        results = self._graph(env, workflow, CollectionSource(list(turns)))
        return [json.loads(line) for line in results.execute_and_collect()]

    # ------------------------------------------------------------------------------ internals

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeStateError("runtime is closed")

    def _environment(self) -> StreamExecutionEnvironment:
        conf = Configuration()
        for key, value in self.config.flink_configuration().items():
            conf.set_string(key, value)
        if self._savepoint:
            conf.set_string(SAVEPOINT_PATH_KEY, self._savepoint)
        env = StreamExecutionEnvironment.get_execution_environment(conf)
        env.set_parallelism(self.config.parallelism)
        env.enable_checkpointing(self.config.checkpoint_interval_ms)
        env.add_jars(*as_urls(classpath_jars(self.config.extra_jars)))
        return env

    def _graph(self, env: StreamExecutionEnvironment, workflow: Workflow, source: Source) -> DataStream:
        source.check_available()
        turns = source.attach(env)
        bridge = _java_class(BRIDGE_CLASS)
        clock_id = self._manual_clock().clockId() if self.clock_kind == "manual" else None
        j_results = bridge.assemble(
            env._j_stream_execution_environment, workflow_json(workflow), turns._j_data_stream, clock_id
        )
        return DataStream(j_results)

    def _manual_clock(self):
        """The Java ``ManualProcessingClock`` handle this runtime owns, registered on first use."""
        if self._clock_id is None:
            raise RuntimeStateError(
                f"runtime {self.name!r} reads wall time; construct it with clock='manual' to control time"
            )
        if self._clock is None:
            add_jars_to_context_class_loader(as_urls(classpath_jars(self.config.extra_jars)))
            self._clock = _java_class(MANUAL_CLOCK_CLASS).named(self._clock_id)
        return self._clock

    def _start(self) -> None:
        assert self._spec is not None and self._source is not None and self._sink is not None
        self._sink.check_available()
        env = self._environment()
        results = self._graph(env, self._spec, self._source)
        self._sink.attach(results)
        self._env = env
        self._job_client = env.execute_async(f"agentic:{self._spec.get('agent', {}).get('id', 'workflow')}")

    def _savepoint_dir(self) -> str:
        configured = self.config.savepoint_dir
        if configured:
            return configured if "://" in configured else Path(configured).resolve().as_uri()
        if self._spool is None:
            self._spool = Path(tempfile.mkdtemp(prefix="agentic-pyflink-"))
        return (self._spool / "savepoints").resolve().as_uri()

    def _write_turn_file(self, docs: list[str]) -> None:
        assert isinstance(self._source, FileSource)
        directory = Path(self._source.directory)
        directory.mkdir(parents=True, exist_ok=True)
        tmp = directory / f".{uuid.uuid4().hex}.jsonl.tmp"
        tmp.write_text("\n".join(docs) + "\n", encoding="utf-8")
        os.replace(tmp, directory / f"{int(time.time() * 1000):013d}-{uuid.uuid4().hex[:8]}.jsonl")

    def _await(self, key) -> Result:
        deadline = time.monotonic() + self.result_timeout
        while True:
            for i, r in enumerate(self._pending):
                if (r.get("conversation_id"), r.get("turn_id")) == key:
                    return self._pending.pop(i)
            if time.monotonic() > deadline:
                raise ResultTimeoutError(
                    f"no result for conversation_id={key[0]!r} turn_id={key[1]!r} within {self.result_timeout}s; "
                    f"job status: {self._job_status()}"
                )
            self._poll_job_failure()
            if not self._drain():
                time.sleep(0.05)

    def _drain(self) -> bool:
        assert isinstance(self._sink, FileSink)
        found = False
        for path in committed_part_files(self._sink.directory):
            name = str(path)
            lines = path.read_text(encoding="utf-8").splitlines()
            already = self._consumed.get(name, 0)
            for line in lines[already:]:
                if line.strip():
                    self._pending.append(json.loads(line))
                    found = True
            self._consumed[name] = len(lines)
        return found

    def _job_status(self) -> str:
        if self._job_client is None:
            return "no job"
        try:
            return str(self._job_client.get_job_status().result())
        except Exception as e:
            return f"unknown ({e})"

    def _poll_job_failure(self) -> None:
        if self._job_client is None:
            return
        status = self._job_client.get_job_status().result()
        if str(status) in ("FAILED", "CANCELED", "FINISHED"):
            error = ""
            try:
                self._job_client.get_job_execution_result().result()
            except Exception as e:
                error = f": {e}"
            raise RuntimeStateError(f"the Flink job is {status}{error}")


def _java_class(name: str):
    jvm = get_gateway().jvm
    parts = name.split(".")
    obj = jvm
    for part in parts:
        obj = getattr(obj, part)
    return obj
