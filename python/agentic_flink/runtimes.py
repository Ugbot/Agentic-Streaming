"""JVM-backed runtimes for the shared contract (``agentic.runtimes`` entry points).

* ``local-jvm`` — :class:`JvmLocalRuntime` over ``org.jagentic.core.LocalRuntime``: the canonical
  core, in-process, with the conversation log as the only state that survives ``restart()``.
* ``flink-jvm`` — :class:`FlinkRuntime` over the Flink adapter (``FlinkPipelineRunner`` /
  ``WorkflowTurnFunction``): by default one long-running streaming job on an in-process local
  Flink environment (``LocalWorkflowSession``), fed turn by turn, with ``restart()`` as
  stop-with-savepoint plus restore; with ``durable=False`` each ``submit``/``submit_all`` runs
  one bounded job instead.
* ``pekko`` — :class:`PekkoRuntime` over ``org.jagentic.pekko.runtime.PekkoBackendProvider`` when
  the agentic-pekko jar is on the classpath.

Every public method returns Python values only: results are the ``TurnResult.toMap()`` mapping
converted with :func:`agentic_flink._proxy.to_py`, Java exceptions are translated, and
``CompletableFuture`` is exposed as :class:`concurrent.futures.Future` / awaitables.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import re
import shutil
import tempfile
from typing import Any, Dict, List, Mapping, Optional, Sequence

from . import _jvm
from ._classpath import MissingJarError, flink_jars, has_class, has_flink_classes, pekko_jars
from ._contract import (
    CAPABILITY_IDS,
    Runtime,
    RuntimeNotAvailableError,
    CapabilityError,
    available_runtimes,
    register_runtime,
)
from ._proxy import as_concurrent_future, as_future, java_calls, to_java, to_py
from .pytools import register_python_tools
from .workflow import AgentSpec, Event, workflow_requirements

_LOCAL_JVM_CAPABILITIES: Dict[str, str] = {
    # Proven by python/tests/test_conformance_jvm.py against spec/conformance/v1 (15 fixtures).
    "routing": "supported",
    "rule_brain": "supported",
    "tools": "supported",
    "structured_tool_args": "supported",
    "guardrails": "supported",
    "verifier": "supported",
    "ordering": "supported",
    "idempotency": "supported",
    "retry": "supported",
    "memory": "supported",
    "retrieval": "supported",
    "context_window": "supported",
    "replay": "supported",
    "suspend_resume": "supported",
    "saga": "supported",
    "a2a": "supported",
    # The in-JVM log outlives restart() within one process (what the fixtures exercise); it is
    # not durability across a crash. Same narrow sense as the reference runtime.
    "durable_store": "partial",
    # The spec's scripted stub provider (fixture llm-brain-scripted) runs through jagentic-core's
    # ScriptedChatClient + LlmBrain; no test here drives a network provider.
    "llm_brain": "supported",
    # Sequence patterns are the core's in-turn fold over the log (fixture cep-sequence).
    "cep": "supported",
    "event_time": "supported",
    # Not offered by LocalRuntime.
    "timers": "unsupported",
    "checkpoint_recovery": "unsupported",
    "parallelism": "unsupported",
}

_FLINK_CAPABILITIES: Dict[str, str] = {
    # Proven by python/tests/test_conformance_jvm.py and test_flink_durable_restart.py (flink):
    # one streaming job per deploy(), fed turn by turn through LocalWorkflowSession.
    "routing": "supported",
    "rule_brain": "supported",
    "tools": "supported",
    "structured_tool_args": "supported",
    "guardrails": "supported",
    "verifier": "supported",
    "ordering": "supported",
    "idempotency": "supported",
    "retry": "supported",
    "memory": "supported",
    "retrieval": "supported",
    "context_window": "supported",
    "saga": "supported",
    "a2a": "supported",
    "parallelism": "supported",
    "llm_brain": "supported",  # fixture llm-brain-scripted: the stub provider ships in the job graph
    # Sequence patterns run inside WorkflowTurnFunction as the core's fold (fixture cep-sequence).
    "cep": "supported",
    "event_time": "supported",
    # restart() stops the job with a savepoint and restores a new job from it: the keyed
    # conversation log is the only state that comes back, and the recorded turns are not re-run.
    "replay": "supported",
    "suspend_resume": "supported",
    # Durable in the same sense as the Flink JUnit binding: a savepoint on the local filesystem
    # survives the job and its local cluster; the JVM process itself stays up across restart().
    "durable_store": "supported",
    # No kill-and-recover-from-checkpoint step is exposed, and timer-driven results are not
    # collected, so nothing here is exercised; Python tools cannot ship in the job graph.
    "checkpoint_recovery": "unsupported",
    "timers": "unsupported",
}

_FLINK_BATCH_CAPABILITIES: Dict[str, str] = {
    # FlinkRuntime(durable=False): one bounded job per submit_all.
    "routing": "supported",
    "rule_brain": "supported",
    "tools": "supported",
    "structured_tool_args": "supported",
    "guardrails": "supported",
    "verifier": "supported",
    "ordering": "supported",
    "idempotency": "supported",
    "retry": "supported",
    "memory": "partial",  # conversation state lives in keyed state for the life of one job (one submit_all batch)
    "retrieval": "supported",
    "context_window": "supported",
    "saga": "supported",
    "a2a": "supported",
    "parallelism": "supported",
    # The Python binding runs bounded jobs; keyed state does not outlive a job, so nothing can be
    # replayed or resumed across submit() calls and Python tools cannot ship in the job graph.
    "replay": "unsupported",
    "suspend_resume": "unsupported",
    "durable_store": "unsupported",
    "checkpoint_recovery": "unsupported",
    "timers": "unsupported",
    "llm_brain": "supported",  # fixture llm-brain-scripted: the stub provider ships in the job graph
    # Sequence patterns run inside WorkflowTurnFunction as the core's fold (fixture cep-sequence).
    "cep": "supported",
    "event_time": "supported",
}

_PEKKO_CAPABILITIES: Dict[str, str] = {cap: "not_tested" for cap in CAPABILITY_IDS}
_PEKKO_CAPABILITIES.update({
    "replay": "unsupported",  # the Python binding has no restart() for the actor system
    "durable_store": "unsupported",
    "timers": "unsupported", "cep": "unsupported", "event_time": "unsupported",
    "checkpoint_recovery": "unsupported", "parallelism": "unsupported",
})


def _as_document(spec: Any) -> Dict[str, Any]:
    if isinstance(spec, AgentSpec):
        return spec.to_dict()
    if isinstance(spec, Mapping):
        return AgentSpec(spec).to_dict()
    raise TypeError(f"deploy() needs an AgentSpec or a workflow mapping, got {type(spec).__name__}")


def _bindings(spec: Any) -> Dict[str, Any]:
    return spec.bindings if isinstance(spec, AgentSpec) else {}


def _check_requirements(name: str, capabilities: Mapping[str, str], doc: Mapping[str, Any],
                        extra_unsupported: Mapping[str, str] = {}) -> None:
    required = workflow_requirements(doc)
    bad = [f"{cap} ({capabilities.get(cap, 'unsupported')}, needed by {where})"
           for cap, where in required.items() if capabilities.get(cap, "unsupported") == "unsupported"]
    bad += [f"{k} ({v})" for k, v in extra_unsupported.items()]
    if bad:
        raise CapabilityError(name, bad)


def _java_event(event: Event):
    JEvent = _jvm.jclass("org.jagentic.core.Event")
    if event.is_resume:
        return JEvent.resume(event.conversation_id, event.turn_id, to_java(dict(event.signal or {})))
    if event.metadata:
        return JEvent(event.conversation_id, event.turn_id, event.user_id, event.text or "",
                      to_java(dict(event.metadata)), None)
    return JEvent.turn(event.conversation_id, event.turn_id, event.user_id, event.text or "")


def _result(turn_result) -> Dict[str, Any]:
    return to_py(turn_result.toMap())


def _rewrite_function_tools(doc: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """The core cannot bind ``kind: function`` tools from a document. Rewrite them in the copy
    handed to the JVM as ``constant`` entries (so the document still validates and the paths'
    tool references resolve) and return the originals by id; the Python callables are then
    registered over those ids on the built ``ToolRegistry``."""
    declared: Dict[str, Dict[str, Any]] = {}
    rewritten = []
    for t in doc.get("tools", []):
        if t.get("kind") == "function":
            declared[t["id"]] = t
            t = {**t, "kind": "constant", "value": f"python:{t['id']}"}
        rewritten.append(t)
    if declared:
        doc["tools"] = rewritten
    return declared


class _JvmRuntime(Runtime):
    """Shared plumbing: build the core graph from a document and bind Python tools onto it."""

    name = "jvm"
    _capabilities: Dict[str, str] = {}

    def __init__(self, *, jvm_args: Sequence[str] = (), extra_jars: Sequence[str] = ()) -> None:
        self._jvm_args = list(jvm_args)
        self._extra_jars = list(extra_jars)
        self._doc: Optional[Dict[str, Any]] = None
        self._built: Any = None
        self._closed = False

    def capabilities(self) -> Dict[str, str]:
        return dict(self._capabilities)

    def _start_jvm(self) -> None:
        if not _jvm.is_started():
            _jvm.start_jvm(extra_jars=self._extra_jars, jvm_args=self._jvm_args)

    def _build(self, doc: Dict[str, Any], bindings: Mapping[str, Any], peers: Mapping[str, Any] = {}):
        GraphBuilder = _jvm.jclass("org.jagentic.core.pipeline.GraphBuilder")
        declared = _rewrite_function_tools(doc)
        missing = set(declared) - set(bindings)
        if missing:
            raise CapabilityError(self.name, [
                f"tools[{m}] (kind=function tool has no Python callable bound; use_tool / load(tools=...))"
                for m in sorted(missing)])
        JMap = _jvm.jclass("java.util.LinkedHashMap")
        jpeers = JMap()
        for peer_name, peer_rt in peers.items():
            jpeers.put(peer_name, peer_rt)
        with java_calls():
            built = GraphBuilder.build(to_java(doc), None, jpeers)
            register_python_tools(built.tools(), bindings, declared)
        return built

    def _require_deployed(self) -> None:
        if self._closed:
            raise RuntimeError(f"runtime {self.name!r} is closed")
        if self._built is None:
            raise RuntimeError(f"runtime {self.name!r} has no workflow deployed; call deploy(spec) first")

    @property
    def document(self) -> Optional[Dict[str, Any]]:
        return dict(self._doc) if self._doc is not None else None


class JvmLocalRuntime(_JvmRuntime):
    """``local-jvm``: the canonical core's in-process runtime."""

    name = "local-jvm"
    _capabilities = _LOCAL_JVM_CAPABILITIES

    def __init__(self, *, peers: Optional[Mapping[str, "JvmLocalRuntime"]] = None, **jvm: Any) -> None:
        super().__init__(**jvm)
        self._peers = dict(peers or {})
        self._log: Any = None
        self._runtime: Any = None

    def deploy(self, spec: Any) -> None:
        doc = _as_document(spec)
        _check_requirements(self.name, self._capabilities, doc)
        self._start_jvm()
        for peer_name, peer in self._peers.items():
            if not isinstance(peer, JvmLocalRuntime) or peer._runtime is None:
                raise CapabilityError(self.name, [f"a2a[{peer_name}] (inproc peers must be deployed JvmLocalRuntime instances)"])
        self._built = self._build(doc, _bindings(spec), {n: p._runtime for n, p in self._peers.items()})
        self._doc = doc
        ConversationLog = _jvm.jclass("org.jagentic.core.ConversationLog$InMemory")
        self._log = ConversationLog()
        self._runtime = self._new_runtime()

    def _new_runtime(self):
        LocalRuntime = _jvm.jclass("org.jagentic.core.LocalRuntime")
        ConversationStore = _jvm.jclass("org.jagentic.core.ConversationStore$InMemory")
        KeyedStateStore = _jvm.jclass("org.jagentic.core.KeyedStateStore$InMemory")
        with java_calls():
            return LocalRuntime(self._built.graph(), ConversationStore(), KeyedStateStore(),
                                self._built.tools(), self._built.retriever(), self._log)

    def restart(self) -> None:
        """Model a process restart: drop every materialized view, keep only the event log."""
        self._require_deployed()
        self._runtime = self._new_runtime()

    def submit(self, event: Event) -> Dict[str, Any]:
        self._require_deployed()
        with java_calls():
            return _result(self._runtime.submit(_java_event(event)))

    def submit_async(self, event: Event) -> concurrent.futures.Future:
        """Queue ``event`` behind the conversation's in-flight turns; completes with its result."""
        self._require_deployed()
        with java_calls():
            return as_concurrent_future(self._runtime.submitAsync(_java_event(event)), _result)

    def asubmit(self, event: Event) -> asyncio.Future:
        """``await``-able form of :meth:`submit_async`."""
        self._require_deployed()
        with java_calls():
            return as_future(self._runtime.submitAsync(_java_event(event)), _result)

    def submit_all(self, events: Sequence[Event]) -> List[Dict[str, Any]]:
        """Submit events back to back (arrival order) and return one result per event."""
        futures = [self.submit_async(e) for e in events]
        return [f.result() for f in futures]

    def events(self, conversation_id: str) -> List[Dict[str, Any]]:
        """The conversation's event log as Python mappings."""
        self._require_deployed()
        with java_calls():
            return [to_py(e.toMap()) for e in self._log.events(conversation_id)]

    def close(self) -> None:
        self._runtime = None
        self._built = None
        self._closed = True


_DURATION = re.compile(r"^\s*(\d+)\s*(ms|s|m|h)?\s*$")


def parse_duration_ms(value: Any) -> int:
    """``"30s"`` / ``"500ms"`` / ``5`` (seconds) → milliseconds."""
    if isinstance(value, (int, float)):
        return int(value * 1000)
    m = _DURATION.match(str(value))
    if not m:
        raise ValueError(f"cannot parse duration {value!r}; use e.g. '30s', '500ms', '5m'")
    n, unit = int(m.group(1)), m.group(2) or "s"
    return n * {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}[unit]


class FlinkRuntime(_JvmRuntime):
    """``flink-jvm``: run the workflow as a Flink job on an in-process local environment.

    The Python binding cannot ship Python code into a Flink job graph, so ``kind: function``
    tools are rejected at :meth:`deploy`. Two execution modes:

    * ``durable=True`` (default): :meth:`deploy` starts one streaming job
      (``org.agentic.flink.runtime.LocalWorkflowSession``) that stays up until :meth:`close`;
      :meth:`submit` / :meth:`submit_all` feed it turn by turn, so keyed state (the conversation
      log) persists across calls. :meth:`restart` stops the job with a savepoint under
      ``savepoint_dir`` and starts a new job restored from it; the recorded turns are rebuilt from
      the log and are not executed again (no brain or tool runs for them).
    * ``durable=False``: each :meth:`submit` / :meth:`submit_all` executes one bounded job over
      the given events (keyed by conversation, so per-conversation ordering and idempotency hold
      within the batch). Keyed state does not outlive the job and :meth:`restart` is refused."""

    name = "flink-jvm"
    _capabilities = _FLINK_CAPABILITIES

    def __init__(self, *, parallelism: int = 1, checkpoint_interval: Optional[Any] = None,
                 job_name: str = "agentic-flink", timeout: Any = "120s", durable: bool = True,
                 savepoint_dir: Optional[str] = None, **jvm: Any) -> None:
        super().__init__(**jvm)
        if int(parallelism) < 1:
            raise ValueError("parallelism must be >= 1")
        self.parallelism = int(parallelism)
        self.checkpoint_interval_ms = parse_duration_ms(checkpoint_interval) if checkpoint_interval is not None else None
        self.job_name = job_name
        self.timeout_ms = parse_duration_ms(timeout)
        self.durable = bool(durable)
        self.savepoint_dir = savepoint_dir
        self._options = None
        self._session: Any = None
        self._owned_savepoint_dir: Optional[str] = None

    def capabilities(self) -> Dict[str, str]:
        return dict(_FLINK_CAPABILITIES if self.durable else _FLINK_BATCH_CAPABILITIES)

    def _start_jvm(self) -> None:
        if _jvm.is_started():
            if not has_flink_classes():
                raise RuntimeNotAvailableError(
                    "the JVM is already running without the Flink distribution on its classpath; start it "
                    "with agentic_flink.start_jvm(extra_jars=agentic_flink.flink_jars()) before selecting "
                    "the 'flink-jvm' runtime, or select 'flink-jvm' first in this process."
                )
            return
        try:
            jars = flink_jars()
        except MissingJarError as e:
            raise RuntimeNotAvailableError(str(e)) from e
        _jvm.start_jvm(extra_jars=list(jars) + self._extra_jars, jvm_args=self._jvm_args)
        if not has_flink_classes():
            raise RuntimeNotAvailableError(
                f"Flink classes not loadable from the discovered jars ({jars[:3]}...); "
                "check AGENTIC_FLINK_CLASSPATH / FLINK_HOME"
            )

    def deploy(self, spec: Any) -> None:
        doc = _as_document(spec)
        bindings = _bindings(spec)
        extra = {f"tools[{tid}]": "kind=function (Python) tools cannot run inside a Flink job graph"
                 for tid in sorted(t["id"] for t in doc.get("tools", []) if t.get("kind") == "function")}
        _check_requirements(self.name, self.capabilities(), doc, extra)
        self._start_jvm()
        FlinkRuntimeOptions = _jvm.jclass("org.agentic.flink.runtime.FlinkRuntimeOptions")
        WorkflowValidator = _jvm.jclass("org.jagentic.core.pipeline.WorkflowValidator")
        with java_calls():
            jdoc = to_java(doc)
            WorkflowValidator.validate(jdoc)
            self._options = FlinkRuntimeOptions.fromSpec(jdoc)
        self._doc = doc
        self._built = jdoc  # the serialized configuration of the job
        self._bound = bindings
        if self.durable:
            self._start_session()

    def _start_session(self) -> None:
        LocalWorkflowSession = _jvm.jclass("org.agentic.flink.runtime.LocalWorkflowSession")
        Duration = _jvm.jclass("java.time.Duration")
        Paths = _jvm.jclass("java.nio.file.Paths")
        if self.savepoint_dir is None:
            self._owned_savepoint_dir = tempfile.mkdtemp(prefix="agentic-flink-jvm-")
        target = self.savepoint_dir or self._owned_savepoint_dir
        checkpoint = Duration.ofMillis(self.checkpoint_interval_ms) if self.checkpoint_interval_ms is not None else None
        with java_calls():
            self._session = LocalWorkflowSession(self._built, self._options, self.parallelism, checkpoint,
                                                 Paths.get(target), Duration.ofMillis(self.timeout_ms), self.job_name)
            self._session.start()

    @property
    def session(self) -> Any:
        """The Java ``LocalWorkflowSession`` behind a durable runtime (``None`` when ``durable=False``
        or nothing is deployed)."""
        return self._session

    def restart(self) -> None:
        """Stop the streaming job with a savepoint and start a new job restored from it.

        Only checkpointed keyed state (the conversation log, suspended turns) comes back; the
        conversation is rebuilt from the log on its next turn and no brain or tool runs for the
        turns already recorded. Refused with ``durable=False``."""
        self._require_deployed()
        if not self.durable or self._session is None:
            raise RuntimeError("restart() needs FlinkRuntime(durable=True): a bounded job keeps no state to restore")
        with java_calls():
            self._session.restart()

    def submit(self, event: Event) -> Dict[str, Any]:
        return self.submit_all([event])[0]

    def submit_all(self, events: Sequence[Event]) -> List[Dict[str, Any]]:
        """Submit ``events`` and return their results in submission order: through the running
        streaming job when durable, otherwise as one bounded job."""
        self._require_deployed()
        if not events:
            return []
        if self.durable:
            ArrayList = _jvm.jclass("java.util.ArrayList")
            with java_calls():
                jevents = ArrayList()
                for e in events:
                    jevents.add(_java_event(e))
                return [_result(r) for r in self._session.submitAll(jevents)]
        StreamExecutionEnvironment = _jvm.jclass("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")
        Configuration = _jvm.jclass("org.apache.flink.configuration.Configuration")
        FlinkPipelineRunner = _jvm.jclass("org.agentic.flink.pipeline.FlinkPipelineRunner")
        WorkflowTurnFunction = _jvm.jclass("org.agentic.flink.runtime.WorkflowTurnFunction")
        ArrayList = _jvm.jclass("java.util.ArrayList")
        with java_calls():
            conf = Configuration()
            env = StreamExecutionEnvironment.createLocalEnvironment(self.parallelism, conf)
            env.setParallelism(self.parallelism)
            if self.checkpoint_interval_ms is not None:
                env.enableCheckpointing(self.checkpoint_interval_ms)
            jevents = ArrayList()
            for e in events:
                jevents.add(_java_event(e))
            source = env.fromData(jevents, WorkflowTurnFunction.EVENT_TYPE)
            results = FlinkPipelineRunner.assembleResults(env, self._built, source, self._options)
            emitted = [_result(r) for r in results.executeAndCollect(self.job_name)]
        out: List[Dict[str, Any]] = []
        for e in events:
            match = next((i for i, r in enumerate(emitted)
                          if r["conversation_id"] == e.conversation_id and r["turn_id"] == e.turn_id), None)
            if match is None:
                raise RuntimeError(f"Flink job emitted no result for {e.conversation_id}/{e.turn_id}; got {emitted}")
            out.append(emitted.pop(match))
        return out

    def close(self) -> None:
        if self._session is not None:
            with java_calls():
                self._session.close()
            self._session = None
        if self._owned_savepoint_dir is not None:
            shutil.rmtree(self._owned_savepoint_dir, ignore_errors=True)
            self._owned_savepoint_dir = None
        self._built: Any = None
        self._options = None
        self._closed = True


class PekkoRuntime(_JvmRuntime):
    """``pekko``: the actor runtime from ``agentic-pekko`` through the core's ``BackendProvider`` SPI."""

    name = "pekko"
    _capabilities = _PEKKO_CAPABILITIES
    PROVIDER = "org.jagentic.pekko.runtime.PekkoBackendProvider"

    def __init__(self, **jvm: Any) -> None:
        super().__init__(**jvm)
        self._runtime: Any = None

    def _start_jvm(self) -> None:
        try:
            jars = pekko_jars()
        except MissingJarError as e:
            raise RuntimeNotAvailableError(str(e)) from e
        if not _jvm.is_started():
            # Pekko's Jackson Scala module needs a newer Jackson than the shaded Flink jar bundles,
            # so the Pekko jars go first on the classpath.
            _jvm.start_jvm(prepend_jars=jars, extra_jars=self._extra_jars, jvm_args=self._jvm_args)
        if not has_class(self.PROVIDER):
            raise RuntimeNotAvailableError(
                f"{self.PROVIDER} is not on the JVM classpath. Build agentic-pekko "
                "(`mvn -f agentic-pekko/pom.xml package -DskipTests`) and set AGENTIC_PEKKO_CLASSPATH to its "
                "jar plus dependencies before the JVM starts; the 'pekko' runtime has no pip extra yet."
            )

    def deploy(self, spec: Any) -> None:
        doc = _as_document(spec)
        _check_requirements(self.name, self._capabilities, doc)
        self._start_jvm()
        self._built = self._build(doc, _bindings(spec))
        self._doc = doc
        Provider = _jvm.jclass(self.PROVIDER)
        ConversationStore = _jvm.jclass("org.jagentic.core.ConversationStore$InMemory")
        with java_calls():
            self._runtime = Provider().create(self._built, ConversationStore())

    def submit(self, event: Event) -> Dict[str, Any]:
        self._require_deployed()
        with java_calls():
            return _result(self._runtime.submit(_java_event(event)))

    def close(self) -> None:
        if self._runtime is not None:
            with java_calls():
                self._runtime.close()
        self._runtime = None
        self._built = None
        self._closed = True


# ---- factories (the `agentic.runtimes` entry points) ----

def local_jvm(**options: Any) -> JvmLocalRuntime:
    return JvmLocalRuntime(**options)


def flink(**options: Any) -> FlinkRuntime:
    return FlinkRuntime(**options)


def pekko(**options: Any) -> PekkoRuntime:
    return PekkoRuntime(**options)


def register_jvm_runtimes() -> None:
    """Register the JVM runtimes explicitly (a source checkout that is not pip-installed has no
    entry points). ``local`` aliases ``local-jvm`` unless another package already provides it."""
    register_runtime("local-jvm", local_jvm)
    register_runtime("flink-jvm", flink)
    register_runtime("pekko", pekko)
    if "local" not in available_runtimes():
        register_runtime("local", local_jvm)


register_jvm_runtimes()

__all__ = [
    "FlinkRuntime",
    "JvmLocalRuntime",
    "PekkoRuntime",
    "flink",
    "local_jvm",
    "parse_duration_ms",
    "pekko",
    "register_jvm_runtimes",
]
