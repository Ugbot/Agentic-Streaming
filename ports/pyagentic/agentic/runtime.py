"""The full-control surface: `Runtime`, `get_runtime`, `register_runtime`, `LocalRuntime`.

Runtimes are found by name in three places, in order: `register_runtime(name, factory)`,
the `agentic.runtimes` entry-point group, and the built-in `local`. Selecting a name that
resolves to nothing raises `RuntimeNotAvailableError` naming the extra to install.
"""

from __future__ import annotations

import random
import threading
import time
from abc import ABC, abstractmethod
from importlib.metadata import entry_points
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

from . import ir
from .bindings import Bindings
from .engine import Clock, Engine, Sleep, wall_clock_ms
from .errors import CapabilityError, RuntimeNotAvailableError, ValidationError
from .events import EventLog, FileEventLog, InMemoryEventLog, Turn
from .tools import ToolRegistry, ToolSpec

ENTRY_POINT_GROUP = "agentic.runtimes"

CAPABILITIES: Tuple[str, ...] = (
    "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails",
    "verifier", "ordering", "idempotency", "retry", "memory", "retrieval", "context_window",
    "replay", "suspend_resume", "timers", "saga", "a2a", "cep", "event_time",
    "checkpoint_recovery", "parallelism", "durable_store",
)
CAPABILITY_VALUES = frozenset({"supported", "partial", "unsupported", "not_tested"})

# Runtime names that other packages of this project provide, with the extra that installs them.
KNOWN_EXTRAS: Dict[str, str] = {
    "flink-jvm": "pyagentic[flink]",
    "local-jvm": "pyagentic[jvm]",
    "pekko": "pyagentic[jvm]",
    "pyflink": "pyagentic[pyflink]",
}

# Names that used to be registered and now resolve to nothing; the error says what replaced them.
RENAMED_RUNTIMES: Dict[str, str] = {
    "flink": "'flink-jvm' (the JPype facade, agentic-flink) or 'pyflink' (agentic-pyflink)",
    "jvm": "'local-jvm' (agentic-flink)",
}

Factory = Callable[..., "Runtime"]
_registry: Dict[str, Factory] = {}
_registry_guard = threading.Lock()


class Runtime(ABC):
    """One deployed workflow, one runtime. `deploy` before `submit`; `close` when done."""

    name: str = "abstract"

    @abstractmethod
    def capabilities(self) -> Dict[str, str]:
        """Every v1 capability id mapped to supported | partial | unsupported | not_tested."""

    @abstractmethod
    def deploy(self, spec: Any) -> None:
        """Validate `spec` (an `AgentSpec` or a workflow document) against this runtime and
        make it live. Raises `CapabilityError` listing every unsupported requirement."""

    @abstractmethod
    def submit(self, event: Union[Turn, Mapping[str, Any]]) -> Dict[str, Any]:
        """Process one turn to a terminal status and return the normalized result."""

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> "Runtime":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def requirements(self, doc: Mapping[str, Any]) -> List[str]:
        return required_capabilities(doc)

    def check_capabilities(self, doc: Mapping[str, Any]) -> None:
        declared = self.capabilities()
        bad = [f"{cap} ({declared.get(cap, 'not declared')})" for cap in self.requirements(doc)
               if declared.get(cap) not in ("supported", "partial")]
        if bad:
            raise CapabilityError(self.name, bad)


def required_capabilities(doc: Mapping[str, Any]) -> List[str]:
    """The capability ids a validated workflow document needs from any runtime."""
    agent = doc["agent"]
    paths: Mapping[str, Mapping[str, Any]] = agent["paths"]
    policies = doc.get("policies") or {}
    needs = ["routing"]
    brains = {p.get("brain", "rule") for p in paths.values() if "x-brain" not in p}
    if "rule" in brains:
        needs.append("rule_brain")
    if "llm" in brains:
        needs.append("llm_brain")
    if doc.get("tools") or doc.get("mcp") or doc.get("a2a"):
        needs.append("tools")
    if any("parameters" in t for t in doc.get("tools") or []):
        needs.append("structured_tool_args")
    if doc.get("guardrails"):
        needs.append("guardrails")
    verifiers = [agent.get("verifier") or {}] + [p.get("verifier") or {} for p in paths.values()]
    if any(v.get("kind", "prefix") != "none" for v in verifiers):
        needs.append("verifier")
    if policies.get("ordering", "per-conversation") == "per-conversation":
        needs.append("ordering")
    if policies.get("idempotency", "turn-id") == "turn-id":
        needs.append("idempotency")
    if (policies.get("retry") or {}).get("kind", "none") != "none":
        needs.append("retry")
    needs.append("memory")
    if doc.get("retrieval"):
        needs.append("retrieval")
    if doc.get("context"):
        needs.append("context_window")
    if any("x-suspend-until" in p for p in paths.values()):
        needs.append("suspend_resume")
    if doc.get("timers"):
        needs.append("timers")
    if doc.get("saga"):
        needs.append("saga")
    if doc.get("a2a"):
        needs.append("a2a")
    if doc.get("cep"):
        needs.append("cep")
    if doc.get("stores"):
        needs.append("durable_store")
    return needs


# -- discovery ---------------------------------------------------------------

def register_runtime(name: str, factory: Factory) -> None:
    """Make `get_runtime(name, **options)` return `factory(**options)`."""
    if not name or not callable(factory):
        raise ValidationError("register_runtime needs a non-empty name and a callable factory")
    with _registry_guard:
        _registry[name] = factory


def unregister_runtime(name: str) -> None:
    with _registry_guard:
        _registry.pop(name, None)


def available_runtimes() -> Dict[str, str]:
    """Runtime names that would resolve right now, mapped to where they come from."""
    found: Dict[str, str] = {"local": "builtin"}
    for ep in _entry_points():
        found[ep.name] = f"entry point {ep.value}"
    with _registry_guard:
        for name in _registry:
            found[name] = "register_runtime"
    return found


def get_runtime(name: str = "local", **options: Any) -> Runtime:
    with _registry_guard:
        factory = _registry.get(name)
    if factory is None:
        for ep in _entry_points():
            if ep.name == name:
                factory = ep.load()
                break
    if factory is None and name == "local":
        factory = LocalRuntime
    if factory is None:
        extra = KNOWN_EXTRAS.get(name)
        renamed = RENAMED_RUNTIMES.get(name)
        hint = (f"install it with `pip install '{extra}'`" if extra
                else f"it was renamed; use {renamed}" if renamed
                else "install the package that provides it, or call register_runtime()")
        known = ", ".join(sorted(available_runtimes()))
        raise RuntimeNotAvailableError(
            f"runtime {name!r} is not available; {hint}. Available runtimes: {known}")
    runtime = factory(**options)
    if not isinstance(runtime, Runtime):
        raise RuntimeNotAvailableError(
            f"factory for runtime {name!r} returned {type(runtime).__name__}, not a Runtime")
    return runtime


def _entry_points() -> Iterator[Any]:
    eps = entry_points()
    if hasattr(eps, "select"):
        return iter(eps.select(group=ENTRY_POINT_GROUP))
    return iter(eps.get(ENTRY_POINT_GROUP, []))  # pragma: no cover - Python < 3.10


# -- the local runtime -------------------------------------------------------

class _ConversationGate:
    """Arrival-order single writer for one conversation: ticket in, wait your turn."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._next = 0
        self._serving = 0

    def take(self) -> int:
        with self._cond:
            ticket = self._next
            self._next += 1
            return ticket

    def enter(self, ticket: int) -> None:
        with self._cond:
            while self._serving != ticket:
                self._cond.wait()

    def leave(self) -> None:
        with self._cond:
            self._serving += 1
            self._cond.notify_all()


class LocalRuntime(Runtime):
    """The reference runtime: in-process, ordered per conversation, event-sourced.

    `log` is where events live. It defaults to a fresh `InMemoryEventLog`; pass the same
    log to a second `LocalRuntime` (or call `restart()`) to replay into a new instance.
    A `stores.conversation` block of kind `file` (or `store_dir=`) uses a `FileEventLog`.
    """

    name = "local"

    _CAPABILITIES: Dict[str, str] = {
        "routing": "supported", "rule_brain": "supported",
        "llm_brain": "partial",  # only the scripted `stub` provider runs locally
        "tools": "supported", "structured_tool_args": "supported", "guardrails": "supported",
        "verifier": "supported", "ordering": "supported", "idempotency": "supported",
        "retry": "supported", "memory": "supported", "retrieval": "supported",
        "context_window": "unsupported", "replay": "supported", "suspend_resume": "supported",
        "timers": "unsupported", "saga": "supported", "a2a": "supported", "cep": "unsupported",
        "event_time": "unsupported", "checkpoint_recovery": "unsupported",
        "parallelism": "unsupported", "durable_store": "supported",
    }

    def __init__(
        self,
        log: Optional[EventLog] = None,
        store_dir: Optional[str] = None,
        clock: Clock = wall_clock_ms,
        sleep: Sleep = time.sleep,
        seed: Optional[int] = None,
        **unknown: Any,
    ) -> None:
        if unknown:
            raise ValidationError(f"LocalRuntime does not take options {sorted(unknown)}; "
                                  f"it accepts log, store_dir, clock, sleep, seed")
        if log is not None and store_dir is not None:
            raise ValidationError("pass either log= or store_dir=, not both")
        self._explicit_log = log
        self._store_dir = store_dir
        self.clock = clock
        self.sleep = sleep
        self.rng = random.Random(seed) if seed is not None else None
        self.log: Optional[EventLog] = None
        self.engine: Optional[Engine] = None
        self.tools = ToolRegistry()
        self.doc: Optional[Dict[str, Any]] = None
        self.bindings = Bindings()
        self.degradations: List[str] = []
        self._gates: Dict[str, _ConversationGate] = {}
        self._gates_guard = threading.Lock()
        self._closed = False

    def capabilities(self) -> Dict[str, str]:
        return dict(self._CAPABILITIES)

    def deploy(self, spec: Any) -> None:
        if self._closed:
            raise ValidationError("this runtime is closed")
        doc, bindings = _unpack(spec)
        doc = ir.validate_document(doc)
        self.check_capabilities(doc)
        missing = bindings.missing(doc)
        if missing:
            raise ValidationError("the workflow references Python bindings the spec did not carry:\n"
                                  + "\n".join(f"  - {m}" for m in missing))
        self.log = self._open_log(doc)
        self.tools = ToolRegistry()
        for entry in doc.get("tools") or []:
            self.tools.declare(ToolSpec.from_document(entry))
        for peer in doc.get("a2a") or []:
            self.tools.declare(ToolSpec.from_peer(peer))
        for tool_id, fn in bindings.tools.items():
            self.tools.bind_function(tool_id, fn)
        for name, fn in bindings.peers.items():
            self.tools.bind_peer(name, fn)
        self.doc = doc
        self.bindings = bindings
        self.engine = Engine(doc, self.tools, self.log, bindings, self.clock, self.sleep, self.rng)
        self.engine.recover()

    def _open_log(self, doc: Mapping[str, Any]) -> EventLog:
        if self._explicit_log is not None:
            return self._explicit_log
        conversation = (doc.get("stores") or {}).get("conversation") or {}
        kind = conversation.get("kind", "file" if self._store_dir else "memory")
        if kind == "file":
            directory = self._store_dir or conversation.get("url")
            if not directory:
                raise ValidationError("a file conversation store needs url: <directory>",
                                      "/stores/conversation/url")
            return FileEventLog(directory)
        if kind == "memory":
            return InMemoryEventLog()
        if conversation.get("on_unavailable", "fail") == "degrade":
            self.degradations.append(f"stores.conversation kind {kind!r} unavailable locally; using memory")
            return InMemoryEventLog()
        raise ValidationError(f"conversation store kind {kind!r} is not available in the local runtime "
                              f"(memory, file); set on_unavailable: degrade to fall back",
                              "/stores/conversation/kind")

    def submit(self, event: Union[Turn, Mapping[str, Any]]) -> Dict[str, Any]:
        if self._closed:
            raise ValidationError("this runtime is closed")
        if self.engine is None:
            raise ValidationError("deploy() a spec before submit()")
        turn = event if isinstance(event, Turn) else Turn(**dict(event))
        gate = self._gate(turn.conversation_id)
        ticket = gate.take()
        gate.enter(ticket)
        try:
            result = self.engine.handle(turn)
        finally:
            gate.leave()
        if self.degradations:
            result["runtime_detail"]["degraded"] = list(self.degradations)
        ir.validate_result(result)
        return result

    def _gate(self, conversation_id: str) -> _ConversationGate:
        with self._gates_guard:
            gate = self._gates.get(conversation_id)
            if gate is None:
                gate = self._gates[conversation_id] = _ConversationGate()
            return gate

    def restart(self) -> "LocalRuntime":
        """Close this instance and return a fresh one over the same log, replayed."""
        if self.doc is None or self.log is None:
            raise ValidationError("nothing deployed; restart() needs a deployed workflow")
        self.close()
        fresh = LocalRuntime(log=self.log, clock=self.clock, sleep=self.sleep)
        fresh.rng = self.rng
        fresh.deploy(_Deployable(self.doc, self.bindings))
        return fresh

    def state(self, conversation_id: str) -> Dict[str, Any]:
        if self.engine is None:
            raise ValidationError("deploy() a spec before reading state")
        return self.engine.state(conversation_id)

    def events(self, conversation_id: str) -> Sequence[Any]:
        if self.log is None:
            raise ValidationError("deploy() a spec before reading events")
        return self.log.read(conversation_id)

    def close(self) -> None:
        self._closed = True
        self.engine = None


class _Deployable:
    def __init__(self, doc: Mapping[str, Any], bindings: Bindings) -> None:
        self.document = doc
        self.bindings = bindings


def _unpack(spec: Any) -> Tuple[Mapping[str, Any], Bindings]:
    document = getattr(spec, "document", None)
    if isinstance(document, Mapping):
        bindings = getattr(spec, "bindings", None)
        return document, bindings if isinstance(bindings, Bindings) else Bindings()
    if isinstance(spec, Mapping):
        return spec, Bindings()
    raise ValidationError(f"deploy() takes an AgentSpec or a workflow document, not {type(spec).__name__}")
