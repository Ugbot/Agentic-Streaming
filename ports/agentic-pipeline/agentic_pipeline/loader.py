"""YAML pipeline loader: parse a ``pipeline.yaml``, build the system and select the backend. The
result is a runnable system: an object with ``submit(Event) -> TurnResult``.

    from agentic_pipeline import Event, load
    system = load("banking.yaml")          # backend chosen by the YAML
    print(system.submit(Event("c1", "what is my balance?")).reply)

A pipeline runs on the ``agentic`` core: a turn is a turn of ``spec/v1``, on the event log,
deduplicated by ``turn_id``, with its effects journaled. A document that needs something only the
legacy ``pyagentic`` engine has runs there instead and says so (``legacy_features``), because the
core would otherwise load it and quietly do less. That engine has no event log, no ``turn_id`` and
no effect journal.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from agentic import ir
from agentic.cep import without_tool_actions

from . import backends
from .backends import CoreBackend, Event, TurnResult

LOG = logging.getLogger(__name__)

LLM_PROVIDERS = ("ollama", "openai", "stub")


def _chat_client_factory(llm_spec: Dict[str, Any]):
    """Build a ChatClient from the ``llm:`` section. ``ollama`` and ``openai`` require an
    explicit ``model`` (there is no default model name). ``stub`` needs an inline ``script``
    of {tool|text, args} steps (handy for offline tests / deterministic demos)."""
    from pyagentic.llm import ChatResult, OllamaChatClient, OpenAIChatClient, StubChatClient

    llm_spec = llm_spec or {}
    provider = llm_spec.get("provider", "ollama")
    if provider in ("ollama", "openai"):
        model = llm_spec.get("model")
        if not model or not str(model).strip():
            raise ValueError(
                f"llm.model is required for provider {provider!r}; name the model you have deployed "
                "(for example llm: {provider: ollama, model: qwen2.5:3b})")
        client = OllamaChatClient if provider == "ollama" else OpenAIChatClient
        return client(model=str(model), base_url=llm_spec.get("base_url"))
    if provider == "stub":
        script = [
            ChatResult(tool=s["tool"], args=s.get("args", {})) if s.get("tool") else ChatResult(text=s.get("text", ""))
            for s in llm_spec.get("script", [{"text": "ok"}])
        ]
        return StubChatClient(script)
    raise ValueError(f"unknown llm provider {provider!r}; supported providers: {', '.join(LLM_PROVIDERS)}")


def compile_deps(spec: Dict[str, Any]):
    """Compile a spec dict to ``(graph, tools, retriever)`` for the legacy engine."""
    from pyagentic import builder

    return builder.build(spec, chat_client_factory=_chat_client_factory)


def legacy_features(spec: Dict[str, Any]) -> List[str]:
    """What this document needs that only the legacy engine has. Empty means the core runs it."""
    needs: List[str] = []
    if spec.get("mcp"):
        needs.append("mcp")
    needs += [f"tools kind {tool['kind']}" for tool in spec.get("tools") or [] if tool.get("kind") in ("agent", "mcp")]
    needs += [f"a2a transport {peer.get('transport', 'http')}" for peer in spec.get("a2a") or []
              if peer.get("transport", "http") != "inproc"]
    stores = spec.get("stores") or {}
    kind = (stores.get("conversation") or {}).get("kind", "memory")
    if kind not in ("memory", "file"):
        needs.append(f"stores.conversation kind {kind}")
    needs += [f"stores.{name}" for name in ("long_term", "keyed_state", "vector") if stores.get(name)]
    for rail in spec.get("guardrails") or []:
        if rail.get("kind", "regex") != "regex":
            needs.append(f"guardrails kind {rail.get('kind')}")
        if rail.get("check_outputs"):
            needs.append("guardrails check_outputs")
    if spec.get("skills"):
        needs.append("skills")
    if (spec.get("retrieval") or {}).get("vector_store"):
        needs.append("retrieval.vector_store")
    provider = (spec.get("embeddings") or {}).get("provider", "hashing")
    if provider != "hashing":
        needs.append(f"embeddings provider {provider}")
    context = spec.get("context") or {}
    compaction = context.get("compaction", "none")
    if compaction not in ("none", "window"):
        needs.append(f"context compaction {compaction}")
    if "max_tokens" in context:
        needs.append("context max_tokens")
    for name, path in ((spec.get("agent") or {}).get("paths") or {}).items():
        if path.get("brain") == "llm" and path.get("output_schema"):
            needs.append(f"paths.{name}.output_schema")
    return needs + _legacy_cep(spec.get("cep"))


def _legacy_cep(patterns: Any) -> List[str]:
    """The core runs a pattern in the turn when it invokes a tool on plain text conditions. It leaves
    every other action to stream wiring it does not have, so such a rule would never fire."""
    needs = [f"cep on_match {(p.get('on_match') or {}).get('kind', 'none')}" for p in without_tool_actions(patterns)]
    for pattern in patterns or []:
        for stage in pattern.get("pattern") or []:
            for key, value in (stage.get("where") or {}).items():
                if key != "text_contains":
                    needs.append(f"cep where {key}")
                elif not isinstance(value, str):
                    needs.append("cep where text_contains list")
    return needs


class PipelineSystem:
    """A built, deployed agentic system: a backend runtime + the spec that produced it.

    Implements ``submit(Event) -> TurnResult`` so it can sit behind a ``StreamRuntime``
    (the stream path drives the same CEP-bearing submit). On the core, CEP is part of the turn and
    ``cep`` is empty. ``legacy_features`` names what put a system on the legacy engine."""

    def __init__(self, spec: Dict[str, Any], backend, graph, tools, retriever, long_term=None, cep=None,
                 legacy: Optional[List[str]] = None):
        self.spec = spec
        self.backend = backend
        self.graph = graph
        self.tools = tools
        self.retriever = retriever
        self.long_term = long_term  # optional LongTermStore for resumption + fact archive
        # Declarative CEP rules from the spec's ``cep:`` section (empty if none).
        self.cep = list(cep or [])
        self.legacy_features = list(legacy or [])

    @property
    def backend_name(self) -> str:
        return self.backend.name

    def submit(self, event: Any):
        """Run the turn, then feed the inbound event to every CEP rule (which may fire
        tool/submit actions through the inner backend runtime). CEP runs exactly once per
        inbound event, on this path; submit actions tag their derived events so they can't
        recurse."""
        result = self.backend.submit(event)
        for wiring in self.cep:
            wiring.on_event(event, self.backend, self.tools)
        return result

    def stream(self):
        """Drive a live event stream through this system (CEP fires on the same submit
        path). Returns a ``StreamRuntime`` over this system."""
        from pyagentic.stream import StreamRuntime

        return StreamRuntime(self)


def build_system(spec: Dict[str, Any], backend: Optional[str] = None,
                 store_dir: Optional[str] = None) -> PipelineSystem:
    """Compile a spec dict and wire it onto the chosen backend (spec ``backend:`` unless overridden).
    ``store_dir`` puts the event log and the journal on disk. It belongs to the core, as a store
    section the core cannot run belongs to the legacy engine."""
    backends.resolve(backend or spec.get("backend", "local"))
    legacy = legacy_features(spec)
    if legacy:
        if store_dir is not None:
            raise ValueError(f"store_dir needs the core, and this pipeline is on the legacy engine: {legacy}")
        return _build_legacy(spec, backend, legacy)
    core = CoreBackend(ir.validate_document(spec), store_dir)
    return PipelineSystem(spec, core, None, core.tools, None)


def _build_legacy(spec: Dict[str, Any], backend: Optional[str], legacy: List[str]) -> PipelineSystem:
    """``stores.conversation`` hot-swaps the durable store (e.g. Redis/Valkey) behind the
    ConversationStore SPI; ``stores.long_term`` selects the resumption + fact archive (memory or
    postgres) behind the LongTermStore SPI."""
    from pyagentic.cep import compile_cep
    from pyagentic.longterm import make_long_term_store
    from pyagentic.stores import make_conversation_store

    LOG.warning("this pipeline needs %s, so it runs on the legacy engine: no event log, turn_id or journal",
                ", ".join(legacy))
    graph, tools, retriever = compile_deps(spec)
    name = backend or spec.get("backend", "local")
    stores_spec = spec.get("stores") or {}
    store_spec = stores_spec.get("conversation")
    store = make_conversation_store(store_spec) if store_spec else None
    long_term = make_long_term_store(stores_spec.get("long_term")) if stores_spec.get("long_term") else None
    rt = backends.make_backend(name, graph, tools, retriever, store=store)
    cep = compile_cep(spec.get("cep"))
    return PipelineSystem(spec, rt, graph, tools, retriever, long_term=long_term, cep=cep, legacy=legacy)


def load(path: str, backend: Optional[str] = None, store_dir: Optional[str] = None) -> PipelineSystem:
    """Load a ``pipeline.yaml`` and build the system."""
    spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return build_system(spec, backend=backend, store_dir=store_dir)
