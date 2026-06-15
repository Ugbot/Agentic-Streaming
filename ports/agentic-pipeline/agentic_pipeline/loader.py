"""YAML pipeline loader — parse a ``pipeline.yaml``, build the agentic system with the
core GraphBuilder, and select the backend. The result is a runnable system: an object
with ``submit(Event) -> TurnResult`` on the chosen backend.

    from agentic_pipeline.loader import load
    system = load("banking.yaml")          # backend chosen by the YAML
    print(system.submit(Event("c1", "what is my balance?")).reply)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

_PORTS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PORTS / "pyagentic"))

from pyagentic import builder  # noqa: E402
from pyagentic.core import Event  # noqa: E402

from . import backends  # noqa: E402


def _chat_client_factory(llm_spec: Dict[str, Any]):
    """Build a ChatClient from the ``llm:`` section. ``stub`` needs an inline ``script``
    of {tool|text, args} steps (handy for offline tests / deterministic demos)."""
    from pyagentic.llm import ChatResult, OllamaChatClient, OpenAIChatClient, StubChatClient

    provider = (llm_spec or {}).get("provider", "ollama")
    if provider == "ollama":
        return OllamaChatClient(model=llm_spec.get("model", "qwen2.5:3b"), base_url=llm_spec.get("base_url"))
    if provider == "openai":
        return OpenAIChatClient(model=llm_spec.get("model", "gpt-5.4-mini"), base_url=llm_spec.get("base_url"))
    if provider == "stub":
        script = [
            ChatResult(tool=s["tool"], args=s.get("args", {})) if s.get("tool") else ChatResult(text=s.get("text", ""))
            for s in llm_spec.get("script", [{"text": "ok"}])
        ]
        return StubChatClient(script)
    raise ValueError(f"unknown llm provider {provider!r}")


class PipelineSystem:
    """A built, deployed agentic system: a backend runtime + the spec that produced it.

    Implements ``submit(Event) -> TurnResult`` so it can sit behind a ``StreamRuntime``
    (the stream path drives the same CEP-bearing submit)."""

    def __init__(self, spec: Dict[str, Any], backend, graph, tools, retriever, long_term=None, cep=None):
        self.spec = spec
        self.backend = backend
        self.graph = graph
        self.tools = tools
        self.retriever = retriever
        self.long_term = long_term  # optional LongTermStore for resumption + fact archive
        # Declarative CEP rules from the spec's ``cep:`` section (empty if none).
        self.cep = list(cep or [])

    @property
    def backend_name(self) -> str:
        return self.backend.name

    def submit(self, event: Event):
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


def build_system(spec: Dict[str, Any], backend: Optional[str] = None) -> PipelineSystem:
    """Compile a spec dict and wire it onto the chosen backend (spec ``backend:`` unless
    overridden). A ``stores.conversation`` section hot-swaps the durable store (e.g.
    Redis/Valkey) behind the ConversationStore SPI; ``stores.long_term`` selects the
    resumption + fact archive (memory/postgres) behind the LongTermStore SPI."""
    from pyagentic.cep import compile_cep
    from pyagentic.longterm import make_long_term_store
    from pyagentic.stores import make_conversation_store

    graph, tools, retriever = builder.build(spec, chat_client_factory=_chat_client_factory)
    name = backend or spec.get("backend", "local")
    stores_spec = spec.get("stores") or {}
    store_spec = stores_spec.get("conversation")
    store = make_conversation_store(store_spec) if store_spec else None
    long_term = make_long_term_store(stores_spec.get("long_term")) if stores_spec.get("long_term") else None
    rt = backends.make_backend(name, graph, tools, retriever, store=store)
    cep = compile_cep(spec.get("cep"))
    return PipelineSystem(spec, rt, graph, tools, retriever, long_term=long_term, cep=cep)


def load(path: str, backend: Optional[str] = None) -> PipelineSystem:
    """Load a ``pipeline.yaml`` and build the system."""
    spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return build_system(spec, backend=backend)
