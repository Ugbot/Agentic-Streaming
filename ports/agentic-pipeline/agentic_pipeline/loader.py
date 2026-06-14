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
    """A built, deployed agentic system: a backend runtime + the spec that produced it."""

    def __init__(self, spec: Dict[str, Any], backend, graph, tools, retriever):
        self.spec = spec
        self.backend = backend
        self.graph = graph
        self.tools = tools
        self.retriever = retriever

    @property
    def backend_name(self) -> str:
        return self.backend.name

    def submit(self, event: Event):
        return self.backend.submit(event)


def build_system(spec: Dict[str, Any], backend: Optional[str] = None) -> PipelineSystem:
    """Compile a spec dict and wire it onto the chosen backend (spec ``backend:`` unless
    overridden)."""
    graph, tools, retriever = builder.build(spec, chat_client_factory=_chat_client_factory)
    name = backend or spec.get("backend", "local")
    rt = backends.make_backend(name, graph, tools, retriever)
    return PipelineSystem(spec, rt, graph, tools, retriever)


def load(path: str, backend: Optional[str] = None) -> PipelineSystem:
    """Load a ``pipeline.yaml`` and build the system."""
    spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return build_system(spec, backend=backend)
