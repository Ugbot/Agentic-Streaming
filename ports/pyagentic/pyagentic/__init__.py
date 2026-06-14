"""pyagentic — the pure-Python, engine-agnostic essence of Agentic-Flink.

A faithful, dependency-free distillation of the project's core abstractions
(see ../../docs/portability/00-essence-and-core-abstractions.md): per-conversation
memory, keyed state, tools, the ReAct-style turn, the router->path->verifier routed
graph, and hot+cold retrieval — plus a LocalRuntime. Engine adapters (Faust, Ray,
Dask, Airflow) import this core and provide their own Runtime/state backing.
"""

from __future__ import annotations

from .core import Agent, AgentContext, Event, RoutedGraph, TurnResult
from .memory import (
    ChatMessage,
    ConversationStore,
    InMemoryConversationStore,
    InMemoryKeyedStateStore,
    KeyedStateStore,
)
from .retrieval import (
    InMemoryHotVectorIndex,
    Scored,
    TwoTierRetriever,
    cosine,
    hashing_embedder,
)
from .llm import (
    ChatClient,
    ChatResult,
    LlmBrain,
    OllamaChatClient,
    OpenAIChatClient,
    StubChatClient,
)
from .runtime import LocalRuntime, Runtime
from .tools import Tool, ToolRegistry

__all__ = [
    "Agent",
    "AgentContext",
    "Event",
    "RoutedGraph",
    "TurnResult",
    "ChatMessage",
    "ConversationStore",
    "InMemoryConversationStore",
    "InMemoryKeyedStateStore",
    "KeyedStateStore",
    "InMemoryHotVectorIndex",
    "Scored",
    "TwoTierRetriever",
    "cosine",
    "hashing_embedder",
    "LocalRuntime",
    "Runtime",
    "Tool",
    "ToolRegistry",
    "ChatClient",
    "ChatResult",
    "LlmBrain",
    "StubChatClient",
    "OllamaChatClient",
    "OpenAIChatClient",
]

__version__ = "0.1.0"
