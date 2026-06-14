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
from .embeddings import Embedder, HashingEmbedder, LiteLLMEmbedder, make_embedder
from .a2a import A2AClient, peer_tool
from .context import ContextItem, ContextWindowManager, Priority
from .guardrails import Guardrail, RegexGuardrail
from .mcp_client import McpClient, register_mcp_server
from .saga import CompensationAction, Saga
from .listeners import AgentListener, CompositeListener, LoggingListener, MetricsListener
from .skills import Skill, SkillRegistry
from .structured import parse_structured, schema_instruction, validate
from .llm import (
    ChatClient,
    ChatResult,
    LiteLLMChatClient,
    LlmBrain,
    OllamaChatClient,
    OpenAIChatClient,
    StubChatClient,
)
from .longterm import (
    InMemoryLongTermStore,
    LongTermStore,
    PostgresLongTermStore,
    make_long_term_store,
)
from .runtime import LocalRuntime, Runtime
from .stores import RedisConversationStore, make_conversation_store
from .vectorstores import (
    InMemoryVectorStore,
    QdrantVectorStore,
    VectorStore,
    make_vector_store,
)
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
    "Guardrail",
    "RegexGuardrail",
    "AgentListener",
    "LoggingListener",
    "MetricsListener",
    "RedisConversationStore",
    "make_conversation_store",
    "Embedder",
    "HashingEmbedder",
    "LiteLLMEmbedder",
    "make_embedder",
    "LiteLLMChatClient",
    "CompositeListener",
    "Skill",
    "SkillRegistry",
    "validate",
    "parse_structured",
    "schema_instruction",
    "VectorStore",
    "InMemoryVectorStore",
    "QdrantVectorStore",
    "make_vector_store",
    "LongTermStore",
    "InMemoryLongTermStore",
    "PostgresLongTermStore",
    "make_long_term_store",
    "Saga",
    "CompensationAction",
    "ContextItem",
    "ContextWindowManager",
    "Priority",
    "A2AClient",
    "peer_tool",
    "McpClient",
    "register_mcp_server",
]

__version__ = "0.1.0"
