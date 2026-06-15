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
from .cep import (
    CepMatcher,
    CepObserver,
    Condition,
    Contiguity,
    Match,
    Pattern,
    Stage,
    any_,
    simple,
)
from .context import ContextItem, ContextWindowManager, Priority
from .guardrails import Guardrail, RegexGuardrail
from .inference import (
    Classification,
    Classifier,
    ClassifierGuardrail,
    ClassifierScorer,
    EmbeddingClassifier,
    LexiconClassifier,
    Scorer,
)
from .mcp_client import McpClient, register_mcp_server
from .web import register_web_tools, web_fetch
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
from .suspend import (
    HumanGate,
    InMemorySuspensionService,
    Suspension,
    SuspensionService,
)
from .replay import EventLog, InMemoryEventLog, replay, replay_until
from .stream import Channel, EventObserver, QueueChannel, SeedChannel, StreamRuntime
from .timers import (
    DurableTimerService,
    InMemoryTimerService,
    Timer,
    TimerService,
)
from .stores import RedisConversationStore, make_conversation_store
from .hnsw import HnswIndex
from .vectorstores import (
    DuckDBVectorStore,
    HnswVectorStore,
    InMemoryVectorStore,
    QdrantVectorStore,
    VectorStore,
    make_vector_store,
)
from .tools import Tool, ToolRegistry
from .windows import (
    Bucket,
    Session,
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
    WindowState,
)

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
    "HumanGate",
    "InMemorySuspensionService",
    "Suspension",
    "SuspensionService",
    "Channel",
    "SeedChannel",
    "QueueChannel",
    "EventObserver",
    "StreamRuntime",
    "EventLog",
    "InMemoryEventLog",
    "replay",
    "replay_until",
    "Timer",
    "TimerService",
    "InMemoryTimerService",
    "DurableTimerService",
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
    "HnswVectorStore",
    "HnswIndex",
    "DuckDBVectorStore",
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
    "register_web_tools",
    "web_fetch",
    "Classification",
    "Classifier",
    "Scorer",
    "LexiconClassifier",
    "EmbeddingClassifier",
    "ClassifierScorer",
    "ClassifierGuardrail",
    "WindowState",
    "Bucket",
    "Session",
    "SlidingWindow",
    "TumblingWindow",
    "SessionWindow",
    "CepMatcher",
    "CepObserver",
    "Condition",
    "Contiguity",
    "Match",
    "Pattern",
    "Stage",
    "any_",
    "simple",
]

__version__ = "0.1.0"
