"""Agentic Flink — Python bindings (JPype thread-mode bridge).

Top-level surface:

- :func:`start_jvm` — boot the JVM once at process start.
- :class:`Agent`, :func:`Agent.builder` — fluent agent definition.
- :class:`ChatSetup`, :func:`langchain4j_ollama` — chat configuration.
- :func:`tool` — decorator that turns a Python function into an agent tool.
- ``flink_state_hnsw``, ``flink_state_brute_force`` — vector memory specs.
- ``SingleOperatorCorpus``, ``BroadcastCorpus``, ``ExternalCorpus`` — corpus
  flavours.
- Channel / web / inference / listener / skill subpackages re-export the
  primitives users actually call.

Every wrapper resolves Java classes lazily — importing this package does not
require a running JVM. Call :func:`start_jvm` before invoking any wrapper.
"""

from ._jvm import (
    JvmNotStartedError,
    is_started,
    jclass,
    shutdown_jvm,
    start_jvm,
)
from .runtime import (
    EmbeddedClusterRuntime,
    EmbeddedJobHandle,
    InProcRuntime,
    Runtime,
    RuntimeModeError,
    SessionRuntime,
    bootstrap,
    embedded,
    from_env,
    inproc,
    session_cluster,
)

# Lazy attribute resolution (PEP 562). Importing this package must not
# require a running JVM; wrappers resolve their Java classes only when used.
_LAZY = {
    "Agent": "agentic_flink.agent",
    "AgentBuilder": "agentic_flink.agent",
    "ChatSetup": "agentic_flink.llm",
    "ChatMessage": "agentic_flink.llm",
    "langchain4j_ollama": "agentic_flink.llm",
    "langchain4j_openai": "agentic_flink.llm",
    "chat": "agentic_flink.llm",
    "tool": "agentic_flink.tools",
    "PythonTool": "agentic_flink.tools",
}


def __getattr__(name):
    if name in _LAZY:
        from importlib import import_module

        return getattr(import_module(_LAZY[name]), name)
    raise AttributeError(f"module 'agentic_flink' has no attribute {name!r}")


__all__ = [
    "JvmNotStartedError",
    "is_started",
    "jclass",
    "shutdown_jvm",
    "start_jvm",
    # Runtime selector — picks between in-JVM, remote session cluster, and
    # embedded MiniCluster modes.
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
    # Agent surface.
    "Agent",
    "AgentBuilder",
    "ChatSetup",
    "ChatMessage",
    "langchain4j_ollama",
    "langchain4j_openai",
    "chat",
    "tool",
    "PythonTool",
]
