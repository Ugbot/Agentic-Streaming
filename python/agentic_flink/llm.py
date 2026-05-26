"""Chat-model wrappers.

Surfaces the Java :class:`org.agentic.flink.llm.ChatConnection` /
:class:`ChatSetup` / :class:`ChatMessage` / :class:`OutputSchema` types as
Pythonic dataclasses + factory helpers. Every Python value carries a
:meth:`_to_java` for boundary crossing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

from ._jvm import jclass


@dataclass(frozen=True)
class ChatSetup:
    """Per-agent chat configuration. Maps onto ``org.agentic.flink.llm.ChatSetup``."""

    model: str
    temperature: float = 0.7
    max_response_tokens: int = 1000
    stop_sequences: tuple[str, ...] = ()
    seed: Optional[int] = None

    def _to_java(self):
        ChatSetupJ = jclass("org.agentic.flink.llm.ChatSetup")
        b = (
            ChatSetupJ.builder()
            .withModel(self.model)
            .withTemperature(self.temperature)
            .withMaxResponseTokens(self.max_response_tokens)
        )
        if self.stop_sequences:
            ArrayList = jclass("java.util.ArrayList")
            stops = ArrayList()
            for s in self.stop_sequences:
                stops.add(s)
            b = b.withStopSequences(stops)
        if self.seed is not None:
            # Java's withSeed expects Long (boxed); JPype won't auto-box `int` for
            # an overload that takes only Long, so wrap explicitly.
            JLong = jclass("java.lang.Long")
            b = b.withSeed(JLong(int(self.seed)))
        return b.build()


@dataclass(frozen=True)
class ChatMessage:
    """A single chat message. Maps onto ``org.agentic.flink.llm.ChatMessage``."""

    role: str  # one of "system" / "user" / "assistant" / "tool"
    content: str
    tool_call_id: Optional[str] = None
    tool_name: Optional[str] = None

    def _to_java(self):
        CM = jclass("org.agentic.flink.llm.ChatMessage")
        role = (self.role or "").lower()
        if role == "system":
            return CM.system(self.content)
        if role == "assistant":
            return CM.assistant(self.content)
        if role == "tool":
            return CM.tool(self.tool_call_id or "", self.tool_name or "", self.content)
        return CM.user(self.content)


def langchain4j_ollama(base_url: str = "http://localhost:11434"):
    """Build a :class:`LangChain4jChatConnection` configured for an Ollama service."""
    Conn = jclass("org.agentic.flink.llm.langchain4j.LangChain4jChatConnection")
    return Conn.ollama(base_url)


def langchain4j_openai(api_key: str):
    """Build a :class:`LangChain4jChatConnection` configured for OpenAI."""
    Conn = jclass("org.agentic.flink.llm.langchain4j.LangChain4jChatConnection")
    return Conn.openai(api_key)


def chat(connection, messages: Iterable[ChatMessage], setup: ChatSetup):
    """One-shot blocking chat call. Returns the response as a Python dict
    ``{text, model, tokens_used, finish_reason}`` for convenience."""
    ArrayList = jclass("java.util.ArrayList")
    msgs = ArrayList()
    for m in messages:
        msgs.add(m._to_java())

    client = connection.bind(None)
    response = client.chat(msgs, setup._to_java())
    return {
        "text": str(response.getText()),
        "model": str(response.getModelName()) if response.getModelName() else None,
        "tokens_used": (
            int(response.getTokensUsed()) if response.getTokensUsed() is not None else None
        ),
        "finish_reason": str(response.getFinishReason()),
    }


__all__ = [
    "ChatSetup",
    "ChatMessage",
    "langchain4j_ollama",
    "langchain4j_openai",
    "chat",
]
