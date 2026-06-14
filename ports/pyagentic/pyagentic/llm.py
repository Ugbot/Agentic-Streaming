"""LLM brain SPI — the portable analogue of the Flink ``ChatConnection`` +
``ReActProcessFunction``.

A ``ChatClient`` turns (system prompt + transcript + available tools) into either a
final answer or a tool call. ``LlmBrain`` is a ``Brain`` that runs a bounded ReAct loop
over a ``ChatClient`` (thought → tool → observation → final). The model-free
``RuleBrain`` stays the default everywhere; ``LlmBrain`` is opt-in so the *full* agentic
workflow (LLM + tools) runs on any backend.

Providers are dependency-free (urllib + json): ``OllamaChatClient`` and
``OpenAIChatClient`` use a JSON-mode protocol — the model must reply with
``{"tool": "...", "args": {...}}`` to call a tool or ``{"text": "..."}`` to answer — so
no provider-specific function-calling API is needed. ``StubChatClient`` is a scripted,
deterministic client for offline tests.
"""

from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

from .core import AgentContext


@dataclass
class ChatResult:
    """One model step: a final ``text`` answer, or a ``tool`` call with ``args``."""

    text: Optional[str] = None
    tool: Optional[str] = None
    args: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_tool_call(self) -> bool:
        return bool(self.tool)


class ChatClient(Protocol):
    def chat(self, messages: List[Dict[str, str]], tools: List[Dict[str, str]]) -> ChatResult: ...


_REACT_SYSTEM = (
    "You are a tool-using agent. On each step reply with ONE JSON object and nothing "
    'else. To call a tool: {"tool": "<name>", "args": {<json args>}}. To give the final '
    'answer: {"text": "<answer>"}. Available tools: '
)


def _parse_chat_json(content: str) -> ChatResult:
    """Parse a model reply that should be one JSON object; tolerate code fences/prose."""
    s = content.strip()
    start, end = s.find("{"), s.rfind("}")
    if start != -1 and end > start:
        try:
            obj = json.loads(s[start : end + 1])
            if isinstance(obj, dict):
                if obj.get("tool"):
                    return ChatResult(tool=str(obj["tool"]), args=dict(obj.get("args") or {}))
                if "text" in obj:
                    return ChatResult(text=str(obj["text"]))
        except json.JSONDecodeError:
            pass
    return ChatResult(text=content.strip())  # treat freeform as the final answer


class LlmBrain:
    """A ``Brain`` that drives a bounded ReAct loop over a ``ChatClient``."""

    def __init__(
        self,
        chat_client: ChatClient,
        name: str = "agent",
        system_prompt: str = "",
        tools: Optional[List[str]] = None,
        max_iterations: int = 6,
        output_schema: Optional[Dict[str, Any]] = None,
        context_manager: Optional[Any] = None,
    ) -> None:
        self.chat_client = chat_client
        self.name = name
        self.system_prompt = system_prompt
        self.allowed_tools = tools  # None => all registered tools
        self.max_iterations = max(1, max_iterations)
        self.output_schema = output_schema  # optional structured-output contract
        self.context_manager = context_manager  # optional ContextWindowManager for transcript compaction

    def turn(self, user_text: str, ctx: AgentContext) -> str:
        specs = [s for s in ctx.tools.specs() if self.allowed_tools is None or s["name"] in self.allowed_tools]
        sys_prompt = (self.system_prompt + "\n" + _REACT_SYSTEM + json.dumps(specs)).strip()
        if self.output_schema is not None:
            from .structured import schema_instruction
            sys_prompt += "\n" + schema_instruction(self.output_schema)
        messages: List[Dict[str, str]] = [{"role": "system", "content": sys_prompt}]
        # The agent already appended the user turn; replay the persisted transcript,
        # compacting it to the token budget (MoSCoW) if a ContextWindowManager is set.
        transcript = list(ctx.store.history(ctx.conversation_id))
        transcript = self._compact(transcript)
        for m in transcript:
            messages.append({"role": m.role, "content": m.content})
        if not messages or messages[-1]["content"] != user_text:
            messages.append({"role": "user", "content": user_text})

        for _ in range(self.max_iterations):
            result = self.chat_client.chat(messages, specs)
            if result.is_tool_call:
                observation = ctx.call_tool(result.tool, result.args)
                messages.append({"role": "assistant", "content": json.dumps({"tool": result.tool, "args": result.args})})
                messages.append({"role": "tool", "content": str(observation)})
                continue
            return self._finalize(result.text, ctx)
        return f"[{self.name}] (stopped after {self.max_iterations} steps)"

    def _compact(self, transcript):
        """Bound the replayed transcript with the ContextWindowManager if configured.
        The most-recent turns are MUST-keep, older ones SHOULD, the oldest COULD — so the
        compactor drops from the tail of history first when over budget."""
        if self.context_manager is None or not transcript:
            return transcript
        from .context import ContextItem, Priority

        n = len(transcript)
        items = []
        for i, m in enumerate(transcript):
            # rank by recency: last 2 turns MUST, next 4 SHOULD, rest COULD
            age = n - 1 - i
            if age < 2:
                prio = Priority.MUST
            elif age < 6:
                prio = Priority.SHOULD
            else:
                prio = Priority.COULD
            items.append(ContextItem(text=f"{m.role}: {m.content}", priority=prio))
        kept = self.context_manager.compact(items)
        kept_texts = {it.text for it in kept}
        return [m for i, m in enumerate(transcript)
                if f"{m.role}: {m.content}" in kept_texts]

    def _finalize(self, text: Optional[str], ctx: AgentContext) -> str:
        if not text:
            return f"[{self.name}] (no answer)"
        if self.output_schema is not None:
            from .structured import parse_structured
            obj, errors = parse_structured(text, self.output_schema)
            if errors:
                for ln in getattr(ctx, "listeners", []):
                    fn = getattr(ln, "on_error", None)
                    if callable(fn):
                        fn("output_schema", ValueError("; ".join(errors)), ctx)
            elif obj is not None:
                return f"[{self.name}] {json.dumps(obj)}"
        return f"[{self.name}] {text}"


class StubChatClient:
    """Deterministic, scripted ``ChatClient`` for offline tests: returns the next
    ``ChatResult`` from ``script`` each call (repeating the last once exhausted)."""

    def __init__(self, script: List[ChatResult]) -> None:
        if not script:
            raise ValueError("StubChatClient needs at least one scripted ChatResult")
        self._script = list(script)
        self._i = 0

    def chat(self, messages: List[Dict[str, str]], tools: List[Dict[str, str]]) -> ChatResult:
        result = self._script[min(self._i, len(self._script) - 1)]
        self._i += 1
        return result


class _HttpChatClient:
    """Shared JSON-mode HTTP chat client. Subclasses set the URL + request shape."""

    def __init__(self, model: str, url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 60.0):
        self.model = model
        self.url = url
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.timeout = timeout

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        req = urllib.request.Request(
            self.url, data=json.dumps(payload).encode("utf-8"), headers=self.headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))


class OllamaChatClient(_HttpChatClient):
    """Talks to a local Ollama ``/api/chat`` endpoint (JSON mode)."""

    def __init__(self, model: str = "qwen2.5:3b", base_url: Optional[str] = None, timeout: float = 60.0):
        base = (base_url or os.environ.get("AGENTIC_OLLAMA_URL") or "http://localhost:11434").rstrip("/")
        super().__init__(model, base + "/api/chat", timeout=timeout)

    def chat(self, messages: List[Dict[str, str]], tools: List[Dict[str, str]]) -> ChatResult:
        data = self._post({"model": self.model, "messages": messages, "stream": False, "format": "json"})
        return _parse_chat_json((data.get("message") or {}).get("content", ""))


class LiteLLMChatClient:
    """Real chat via `litellm` — one API across OpenAI / Anthropic / Ollama / Gemini /
    etc. Uses the same JSON-mode ReAct protocol as the rest of the framework
    (``{"tool": ...}`` / ``{"text": ...}``), so ``LlmBrain`` works unchanged. ``model``
    is a litellm model string, e.g. ``ollama/llama3.2`` or ``gpt-4o-mini`` or
    ``anthropic/claude-3-5-haiku-latest``."""

    def __init__(self, model: str = "ollama/llama3.2", api_base: Optional[str] = None,
                 json_mode: bool = True, **kwargs) -> None:
        import litellm  # opt-in heavy dep

        self._litellm = litellm
        self.model = model
        self.json_mode = json_mode
        self._kwargs = dict(kwargs)
        if api_base:
            self._kwargs["api_base"] = api_base

    def chat(self, messages: List[Dict[str, str]], tools: List[Dict[str, str]]) -> ChatResult:
        kwargs = dict(self._kwargs)
        if self.json_mode:
            kwargs.setdefault("response_format", {"type": "json_object"})
        try:
            resp = self._litellm.completion(model=self.model, messages=messages, **kwargs)
        except Exception:
            # Some providers reject response_format; retry once without it.
            kwargs.pop("response_format", None)
            resp = self._litellm.completion(model=self.model, messages=messages, **kwargs)
        choice = resp["choices"][0] if isinstance(resp, dict) else resp.choices[0]
        msg = choice["message"] if isinstance(choice, dict) else choice.message
        content = (msg["content"] if isinstance(msg, dict) else msg.content) or ""
        return _parse_chat_json(content)


class OpenAIChatClient(_HttpChatClient):
    """Talks to the OpenAI (or compatible) ``/chat/completions`` endpoint (JSON mode)."""

    def __init__(self, model: str = "gpt-5.4-mini", api_key: Optional[str] = None, base_url: Optional[str] = None, timeout: float = 60.0):
        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY not set")
        base = (base_url or os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        super().__init__(model, base + "/chat/completions", headers={"Authorization": f"Bearer {key}"}, timeout=timeout)

    def chat(self, messages: List[Dict[str, str]], tools: List[Dict[str, str]]) -> ChatResult:
        data = self._post(
            {"model": self.model, "messages": messages, "response_format": {"type": "json_object"}}
        )
        content = data["choices"][0]["message"]["content"]
        return _parse_chat_json(content)
