"""Real model clients for `brain: llm`, and the JSON protocol they share.

The model replies with one JSON object, `{"tool": "<id>", "args": {...}}` to call a tool or
`{"text": "<answer>"}` to answer, so no provider's function-calling API is needed. The loop itself
is in the engine, where every call goes through the journal; a client only turns messages into one
answer and says how it failed: `TransientError` for what may pass, `ModelRefused` for what the
provider will repeat.

Dependency-free: urllib and json. Keys come from the environment and never from the workflow
document, which travels with the job and lands in logs.
"""

from __future__ import annotations

import http.client
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Mapping, Optional, Protocol, Tuple

from . import egress
from .errors import AgenticError, OutcomeUnknown, TransientError, ValidationError
from .tools import bounded

DEFAULT_LLM_TIMEOUT_MS = 60_000
DEFAULT_MAX_ITERATIONS = 6
PROVIDERS = ("anthropic", "gemini", "ollama", "openai", "stub")
# The Claude model a document that names none gets, as on the JVM (`AnthropicChatClient.DEFAULT_MODEL`).
DEFAULT_CLAUDE_MODEL = "claude-sonnet-5-5"
DEFAULT_CLAUDE_MAX_TOKENS = 16_000
ANTHROPIC_VERSION = "2023-06-01"

REACT_SYSTEM = (
    "You are a tool-using agent. On each step reply with ONE JSON object and nothing "
    'else. To call a tool: {"tool": "<name>", "args": {<json args>}}. To give the final '
    'answer: {"text": "<answer>"}. Available tools: '
)

_OPENAI_REASONING = re.compile(r"^(o\d.*|gpt-([5-9]|[1-9]\d).*)$")
_SECRET_KEY = re.compile(r"(?i).*(api[_-]?key|secret|password|credential|auth|bearer|access[_-]?token).*")
_ERROR_BODY_LIMIT = 200

Answer = Dict[str, Any]
Messages = List[Dict[str, str]]


class ModelRefused(AgenticError):
    """A failure the provider will repeat, such as a refused request or a rejected key. Not
    retried. Error class `fatal`."""


class ChatClient(Protocol):
    model: Optional[str]

    def chat(self, messages: Messages, tools: List[Dict[str, str]]) -> Answer: ...


def is_openai_reasoning(model: Optional[str]) -> bool:
    """True for OpenAI models that take `max_completion_tokens` and refuse a custom `temperature`.
    Written so an id this code has never seen gets what newer models need: an unsent temperature is
    a default, a sent one is a 400 on every call."""
    return model is not None and _OPENAI_REASONING.match(model.lower()) is not None


def parse_chat_json(content: Optional[str]) -> Answer:
    """One model reply as `{"tool", "args"}` or `{"text"}`, tolerating fences and prose around the
    object. A reply that holds neither is taken as the answer itself."""
    stripped = (content or "").strip()
    start, end = stripped.find("{"), stripped.rfind("}")
    if start != -1 and end > start:
        try:
            parsed = json.loads(stripped[start:end + 1])
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            if parsed.get("tool"):
                args = parsed.get("args")
                return {"tool": str(parsed["tool"]), "args": dict(args) if isinstance(args, dict) else {}}
            if "text" in parsed:
                return {"text": str(parsed["text"])}
    return {"text": stripped}


def is_transient_status(status: int) -> bool:
    """Worth asking again: timeouts, rate limits and server errors, not a refused request."""
    return status in (408, 425, 429) or status >= 500


class _HttpChatClient:
    """One POST per call. `timeout_ms` bounds the whole call, not each socket read, so a server
    that keeps a connection alive with a trickle of bytes cannot hold a turn."""

    def __init__(self, model: str, url: str, timeout_ms: int, headers: Optional[Mapping[str, str]] = None) -> None:
        self.model: Optional[str] = model
        self.url = url
        self.timeout_ms = timeout_ms
        self.headers = {"Content-Type": "application/json", **(headers or {})}

    def _body(self, messages: Messages) -> Dict[str, Any]:
        raise NotImplementedError

    def _content(self, response: Mapping[str, Any]) -> str:
        raise NotImplementedError

    def chat(self, messages: Messages, tools: List[Dict[str, str]]) -> Answer:
        try:
            return bounded("model", self.timeout_ms, lambda: self._post(messages))
        except OutcomeUnknown as exc:
            raise TransientError(str(exc)) from exc

    def _post(self, messages: Messages) -> Answer:
        try:
            pin = egress.check(self.url)
        except egress.EgressBlocked as exc:
            raise ModelRefused(f"model call blocked by egress policy: {exc}") from exc
        request = urllib.request.Request(self.url, data=json.dumps(self._body(messages)).encode("utf-8"),
                                         headers=self.headers, method="POST")
        try:
            with egress.urlopen(request, self.timeout_ms / 1000.0, pin) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = self._without_credentials(exc.read().decode("utf-8", "replace"))[:_ERROR_BODY_LIMIT]
            failure = TransientError if is_transient_status(exc.code) else ModelRefused
            raise failure(f"model call failed: HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            raise TransientError(f"model call failed: {exc}") from exc
        try:
            return parse_chat_json(self._content(json.loads(raw)))
        except ModelRefused:
            raise
        except (ValueError, KeyError, IndexError, TypeError, AttributeError) as exc:
            raise TransientError(f"model answered with something unreadable: {type(exc).__name__}") from exc

    def _without_credentials(self, body: str) -> str:
        """An error body ends up on the turn and in logs, and a provider may echo what it was sent."""
        for name, value in self.headers.items():
            if name.lower() == "content-type" or not value:
                continue
            body = body.replace(value, "[redacted]")
            if value.startswith("Bearer "):
                body = body.replace(value[len("Bearer "):], "[redacted]")
        return body


def _split_system(messages: Messages) -> Tuple[str, Messages]:
    """The system messages joined, and the rest: Anthropic and Gemini have no system role in the turns."""
    system = "\n".join(m["content"] for m in messages if m["role"] == "system")
    return system, [m for m in messages if m["role"] != "system"]


class OpenAIChatClient(_HttpChatClient):
    """The OpenAI `/chat/completions` format, which most compatible servers also speak."""

    def __init__(self, model: str, base_url: Optional[str], timeout_ms: int, api_key: str,
                 temperature: Optional[float] = None, max_tokens: Optional[int] = None) -> None:
        base = (base_url or os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        super().__init__(model, base + "/chat/completions", timeout_ms, {"Authorization": f"Bearer {api_key}"})
        self.temperature = temperature
        self.max_tokens = max_tokens

    def _body(self, messages: Messages) -> Dict[str, Any]:
        body: Dict[str, Any] = {"model": self.model, "messages": messages,
                                "response_format": {"type": "json_object"}}
        reasoning = is_openai_reasoning(self.model)
        if self.temperature is not None and not reasoning:
            body["temperature"] = self.temperature
        if self.max_tokens is not None:
            body["max_completion_tokens" if reasoning else "max_tokens"] = self.max_tokens
        return body

    def _content(self, response: Mapping[str, Any]) -> str:
        return str(response["choices"][0]["message"]["content"])


class OllamaChatClient(_HttpChatClient):
    """A local Ollama `/api/chat` endpoint in JSON mode."""

    def __init__(self, model: str, base_url: Optional[str], timeout_ms: int,
                 temperature: Optional[float] = None, max_tokens: Optional[int] = None) -> None:
        base = (base_url or os.environ.get("AGENTIC_OLLAMA_URL") or "http://localhost:11434").rstrip("/")
        super().__init__(model, base + "/api/chat", timeout_ms)
        self.temperature = temperature
        self.max_tokens = max_tokens

    def _body(self, messages: Messages) -> Dict[str, Any]:
        body: Dict[str, Any] = {"model": self.model, "messages": messages, "stream": False, "format": "json"}
        options: Dict[str, Any] = {}
        if self.temperature is not None:
            options["temperature"] = self.temperature
        if self.max_tokens is not None:
            options["num_predict"] = self.max_tokens
        if options:
            body["options"] = options
        return body

    def _content(self, response: Mapping[str, Any]) -> str:
        return str(response["message"]["content"])


class AnthropicChatClient(_HttpChatClient):
    """Claude, on Anthropic's Messages API. The key travels in `x-api-key`. A sampling setting is sent
    only when the document declares one, since current models refuse a non-default one."""

    def __init__(self, model: str, base_url: Optional[str], timeout_ms: int, api_key: str,
                 temperature: Optional[float] = None, max_tokens: Optional[int] = None) -> None:
        base = (base_url or os.environ.get("ANTHROPIC_BASE_URL") or "https://api.anthropic.com").rstrip("/")
        super().__init__(model, base + "/v1/messages", timeout_ms,
                         {"x-api-key": api_key, "anthropic-version": ANTHROPIC_VERSION})
        self.temperature = temperature
        self.max_tokens = DEFAULT_CLAUDE_MAX_TOKENS if max_tokens is None else max_tokens

    def _body(self, messages: Messages) -> Dict[str, Any]:
        system, turns = _split_system(messages)
        body: Dict[str, Any] = {"model": self.model, "max_tokens": self.max_tokens}
        if system:
            body["system"] = system
        body["messages"] = [{"role": m["role"], "content": m["content"]} for m in turns]
        if self.temperature is not None:
            body["temperature"] = self.temperature
        return body

    def _content(self, response: Mapping[str, Any]) -> str:
        """A refusal arrives as a 200 and is the same next time, so it is not retried."""
        if response.get("stop_reason") == "refusal":
            raise ModelRefused("the model refused the request")
        if not isinstance(response.get("content"), list):
            raise KeyError("content")
        return "".join(str(b.get("text", "")) for b in response["content"] if b.get("type") == "text")


class GeminiChatClient(_HttpChatClient):
    """Gemini's `generateContent`. The key travels in `x-goog-api-key`, not the url, so it stays out of
    access logs. A document must name its model: this code does not guess one."""

    def __init__(self, model: str, base_url: Optional[str], timeout_ms: int, api_key: str,
                 temperature: Optional[float] = None, max_tokens: Optional[int] = None) -> None:
        base = (base_url or os.environ.get("GEMINI_BASE_URL")
                or "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
        url = f"{base}/models/{urllib.parse.quote(model, safe='')}:generateContent"
        super().__init__(model, url, timeout_ms, {"x-goog-api-key": api_key})
        self.temperature = temperature
        self.max_tokens = max_tokens

    def _body(self, messages: Messages) -> Dict[str, Any]:
        system, turns = _split_system(messages)
        config: Dict[str, Any] = {"responseMimeType": "application/json"}
        if self.temperature is not None:
            config["temperature"] = self.temperature
        if self.max_tokens is not None:
            config["maxOutputTokens"] = self.max_tokens
        body: Dict[str, Any] = {}
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        body["contents"] = [{"role": "model" if m["role"] == "assistant" else "user", "parts": [{"text": m["content"]}]}
                            for m in turns]
        body["generationConfig"] = config
        return body

    def _content(self, response: Mapping[str, Any]) -> str:
        """A blocked prompt or answer arrives as a 200 and is the same next time, so it is not retried."""
        blocked = (response.get("promptFeedback") or {}).get("blockReason")
        if blocked:
            raise ModelRefused(f"the model refused the request: {blocked}")
        candidate = response["candidates"][0]
        if candidate.get("finishReason") == "SAFETY":
            raise ModelRefused("the model refused the request: SAFETY")
        return "".join(str(p.get("text", "")) for p in (candidate.get("content") or {}).get("parts") or [])


_KEYS = {"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY", "gemini": "GEMINI_API_KEY"}
_CLIENTS = {"openai": OpenAIChatClient, "anthropic": AnthropicChatClient, "gemini": GeminiChatClient}


def make_chat_client(llm: Mapping[str, Any]) -> ChatClient:
    """The client an `llm:` block names, or a `ValidationError` before any turn runs. The message
    names an offending field and never its value."""
    for key in llm:
        if _SECRET_KEY.match(str(key)):
            raise ValidationError(f"llm.{key} looks like a credential; a workflow document must not carry one. "
                                  f"Set it in the environment of the process running the model call", "/llm")
    provider = llm.get("provider", "stub")
    if provider not in PROVIDERS or provider == "stub":
        raise ValidationError(f"unknown llm provider {provider!r}; supported providers: {', '.join(PROVIDERS)}",
                              "/llm/provider")
    model = str(llm.get("model") or "").strip()
    if not model and provider == "anthropic":
        model = DEFAULT_CLAUDE_MODEL
    if not model:
        raise ValidationError(f"llm.model is required for provider {provider!r}; name the model you have deployed",
                              "/llm/model")
    timeout_ms = int(llm.get("timeout_ms", DEFAULT_LLM_TIMEOUT_MS))
    settings = {"temperature": llm.get("temperature"), "max_tokens": llm.get("max_tokens")}
    if provider == "ollama":
        return OllamaChatClient(model, llm.get("base_url"), timeout_ms, **settings)
    variable = _KEYS[provider]
    key = os.environ.get(variable)
    if not key:
        raise ValidationError(f"{variable} is not set; provider {provider} reads its key from the environment",
                              "/llm/provider")
    return _CLIENTS[provider](model, llm.get("base_url"), timeout_ms, key, **settings)
