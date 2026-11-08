"""A2A (Agent-to-Agent) client — call a peer agent's HTTP gateway as a tool, with the
Agent Card discovery + retries. ``peer_tool`` turns a peer into a ``ToolRegistry`` tool so
a path can delegate to another agent (on any backend).
"""

from __future__ import annotations

import json
import time
import urllib.request
from typing import Any, Callable, Dict, Optional


from agentic import egress
from agentic.secret_refs import resolve


class A2AClient:
    """Minimal A2A client over a peer gateway's HTTP surface (``/agent`` +
    ``/.well-known/agent-card.json``), with bounded retries + backoff."""

    def __init__(self, base_url: str, retries: int = 2, backoff: float = 0.2, timeout: float = 30.0,
                 bearer: Optional[str] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.bearer = bearer  # a secret reference, resolved on each call
        self.retries = max(0, retries)
        self.backoff = backoff
        self.timeout = timeout

    def _get(self, path: str) -> Dict[str, Any]:
        pin = egress.check(self.base_url + path)
        with egress.urlopen(urllib.request.Request(self.base_url + path), self.timeout, pin) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.bearer is not None:
            headers["Authorization"] = f"Bearer {resolve(self.bearer)}"
        return headers

    def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        pin = egress.check(self.base_url + path)
        last: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                req = urllib.request.Request(
                    self.base_url + path, data=json.dumps(body).encode("utf-8"),
                    headers=self._headers(), method="POST")
                # No redirect is followed: urllib would carry the bearer to wherever it pointed.
                with egress.urlopen(req, self.timeout, pin) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as exc:  # transient network error
                last = exc
                if attempt < self.retries:
                    time.sleep(self.backoff * (2 ** attempt))
        raise RuntimeError(f"A2A call to {self.base_url}{path} failed after {self.retries + 1} tries: {last}")

    def card(self) -> Dict[str, Any]:
        return self._get("/.well-known/agent-card.json")

    def send(self, conversation_id: str, text: str, user_id: str = "anonymous") -> Dict[str, Any]:
        # A peer's front door refuses a turn with no text (400), so say so here and send nothing.
        if not isinstance(text, str) or not text.strip():
            raise ValueError("an A2A turn needs text")
        return self._post("/agent", {"conversation_id": conversation_id, "text": text, "user_id": user_id})


def _turn_text(params: Dict[str, Any]) -> str:
    """The text handed to the peer: ``text`` when the caller gave one, otherwise the call's
    arguments as JSON, so a tool trigger that passes only ``{user}`` still sends a turn."""
    text = params.get("text")
    if isinstance(text, str) and text.strip():
        return text
    return json.dumps(params, sort_keys=True, default=str)


def peer_tool(base_url: str, **client_kwargs) -> Callable[[Dict[str, Any]], Any]:
    """A ToolRegistry tool that delegates a turn to a peer agent. Params:
    ``{conversation_id, text, user_id}``, each with a default (``user`` stands in for
    ``user_id``, and the arguments themselves for a missing ``text``); returns the peer reply."""
    client = A2AClient(base_url, **client_kwargs)

    def call(params: Dict[str, Any]) -> Any:
        return client.send(
            conversation_id=str(params.get("conversation_id", "a2a")),
            text=_turn_text(params),
            user_id=str(params.get("user_id", params.get("user", "anonymous"))))

    return call
