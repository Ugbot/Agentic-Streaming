"""A2A (Agent-to-Agent) client — call a peer agent's HTTP gateway as a tool, with the
Agent Card discovery + retries. ``peer_tool`` turns a peer into a ``ToolRegistry`` tool so
a path can delegate to another agent (on any backend).
"""

from __future__ import annotations

import json
import time
import urllib.request
from typing import Any, Callable, Dict, Optional


class A2AClient:
    """Minimal A2A client over a peer gateway's HTTP surface (``/agent`` +
    ``/.well-known/agent-card.json``), with bounded retries + backoff."""

    def __init__(self, base_url: str, retries: int = 2, backoff: float = 0.2, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.retries = max(0, retries)
        self.backoff = backoff
        self.timeout = timeout

    def _get(self, path: str) -> Dict[str, Any]:
        with urllib.request.urlopen(self.base_url + path, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        last: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                req = urllib.request.Request(
                    self.base_url + path, data=json.dumps(body).encode("utf-8"),
                    headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except Exception as exc:  # transient network error
                last = exc
                if attempt < self.retries:
                    time.sleep(self.backoff * (2 ** attempt))
        raise RuntimeError(f"A2A call to {self.base_url}{path} failed after {self.retries + 1} tries: {last}")

    def card(self) -> Dict[str, Any]:
        return self._get("/.well-known/agent-card.json")

    def send(self, conversation_id: str, text: str, user_id: str = "anonymous") -> Dict[str, Any]:
        return self._post("/agent", {"conversation_id": conversation_id, "text": text, "user_id": user_id})


def peer_tool(base_url: str, **client_kwargs) -> Callable[[Dict[str, Any]], Any]:
    """A ToolRegistry tool that delegates a turn to a peer agent. Params:
    ``{conversation_id, text, user_id}`` (sensible defaults); returns the peer reply."""
    client = A2AClient(base_url, **client_kwargs)

    def call(params: Dict[str, Any]) -> Any:
        return client.send(
            conversation_id=str(params.get("conversation_id", "a2a")),
            text=str(params.get("text", "")),
            user_id=str(params.get("user_id", "anonymous")))

    return call
