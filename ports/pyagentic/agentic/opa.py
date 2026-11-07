"""Asks an Open Policy Agent about each tool call, over its REST data API.

The Python peer of `OpaToolPolicy`. The document's policy is the floor: this runs after it and can only
narrow it. Anything but a clear yes is a no, so an OPA that is down, slow, undefined or confused
refuses the call. The input is `{tool, origin, path, effect, principal, args}`; the answer is a boolean
at the policy's path, or an object with `allow` and optionally `reason`.
"""

from __future__ import annotations

import http.client
import json
import os
import urllib.error
import urllib.request
from typing import Any, Mapping, Optional

from .policy import ALLOW, DENY, Decision, ToolPolicy, ToolRequest


class OpaToolPolicy:
    outside_this_process = True

    def __init__(self, base_url: str, policy_path: str = "agentic/tools", timeout_ms: int = 2000) -> None:
        self.url = f"{base_url.rstrip('/')}/v1/data/{policy_path.lstrip('/')}"
        self.timeout = timeout_ms / 1000.0

    def decide(self, request: ToolRequest) -> Decision:
        body = {"input": {"tool": request.tool, "origin": request.origin, "path": request.path,
                          "effect": request.effect, "principal": request.principal, "args": dict(request.args)}}
        http_request = urllib.request.Request(self.url, data=json.dumps(body).encode("utf-8"), method="POST",
                                              headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(http_request, timeout=self.timeout) as response:
                result = json.loads(response.read().decode("utf-8")).get("result")
        except (urllib.error.URLError, OSError, http.client.HTTPException, ValueError, AttributeError, TypeError):
            return Decision(DENY, f"opa unavailable for tool {request.tool}")
        allowed = result if isinstance(result, bool) else isinstance(result, dict) and result.get("allow") is True
        if allowed:
            return Decision(ALLOW)
        reason = result.get("reason") if isinstance(result, dict) else None
        return Decision(DENY, str(reason) if reason else f"opa refused tool {request.tool}")


def from_environment(env: Optional[Mapping[str, str]] = None) -> Optional[OpaToolPolicy]:
    """The policy the operator configured with `AGENTIC_OPA_URL`, or None when there is none."""
    settings = os.environ if env is None else env
    url = settings.get("AGENTIC_OPA_URL")
    if not url:
        return None
    return OpaToolPolicy(url, settings.get("AGENTIC_OPA_POLICY", "agentic/tools"),
                         int(settings.get("AGENTIC_OPA_TIMEOUT_MS", "2000")))


class Both:
    """Both must allow, and `first` is asked first."""

    def __init__(self, first: ToolPolicy, second: ToolPolicy) -> None:
        self.first = first
        self.second = second
        self.outside_this_process = (getattr(first, "outside_this_process", False)
                                     or getattr(second, "outside_this_process", False))

    def decide(self, request: ToolRequest) -> Decision:
        decision = self.first.decide(request)
        return self.second.decide(request) if decision.verdict == ALLOW else decision
