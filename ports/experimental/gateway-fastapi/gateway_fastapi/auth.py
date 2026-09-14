"""Bearer token authentication for the FastAPI gateway.

Tokens are configured through the environment:

  AGENTIC_GATEWAY_TOKENS="alice=tok-a,bob=tok-b"   several principals, one token each
  AGENTIC_GATEWAY_TOKEN="tok"                       one token, principal ``gateway-client``
  AGENTIC_GATEWAY_AUTH_DEV_MODE=true                no token configured: admit everyone as ``anonymous``

With no token configured and dev mode off the gateway fails closed: every request other than
``/healthz`` and the Agent Card is rejected with 401. The authenticated principal is the only
user identity the backend ever sees; a client cannot choose its ``user_id``.
"""

from __future__ import annotations

import hmac
import os
from dataclasses import dataclass
from typing import Dict, Mapping, Optional

from fastapi import HTTPException, Request

DEFAULT_SUBJECT = "gateway-client"
DEV_SUBJECT = "anonymous"


@dataclass(frozen=True)
class Principal:
    subject: str
    dev_mode: bool = False


def parse_tokens(spec: Optional[str]) -> Dict[str, str]:
    """``subject=token,subject2=token2`` or a bare token -> {token: subject}."""
    out: Dict[str, str] = {}
    if not spec or not spec.strip():
        return out
    for raw in spec.split(","):
        entry = raw.strip()
        if not entry:
            continue
        if "=" in entry:
            subject, _, token = entry.partition("=")
            subject, token = subject.strip(), token.strip()
            if not subject or not token:
                raise ValueError("malformed AGENTIC_GATEWAY_TOKENS entry (expected subject=token)")
            out[token] = subject
        else:
            out[entry] = DEFAULT_SUBJECT
    return out


class BearerAuth:
    def __init__(self, tokens: Mapping[str, str], dev_mode: bool = False) -> None:
        self._tokens = dict(tokens)
        self._dev_mode = dev_mode

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "BearerAuth":
        env = os.environ if env is None else env
        spec = env.get("AGENTIC_GATEWAY_TOKENS") or env.get("AGENTIC_GATEWAY_TOKEN") or ""
        dev = env.get("AGENTIC_GATEWAY_AUTH_DEV_MODE", "false").strip().lower() in ("1", "true", "yes")
        return cls(parse_tokens(spec), dev_mode=dev)

    @property
    def configured(self) -> bool:
        return bool(self._tokens)

    def authenticate(self, authorization: Optional[str]) -> Principal:
        if not self._tokens:
            if self._dev_mode:
                return Principal(DEV_SUBJECT, dev_mode=True)
            raise HTTPException(status_code=401, detail="gateway authentication is not configured",
                                headers={"WWW-Authenticate": "Bearer"})
        if not authorization:
            raise HTTPException(status_code=401, detail="missing Authorization header",
                                headers={"WWW-Authenticate": "Bearer"})
        scheme, _, presented = authorization.partition(" ")
        presented = presented.strip()
        if scheme.lower() != "bearer" or not presented:
            raise HTTPException(status_code=401, detail="Authorization scheme must be Bearer",
                                headers={"WWW-Authenticate": "Bearer"})
        subject: Optional[str] = None
        for token, subj in self._tokens.items():
            if hmac.compare_digest(token.encode("utf-8"), presented.encode("utf-8")):
                subject = subj
        if subject is None:
            raise HTTPException(status_code=401, detail="invalid bearer token",
                                headers={"WWW-Authenticate": "Bearer"})
        return Principal(subject)

    def __call__(self, request: Request) -> Principal:
        return self.authenticate(request.headers.get("authorization"))
