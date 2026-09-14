"""The FastAPI application — HTTP front door for the banking agent.

Endpoints:
  GET  /healthz                       -> {"status": "ok", "backend": "<name>"}
  GET  /.well-known/agent-card.json   -> A2A-style Agent Card (matches the Go sibling)
  POST /agent                         -> run one turn -> {conversation_id, reply, path, ok, tool_calls}
  GET  /conversations/{conversation_id} -> {conversation_id, messages, message_count}

The chosen backend (local/celery/nats) is wired into ``app.state`` so the routes stay
backend-agnostic. Errors are sanitized: clients get clean JSON, never a stack trace.

``/agent`` and ``/conversations/*`` require a bearer token (see ``auth.py``). The authenticated
principal is the user id handed to the backend, and conversations are scoped per principal: the
backend key is ``<subject>/<conversation_id>``, so one principal can never read or continue
another principal's conversation, whatever id it guesses.
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .auth import BearerAuth, Principal
from .backends import Backend, make_backend

AGENT_CARD = {
    "name": "Agentic-Flink Banking Agent",
    "description": "Router -> path -> verifier banking agent (cards/payments/general) over the pyagentic essence.",
    "version": "0.1.0",
    "url": "/agent",
    "capabilities": {"streaming": False, "pushNotifications": False},
    "defaultInputModes": ["text"],
    "defaultOutputModes": ["text"],
    "skills": [
        {
            "id": "banking",
            "name": "Banking Q&A",
            "description": "Answers card, payment, and general banking questions with tool use + retrieval.",
            "tags": ["banking", "router", "rag"],
        }
    ],
}


class TurnRequest(BaseModel):
    """Inbound turn. ``user_id`` is optional and, when present, must equal the authenticated
    principal; the principal is what the backend receives either way."""

    conversation_id: str = Field(..., min_length=1, pattern=r"^[^/]+$")
    text: str = Field(..., min_length=1)
    user_id: Optional[str] = None


class TurnResponse(BaseModel):
    conversation_id: str
    reply: str
    path: Optional[str] = None
    ok: bool = True
    tool_calls: List[str] = Field(default_factory=list)


class Message(BaseModel):
    role: str
    content: str


class ConversationResponse(BaseModel):
    conversation_id: str
    messages: List[Message]
    message_count: int


def scoped_conversation_id(principal: Principal, conversation_id: str) -> str:
    return f"{principal.subject}/{conversation_id}"


def create_app(backend: Optional[Backend] = None, auth: Optional[BearerAuth] = None) -> FastAPI:
    """Build the gateway app. ``backend`` defaults to the env-selected backend
    (``AGENTIC_GATEWAY_BACKEND``, else ``local``); ``auth`` defaults to the env-configured tokens."""
    backend = backend if backend is not None else make_backend()
    auth = auth if auth is not None else BearerAuth.from_env()

    app = FastAPI(
        title="Agentic-Flink Banking Gateway",
        version=AGENT_CARD["version"],
        description=AGENT_CARD["description"],
    )
    app.state.backend = backend

    @app.exception_handler(Exception)
    async def _sanitized_error(_request: Request, exc: Exception) -> JSONResponse:
        # Never leak stack traces to the client; surface a clean, typed message.
        return JSONResponse(status_code=500, content={"error": type(exc).__name__, "detail": str(exc)})

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok", "backend": app.state.backend.name}

    @app.get("/.well-known/agent-card.json")
    async def agent_card() -> dict:
        return AGENT_CARD

    @app.post("/agent", response_model=TurnResponse)
    async def agent(req: TurnRequest, principal: Principal = Depends(auth)) -> TurnResponse:
        if req.user_id is not None and req.user_id != principal.subject:
            raise HTTPException(status_code=403, detail="user_id does not match the authenticated principal")
        result = app.state.backend.submit(
            scoped_conversation_id(principal, req.conversation_id), req.text, principal.subject
        )
        result = dict(result)
        result["conversation_id"] = req.conversation_id
        return TurnResponse(**result)

    @app.get("/conversations/{conversation_id}", response_model=ConversationResponse)
    async def conversation(conversation_id: str, principal: Principal = Depends(auth)) -> ConversationResponse:
        if "/" in conversation_id:
            raise HTTPException(status_code=422, detail="conversation_id must not contain '/'")
        messages = app.state.backend.history(scoped_conversation_id(principal, conversation_id))
        return ConversationResponse(
            conversation_id=conversation_id,
            messages=[Message(**m) for m in messages],
            message_count=len(messages),
        )

    return app
