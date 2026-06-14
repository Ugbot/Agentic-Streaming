# gateway-fastapi — Agentic-Flink HTTP gateway

A **FastAPI** HTTP front door for the pure-Python ports, reusing the Flink-free
`pyagentic` core. It exposes the banking `router -> path -> verifier` agent
(cards / payments / general) over HTTP with an A2A-style **Agent Card**, a turn
endpoint, and conversation-transcript inspection.

**The fit:** the gateway is the *online request/response* edge — the same role as the
sibling A2A gateway, but pure Python. The agent logic, tools, and retrieval are reused
**verbatim** from `pyagentic`; this module only adds the HTTP seam and a pluggable
**backend runtime** so the same endpoints run over different substrates.

| Symbol | Role |
|--------|------|
| `create_app(backend=None)` | builds the FastAPI app; backend defaults to the env-selected one |
| `LocalBackend` | default — `LocalRuntime` over the banking graph + a shared `InMemoryConversationStore`. Zero third-party deps |
| `CeleryBackend` | wraps the Celery adapter (`ports/celery`) in eager (in-process) mode; needs `celery` |
| `NatsBackend` | wraps the NATS adapter (`ports/nats`); one persistent asyncio loop in a background thread (a NATS connection is loop-bound). Needs a JetStream server |
| `make_backend(name)` | factory: `local` (default) \| `celery` \| `nats`, selected by `AGENTIC_GATEWAY_BACKEND` |

## Endpoints

| Method | Path | Body / Result |
|--------|------|---------------|
| `GET`  | `/healthz` | `{"status": "ok", "backend": "<name>"}` |
| `GET`  | `/.well-known/agent-card.json` | A2A-style Agent Card (matches the Go sibling) |
| `POST` | `/agent` | `{conversation_id, text, user_id?}` -> `{conversation_id, reply, path, ok, tool_calls}` |
| `GET`  | `/conversations/{conversation_id}` | `{conversation_id, messages: [{role, content}], message_count}` |

Request bodies are validated with Pydantic — bad input returns `422` automatically.
Internal errors are sanitized to clean JSON (`{"error", "detail"}`), never a stack trace.

## Run

```bash
# default (LocalBackend, no external services):
python -m gateway_fastapi
# or via uvicorn:
uvicorn gateway_fastapi.__main__:app --host 127.0.0.1 --port 8000

# pick a backend:
AGENTIC_GATEWAY_BACKEND=celery python -m gateway_fastapi          # in-process Celery (eager)
AGENTIC_GATEWAY_BACKEND=nats   python -m gateway_fastapi          # needs a JetStream server
#   podman run -d --name nats-js -p 4222:4222 nats:latest -js
#   point elsewhere with AGENTIC_NATS_URL
```

Host/port are configurable via `AGENTIC_GATEWAY_HOST` / `AGENTIC_GATEWAY_PORT`
(defaults `127.0.0.1:8000`). Run from `ports/gateway-fastapi/` (the package dir is on
the path), or add it to `PYTHONPATH`.

### curl example

```bash
curl -s localhost:8000/healthz
# {"status":"ok","backend":"local"}

curl -s -X POST localhost:8000/agent \
  -H 'content-type: application/json' \
  -d '{"conversation_id":"c1","text":"what is my balance?","user_id":"alice"}'
# {"conversation_id":"c1","reply":"[payments] Your balance is 1234.56.","path":"payments","ok":true,"tool_calls":["get_balance"]}

curl -s -X POST localhost:8000/agent \
  -H 'content-type: application/json' \
  -d '{"conversation_id":"c1","text":"tell me about crypto cash-back"}'
# {"conversation_id":"c1","reply":"[cards] Crypto cash-back can be redeemed...","path":"cards","ok":true,"tool_calls":[]}

curl -s localhost:8000/conversations/c1
# {"conversation_id":"c1","messages":[{"role":"user",...},{"role":"assistant",...}, ...],"message_count":4}
```

## Tests

```bash
/tmp/af-venv/bin/python -m pytest ports/gateway-fastapi/tests -q
```

The suite drives the app through `fastapi.testclient.TestClient` (no running server) with
randomized inputs (uuid4 conversation ids, random texts per path): healthz, agent-card
shape, card/payment/general routing (balance turn calls `get_balance` and replies with
`1234.56`), multi-turn transcript accumulation, conversation isolation, and `422` on bad
input. The `local` backend runs without `celery`/`nats` installed.

## Backend caveats

- **local** — always available, single-process, in-memory transcript. Transcript is
  fully inspectable via `/conversations/{id}`.
- **celery** — eager mode runs the task body in-process (no broker); a shared
  `InMemoryConversationStore` is injected via the adapter's `configure(...)` so history
  works. For distributed Celery the store would be Redis-backed (see `ports/celery`).
- **nats** — requires a reachable JetStream server (`AGENTIC_NATS_URL`, default
  `nats://127.0.0.1:4222`); construction raises a clear `RuntimeError` if the server is
  unreachable or `nats-py` is missing. History is read back from the durable JetStream
  KV envelope. All `connect`/`submit` calls share one event loop (a NATS connection is
  bound to its creating loop), driven from a background thread.
