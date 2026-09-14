# agentic-celery: Agentic-Flink on Celery

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

The agent essence on the Python **distributed task queue**, reusing the Flink-free
`pyagentic` core. See the design in
[`../../../docs/portability/celery.md`](../../../docs/portability/celery.md).

**The fit:** **one conversational turn = one Celery task**. Celery hosts the *online*
request/response turn (unlike Dask/Airflow's batch). It has no native keyed ordering
or keyed state, so the port recovers them: route a conversation to a stable queue
(`conversation_queue(cid)`) consumed by one worker + a per-conversation lock
(**C2**); keep durable state in an external `ConversationStore` (Redis, which Celery
already runs on) so a redelivered turn is idempotent; `acks_late` + retries give
**C3**; a `chord`/`chain` gives the async stage (**C4**).

| Symbol | Role |
|--------|------|
| `process_turn` | the Celery task, one conversational turn; routed to the conversation's queue, retried with backoff |
| `CeleryRuntime` | `pyagentic.Runtime` over Celery; `eager=True` runs in-process (no broker), `eager=False` sends to a worker |
| `conversation_queue(cid)` | the C2 seam, stable conversation→queue mapping (single-writer per conversation) |
| `configure(...)` | inject a production `ConversationStore` (Redis/Fluss) or an extended graph/tool set; the portable logic stays in the core |

The portable router→path→verifier graph + tools + retrieval are reused verbatim from
`pyagentic`; only the task/runtime seam is Celery-specific.

## Run

```bash
# live, in-process (eager mode, no broker) - what the test uses:
python ports/experimental/celery/agentic_celery.py
# ->
# [c1] queue=agentic.conv.0 path=cards    ok=True reply=...
# [c2] queue=agentic.conv.0 path=payments ok=True reply=[payments] Your balance is 1234.56. tools=['get_balance']
# [c1] queue=agentic.conv.0 path=cards    ok=True reply=[cards] Crypto cash-back can be redeemed...
# [c3] queue=agentic.conv.1 path=general  ok=True reply=...

# distributed (needs Redis + a worker):
AGENTIC_CELERY_BROKER=redis://localhost:6379/0 \
AGENTIC_CELERY_BACKEND=redis://localhost:6379/1 \
    celery -A agentic_celery:app worker -Q agentic.conv.0,agentic.conv.1 -l info
```

Covered by the adapter suite (`ports/experimental/tests/test_adapters.py`), including a check that
an **extended** core graph (a new path + tool) flows through the real Celery task seam.
