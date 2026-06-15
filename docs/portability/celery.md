# Agentic Streaming on Celery

> Per the keystone [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md).
> Celery is the ubiquitous Python distributed **task queue** (on Redis/RabbitMQ). A
> working port lives in [`../../ports/celery/`](../../ports/celery/) — it **runs live**
> here in eager mode and is covered by the adapter test suite.

## 1. Verdict

Celery hosts the *online* request/response turn — **one conversational turn = one
Celery task** — which sets it apart from the batch/scheduled Python engines (Dask,
Airflow). It is pure Python and reuses the `pyagentic` core unchanged. The catch is
that Celery is a task queue, not a keyed stream processor: it has **no native
single-writer-per-key ordering** and **no built-in keyed state**. The port recovers
both the way the keystone prescribes — route every task for a conversation to the
*same* queue (a stable hash of `conversationId`) consumed by one worker, take a
per-conversation lock inside the worker (C2), and keep durable state in an external
`ConversationStore` (Redis — which Celery already runs on) so a redelivered task is
idempotent (`acks_late` + retries = C3). Async stages (C4) are a Celery `chord`/`chain`
or an async task body. So Celery *assembles* the keyed-state heart rather than
supplying it natively — closer to Ray's request/response shape than to Faust's
streaming, but with the state externalized rather than living in an actor.

## 2. Capability mapping (C1..C12)

| Cap | How Celery supplies it |
|-----|-------------------------|
| **C1** durable keyed state | **X** — not native; an external `ConversationStore` (Redis/Fluss) keyed by `conversationId`. The result backend stores task results, not conversation state. |
| **C2** per-key ordered processing | **L** — no native keyed ordering; route a conversation to one queue (`conversation_queue(cid)`) consumed by a single worker + a per-conversation lock. |
| **C3** fault tolerance / durability | **L/N** — `task_acks_late` + `reject_on_worker_lost` redeliver a lost turn; `max_retries` with backoff for transient failures; the durable store makes a redelivered turn idempotent. |
| **C4** async I/O | **L** — a `chord`/`chain` (LLM/A2A call = its own task feeding the verifier task), or an async task body. |
| **C5** backpressure | **L** — `worker_prefetch_multiplier` + queue length + rate limits. |
| **C6** connectors | **L** — broker transports (Redis/RabbitMQ/SQS); Celery Beat for scheduled triggers. |
| **C7** side outputs | **L** — dispatch a follow-on task to another queue. |
| **C8** broadcast state | **L** — a shared store / a fanout broadcast queue. |
| **C9** event-time / windows | **—** — not a streaming engine. |
| **C10** CEP | **—** — out of scope. |
| **C11** distributed scale | **N** — add workers; queues shard the conversation space. |
| **C12** topology builder | **L** — `chain`/`group`/`chord` canvas wires multi-stage flows. |

## 3. The core abstractions on Celery

- **Agent / one turn = one task.** [`process_turn`](../../ports/celery/agentic_celery.py)
  is a Celery task that builds an `AgentContext` over the shared stores and runs the
  portable graph. The task is stateless; all state is in the `ConversationStore`.

  ```python
  @app.task(name="agentic.process_turn", bind=True, max_retries=2, acks_late=True)
  def process_turn(self, conversation_id, text, user_id="anonymous"):
      deps = _Deps.get()                                  # shared graph/tools/retriever/store
      with deps.lock_for(conversation_id):                # C2 within the worker
          ctx = AgentContext(conversation_id, user_id, deps.store, deps.state,
                             deps.tools, deps.retriever)
          return deps.graph.handle(Event(conversation_id, text, user_id), ctx).__dict__
  ```

- **Single-writer per conversation (C2).** `conversation_queue(cid)` maps a
  conversation to a stable queue; a worker bound to that queue (with
  `worker_prefetch_multiplier=1`) processes its conversations one at a time. The
  per-conversation lock covers the case of a worker with thread/eventlet concurrency.
- **ConversationStore (C1).** In-memory for the demo (shared in one eager process);
  in production inject a Redis-backed `ConversationStore` via `configure(...)` so state
  is shared across workers and survives restarts — the same SPI as every other port.
- **Fault tolerance (C3).** `acks_late` means a turn is only acked after it completes,
  so a worker crash redelivers it; the durable store makes the replay idempotent.
- **Async (C4).** Model the LLM/A2A call as its own task and compose with a `chord`:
  `chord(llm_call.s(...), verify.s())` — the verifier runs when the call completes,
  off the original worker thread.
- **Inbound edge.** A producer calls `process_turn.apply_async(...)`, or Celery Beat
  schedules turns, or a web handler enqueues them.

## 4. Worked example — banking router→path→verifier

[`agentic_celery.py`](../../ports/celery/agentic_celery.py)'s `CeleryRuntime(eager=True)`
runs the task body in-process with **no broker** (this is what the test uses):

```
[c1] queue=agentic.conv.0 path=cards    ok=True reply=[cards] I can help with cards questions...
[c2] queue=agentic.conv.0 path=payments ok=True reply=[payments] Your balance is 1234.56.  tools=['get_balance']
[c1] queue=agentic.conv.0 path=cards    ok=True reply=[cards] Crypto cash-back can be redeemed...
[c3] queue=agentic.conv.1 path=general  ok=True reply=[general] I can help with general questions...
```

`c1`'s turns route to the same queue (single-writer); `c3` lands on a different queue.
Distributed:

```
AGENTIC_CELERY_BROKER=redis://localhost:6379/0 \
AGENTIC_CELERY_BACKEND=redis://localhost:6379/1 \
    celery -A agentic_celery:app worker -Q agentic.conv.0,agentic.conv.1 -l info
```

## 5. What doesn't fit

- **Live keyed streaming.** Celery has no partitions/keyed ordering; the queue-routing
  + lock pattern works but is a convention you enforce, not an engine guarantee. For a
  true keyed stream, Faust (pure Python) or Kafka Streams is the better home.
- **High-throughput per-event state.** Every turn round-trips to an external store; for
  very high event rates an in-process keyed-state engine (Faust/Flink) wins.
- **Event-time / windows / CEP.** Out of scope — Celery is a task executor.
- **Strict ordering across a conversation under concurrency.** Requires the
  single-queue-per-conversation discipline; fan-out concurrency trades ordering away.

## 6. When to choose Celery

Choose Celery when you already run a **Celery + Redis/RabbitMQ** stack and want online,
request/response agentic turns, scheduled agentic jobs (Celery Beat), or fan-out work
(batch a tool across many inputs) without adopting a stream processor. It is the
pragmatic Python choice for "agents as tasks on the queue we already operate." If you
need native keyed state + ordering, reach for Faust or Ray; for heavy offline RAG/eval,
Dask; for scheduled DAG-shaped pipelines, Airflow.
