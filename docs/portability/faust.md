# Agentic-Flink on Faust (faust-streaming) — pure Python, no JVM

> Per-engine doc in the `docs/portability/` series. Read
> [`00-essence-and-core-abstractions.md`](./00-essence-and-core-abstractions.md)
> first: this doc is written against its core abstractions, the Engine SPI
> (§4c), the capability inventory (§3 / C1..C12), and the §6 matrix. It follows
> the §9 six-section template.

## 1. Verdict

Faust is the **most natural pure-Python target** for the live keyed-stateful
essence of Agentic-Flink. The mapping is almost embarrassing: our central idea —
"an agent is a keyed, stateful, event-driven processor, one logical instance per
conversation, processing its events in order" — is *literally* what Faust calls
an **agent** (`@app.agent`, a coroutine consuming a keyed Kafka stream). Our C1
(durable keyed state) is a Faust **Table** (RocksDB + a changelog topic); our C2
(per-key ordered processing) is Kafka partitioning; our C4 (async I/O) is
`asyncio`, which Faust is built on top-to-bottom, so awaiting an LLM, a tool, or
an A2A peer inside an agent is the idiomatic thing rather than a bridge you have
to engineer. You keep the entire §4a portable core verbatim (re-expressed in
Python): the ConversationStore SPI, the two-tier retriever, the ReAct/TurnBrain
loop, the resilient A2A client, the tool registry. You re-implement only the
§4c Engine seam — `keyedAgent`, `asyncStage`, `route`, `KeyedStateStore` — and on
Faust each of those is a thin wrapper over a primitive that already exists. What
you **give up** versus Flink: transactional exactly-once (Kafka offset commits
give you at-least-once, so you lean on idempotency + the ConversationStore as the
source of truth), and the rich event-time/windowing/CEP machinery (Faust has
windowed Tables but they are coarser). Everything load-bearing survives; the
weak spots are exactly the analytics-flavoured capabilities the agentic core
barely uses.

## 2. Capability mapping (C1..C12)

Following the §6 matrix's **Faust** column. Legend: **N**ative · **L**ibrary/
idiom · **X**ternal service · **—** drop.

| Cap | §6 | Faust mechanism |
|-----|:--:|-----------------|
| **C1** durable keyed state | **N** | `app.Table(...)` — a dict-like keyed store backed by RocksDB locally and a compacted **changelog topic** in Kafka, recovered on rebalance. This *is* our per-conversation keyed state (short-term memory, `RoutingBudget`, A2A `contextId`, dedup set) and a direct ConversationStore backend. |
| **C2** per-key ordered proc | **N** | Kafka **partitioning**: a stream keyed on `conversationId` lands every event for that key on one partition, consumed by one agent instance, in offset order → single-writer-per-conversation without locks. Same guarantee Flink's `keyBy` gives. |
| **C3** fault tolerance / EOS | **L** | Kafka **offset commits** + Table changelog recovery = **at-least-once**, *not* Flink-style transactional EOS. Mitigation: idempotent tool/A2A calls (the resilient client already assumes this) and treat the ConversationStore as the durable source of truth; dedup on a per-turn id held in a Table. |
| **C4** async I/O | **N** | `asyncio` is native. Inside an agent you simply `await llm(...)`, `await tool(...)`, `await a2a_client.send(...)`. Bounded in-flight via `stream.take`/an `asyncio.Semaphore` or `app.agent(concurrency=N)`. No Async-I/O-operator bridge needed. |
| **C5** backpressure | **L** | asyncio flow control + Kafka consumer fetch pacing; cap concurrent model calls with a `Semaphore`; `app.agent(concurrency=...)` bounds parallel coroutines per worker. |
| **C6** connectors | **N** | Kafka is Faust's home transport (topics in/out). Other `Channel<T>` transports (Redis pub/sub, webhook, Postgres CDC, static seed) become small async producers/consumers or a Faust **web view** feeding a topic. |
| **C7** side outputs | **L** | Publish to an extra topic (`await debug_topic.send(...)`) — the debug stream and tool-invocation channel become their own topics, consumable independently. |
| **C8** broadcast state | **L** | A **global Table** (`app.GlobalTable`, every worker has the full copy) for control-plane directives / enrichment dims, fed by a compacted topic. |
| **C9** event-time / windows | **N (coarse)** | Faust **windowed Tables** (`Table(...).hopping/tumbling(...)`) exist for feature aggregation, but are simpler than Flink's watermark/allowed-lateness model. Fine for the streaming-analytics side flows; don't expect Flink parity. |
| **C10** CEP | **L** | No CEP library; encode patterns as a small state machine over a Table (last-N events per key) inside an agent. The optional CEP module degrades to hand-rolled. |
| **C11** distributed scale | **N** | Scale by running more Faust worker processes; Kafka rebalances partitions across them. Parallelism is bounded by partition count, same as Flink keyed parallelism. |
| **C12** topology builder | **N** | The "topology" is the set of `@app.agent`s plus the topics wiring them. The routed graph is either multiple agents chained by topics, or one agent branching on a phase Table (see §3/§4). |

## 3. The core abstractions on this engine

A Python mirror of the §4c Engine SPI. The portable core (§4a) is consumed
unchanged; only this seam is Faust-specific.

### Agent — a keyed Kafka stream processor

The Flink `KeyedProcessFunction` becomes a Faust agent coroutine. One instance
per partition; events for a `conversationId` arrive in order.

```python
import faust

app = faust.App("agentic", broker="kafka://localhost:9092",
                store="rocksdb://", topic_partitions=24)

class Turn(faust.Record, serializer="json"):
    conversation_id: str
    user_id: str
    text: str
    turn_id: str            # idempotency key (C3 mitigation)

turns = app.topic("turns", key_type=str, value_type=Turn)
replies = app.topic("replies", value_type=Reply)

@app.agent(turns)
async def agent(stream):
    async for turn in stream.group_by(Turn.conversation_id):  # C2: key → partition
        out = await handle_turn(turn)        # ReAct loop (portable core)
        await replies.send(value=out)
```

`stream.group_by(...)` re-partitions so all of a conversation's turns are
single-writer. `handle_turn` is the **portable** TurnBrain — engine-free.

### Keyed state — `KeyedStateStore` over a Table

The §4c `KeyedStateStore` (`get/put/update/clear` for a key) is a one-class
adapter over a Faust Table. This is the portable C1 replacement for
`FlinkStateShortTermMemory`.

```python
class TableStateStore:
    """Engine SPI KeyedStateStore, backed by a Faust Table (RocksDB + changelog)."""
    def __init__(self, table: faust.Table):
        self._t = table
    def get(self, key, default=None):     return self._t.get(key, default)
    def put(self, key, value):            self._t[key] = value
    def update(self, key, fn):            self._t[key] = fn(self._t.get(key))
    def clear(self, key):                 self._t.pop(key, None)

short_term = app.Table("short_term", default=dict, partitions=24)
state = TableStateStore(short_term)        # per-conversation working memory
```

Because the Table is partitioned identically to the agent's stream, reads/writes
for `conversation_id` are local to the worker owning that partition — no remote
hop, single-writer, recovered from the changelog on failover.

### ConversationStore — a Table (or keep the Redis/Fluss SPI)

The `ConversationStore` interface (append / history / recent / attributes /
user-index) ports directly. The **default** Faust backend is a Table; for
cross-process sharing outside the Faust worker set (e.g. a separate inbound
proxy) keep the existing **Redis/Fluss** backend behind the same SPI.

```python
class TableConversationStore:           # implements the ConversationStore SPI
    def __init__(self, app, max_msgs=200):
        self._tx   = app.Table("cs_transcript", default=list, partitions=24)
        self._attr = app.Table("cs_attrs",      default=dict, partitions=24)
        self._byuser = app.Table("cs_by_user",  default=list, partitions=24)
        self._max = max_msgs

    def append(self, cid, msg):
        if cid is None: return
        hist = self._tx[cid]; hist.append(msg)
        self._tx[cid] = hist[-self._max:]            # bounded transcript
    def history(self, cid):  return list(self._tx.get(cid, []))
    def recent(self, cid, n): h = self.history(cid); return h if n <= 0 else h[-n:]
    def put_attribute(self, cid, k, v):
        a = self._attr[cid]; a[k] = v; self._attr[cid] = a
    def get_attribute(self, cid, k):  return self._attr.get(cid, {}).get(k)
    def associate_user(self, cid, uid):
        lst = self._byuser[uid]
        if cid not in lst: lst.append(cid); self._byuser[uid] = lst
    def conversations_for_user(self, uid): return list(self._byuser.get(uid, []))
```

The transcript Table partitioned by `conversation_id` means the router, path and
verifier agents (all keyed on the same `conversation_id`) **share** one
conversation view across operators — exactly the gap §4a says the ConversationStore
fills, and the reason a routed graph can progress across turns.

### Tools — async functions

`ToolExecutor` (`Map<String,Object> → CompletableFuture<Object>`) becomes an
`async def(params: dict) -> Any`. A registry is just a dict.

```python
class ToolRegistry:
    def __init__(self): self._tools = {}
    def register(self, name, fn): self._tools[name] = fn
    async def invoke(self, name, params: dict):
        return await self._tools[name](params)

tools = ToolRegistry()

async def get_balance(params: dict):
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{CORE_BANK}/accounts/{params['account']}/balance")
        return r.json()
tools.register("get_balance", get_balance)
```

### Async — native, with a concurrency cap

Flink's Async-I/O operator (bounded in-flight) maps to `asyncio` + a semaphore
(or `@app.agent(concurrency=N)`), protecting the model endpoint (C5).

```python
llm_gate = asyncio.Semaphore(8)          # bound concurrent model calls
async def llm(messages, **kw):
    async with llm_gate:
        return await chat_client.chat(messages, **kw)   # portable ChatConnection
```

### Routed graph — `router → path → verifier`

Two idiomatic shapes; both keep all stages keyed on `conversation_id`:

**(a) One agent, branch on a phase Table** (lowest latency, one process):

```python
@app.agent(turns)
async def banking(stream):
    async for t in stream.group_by(Turn.conversation_id):
        path  = router(t, cs)                 # rule-based classify (no LLM)
        out   = await PATH_BRAINS[path](t, cs, tools)   # ReAct path brain
        reply = verifier(out, cs)             # advance phase, validate
        await replies.send(value=reply)
```

**(b) Multiple agents wired by topics** (independent scaling / isolation):

```python
routed   = app.topic("routed",   value_type=Turn)   # tagged with .path
verified = app.topic("replies",  value_type=Reply)

@app.agent(turns)
async def router_a(stream):
    async for t in stream.group_by(Turn.conversation_id):
        t.path = router(t, cs)
        await routed.send(key=t.conversation_id, value=t)

@app.agent(routed)
async def path_a(stream):
    async for t in stream.group_by(Turn.conversation_id):
        out = await PATH_BRAINS[t.path](t, cs, tools)
        await verified.send(key=out.conversation_id, value=verifier(out, cs))
```

Shape (a) mirrors the single-operator banking brain; shape (b) mirrors
`BankingAgentGraph`'s multi-operator DAG. Cross-turn chaining happens through the
shared phase attribute in the ConversationStore, **not** through a cycle in the
topology — identical to the Flink design.

### A2A — call the peer over aiohttp/httpx, resilient client re-impl

The A2A protocol types + `RemoteAgentSpec` port directly; `SdkA2AClient` becomes
a small async client with **retry / backoff / circuit-breaker** (the resilient
behaviour `A2AClient` mandates). `A2AStep.applyToStateful` (keyed pre → async →
keyed post) collapses on Faust into: read `contextId` from the ConversationStore,
`await` the peer, write the returned `contextId` back — all inside the keyed
agent, because the agent coroutine is *already* both keyed and async.

```python
class A2AClient:                          # resilient: retry/backoff/breaker
    def __init__(self, spec: RemoteAgentSpec):
        self.spec, self._breaker = spec, CircuitBreaker(threshold=5, reset=30)
    async def send(self, text, context_id=None):
        if self._breaker.open: raise CircuitOpen(self.spec.name)
        for attempt in range(self.spec.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.spec.timeout_s) as c:
                    r = await c.post(self.spec.url, json=rpc("message/send",
                            {"text": text, "contextId": context_id}))
                    self._breaker.record_success()
                    return r.json()["result"]
            except (httpx.HTTPError, asyncio.TimeoutError) as e:
                self._breaker.record_failure()
                await asyncio.sleep(min(2 ** attempt * 0.2, 5.0))   # backoff
        raise A2AError(self.spec.name)

# A2AStep.applyToStateful — keyed + async fused into one coroutine step:
async def a2a_step(turn, cs, client: A2AClient):
    ctx = cs.get_attribute(turn.conversation_id, "a2a.contextId")
    res = await client.send(turn.text, context_id=ctx)             # non-blocking
    cs.put_attribute(turn.conversation_id, "a2a.contextId", res["contextId"])
    return res["artifact"]["text"]
```

Exposing a peer *as a tool* (`A2AToolExecutor`) is just registering
`a2a_step` in the `ToolRegistry`.

### Inbound proxy — a Faust web view or a sidecar FastAPI

The Quarkus A2A gateway maps to either Faust's built-in web server
(`@app.page` / `app.web`, runs inside the worker) or a separate **FastAPI**
sidecar that produces onto the `turns` topic and consumes `replies`. The web
view is simplest; FastAPI is better when you want the proxy to scale or deploy
independently of the workers.

```python
@app.page("/a2a/message:send")
async def a2a_inbound(self, request):
    body = await request.json()
    cid  = body.get("contextId") or new_context_id()
    await turns.send(key=cid, value=Turn(
        conversation_id=cid, user_id=body["userId"],
        text=body["params"]["text"], turn_id=str(uuid4())))
    return self.json({"status": "accepted", "contextId": cid})
```

For synchronous JSON-RPC/SSE replies, correlate on `turn_id`: the proxy awaits a
future resolved by a consumer of the `replies` topic (or streams SSE from it).

### RAG — hot tier in Redis/in-mem, cold tier in a vector store

`TwoTierRetriever` (hot recent window ∪ cold durable corpus, dedup by id,
degrade if either tier fails) ports verbatim — it has no Flink in it. The hot
index is an in-process index (or Redis for cross-worker sharing); the cold tier
is pgvector/Qdrant via the `VectorStore` SPI. `RetrievalPipeline`
(embed → search → rerank → answer) becomes an async function chain.

```python
async def retrieve_answer(question: str) -> Answer:
    qv   = await embed(question)                          # EmbeddingClient SPI
    hits = two_tier.retrieve(qv, k=6)                     # hot ∪ cold, dedup
    hits = await rerank(question, hits)                   # cross-encoder (optional)
    ctx  = "\n".join(f"[{i+1}] {h.text}" for i, h in enumerate(hits[:3]))
    resp = await llm([sys("Answer using ONLY the numbered sources; cite [n]."),
                      user(f"Sources:\n{ctx}\nQuestion: {question}")])
    return Answer(question, resp.text.strip(), hits[:3])
```

The hot tier makes retrieval *live*: a doc is searchable the instant it lands in
the hot window, before the cold index catches up — same semantics as the Java
`TwoTierRetriever`.

## 4. Worked example — the banking router→path→verifier, end-to-end

Faithful port of `BankingAgentGraph`: a rule-based router screens + classifies
(no LLM), a per-path ReAct brain handles the turn (LLM only here), a rule-based
verifier advances the cross-turn phase and emits the A2A response. All keyed on
A2A `contextId` (== `conversation_id`). Paths mirror the Java enum
(`KNOWLEDGE, GATHER, DELEGATE, ACTION, DISPUTE, ESCALATE, REFUSE`); phases mirror
`NEW → NEED_INFO → READY_TO_ACT → ACTED → DONE`.

```python
import faust, asyncio
from uuid import uuid4

app = faust.App("banking", broker="kafka://localhost:9092",
                store="rocksdb://", topic_partitions=24)

class A2AReq(faust.Record, serializer="json"):
    context_id: str; user_id: str; text: str; turn_id: str
class A2AResp(faust.Record, serializer="json"):
    context_id: str; text: str; phase: str

requests = app.topic("a2a.requests", key_type=str, value_type=A2AReq)
responses = app.topic("a2a.responses", value_type=A2AResp)

cs    = TableConversationStore(app)            # shared across router/path/verifier
tools = ToolRegistry()
tools.register("get_balance", get_balance)
tools.register("open_dispute", open_dispute)   # idempotent on turn_id

DISPUTE_CLIENT = A2AClient(RemoteAgentSpec(
    name="disputes", url="http://disputes:8080/a2a", timeout_s=20, max_retries=3))

# ---- ROUTER: rule-based screen + classify, NO LLM ----
def router(req: A2AReq) -> str:
    t = req.text.lower()
    if any(w in t for w in ("hack", "ignore previous", "ssn")): return "REFUSE"
    if "dispute" in t or "fraud" in t:                          return "DISPUTE"
    if "balance" in t or "how much" in t:                       return "KNOWLEDGE"
    if "transfer" in t or "pay" in t:                           return "ACTION"
    if "agent" in t or "human" in t:                            return "ESCALATE"
    return "GATHER"

# ---- PATH BRAINS: focused ReAct turn brains (LLM lives here) ----
async def brain_knowledge(req, cs, tools):
    cs.append(req.context_id, user(req.text))
    bal = await tools.invoke("get_balance", {"account": req.user_id})
    msg = await llm(cs.recent(req.context_id, 10) +
                    [system(f"Account balance: {bal}")])
    return msg.text

async def brain_action(req, cs, tools):
    cs.append(req.context_id, user(req.text))
    # ReAct loop: think → tool → observe, bounded by maxIterations
    return await react_loop(req, cs, tools, max_iters=6)

async def brain_dispute(req, cs, tools):           # delegate to a peer over A2A
    cs.append(req.context_id, user(req.text))
    return await a2a_step(req, cs, DISPUTE_CLIENT)  # resilient client

async def brain_refuse(req, cs, tools):
    return "I can't help with that request."

PATH_BRAINS = {
    "KNOWLEDGE": brain_knowledge, "ACTION": brain_action,
    "DISPUTE":   brain_dispute,   "GATHER": brain_action,
    "ESCALATE":  brain_refuse,    "REFUSE": brain_refuse,
}

# ---- VERIFIER: rule-based, advances the cross-turn phase ----
def verifier(req: A2AReq, path: str, answer: str) -> A2AResp:
    phase = cs.get_attribute(req.context_id, "phase") or "NEW"
    if path == "REFUSE":               phase = "DONE"
    elif path in ("ACTION",):          phase = "ACTED"
    elif path == "GATHER":             phase = "NEED_INFO"
    elif phase == "NEED_INFO":         phase = "READY_TO_ACT"
    cs.put_attribute(req.context_id, "phase", phase)
    cs.append(req.context_id, assistant(answer))     # record the turn
    return A2AResp(context_id=req.context_id, text=answer, phase=phase)

# ---- ONE keyed agent = the whole Router→Path→Verifier DAG for a turn ----
@app.agent(requests)
async def banking(stream):
    async for req in stream.group_by(A2AReq.context_id):     # single-writer per session
        cs.associate_user(req.context_id, req.user_id)
        # C3 idempotency: skip a re-delivered turn (at-least-once safety)
        if cs.get_attribute(req.context_id, f"done.{req.turn_id}"):
            continue
        path   = router(req)                                 # no LLM
        answer = await PATH_BRAINS[path](req, cs, tools)     # LLM only on the path
        resp   = verifier(req, path, answer)
        cs.put_attribute(req.context_id, f"done.{req.turn_id}", "1")
        await responses.send(key=req.context_id, value=resp)
```

One A2A turn = Router → Path → Verifier, a clean DAG with no cycle. Multi-step
chaining (e.g. `GATHER` → next turn `ACTION`) happens **across turns** via the
shared `phase` attribute and transcript in the ConversationStore — identical to
the Flink `PhaseStore`/`ConversationMemory` design. Concurrent sessions are
isolated because every stage runs under `group_by(context_id)`. Reserving the
LLM for path brains (router + verifier are rule-based) keeps model-call count per
turn flat — the same cost discipline as the Java example.

To split this into the multi-operator shape (§3b) — separate `router_a`,
`path_a`, `verifier_a` agents wired by `routed`/`replies` topics — every stage
still `group_by(context_id)`s and shares the same `cs`, so the ConversationStore
remains the integration point across operators.

## 5. What doesn't fit (honest gaps + workarounds)

- **No transactional exactly-once (C3).** Kafka offset commits give
  at-least-once; a worker crash mid-turn can re-deliver. *Workaround:* the
  `done.{turn_id}` guard above + idempotent tools/A2A (the resilient client
  already assumes idempotency), and treat the ConversationStore as the source of
  truth. This is the single most important porting decision — exactly the §8
  cross-cutting concern.
- **faust-streaming is community-maintained.** The original Robinhood Faust is
  unmaintained; `faust-streaming` is the active fork. Pin versions, vet the
  RocksDB store wheel for your platform, and budget for occasional rough edges.
  This is an operational risk, not an architectural one.
- **Windowing / event-time (C9) is coarser than Flink.** Faust windowed Tables
  lack Flink's watermark/allowed-lateness/late-firing richness. *Workaround:* fine
  for the agentic flows (which barely use it); for serious streaming analytics
  side-flows, keep those on Flink or push to a warehouse.
- **No CEP library (C10).** *Workaround:* hand-roll patterns as a small state
  machine over a last-N Table per key. Fine for simple "3 declines in 5 min"
  rules; don't attempt rich Flink-CEP semantics.
- **State scales with partitions, not freely.** A Table's parallelism is bounded
  by topic partition count (same as Flink keyed parallelism, but you must choose
  partition count up front and repartitioning is operationally painful). *Choose
  generously at the start.*
- **Cross-process state sharing.** Tables are local to the Faust worker set. If a
  *separate* process (e.g. a standalone FastAPI proxy or an offline eval job)
  needs the conversation state, **don't** reach into RocksDB — use the
  Redis/Fluss ConversationStore backend behind the SPI instead. That's precisely
  why the SPI exists.
- **Synchronous request/response over an async stream.** The inbound proxy must
  correlate a reply on the `replies` topic back to the awaiting HTTP request
  (future keyed by `turn_id`, or SSE streamed from the topic). Workable, but it's
  glue you write — Flink's bridge hid it.

## 6. When to choose Faust

Choose Faust when:

- You want **pure Python, no JVM, no JPype** and the *live, keyed-stateful,
  multi-turn conversational* core is the point — this is its strongest niche
  among the Python targets.
- You are **already on Kafka** (or happy to be); Faust is Kafka-native and the
  Table changelog model gives you durable keyed state for free.
- Your team thinks in **`async def`** and wants LLM/tool/A2A calls to be ordinary
  awaits, not an operator bridge (the Flink Async-I/O ceremony disappears).
- **At-least-once + idempotency** is an acceptable correctness model (it is for
  most agentic workloads once tools are idempotent and the ConversationStore is
  authoritative).

Prefer something else when: you need transactional exactly-once or rich
event-time windowing/CEP (→ stay on **Flink**, or **Kafka Streams** on the JVM);
your shape is **batch RAG / offline eval** rather than live streaming (→ **Dask**
for parallel embed/index, **Airflow** for scheduled ingestion DAGs); or
conversations are short-lived request/response with no streaming spine (→ **Ray**
actor-per-conversation, or a **FastAPI** + Redis-backed ConversationStore service
without a stream engine at all).

> Among all the targets in this series, Faust is the one where the per-engine
> adapter is *thinnest*: `@app.agent` ≈ our agent, `Table` ≈ our keyed state,
> `asyncio` ≈ our async stage. The port is mostly re-typing the portable core in
> Python and accepting at-least-once.
