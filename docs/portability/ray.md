# Agentic Streaming on Ray — pure Python, no JVM

> Per-engine doc in the `docs/portability/` series. Read
> [`00-essence-and-core-abstractions.md`](./00-essence-and-core-abstractions.md)
> first: this doc is written against its core abstractions, the Engine SPI
> (§4c), the capability inventory (§3 / C1..C12), and the §6 matrix. It follows
> the §9 six-section template.

## 1. Verdict

Ray is the most **idiomatic pure-Python home for the stateful-agent essence** —
even though it is *not* a streaming engine. The reason is a single, beautiful
mapping: our central idea, "an agent is a keyed, stateful, event-driven
processor, one logical instance per conversation, processing its events in
order," is *exactly* a **named Ray actor per `conversationId`**. A
`@ray.remote class ConversationAgent` is inherently single-writer (one actor =
one Python instance), inherently ordered (actor method calls execute serially on
the actor's event loop), and inherently an in-memory state holder (its fields).
That is C1+C2 (in memory) handed to you for free, with none of the
keyBy/partition ceremony — you address an actor *by conversation id*
(`get_if_exists=True`) and Ray routes the call to the one live instance. Make
the methods `async def` (async actors) and LLM/tool/A2A awaits are native (C4);
fan tool calls out as Ray **tasks** for parallelism. Ray **Serve** is the
inbound edge (the A2A/RAG HTTP proxy that routes each request to the right
conversation actor); Ray **Data** does batch embed/index of the cold corpus. You
keep the entire §4a portable core verbatim in Python — the ConversationStore
SPI, `TwoTierRetriever`, the ReAct/TurnBrain loop, the resilient A2A client, the
tool registry — and re-implement only the §4c seam, which on Ray is mostly "wrap
a class in `@ray.remote`."

The **load-bearing caveat is durability (C3)**: actor state is *volatile*. If a
worker dies, the actor and its in-memory state are gone. So the single most
important porting decision (the §8 cross-cutting concern) is: **checkpoint
per-conversation state to the ConversationStore (Redis/Fluss/Postgres) on every
turn** — which is *precisely* what that SPI exists for. The actor becomes a
write-through in-memory cache over a durable store; on actor restart, hydrate
from the store. What you **give up** versus Flink: continuous streaming (events
arrive via Serve/queues, not a Kafka topology), exactly-once, event-time/
windowing/CEP, and topology-level backpressure (you get Serve/ref-level
backpressure instead). For request/response conversational agents that is a fine
trade; for a continuous-ingest analytics spine it is the wrong tool.

## 2. Capability mapping (C1..C12)

Following the §6 matrix's **Ray** column. Legend: **N**ative · **L**ibrary/
idiom · **X**ternal service · **—** drop.

| Cap | §6 | Ray mechanism |
|-----|:--:|---------------|
| **C1** durable keyed state | **N\*** (actor fields) **+ X** for durability | The actor's Python fields *are* the per-conversation state (short-term memory, `RoutingBudget`, A2A `contextId`, dedup set) — fast, local, single-writer. But fields are **volatile**: durability comes from write-through to the ConversationStore (Redis/Fluss/Postgres) on each turn. `*` = native in-memory, external for survival. This is the keystone caveat. |
| **C2** per-key ordered proc | **N** | A **named actor per `conversationId`** is single-writer by construction; the actor **mailbox** serializes method calls so a conversation's turns execute in arrival order — the same guarantee Flink's `keyBy` gives, achieved by *addressing* rather than partitioning. (Async actors interleave at `await` points; see §5.) |
| **C3** fault tolerance / EOS | **X** | No engine-level checkpointing. Use `max_restarts` to auto-respawn a dead actor, but its in-memory state is lost — **rehydrate from the ConversationStore on `__init__`/restart**. No exactly-once; lean on idempotent tools/A2A + a per-turn dedup guard persisted in the store. |
| **C4** async I/O | **N** | **Async actors** (`async def` methods) run on an asyncio loop — `await llm(...)`, `await tool(...)`, `await peer.send(...)` are native and non-blocking. Parallel tool fan-out = Ray **tasks** (`ray.get([f.remote(x) for x in ...])`). No Async-I/O-operator bridge. |
| **C5** backpressure | **L** | Not a streaming topology, so no end-to-end backpressure. Bound it with Serve **autoscaling + `max_ongoing_requests`**, `max_concurrency` on the actor, and an `asyncio.Semaphore` over model calls. Object-ref dependencies pace task fan-out. |
| **C6** connectors | **L** | No Flink-style connector model. **Ray Serve** (HTTP/gRPC ingress) and **Ray Data** (`read_parquet`/`read_*`, batch I/O) cover most; Kafka/Redis/webhook `Channel<T>` transports become small async producers/consumers or a Serve deployment feeding actors. |
| **C7** side outputs | **L** | An actor method returning a tuple, or publishing to a separate sink actor / log / queue. The debug stream and tool-invocation channel become extra method emits or a dedicated `@ray.remote` collector actor. |
| **C8** broadcast state | **L** | A single **named broadcast actor** (control-plane directives, enrichment dims) that other actors read, or push immutable snapshots into the **object store** (`ray.put`) and hand out the ref. No native broadcast-state operator. |
| **C9** event-time / windows | **—** | Not a fit. Ray has no watermarks/windowing. Drop, or push streaming-analytics flows to Flink/a warehouse. |
| **C10** CEP | **—** | No CEP. Hand-roll a small state machine inside an actor over its last-N events if genuinely needed; otherwise drop. |
| **C11** distributed scale | **N** | Native. The Ray cluster places actors and tasks across nodes; thousands of lightweight conversation actors spread automatically; Serve autoscales replicas. Scale is one of Ray's core strengths. |
| **C12** topology builder | **N** | The "topology" is the **actor/task graph**: a router actor (or function) dispatches to path actors, which call a verifier actor; tasks compose by passing object refs. The DAG is expressed in plain Python control flow, not a builder DSL. |

The crucial line from the keystone §3 holds: **C1+C2 are the heart, and Ray
gives them natively in memory** — the actor-per-key model is arguably *cleaner*
than Flink's `keyBy` because there is no partition arithmetic. The price is C3:
you trade Flink's automatic checkpointing for explicit write-through to a store
you already have an SPI for.

## 3. The core abstractions on this engine

A Python mirror of the §4c Engine SPI. The portable core (§4a) is consumed
unchanged; only this seam is Ray-specific.

### Agent — a named actor per conversation

The Flink `KeyedProcessFunction` becomes a `@ray.remote` class. One actor
*instance* per `conversationId`; `get_if_exists=True` makes "get-or-create the
actor for this conversation" atomic, so the inbound edge never races two actors
for the same key. `max_concurrency` lets an async actor process overlapping I/O
without losing per-conversation ordering at the await boundary (§5 covers the
nuance).

```python
import ray

@ray.remote(max_restarts=-1, max_concurrency=8)   # auto-respawn; async actor
class ConversationAgent:
    def __init__(self, conversation_id: str, store, tools, brains):
        self.cid = conversation_id
        self.store = store                 # ConversationStore SPI (durable, §4a)
        self.tools = tools                 # ToolRegistry (portable, §4a)
        self.brains = brains               # path -> ReAct TurnBrain (portable)
        # C3: rehydrate volatile working memory from the durable store on (re)start
        self.short_term = store.load_short_term(conversation_id) or {}

    async def handle_turn(self, turn: dict) -> dict:
        # C3 idempotency: skip a re-delivered turn (at-least-once safety)
        if self.store.get_attribute(self.cid, f"done.{turn['turn_id']}"):
            return self.store.last_response(self.cid)
        self.store.append(self.cid, user_msg(turn["text"]))   # durable transcript
        out = await self._run(turn)                           # ReAct loop (portable)
        self.store.put_attribute(self.cid, f"done.{turn['turn_id']}", "1")
        return out

def agent_for(cid: str, store, tools, brains) -> "ray.actor.ActorHandle":
    """The §4c keyedAgent(key) lookup: get-or-create the single actor for a key."""
    return ConversationAgent.options(
        name=f"conv:{cid}", namespace="agentic",
        get_if_exists=True, lifetime="detached",      # survives the caller
    ).remote(cid, store, tools, brains)
```

`name=f"conv:{cid}"` + `get_if_exists=True` is the Ray realization of
`keyedAgent(KeySelector)`: the *name* is the key, and Ray guarantees one live
actor per name. `lifetime="detached"` keeps the conversation actor alive across
the request that created it (a multi-turn session outlives any one HTTP call).

### Keyed state — `KeyedStateStore` = actor fields + write-through

The §4c `KeyedStateStore` (`get/put/update/clear` for a key) has *two* halves on
Ray: the **hot** half is the actor's own fields (no remote hop, single-writer);
the **durable** half is the ConversationStore. The portable C1 replacement for
`FlinkStateShortTermMemory` is a write-through wrapper the actor holds:

```python
class ActorStateStore:
    """Engine SPI KeyedStateStore: in-actor dict, write-through to the durable store."""
    def __init__(self, cid: str, store):
        self.cid, self.store = cid, store
        self._mem = store.load_short_term(cid) or {}     # hydrate on construct
    def get(self, name, default=None):  return self._mem.get(name, default)
    def put(self, name, value):
        self._mem[name] = value
        self.store.save_short_term(self.cid, self._mem)  # C3: persist each write
    def update(self, name, fn):         self.put(name, fn(self._mem.get(name)))
    def clear(self):                    self._mem.clear(); self.store.clear(self.cid)
```

Because the dict lives *inside the actor* and the actor is single-writer, reads
are free and writes are local; the only remote cost is the durability flush,
which you can batch to once-per-turn rather than per-mutation if the store round
trip dominates.

### ConversationStore — the durable spine (Redis/Fluss/Postgres)

The `ConversationStore` interface (append / history / recent / attributes /
user-index) ports directly and is **more central on Ray than anywhere else**: it
is not just the cross-operator sharing layer, it is the *only* durability the
agent has. The router, path, and verifier (whether separate actors or methods on
one actor) all read/write the same store keyed on `conversationId`, so a
conversation split across actors still progresses — exactly the gap §4a says the
ConversationStore fills.

```python
import redis.asyncio as redis, json

class RedisConversationStore:           # implements the ConversationStore SPI
    def __init__(self, url, max_msgs=200):
        self.r, self.max = redis.from_url(url), max_msgs
    def append(self, cid, msg):
        if cid is None: return
        self.r.rpush(f"cs:tx:{cid}", json.dumps(msg))
        self.r.ltrim(f"cs:tx:{cid}", -self.max, -1)        # bounded transcript
    def history(self, cid):
        return [json.loads(m) for m in self.r.lrange(f"cs:tx:{cid}", 0, -1)]
    def recent(self, cid, n):
        h = self.history(cid); return h if n <= 0 else h[-n:]
    def put_attribute(self, cid, k, v): self.r.hset(f"cs:attr:{cid}", k, v)
    def get_attribute(self, cid, k):
        v = self.r.hget(f"cs:attr:{cid}", k); return v.decode() if v else None
    def associate_user(self, cid, uid): self.r.sadd(f"cs:user:{uid}", cid)
    def conversations_for_user(self, uid):
        return [c.decode() for c in self.r.smembers(f"cs:user:{uid}")]
    # short-term helpers used by ActorStateStore (C3 rehydrate/persist)
    def load_short_term(self, cid):
        v = self.r.get(f"cs:st:{cid}"); return json.loads(v) if v else None
    def save_short_term(self, cid, mem): self.r.set(f"cs:st:{cid}", json.dumps(mem))
```

The **in-JVM shared map** default (`InMemoryConversationStore.shared()`) maps to
a single named `@ray.remote` store actor for embedded single-cluster dev — but
note even that disappears on cluster restart, so Redis/Fluss is the production
default exactly as in the Flink design.

### Tools — async functions, fan out as Ray tasks

`ToolExecutor` (`Map<String,Object> → CompletableFuture<Object>`) becomes an
`async def(params: dict) -> Any`; a registry is a dict. Independent tool calls in
one ReAct step fan out as Ray **tasks** (parallel across the cluster), then
`await` the gather.

```python
class ToolRegistry:
    def __init__(self): self._tools = {}
    def register(self, name, fn): self._tools[name] = fn
    async def invoke(self, name, params: dict):
        return await self._tools[name](params)

@ray.remote
def run_tool(name, params, registry):       # CPU/IO tool as a distributed task
    return asyncio.run(registry.invoke(name, params))

async def fan_out(calls, registry):          # parallel tool fan-out (C4)
    refs = [run_tool.remote(n, p, registry) for n, p in calls]
    return await asyncio.gather(*[r for r in refs])   # ray refs are awaitable
```

For pure-I/O tools (the common case — HTTP to a core-banking API), skip the task
and just `await` inside the actor; reserve Ray tasks for CPU-heavy or genuinely
parallel fan-out where distribution earns its keep.

### Async — native async actors, with a concurrency cap

Flink's Async-I/O operator (bounded in-flight) maps to an async actor +
`max_concurrency` + an `asyncio.Semaphore` over the model endpoint (C5).

```python
llm_gate = asyncio.Semaphore(8)              # bound concurrent model calls
async def llm(messages, **kw):
    async with llm_gate:
        return await chat_client.chat(messages, **kw)   # portable ChatConnection
```

### Routed graph — router → path → verifier

Two idiomatic shapes; both keep the conversation single-writer:

**(a) One conversation actor, methods for each stage** (lowest latency, simplest,
single-writer guaranteed):

```python
async def _run(self, turn):
    path   = router(turn)                        # rule-based classify (no LLM)
    answer = await self.brains[path](turn, self.store, self.tools)  # ReAct path
    return verifier(turn, path, answer, self.store)   # advance phase, validate
```

**(b) Dedicated router/path/verifier actors** (independent scaling / isolation):
a router actor (or function) dispatches to a *path-pool* actor, which calls a
verifier actor — all passing `conversationId` so each stage reads the shared
ConversationStore.

```python
@ray.remote
class Router:
    async def route(self, turn):  return router(turn)          # no LLM, stateless

@ray.remote(max_concurrency=16)
class PathPool:                                 # one pool, brains keyed by path
    def __init__(self, store, tools, brains): self.store, self.tools, self.brains = store, tools, brains
    async def run(self, turn, path):
        return await self.brains[path](turn, self.store, self.tools)

# orchestration (in the conversation actor or a Serve deployment):
path   = await router_actor.route.remote(turn)
answer = await path_pool.run.remote(turn, path)
reply  = verifier(turn, path, answer, store)
```

Shape (a) mirrors the single-operator banking brain; shape (b) mirrors
`BankingAgentGraph`'s multi-operator DAG. **Cross-turn chaining** (e.g. `GATHER`
→ next turn `ACTION`) happens through the shared `phase` attribute in the
ConversationStore, *not* through a cycle — identical to the Flink design with its
`PhaseStore`/`ConversationMemory`. Use shape (a) by default; shape (b) only when
a path is heavy enough to want its own autoscaling pool.

### A2A — aiohttp/httpx client, resilient (retry/backoff/breaker)

The A2A protocol types + `RemoteAgentSpec` port directly; `SdkA2AClient` becomes
a small async client with **retry / backoff / circuit-breaker** (the resilient
behaviour `A2AClient` mandates). `A2AStep.applyToStateful` (Flink's keyed pre →
async → keyed post split, which exists *because* Flink's async operator can't
hold keyed state) **collapses to nothing on Ray**: the conversation actor is
*already* both single-writer-keyed and async, so the round trip is one method —
read `contextId`, `await` the peer, write `contextId` back.

```python
class A2AClient:                              # resilient: retry/backoff/breaker
    def __init__(self, spec): self.spec, self._breaker = spec, CircuitBreaker(5, reset_s=30)
    async def send(self, text, context_id=None):
        if self._breaker.open: raise CircuitOpen(self.spec.name)
        for attempt in range(self.spec.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.spec.timeout_s) as c:
                    r = await c.post(self.spec.url, json=rpc("message/send",
                            {"text": text, "contextId": context_id}))
                    self._breaker.record_success()
                    return r.json()["result"]
            except (httpx.HTTPError, asyncio.TimeoutError):
                self._breaker.record_failure()
                await asyncio.sleep(min(2 ** attempt * 0.2, 5.0))   # backoff
        raise A2AError(self.spec.name)

# A2AStep.applyToStateful — keyed + async fused into one actor method:
async def a2a_step(self, turn, client: "A2AClient"):
    ctx = self.store.get_attribute(self.cid, "a2a.contextId")    # durable continuity
    res = await client.send(turn["text"], context_id=ctx)         # non-blocking
    self.store.put_attribute(self.cid, "a2a.contextId", res["contextId"])
    return res["artifact"]["text"]
```

Exposing a peer *as a tool* (`A2AToolExecutor`) is just registering `a2a_step`'s
closure in the `ToolRegistry`. Note Ray gives you a *bonus* the Flink design
works hard for: because the actor is the single writer, there is no
keyed→async→keyed split to engineer — `applyToKeyed` and `applyToStateful`
become the same trivial method.

### Inbound proxy — a Ray Serve deployment

The Quarkus A2A gateway maps to a **Ray Serve** deployment: it hosts the A2A
JSON-RPC/SSE/REST endpoints, resolves the conversation actor by id
(`get_if_exists=True`), and `await`s the turn. Serve gives HTTP ingress,
autoscaling, and `max_ongoing_requests` backpressure (C5) for free, and it lives
*in the Ray cluster* so the hop to the actor is in-cluster.

```python
from ray import serve
from fastapi import FastAPI

api = FastAPI()

@serve.deployment(num_replicas="auto", max_ongoing_requests=64)
@serve.ingress(api)
class A2AGateway:
    def __init__(self, store, tools, brains):
        self.store, self.tools, self.brains = store, tools, brains
    @api.post("/a2a/message:send")
    async def send(self, body: dict):
        cid = body.get("contextId") or new_context_id()
        self.store.associate_user(cid, body["userId"])
        agent = agent_for(cid, self.store, self.tools, self.brains)   # route to actor
        reply = await agent.handle_turn.remote({                       # await the turn
            "text": body["params"]["text"], "turn_id": str(uuid4())})
        return {"status": "ok", "contextId": cid, **reply}
```

Synchronous JSON-RPC replies are natural here (unlike the Faust topic-correlation
dance): the Serve handler simply `await`s the actor method and returns. For SSE,
stream tokens out of the actor via an async generator method.

### RAG — Ray Data for cold index, in-memory/actor hot tier

`TwoTierRetriever` (hot recent window ∪ cold durable corpus, dedup by id, degrade
if either tier fails) ports verbatim — it has no Flink in it. The **cold** index
is built with **Ray Data** (distributed batch embed + write to pgvector/Qdrant
via the `VectorStore` SPI); the **hot** tier is an in-memory index inside a named
index actor (or object-store-backed for sharing). `RetrievalPipeline`
(embed → search → rerank → answer) becomes an async function chain, and the
batch-ingest map becomes a Ray Data pipeline.

```python
# COLD: distributed batch embed + index with Ray Data (the §4a ingestion pipeline)
import ray.data
def embed_batch(batch):                       # vectorized embed over a block
    return {"id": batch["id"], "vec": embed_model.embed_many(batch["text"]),
            "text": batch["text"]}
(ray.data.read_parquet("s3://corpus/")        # cold corpus
    .map_batches(chunk_batch)                 # RecursiveTextChunker (portable)
    .map_batches(embed_batch, num_gpus=1)     # parallel across the cluster
    .write_sql(VECTOR_STORE_DSN, ...))        # -> pgvector / Qdrant (VectorStore SPI)

# HOT: a named actor holding the recent-window index (TwoTierRetriever ports as-is)
@ray.remote
class HotIndex:
    def __init__(self): self._items = []
    def add(self, id, vec, text): self._items.append((id, vec, text))
    def search(self, qv, k): return brute_force_knn(self._items, qv, k)   # portable

async def retrieve_answer(question, hot_handle, cold_search) -> "Answer":
    qv   = await embed(question)                          # EmbeddingClient SPI
    hot  = await hot_handle.search.remote(qv, 6)
    hits = TwoTierRetriever(hot, cold_search, 6, 6).retrieve(qv, 6)  # dedup, degrade
    hits = await rerank(question, hits)                   # cross-encoder (optional)
    ctx  = "\n".join(f"[{i+1}] {h.text}" for i, h in enumerate(hits[:3]))
    resp = await llm([sys("Answer using ONLY the numbered sources; cite [n]."),
                      user(f"Sources:\n{ctx}\nQuestion: {question}")])
    return Answer(question, resp.text.strip(), hits[:3])
```

The hot tier keeps retrieval *live*: a doc is searchable the instant it lands in
the hot actor, before the (batch, Ray Data) cold index catches up — same
semantics as the Java `TwoTierRetriever`. Ray Data is genuinely *better* than
Flink here for the cold path: distributed batch embed across GPUs is its home
turf.

## 4. Worked example — the banking router→path→verifier, end-to-end

Faithful port of `BankingAgentGraph`: a rule-based router screens + classifies
(no LLM), a per-path ReAct brain handles the turn (LLM only here), a rule-based
verifier advances the cross-turn phase and emits the A2A response. The whole
Router→Path→Verifier DAG for one turn runs inside the **conversation actor**
(shape 3a), which is single-writer for that `contextId` (== `conversationId`).
Paths mirror the Java enum
(`KNOWLEDGE, GATHER, DELEGATE, ACTION, DISPUTE, ESCALATE, REFUSE`); phases mirror
`NEW → NEED_INFO → READY_TO_ACT → ACTED → DONE`. Durability is the
ConversationStore (Redis) — the actor is a write-through cache over it.

```python
import ray, asyncio, httpx
from uuid import uuid4
from ray import serve
from fastapi import FastAPI

ray.init(namespace="agentic")
store = RedisConversationStore("redis://localhost:6379")   # durable spine (C3)

# ---- tools (idempotent on turn_id; portable ToolRegistry) ----
tools = ToolRegistry()
async def get_balance(p):  ...          # HTTP to core-bank, read-only
async def open_dispute(p): ...          # idempotent: keyed on p["turn_id"]
tools.register("get_balance", get_balance)
tools.register("open_dispute", open_dispute)

DISPUTE = A2AClient(RemoteAgentSpec(
    name="disputes", url="http://disputes:8080/a2a", timeout_s=20, max_retries=3))

# ---- ROUTER: rule-based screen + classify, NO LLM ----
def router(turn) -> str:
    t = turn["text"].lower()
    if any(w in t for w in ("hack", "ignore previous", "ssn")): return "REFUSE"
    if "dispute" in t or "fraud" in t:                          return "DISPUTE"
    if "balance" in t or "how much" in t:                       return "KNOWLEDGE"
    if "transfer" in t or "pay" in t:                           return "ACTION"
    if "agent" in t or "human" in t:                            return "ESCALATE"
    return "GATHER"

# ---- VERIFIER: rule-based, advances the cross-turn phase ----
def verifier(turn, path, answer, store) -> dict:
    cid = turn["context_id"]
    phase = store.get_attribute(cid, "phase") or "NEW"
    if   path == "REFUSE":     phase = "DONE"
    elif path == "ACTION":     phase = "ACTED"
    elif path == "GATHER":     phase = "NEED_INFO"
    elif phase == "NEED_INFO": phase = "READY_TO_ACT"
    store.put_attribute(cid, "phase", phase)
    store.append(cid, assistant_msg(answer))            # record the turn (durable)
    return {"text": answer, "phase": phase}

# ---- THE CONVERSATION ACTOR: one per contextId, single-writer, write-through ----
@ray.remote(max_restarts=-1, max_concurrency=8, lifetime="detached")
class BankingAgent:
    def __init__(self, cid, store, tools):
        self.cid, self.store, self.tools = cid, store, tools
        self.brains = {                                  # focused ReAct brains (LLM)
            "KNOWLEDGE": self._knowledge, "ACTION": self._action,
            "GATHER":    self._action,    "DISPUTE": self._dispute,
            "ESCALATE":  self._refuse,    "REFUSE":  self._refuse,
        }

    async def handle_turn(self, turn) -> dict:
        cid = turn["context_id"]
        if self.store.get_attribute(cid, f"done.{turn['turn_id']}"):   # C3 idempotency
            return self.store_last_response(cid)
        self.store.append(cid, user_msg(turn["text"]))    # durable transcript
        path   = router(turn)                             # no LLM
        answer = await self.brains[path](turn)            # LLM only on the path
        reply  = verifier(turn, path, answer, self.store)
        self.store.put_attribute(cid, f"done.{turn['turn_id']}", "1")
        return reply

    # --- path brains: LLM lives here, router/verifier are rule-based ---
    async def _knowledge(self, turn):
        bal = await self.tools.invoke("get_balance", {"account": turn["user_id"]})
        msg = await llm(self.store.recent(self.cid, 10) +
                        [system(f"Account balance: {bal}")])
        return msg.text
    async def _action(self, turn):
        return await react_loop(turn, self.store, self.tools, max_iters=6)  # ReAct
    async def _dispute(self, turn):                       # delegate to a peer (A2A)
        ctx = self.store.get_attribute(self.cid, "a2a.contextId")
        res = await DISPUTE.send(turn["text"], context_id=ctx)   # resilient client
        self.store.put_attribute(self.cid, "a2a.contextId", res["contextId"])
        return res["artifact"]["text"]
    async def _refuse(self, turn): return "I can't help with that request."

def banking_agent_for(cid):
    return BankingAgent.options(name=f"conv:{cid}", namespace="agentic",
                                get_if_exists=True).remote(cid, store, tools)

# ---- INBOUND EDGE: Ray Serve hosts the A2A endpoint, routes to the actor ----
api = FastAPI()
@serve.deployment(num_replicas="auto", max_ongoing_requests=64)
@serve.ingress(api)
class BankingGateway:
    @api.post("/a2a/message:send")
    async def send(self, body: dict):
        cid = body.get("contextId") or str(uuid4())
        store.associate_user(cid, body["userId"])
        agent = banking_agent_for(cid)                    # single-writer per session
        reply = await agent.handle_turn.remote({
            "context_id": cid, "user_id": body["userId"],
            "text": body["params"]["text"], "turn_id": str(uuid4())})
        return {"status": "ok", "contextId": cid, **reply}

serve.run(BankingGateway.bind())
```

One A2A turn = Router → Path → Verifier, a clean DAG with no cycle, all running
inside the single-writer conversation actor. Multi-step chaining (e.g. `GATHER` →
next turn `ACTION`) happens **across turns** via the durable `phase` attribute
and transcript in the ConversationStore — identical to the Flink
`PhaseStore`/`ConversationMemory` design. Concurrent sessions are isolated
because each has its *own* actor (the Ray analog of `keyBy(contextId)`). Reserving
the LLM for path brains (router + verifier are rule-based) keeps the model-call
count per turn flat — the same cost discipline as the Java example.

To split this into shape (3b) — a dedicated `Router` actor, a `PathPool`, and a
verifier — every stage still passes `context_id` and shares the same `store`, so
the ConversationStore stays the integration point across actors, exactly as the
multi-operator `BankingAgentGraph` integrates through its shared stores.

## 5. What doesn't fit (honest gaps + workarounds)

- **Volatile actor state (C3) — the keystone caveat.** An actor's in-memory
  fields die with its worker. `max_restarts=-1` respawns the actor, but it comes
  back *empty*. *Workaround (mandatory):* write-through to the ConversationStore
  (Redis/Fluss/Postgres) every turn and rehydrate in `__init__`. This is the
  single most important porting decision — exactly the §8 cross-cutting concern —
  and the entire reason the ConversationStore SPI exists. Do **not** treat the
  actor as the source of truth.
- **Not a continuous-streaming engine (C6/C9).** Events arrive via Serve/HTTP or
  a queue you poll, **not** a Kafka topology. There is no source/sink connector
  model, no event-time, no windows, no watermarks. *Workaround:* fine for
  request/response agents; if you need a streaming spine, this is the wrong
  engine — use **Flink** or **Faust**. (This is the honest, load-bearing
  difference vs. those engines.)
- **No exactly-once.** Re-delivery (a retried HTTP call, a respawned actor
  re-reading a queue) can re-run a turn. *Workaround:* the `done.{turn_id}` guard
  persisted in the store + idempotent tools/A2A (the resilient client already
  assumes idempotency).
- **Async-actor ordering subtlety (C2).** With `max_concurrency>1`, an async
  actor *interleaves* methods at `await` points — so two turns for the same
  conversation can overlap and the second can observe partial state from the
  first. *Workaround:* keep `max_concurrency=1` for strict per-conversation
  serialization, or guard a turn with an `asyncio.Lock` keyed implicitly by the
  actor (one actor = one conversation, so a single instance lock suffices). The
  Flink keyed operator serializes for free; on Ray you choose.
- **Backpressure is not end-to-end (C5).** No topology means no propagated
  backpressure. *Workaround:* Serve `max_ongoing_requests` + autoscaling at the
  edge, `max_concurrency` per actor, an `asyncio.Semaphore` over the model. Good
  enough to protect rate limits; not the same as Flink's credit-based flow
  control.
- **Actor-per-conversation cardinality.** Millions of *idle* conversations =
  millions of actors is wasteful. *Workaround:* idle-evict actors (a TTL sweep
  that kills actors with no recent turn; state is safe in the store) and lazily
  recreate via `get_if_exists=True` on the next turn. The store, not the actor, is
  the durable home — so eviction is free.
- **No broadcast-state / CEP / windowing operators (C8/C9/C10).** Hand-roll
  broadcast as a named actor or `ray.put` snapshot; drop CEP/windowing or push
  those flows to Flink. None are load-bearing for the agentic core.

## 6. When to choose Ray

Choose Ray when:

- You want **pure Python, no JVM, no JPype**, and your shape is **request/response
  multi-turn conversation** rather than a continuous stream — the
  actor-per-conversation model is the most *idiomatic Python expression* of the
  stateful-agent essence in this whole series.
- You value the **single-writer-per-conversation guarantee handed to you by
  addressing** (`get_if_exists=True`), with no partition arithmetic, and you are
  comfortable making the **ConversationStore the durable source of truth** (you
  should be — the SPI is built for it).
- You need **heavy parallelism / distribution**: thousands of conversation actors
  spread across a cluster, parallel tool fan-out as tasks, and **distributed batch
  RAG** (Ray Data embed/index across GPUs is genuinely best-in-class here).
- Your team thinks in **`async def`** and wants LLM/tool/A2A calls to be ordinary
  awaits inside an actor — and you want the inbound A2A/RAG HTTP edge (**Ray
  Serve**) to live *in the same cluster* as the agents.

Prefer something else when: you need a **continuous streaming spine**, event-time
windowing, CEP, or exactly-once (→ stay on **Flink**, or **Faust**/**Kafka
Streams** for Kafka-native keyed streaming); your workload is **scheduled/batch
agentic DAGs** (→ **Airflow**); or you want durable keyed state as an *engine*
primitive rather than a store you checkpoint to by hand (→ **Faust** Tables /
**Kafka Streams** state stores).

> Among the targets in this series, Ray is the paradox: it is **not** a streaming
> engine, yet `@ray.remote class ConversationAgent` is arguably the *cleanest*
> expression of the project's deepest idea — a stateful thing per conversation,
> processed in order, that remembers, routes, uses tools, and delegates. You pay
> for that elegance once, in C3: the actor is a fast in-memory cache, and the
> ConversationStore is the truth.
