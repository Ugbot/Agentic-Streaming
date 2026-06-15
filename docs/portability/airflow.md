# Agentic Streaming on Apache Airflow — pure Python, no JVM

> Per-engine portability doc. Read [`00-essence-and-core-abstractions.md`](./00-essence-and-core-abstractions.md)
> first — this doc is written against its essence (§2), capability inventory
> (§3, C1..C12), Engine SPI (§4c), matrix (§6), and ranked fit (§7), and follows
> the §9 six-section template. Target: pure Python (Airflow 2.7+ TaskFlow API), no
> JVM, no JPype.

## 1. Verdict

**Airflow is not streaming — and that is the whole story.** It is a
scheduled/triggered **orchestration** engine: a scheduler walks a DAG of tasks,
each task is a *process* (or pod) that runs to completion, the unit of work is a
**DAG run** triggered by a cron schedule, a dataset update, an external sensor,
or a manual/REST trigger. Latency is seconds-to-minutes per task, not
microseconds per event. So the project's **live keyed-stateful agent core
(essence §2.1, C1+C2) does not belong here** and you should not try to host it.

But the project has a second face, and Airflow fits it almost perfectly: the
**routed graph maps cleanly onto a DAG**, and everything *batch/scheduled* —
RAG ingestion, eval sweeps, human-in-the-loop approvals — is exactly what
Airflow was built to run, with **retries, idempotency, backfill, SLAs, sensors,
dataset-aware scheduling, and first-class observability** thrown in for free.

Keep on Airflow, idiomatically:

- **The routed graph as a DAG** (essence §2.3): router = `@task.branch` /
  `BranchPythonOperator`; paths = `@task` (or `TaskGroup`s); verifier = a
  downstream `@task` with a fan-in `trigger_rule`. One DAG run = one request's
  router→path→verifier traversal.
- **RAG ingestion DAGs** (essence §2.2, the `IngestionPipeline` logic): crawl →
  chunk → embed → build the **cold** index, with sensors waiting on source
  freshness, per-task retries with backoff, **backfill** to re-ingest a date
  range, and `@dataset`-aware scheduling so a downstream eval DAG fires the
  moment the index is rebuilt.
- **Eval / benchmark runs**: a parametrised DAG (or dynamic task mapping over a
  config grid) that replays the retrieval/routed pipeline over a labelled set
  and writes a recall@k / nDCG / latency table — scheduled nightly, backfillable.
- **Human-in-the-loop**: a sensor (`@task.sensor` / deferrable trigger) that
  parks the run until an approval lands, then resumes the downstream path. This
  is the one place Airflow's "wait for the world" model is a *feature*.

Give up, deliberately: **per-event keyed state, single-writer-per-conversation
streaming, low-latency conversation.** Airflow schedules *runs*, not events.
Per-conversation state lives in an **external `ConversationStore`** (Redis /
Postgres via the existing SPI), never in XCom — XCom is for small inter-task
payloads only. What you keep unchanged: the entire pure core (§4a) — `Chunker`,
`TwoTierRetriever`, embed→search→rerank→answer *logic*, the `ToolExecutor`
contract, the A2A protocol + resilient client. What you re-implement: the agent
*operator* — you don't; you decompose its *turn* into tasks and persist state
out-of-band.

This port captures the **workflow/orchestration + batch-data essence**
(retries, idempotency, backfill, scheduling, observability, branching DAGs) and
**gives up the live streaming-agent essence** entirely. That is the honest deal.

## 2. Capability mapping (C1..C12 — Airflow column of §6)

| Cap | §6 | On Airflow | Mechanism / honest note |
|-----|:--:|------------|-------------------------|
| **C1** keyed durable state | **X** | external store (XCom tiny) | XCom is a *per-run, per-task* return-value passing channel backed by the metadata DB — fine for "the branch label" or "the chunk count", **not** a keyed conversation store (small payloads, GC'd per run, no cross-run key index). Real per-conversation state = `ConversationStore` on Redis/Postgres. |
| **C2** per-key ordered proc | **—** | per-run, not per-key | No `keyBy`→partition→single-writer. A DAG run processes *one* request; concurrent runs of the same conversation are not serialised by the engine. If two turns of one conversation must not interleave, you enforce it with a `max_active_runs=1` per-conversation DAG, or optimistic concurrency / a lock in the store. There is no native single-writer-per-key. |
| **C3** fault tolerance | **N** | task retry + idempotency | Airflow's core competence: `retries`, `retry_delay`, exponential backoff, `on_failure_callback`, clear-and-rerun, **backfill**. Combined with idempotent tasks (deterministic chunk ids, idempotent upserts) this is *stronger* operational recovery than a streaming checkpoint for batch work. No exactly-once across external side-effects — lean on idempotency. |
| **C4** async I/O | **L** | deferrable operators / async-in-task | A task is a process; inside it you can run `asyncio.gather` over LLM/embedding calls. For *waiting* (a slow peer, an approval) use **deferrable operators** (triggers run on the async triggerer, freeing the worker slot). Not a per-event async operator — async *within* a task, or defer-and-resume. |
| **C5** backpressure | **—** | not a fit (batch) | No end-to-end streaming backpressure. You bound concurrency with `pool` slots, `max_active_tasks`, `max_active_runs`, and a `Semaphore` inside a task to protect the model endpoint. That is rate-limiting, not backpressure. |
| **C6** connectors | **L** | hooks / operators / providers | The provider ecosystem *is* the connector model: `PostgresHook`, `RedisHook`, S3/GCS, Kafka provider, HTTP. Bounded/triggered sources/sinks, not a live subscription. Maps the `Channel<T>` *intent* (named typed source/sink) onto hooks. |
| **C7** side outputs | **L** | extra task / `multiple_outputs` | A `@task(multiple_outputs=True)` returns a dict fanned to several XCom keys; the debug/metrics side-channel is just another downstream task. Trivial. |
| **C8** broadcast state | **L** | Variables / Params / Connections | Control-plane directives and enrichment dims live in Airflow `Variable`s, DAG `Params`, or `Connection`s — read at parse/run time. Idiomatic for "the active model id / prompt version / feature flag." |
| **C9** event-time/windows | **—** | not a fit | No streaming time model. Date-partitioned *runs* (`logical_date`, data intervals) give batch windowing, which is enough for ingestion partitioning but is not event-time/watermarks. |
| **C10** CEP | **—** | not a fit | No pattern-over-stream. Out of scope. |
| **C11** distributed scale | **N** | executors / workers | CeleryExecutor / KubernetesExecutor fan tasks across workers/pods; dynamic task mapping scales fan-out. Native and proven for batch parallelism. |
| **C12** topology builder | **N** | the DAG | The DAG *is* the topology builder, and it is excellent: TaskFlow `@dag`/`@task`, `>>` deps, `TaskGroup`s, branching, fan-in `trigger_rule`s. The routed-graph pattern is a *native* expression here. |

The shape of the column is the verdict: **C3/C11/C12 native, the connector/
async/broadcast capabilities L, and the entire live-streaming core (C2, C5, C9,
C10) flat `—`, with C1 pushed `X`ternal.** Airflow gives you the *orchestration*
capabilities for free and asks you to give up the *streaming* ones.

## 3. The core abstractions on Airflow

### 3a. The pure core ports verbatim (logic, not Flink)

`Chunker`, `RecursiveTextChunker`, `TwoTierRetriever`, the embed→search→rerank→
answer steps, the `ToolExecutor` contract, and the A2A protocol value types have
**no Flink in them**. In Python they are plain callables and dataclasses,
imported into tasks unchanged. The `TwoTierRetriever` merge (query both tiers,
de-dup by id keeping the higher score, top-k, tolerate either tier failing)
transcribes 1:1:

```python
# pyagentic/retrieve.py — pure, engine-free (mirrors TwoTierRetriever.java)
from dataclasses import dataclass

@dataclass(frozen=True)
class ScoredItem:
    id: str; text: str; score: float

def two_tier_retrieve(hot, cold, query, k, hot_k=20, cold_k=50) -> list[ScoredItem]:
    best: dict[str, ScoredItem] = {}
    def merge(s: ScoredItem):
        prior = best.get(s.id)
        if prior is None or s.score > prior.score:
            best[s.id] = s
    if hot is not None:
        try:
            for s in hot.search(query, hot_k): merge(s)
        except Exception as e:           # degrade to cold only
            log.warning("hot tier failed: %s", e)
    if cold is not None:
        try:
            for s in (cold.search(query, cold_k) or []): merge(s)
        except Exception as e:           # degrade to hot only
            log.warning("cold tier failed: %s", e)
    return sorted(best.values(), key=lambda x: x.score, reverse=True)[:max(1, k)]
```

None of this knows it lives in an Airflow task. That is the point of §4a.

### 3b. Keyed state → external `ConversationStore`, never XCom

Flink's C1 (keyed `ValueState`) has no Airflow equivalent and XCom is the wrong
tool (per-run, tiny, GC'd, no cross-run key index). The portable `ConversationStore`
SPI *is* the answer — back it with Redis or Postgres and every task reads/writes
the same conversation by id:

```python
# pyagentic/conversation_store.py — the §4a SPI, Postgres backend
from typing import Protocol, Optional

class ConversationStore(Protocol):
    def append(self, conversation_id: str, message: dict) -> None: ...
    def history(self, conversation_id: str) -> list[dict]: ...
    def put_attribute(self, conversation_id: str, key: str, value: str) -> None: ...
    def get_attribute(self, conversation_id: str, key: str) -> Optional[str]: ...
    def associate_user(self, conversation_id: str, user_id: str) -> None: ...

class PostgresConversationStore:
    """Mirrors PostgresConversationStore.java — transcript + scalar attrs,
    keyed by conversation_id, user index. Upserts are idempotent so a task
    retry replays cleanly (C3)."""
    def __init__(self, hook):           # airflow.providers.postgres PostgresHook
        self._hook = hook
    def append(self, cid, message):
        self._hook.run(
            "insert into conv_msg(cid, seq, role, content) "
            "values (%s, (select coalesce(max(seq),0)+1 from conv_msg where cid=%s), %s, %s) "
            "on conflict do nothing",
            parameters=(cid, cid, message["role"], message["content"]))
    def history(self, cid):
        rows = self._hook.get_records(
            "select role, content from conv_msg where cid=%s order by seq", (cid,))
        return [{"role": r[0], "content": r[1]} for r in rows]
    def put_attribute(self, cid, key, value):
        self._hook.run("insert into conv_attr(cid,k,v) values(%s,%s,%s) "
                       "on conflict (cid,k) do update set v=excluded.v", (cid, key, value))
    # get_attribute / associate_user / etc. analogous
```

Build it once and stash it behind a small factory tasks call — *not* in XCom (it
holds a DB connection). XCom carries only the small handle the next task needs:
the `conversation_id`, the branch label, the chunk count.

### 3c. Tools → Python callables inside tasks

`ToolExecutor` is already `params → result`. On Airflow a tool is a plain
callable; a registry is a dict; a tool *call* happens inside a task. No async
operator is needed — `asyncio` inside the task fans out concurrent tool/LLM
calls, a `Semaphore` rate-limits them:

```python
# pyagentic/tools.py — mirrors ToolExecutor (Map -> CompletableFuture<Object>)
import asyncio
class ToolRegistry:
    def __init__(self): self._tools = {}
    def register(self, name, fn): self._tools[name] = fn; return self
    async def call(self, name, params: dict): return await self._tools[name](params)

async def run_tools(reg, calls, max_concurrent=4):
    sem = asyncio.Semaphore(max_concurrent)          # protect the model endpoint (C5-as-limit)
    async def one(c):
        async with sem: return await reg.call(c["tool"], c["params"])
    return await asyncio.gather(*(one(c) for c in calls))
```

### 3d. The routed graph → a TaskFlow DAG with branching

This is the clean mapping (essence §2.3). Router = `@task.branch` returning the
id of the path task to run; paths = sibling `@task`s; verifier = a downstream
`@task` with `trigger_rule="none_failed_min_one_success"` so it fans in from
whichever single branch ran (the others are skipped, not failed):

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
     params={"conversation_id": "", "user_id": "", "text": ""})
def routed_triage():
    @task
    def ingest_turn(**ctx) -> dict:
        p = ctx["params"]
        store = get_store()                       # 3b factory
        store.associate_user(p["conversation_id"], p["user_id"])
        store.append(p["conversation_id"], {"role": "user", "content": p["text"]})
        return {"cid": p["conversation_id"], "text": p["text"]}

    @task.branch                                  # ← the router (rule-based, no LLM, like BankingRouterFunction)
    def route(turn: dict) -> str:
        t = turn["text"].lower()
        if any(w in t for w in ("fraud", "stolen", "unauthor")): return "path_fraud"
        if any(w in t for w in ("balance", "transfer", "payment")): return "path_txn"
        return "path_general"

    @task
    def path_fraud(turn: dict) -> dict:  return run_path("FRAUD", turn)     # focused ReAct brain
    @task
    def path_txn(turn: dict) -> dict:    return run_path("TXN", turn)
    @task
    def path_general(turn: dict) -> dict: return run_path("GENERAL", turn)

    @task(trigger_rule="none_failed_min_one_success")   # ← the verifier fan-in
    def verify(turn: dict, *path_results) -> dict:
        result = next((r for r in path_results if r is not None), None)
        store = get_store()
        store.append(turn["cid"], {"role": "assistant", "content": result["answer"]})
        store.put_attribute(turn["cid"], "phase", advance_phase(turn, result))
        return {"cid": turn["cid"], "answer": result["answer"], "verified": True}

    t = ingest_turn()
    branch = route(t)
    f, x, g = path_fraud(t), path_txn(t), path_general(t)
    branch >> [f, x, g]
    verify(t, f, x, g)

routed_triage()
```

`run_path("FRAUD", turn)` is where the *pure* ReAct/TurnBrain logic lives — it
reads `store.history(cid)`, runs think→tool→observe over the `ToolRegistry`,
returns the answer. The brain code is identical to every other port; only its
*invocation site* is an Airflow task. Cross-turn chaining (the banking
`PhaseStore`) is the `put_attribute("phase", …)` line: the next DAG run reads it
back, exactly as Flink's keyed verifier advanced `BankingPhase`.

### 3e. A2A → a custom Operator / Hook over the resilient client

A2A is a `RemoteAgentSpec` + a hardened client (retry/backoff/circuit-breaker).
On Airflow it becomes a `Hook` (the connection + resilient client) plus an
`Operator` (the graph step), and you get **task-level retries on top** of the
client's own — defence in depth:

```python
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

class A2AHook(BaseHook):                 # holds the resilient A2AClient (circuit breaker, backoff)
    def __init__(self, conn_id="a2a_default"): self.conn = self.get_connection(conn_id)
    def send(self, spec, prompt, context_id) -> dict:
        return resilient_a2a_client(self.conn).send_message(spec, prompt, context_id)

class A2AStepOperator(BaseOperator):     # mirrors A2AStep — a fixed delegation in the DAG
    def __init__(self, spec, input_key="text", output_key=None, **kw):
        super().__init__(**kw); self.spec = spec
        self.input_key, self.output_key = input_key, (output_key or f"a2a.{spec.name}")
    def execute(self, context):
        ti = context["ti"]; turn = ti.xcom_pull(task_ids="ingest_turn")
        store = get_store()
        ctx_id = store.get_attribute(turn["cid"], f"{self.spec.name}.contextId")  # cross-turn continuity
        resp = A2AHook().send(self.spec, turn[self.input_key], ctx_id)
        store.put_attribute(turn["cid"], f"{self.spec.name}.contextId", resp["contextId"])
        return {self.output_key: resp["text"]}
```

`retries=4, retry_delay=…, retry_exponential_backoff=True` on the operator gives
the task-level safety net; the `ConversationStore` carries the remote `contextId`
across turns the way `applyToStateful`'s keyed pre/post operators did in Flink.

### 3f. Inbound edge → trigger, not proxy

The inbound edge is **a trigger, not a live low-latency proxy** (and you must say
so plainly). Three idiomatic ways in:

- **REST API** — `POST /api/v2/dags/routed_triage/dagRuns` with a conf body
  (`conversation_id`, `text`). A thin FastAPI/Quarkus front door translates an
  A2A JSON-RPC call into this trigger and *polls* (or SSE-streams) the run state.
  Latency is run-scheduling latency (seconds), not conversational.
- **`@dataset` scheduling** — an upstream producer marks a `Dataset` updated; the
  consumer DAG fires automatically. This is how the eval DAG chains off the
  ingestion DAG.
- **Sensors** — a `@task.sensor` (deferrable) waits on a queue/table/approval and
  starts the downstream work when the condition is met.

The outbound edge (essence §2.5) is a hook write at the end of the DAG: publish
the result to Kafka/Redis/webhook, or just persist to the store and let the
front door read it back.

## 4. A worked example

Two DAGs, the two faces of the fit: an **ingestion DAG** that builds the cold
RAG index, and the **routed triage DAG** (§3d) that consumes it.

### `agentic_ingestion` — crawl → chunk → embed → cold index

```python
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from datetime import datetime
from pyagentic.ingest import RecursiveTextChunker   # pure §4a logic

COLD_INDEX = Dataset("pgvector://corpus/support_kb")  # downstream eval DAGs key off this

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=True,  # ← backfillable
     default_args={"retries": 3, "retry_delay": 60, "retry_exponential_backoff": True})
def agentic_ingestion():

    @task.sensor(poke_interval=300, timeout=3600, mode="reschedule")
    def wait_for_source_freshness(**ctx):                 # park until the crawl source is ready
        from airflow.sensors.base import PokeReturnValue
        ready = source_updated_since(ctx["data_interval_start"])
        return PokeReturnValue(is_done=ready, xcom_value={"since": str(ctx["data_interval_start"])})

    @task
    def crawl(window: dict) -> list[dict]:                # WebFetch/Crawler -> CrawledPage dicts
        return crawl_pages(since=window["since"])         # robots-aware crawler from web/

    @task
    def chunk(pages: list[dict]) -> list[dict]:
        chunker = RecursiveTextChunker(max_chars=512)     # idempotent chunk ids -> safe retry
        out = []
        for p in pages:
            out += [c.__dict__ for c in chunker.chunk(p["url"], p["text"])]
        return out

    @task(max_active_tis_per_dag=8)                       # dynamic mapping bounds embed fan-out (C11)
    def embed(chunk: dict) -> dict:
        return {**chunk, "vector": embed_text(chunk["text"])}

    @task(outlets=[COLD_INDEX])                           # ← signals the dataset on success
    def index(embedded: list[dict]) -> int:
        corpus = open_corpus("support_kb")                # pgvector VectorStore via SPI
        for e in embedded:                                # idempotent upsert (C3)
            corpus.upsert(e["id"], e["vector"], e["text"])
        return len(embedded)

    w = wait_for_source_freshness()
    pages = crawl(w)
    chunks = chunk(pages)
    embedded = embed.expand(chunk=chunks)                 # one mapped task per chunk
    index(embedded)

agentic_ingestion()
```

Everything Airflow is good at shows up here: a **sensor** parks the run until the
source is fresh; **retries with exponential backoff** ride every task; **dynamic
task mapping** (`embed.expand`) fans embedding across workers (C11) bounded by
`max_active_tis_per_dag`; idempotent chunk ids + upserts make a retry a no-op
(C3); `catchup=True` lets you **backfill** a month of corpus; and the
`COLD_INDEX` **dataset outlet** fires the downstream eval DAG the instant the
index is rebuilt. The chunker and corpus are §4a code, untouched.

### `routed_triage` — the §3d DAG, end to end

The DAG in §3d is the second half: triggered (REST/dataset/sensor), it runs
`ingest_turn → route(@task.branch) → one of {path_fraud,path_txn,path_general} →
verify(fan-in)`, persisting the transcript and the `phase` attribute to the
`ConversationStore`. That is the banking `router → path → verifier` topology —
the same DAG shape as `BankingAgentGraph`, with `BranchPythonOperator` standing
in for the keyed `BankingRouterFunction`, the path tasks for the keyed
`BankingPathFunction` brains, and the fan-in `verify` for `BankingVerifierFunction`
advancing `BankingPhase`. The structural difference is honest and total: Flink
runs this *per event with single-writer keyed state*; Airflow runs it *per
triggered DAG run with state in an external store*.

## 5. What doesn't fit

- **Per-event keyed state / single-writer-per-conversation (C1+C2).** The heart
  of the essence (keystone §3: "C1+C2 are the heart") has no Airflow primitive.
  XCom is not keyed durable state. If two turns of one conversation can run
  concurrently, the engine will not serialise them — you must use a
  per-conversation DAG with `max_active_runs=1`, or optimistic concurrency / a
  store-level lock. **Don't pretend the store gives you single-writer ordering;
  it doesn't unless you make it.**
- **Low-latency conversation.** Each task is a process/pod; scheduler tick +
  worker pickup is seconds. A turn that touches three tasks is multi-second
  before any model call. Fine for "run this agentic workflow", wrong for "chat".
  **Do not put a synchronous chat endpoint behind a DAG trigger.**
- **Continuous streaming ingest (C6/C9).** There is no live source subscription,
  no event-time, no watermarks, no windows. Airflow processes *bounded* inputs
  per run. A "stream" becomes "micro-batch on a schedule", which changes the
  semantics — say so.
- **Backpressure (C5).** No end-to-end flow control. You get concurrency *limits*
  (pools, `max_active_*`, a `Semaphore`), which protect a model endpoint but do
  not propagate pressure upstream.
- **CEP (C10).** No pattern-over-stream. Out of scope.
- **High-frequency runs.** Airflow's scheduler is not built for thousands of DAG
  runs per second. If a conversation is high-traffic, the per-trigger model
  collapses — route that to Faust/Ray and reserve Airflow for the scheduled and
  human-gated workflows.

The workaround for all of the above is the same and it is not a workaround so
much as a division of labour: **run the live keyed-stateful loop on Faust or Ray
(keystone §7 ranks 1 and 3); use Airflow as the orchestration/batch plane that
feeds and evaluates them** — it builds the index they retrieve from, runs the
eval sweeps that grade them, and hosts the human-approval and scheduled-workflow
DAGs they cannot.

## 6. When to choose Airflow

Choose Airflow when the workload is **a workflow, not a conversation**:

- You need **scheduled or triggered agentic workflows** — "every night, for each
  customer cohort, run the routed analysis and file a report" — where the routed
  graph is a DAG and latency is irrelevant.
- You are building **RAG ingestion / re-indexing pipelines** and want sensors,
  retries, backfill, dynamic mapping, dataset-aware downstream scheduling, and a
  UI that shows you exactly which chunk failed to embed three weeks ago.
- You run **eval / benchmark sweeps** over configs and want them parametrised,
  parallel, scheduled, and historically tracked.
- You have **human-in-the-loop** gates — approvals, reviews — where parking a run
  on a sensor until a human acts is the natural model.
- You already operate Airflow and want the agentic *batch* work to live where
  your data engineers can see, retry, and backfill it alongside everything else.

Do **not** choose Airflow as the home of the live agent. It is the orchestration
and batch-data plane of an Agentic Streaming deployment — the scheduler, the
ingestion engine, the eval harness, the approval gate — sitting *beside* a
streaming engine (Faust/Ray/Kafka Streams) that hosts the per-conversation loop.
Picked for that role, it is excellent and idiomatic; picked as the runtime for a
keyed-stateful streaming agent, it is the wrong tool and will fight you the whole
way.
