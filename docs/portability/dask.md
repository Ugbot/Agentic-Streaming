# Agentic Streaming on Dask — pure Python, no JVM

> Per-engine portability doc. Read [`00-essence-and-core-abstractions.md`](./00-essence-and-core-abstractions.md)
> first — this doc is written against its essence (§2), capability inventory
> (§3, C1..C12), Engine SPI (§4c), matrix (§6), and ranked fit (§7), and follows
> the §9 six-section template. Target: pure Python, no JVM, no JPype.

## 1. Verdict

**Dask is a partial fit, and the part it fits, it fits beautifully.** Dask is a
task-graph + futures + distributed-collections engine: `dask.bag`/`dataframe`
for embarrassingly-parallel data, `dask.delayed`/`distributed.Client.submit`
for arbitrary task DAGs, an adaptive scheduler that fans work across a cluster.
That is **the data plane of this project, not the live agent plane.**

Keep on Dask, idiomatically and at scale:

- **Batch RAG ingestion** (essence §2.2, the `IngestionPipeline` logic): crawl →
  chunk → embed → build the **cold** vector index, parallelised across the
  cluster. Thousands of documents, hundreds of workers, one `bag` pipeline.
- **Offline eval / benchmark sweeps**: run the retrieval pipeline (or a full
  routed graph replay) over a labelled dataset, parametrised across configs
  (k, chunk size, embedder, reranker), in parallel — recall@k / nDCG / latency
  matrices as a DataFrame. This is the thing Dask is *unfairly good at*.

Do **not** put the live keyed-stateful agent loop on Dask. `distributed.Actor`
gives a stateful, single-threaded-per-instance actor, so a conversation-actor is
*technically* constructible (C1/C2 weakly) — but Dask is built for **bounded**
task graphs that complete, not long-lived per-key ordered streaming with
low-latency turn handling and backpressure-as-topology. You would fight the
model the whole way: no partitioned ordered ingest, no native durability of
actor state, no streaming source/sink. The honest recommendation (keystone §7):
**port the ingestion + retrieval-eval pipelines to Dask; run the live loop on
Faust or Ray; use Dask as the heavy offline data engine alongside them.**

What you keep unchanged: the entire pure core (§4a) — `Chunker`,
`TwoTierRetriever`, the embed→search→rerank→answer *logic*, `ToolExecutor`
semantics, `ConversationStore`/`VectorStore` as the durability seam. What you
re-implement: nothing of the agent operator — you simply don't host it here.

## 2. Capability mapping (C1..C12 — Dask column of §6)

| Cap | §6 | On Dask | Mechanism / honest note |
|-----|:--:|---------|-------------------------|
| **C1** keyed durable state | **L\*** | `distributed.Actor` fields | An actor is a single Python object pinned to one worker; its fields *are* per-key state — but in-memory only, lost on worker death. Durability = write through to the `ConversationStore`/`VectorStore` SPI. Weak, and out of the sweet spot. |
| **C2** per-key ordered proc | **—** | not native | No `keyBy`→partition. You can route by key to a per-key actor (one actor per `conversationId`) and the actor's mailbox serialises calls, but there is no ordered partitioned *ingest*, no replay, no rebalance. For batch, ordering is irrelevant. |
| **C3** fault tolerance | **L** | task retry | Dask reruns failed tasks from the graph; great for **idempotent batch** (re-embed a chunk). No checkpointed streaming state. Eval/ingest get C3 for free; the agent loop does not. |
| **C4** async I/O | **L** | futures / asyncio client | `client.submit` returns futures; `as_completed` bounds in-flight work; the async `Client` runs inside `asyncio`. Maps the *embed/LLM call fan-out* well; maps the *per-event agent loop* poorly. |
| **C5** backpressure | **L** | scheduler + batching | The scheduler caps concurrency; you throttle with `as_completed` over a sliding window or a `Semaphore`. Protects the embedder/model endpoint in batch. Not end-to-end streaming backpressure. |
| **C6** connectors | **L** | `read_*` / `to_*` | `db.read_text`, `dd.read_parquet`, `read_sql`, object-store globs, `to_parquet`. Bounded sources/sinks, not Kafka/streaming connectors. Perfect for "ingest this corpus", wrong for "subscribe to a topic". |
| **C7** side outputs | **L** | extra return / extra collection | A task returns a tuple, or you build a second `bag`. Trivial for the debug/metrics side-channel of a batch run. |
| **C8** broadcast state | **L** | `client.scatter(broadcast=True)` | Scatter the embedder config / control dims to every worker once, reference by future. Idiomatic and cheap. |
| **C9** event-time/windows | **—** | not a fit | No streaming time model. N/A for batch. |
| **C10** CEP | **—** | not a fit | No pattern-over-stream. Out of scope. |
| **C11** distributed scale | **N** | adaptive cluster | Dask's home turf: `Client`, `LocalCluster`, `dask-kubernetes`, adaptive scaling. This is *why* you'd reach for it — embed 10⁶ chunks across 200 workers. |
| **C12** topology builder | **N** | task graph | `delayed`/`bag`/`dataframe` build a native lazy DAG the scheduler optimises. The routed-graph *pattern* expresses cleanly as a DAG per input row (router→path→verifier as delayed nodes). |

`*` = present but caveated, as spelled out above. The shape of the column is the
verdict: **C11/C12 native, the data-plane capabilities L, and the
live-streaming-core capabilities (C2, C9, C10) flat `—`.**

## 3. The core abstractions on Dask

### 3a. The pure core ports verbatim (logic, not Flink)

`Chunker`, `RecursiveTextChunker`, `TwoTierRetriever`, the embed→search→rerank→
answer steps, and the `ToolExecutor` contract have **no Flink in them** — they're
pure functions over plain values. In Python they're plain callables. The
`RecursiveTextChunker` separator cascade (`["\n\n","\n",". "," ",""]`, target
`max_chars`, 10% overlap) transcribes 1:1:

```python
# pyagentic/ingest.py — pure, engine-free (mirrors RecursiveTextChunker.java)
from dataclasses import dataclass

@dataclass(frozen=True)
class Chunk:
    id: str; text: str; source_id: str; position: int; token_count: int

class RecursiveTextChunker:
    SEPARATORS = ("\n\n", "\n", ". ", " ", "")
    def __init__(self, max_chars: int, overlap_chars: int | None = None):
        if max_chars <= 0: raise ValueError("max_chars must be positive")
        self.max_chars = max_chars
        self.overlap = max_chars // 10 if overlap_chars is None else overlap_chars

    def chunk(self, source_id: str, text: str) -> list[Chunk]:
        if not text: return []
        merged = self._merge_overlap(self._split(text, 0))
        return [Chunk(f"{source_id}::{i}", t, source_id, i, max(1, len(t)//4))
                for i, t in enumerate(merged)]
    # _split / _merge_overlap: direct transcription of the Java cascade.
```

### 3b. ConversationStore / VectorStore = the durability seam (unchanged SPI)

These are *already* the portable contract (keystone §4a). On Dask they are
plain Protocols backed by Postgres/pgvector/Redis — exactly the existing
backends, addressed over the network instead of via Flink keyed state. Dask
workers are stateless; **all durable state lives behind these Protocols.**

```python
from typing import Protocol, Optional
from pyagentic.llm import ChatMessage

class ConversationStore(Protocol):                 # mirrors ConversationStore.java
    def append(self, conversation_id: str, message: ChatMessage) -> None: ...
    def history(self, conversation_id: str) -> list[ChatMessage]: ...
    def put_attribute(self, conversation_id: str, key: str, value: str) -> None: ...
    def get_attribute(self, conversation_id: str, key: str) -> Optional[str]: ...

class VectorStore(Protocol):                        # the cold tier; pgvector/Qdrant
    def upsert(self, id: str, embedding: list[float], text: str,
               metadata: dict[str, str]) -> None: ...
    def search(self, query: list[float], k: int) -> list["ScoredItem"]: ...
```

### 3c. TwoTierRetriever — pure, used at *query/eval* time

The hot+cold merge (query both tiers, dedupe by id keeping the higher score,
degrade gracefully if a tier fails) is pure logic. On Dask the "hot" tier is
usually empty/irrelevant for offline eval (no live ingest stream), so you run it
**cold-only** — but the class is identical to the Java one so a future live
engine reuses it:

```python
# pyagentic/retrieve.py — mirrors TwoTierRetriever.java, degradation included
class TwoTierRetriever:
    def __init__(self, hot, cold, hot_k: int, cold_k: int):
        self.hot, self.cold = hot, cold
        self.hot_k, self.cold_k = max(1, hot_k), max(1, cold_k)
    def retrieve(self, query, k: int):
        best: dict[str, ScoredItem] = {}
        for tier, kk in ((self.hot, self.hot_k), (self.cold, self.cold_k)):
            if tier is None: continue
            try:
                for s in tier.search(query, kk):
                    p = best.get(s.id)
                    if p is None or s.score > p.score: best[s.id] = s
            except Exception as e:               # a dead tier degrades recall, not the query
                log.warning("tier search failed (degrading): %s", e)
        return sorted(best.values(), key=lambda s: s.score, reverse=True)[:max(1, k)]
```

### 3d. The Engine SPI (§4c) realised on Dask — bounded, not streaming

Dask realises the SPI for **bounded collections**, not live streams. `source`
becomes "read a bounded corpus", `sink` becomes "write results", and the unit of
parallelism is a partition of a `bag`, not a keyed operator:

```python
import dask.bag as db
from dask.distributed import Client

class DaskRuntime:
    """Engine SPI (§4c) for the DATA PLANE only. No keyedAgent — see §5."""
    def __init__(self, client: Client): self.client = client

    def source(self, glob: str) -> db.Bag:                 # C6 (bounded)
        return db.read_text(glob)                          # or read_parquet / read_sql

    def map_stage(self, bag: db.Bag, fn) -> db.Bag:        # C12 — a DAG node, map per element
        return bag.map(fn)

    def sink(self, bag: db.Bag, path: str):                # C6 sink
        bag.to_textfiles(path)

    def broadcast(self, obj):                              # C8
        return self.client.scatter(obj, broadcast=True)

    def execute(self, *collections):                       # C11 — fan across the cluster
        return self.client.compute(list(collections), sync=True)
```

`asyncStage` (C4) is `client.submit` + `as_completed`; there is **deliberately no
`keyedAgent`** — that's the line §5 will not cross.

### 3e. Async / tools (C4) — fan-out, not per-event loop

A `ToolExecutor` is `params -> result`. In a *batch* eval each tool/LLM call is a
submitted future; `as_completed` bounds in-flight work (C5, protecting the model
endpoint). This is the right home for "run 5 000 LLM-judge scorings in
parallel"; it is the *wrong* home for "one ReAct loop per live turn":

```python
from dask.distributed import as_completed
def run_bounded(client, fn, items, max_in_flight=64):     # C4 + C5
    pending, results = set(), []
    it = iter(items)
    for x in (next(it, None) for _ in range(max_in_flight)):
        if x is not None: pending.add(client.submit(fn, x))
    for done in as_completed(pending):
        results.append(done.result())
        nxt = next(it, None)
        if nxt is not None: as_completed.add(client.submit(fn, nxt))  # refill window
    return results
```

## 4. Worked example — corpus ingest → cold index → retrieval eval

The faithful, Dask-shaped worked example is **not** the live banking turn loop —
it's the data plane that *feeds* it. Two pipelines, both pure-core logic wired
onto Dask collections.

### 4a. Parallel ingestion → cold vector index (mirrors `IngestionPipeline`)

`IngestionPipeline.from(pages).chunk(...).embed(...).into(corpus)` becomes a
`bag` pipeline. Each Flink operator (`chunk`/`embed`/`index`) is one `bag` stage;
the embedder ships to every worker once via broadcast (C8); the cold index is
the `VectorStore` SPI (pgvector), written idempotently by chunk id (C3 retry-safe):

```python
import dask.bag as db
from dask.distributed import Client, get_worker

client = Client("tcp://scheduler:8786")          # C11: adaptive cluster
chunker  = RecursiveTextChunker(max_chars=512)   # §3a pure
embed_cfg = client.scatter(EmbedSetup(model="bge-small-en", dim=384), broadcast=True)  # C8

def chunk_page(page):                            # CrawledPage -> [Chunk]   (flatMap)
    return chunker.chunk(page["url"], page["text"])

def embed_chunk(c, cfg):                          # Chunk -> EmbeddedChunk   (ProcessFunction)
    model = get_worker()._embed_cache(cfg.result())   # one model per worker, lazy (≈ open())
    return {"id": c.id, "text": c.text, "vec": model.embed(c.text),
            "meta": {"source_url": c.source_id}}

def index_chunk(e):                               # EmbeddedChunk -> IngestAck  (idempotent upsert)
    store = vector_store()                        # pgvector, addressed over the network
    store.upsert(e["id"], e["vec"], e["text"], e["meta"])
    return e["id"]

pages = db.read_text("s3://corpus/*.jsonl").map(json.loads)   # C6 bounded source
acks  = (pages
         .map(chunk_page).flatten()               # chunk
         .map(embed_chunk, embed_cfg)             # embed (broadcast cfg)
         .map(index_chunk))                        # into(corpus)
n_indexed = acks.count().compute()                # C11: whole DAG fans across the cluster
```

This is the *exact* dataflow of the Java `IngestionPipeline` — chunk → embed →
upsert — except the partitioned operators are `bag` partitions and the scheduler
provides the parallelism Flink got from operator parallelism.

### 4b. Retrieval-quality eval sweep (the thing Dask wins at)

Run embed→search→(rerank)→answer over a labelled QA set, **parametrised across
configs**, and produce a recall@k / nDCG matrix as a DataFrame. Each (query,
config) is an independent task; the Cartesian product fans across the cluster:

```python
import dask.bag as db, itertools, pandas as pd

queries = load_eval_set("eval/banking_qa.jsonl")              # [{q, relevant_ids}]
configs = [{"k": k, "rerank": r} for k, r in
           itertools.product([3, 6, 10], [False, True])]      # parameter sweep

def eval_one(item):
    q, cfg = item
    retr = TwoTierRetriever(hot=None, cold=vector_store(), hot_k=cfg["k"], cold_k=cfg["k"])  # §3c
    qvec = embedder().embed(q["question"])
    hits = retr.retrieve(qvec, cfg["k"])
    if cfg["rerank"]:
        hits = cross_encoder().rerank(q["question"], hits)    # pure RerankFn logic
    got = {h.id for h in hits[:cfg["k"]]}
    rel = set(q["relevant_ids"])
    return {**{f"cfg_{k}": v for k, v in cfg.items()},
            "recall_at_k": len(got & rel) / max(1, len(rel)),
            "ndcg": ndcg(hits, rel)}

grid = db.from_sequence(list(itertools.product(queries, configs)))
report = pd.DataFrame(grid.map(eval_one).compute())           # C11 fan-out
print(report.groupby([c for c in report if c.startswith("cfg_")])
            [["recall_at_k", "ndcg"]].mean())                 # the benchmark matrix
```

### 4c. Optional — batch replay of the routed banking graph over N transcripts

You *can* express `BankingAgentGraph`'s router→path→verifier as a per-row
`delayed` DAG and replay it over historical transcripts in parallel — for
regression eval, **not** live serving. Note the verifier's cross-turn phase
(`BankingPhase` via the `ConversationStore`/`PhaseStore`) means each *transcript*
must run its turns sequentially (state threads through), but **transcripts are
independent** — so parallelism is across transcripts, not within one:

```python
import dask
@dask.delayed
def replay_transcript(transcript):                # one independent unit of parallelism
    store = InMemoryConversationStore()           # per-transcript scratch (no shared live state)
    out = []
    for turn in transcript["turns"]:              # sequential within a transcript
        path = banking_router(turn, store)        # rule-based classify  (router)
        result = path_brain(path).run(turn, store)# focused ReAct brain   (path)
        out.append(banking_verifier(result, store))  # advance phase, emit (verifier)
    return {"id": transcript["id"], "outputs": out}

results = dask.compute(*[replay_transcript(t) for t in transcripts])  # parallel across transcripts
```

This reuses the pure router/brain/verifier logic; the *graph wiring* is a Python
loop, not a Flink topology. It's an offline harness, and it's honest about why:
the live version needs C1+C2, which Dask doesn't give.

## 5. What doesn't fit (honest gaps)

- **Live keyed streaming agents (C2).** No partitioned ordered ingest, no
  `keyBy`, no rebalance, no replay. `distributed.Actor` per `conversationId`
  *technically* gives single-writer ordered handling, but you'd hand-build the
  router (key→actor lookup), the actor lifecycle, and eviction — reinventing a
  worse Faust/Ray. **Don't do this on Dask.**
- **Durable keyed state (C1).** Actor fields are in-memory and die with the
  worker. Durability is *only* via the `ConversationStore`/`VectorStore` SPI
  (Postgres/Redis), with no checkpoint coordination — so no exactly-once on the
  agent path; you lean on idempotent tool/A2A calls (keystone §8).
- **Low-latency conversation.** Dask's scheduler is tuned for throughput on a
  task graph, not millisecond per-event dispatch. Turn latency would be at the
  mercy of scheduler round-trips. Wrong tool.
- **Streaming connectors (C6), event-time/windows (C9), CEP (C10).** Flat `—`.
  Dask sources/sinks are bounded; there is no time model or pattern engine.
- **Backpressure-as-topology (C5).** You get scheduler-level concurrency caps
  and `as_completed` windows, not end-to-end streaming backpressure protecting a
  live pipeline.
- **The inbound A2A proxy / outbound live connectors.** Out of scope — those
  belong to the live engine (Quarkus proxy + Faust/Ray loop). Dask has no
  business terminating JSON-RPC/SSE.

The clean line: **anything bounded and parallel → Dask; anything live, keyed,
ordered, low-latency → not Dask.**

## 6. When to choose Dask

Choose Dask when the *workload* in front of you is the data plane, and you
already run (or will run) the live loop elsewhere:

- **You have a large corpus to (re)ingest** — crawl/chunk/embed/index 10⁵–10⁷
  documents across a cluster, faster than a single-JVM Flink embedded run, with
  free task-retry idempotency. The cold index it builds is then served by the
  live engine via the same `VectorStore` SPI.
- **You run retrieval/agent eval sweeps** — recall@k, nDCG, latency, LLM-judge
  scoring across a parameter grid and a dataset. This is Dask's strongest case
  here: embarrassingly parallel, bounded, throughput-bound, DataFrame-native
  reporting.
- **You already live in the PyData stack** — pandas/NumPy/Parquet, a
  `dask-kubernetes` cluster, scientists who think in DataFrames. Dask slots in
  with zero new infrastructure.
- **You want one heavy offline engine alongside a streaming one** — the keystone
  recommendation: **Dask for the data plane, Faust or Ray for the live agent
  plane**, both sharing the pure core (§4a) and the durable SPIs.

Do **not** choose Dask as the home for the agentic essence (§2.1) itself. It
hosts the parts of this project that are *batch data engineering with an LLM in
the loop* — and for those it is excellent and idiomatic. For "a fleet of
per-conversation stateful agents driven by events," look at Faust (#1) or Ray
(#3) and let Dask be the data engine they stand on.
