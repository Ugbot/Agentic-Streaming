# Corpus

A `Corpus` is the framework abstraction for "a vector index plus metadata,
addressed by name, possibly shared between operators." It's the layer that
makes ingest and retrieve pipelines composable: both operate against the
same named corpus, the framework picks the right flavour underneath.

## The three flavours

| Flavour | State | Read latency | Write latency | Use when |
|---------|-------|--------------|---------------|----------|
| `SingleOperatorCorpus` | Flink keyed state | <1 ms | <1 ms | Small / medium corpora in one operator (the `KeyedCoProcessFunction` pattern). |
| `BroadcastCorpus` | Per-replica copy via Flink broadcast state | <1 ms | one cross-task hop per update | Moderate corpora (≤10⁵ vectors per replica), low write rate, multiple readers want independent parallelism. |
| `ExternalCorpus` | pgvector / Qdrant / any `VectorStore` | 5-20 ms | 5-20 ms | Large corpora, cross-job sharing, durable beyond one job's lifetime. |

All three implement the same `Corpus` interface:

```java
public interface Corpus {
    CompletableFuture<Void>             upsert(String id, float[] embedding, ContextItem item);
    CompletableFuture<List<ScoredItem>> search(float[] query, int k);
    CorpusStats                         stats();
}
```

So the user code that *consumes* the corpus is identical regardless of
flavour. Swap flavours at the `CorpusSpec` level:

```java
// 1. In-operator, HNSW over Flink state.
CorpusSpec a = SingleOperatorCorpus.spec(
    "kb", FlinkStateHnswVectorMemory.spec(384));

// 2. Broadcast to many readers.
CorpusSpec b = BroadcastCorpus.spec(
    "kb", FlinkStateHnswVectorMemory.spec(384));

// 3. External pgvector store.
CorpusSpec c = ExternalCorpus.spec(
    "kb", "pgvector",
    Map.of("postgres.url", "jdbc:postgresql://localhost:5432/agentic_flink",
           "postgres.dimension", "384"),
    384);
```

## `SingleOperatorCorpus`

The simplest flavour. Both reads and writes happen on the same operator,
typically a `KeyedCoProcessFunction` whose two inputs are the ingest stream
(`processElement1`) and the query stream (`processElement2`). Bind the
corpus in `open()`:

```java
corpus = SingleOperatorCorpus.spec("kb", FlinkStateHnswVectorMemory.spec(384))
                             .bind(getRuntimeContext());
```

Every write is immediately visible to subsequent reads on the same operator
, there's no replication lag. The constraint is that ingest and retrieve
must share the same `keyBy` and the same operator chain.

The shipped pipelines wire this for you. `IngestionPipeline...embed(...).embedded()`
exposes the embedded chunks and `RetrievalPipeline...search(corpus, k, embeddedChunks)`
keys both the chunks and the queries by the corpus name and runs them through one
`KeyedCoProcessFunction`; the ingest acknowledgements come out of
`ingestAcks()` on the returned stage:

```java
DataStream<IngestionPipeline.EmbeddedChunk> indexed =
    IngestionPipeline.from(pages).chunk(new RecursiveTextChunker(512)).embed(embeddings).embedded();

RetrievalPipeline.StageRerank searched =
    RetrievalPipeline.from(queries).embed(embeddings).search(corpus, 6, indexed);
searched.ingestAcks().print();
searched.rerankSkip().answer(chat, chatSetup).print();
```

A Flink-state vector memory is keyed state, so binding it from an unkeyed
operator (for example `IngestionPipeline...into(corpus)` or the two-argument
`RetrievalPipeline...search(corpus, k)`) fails in `open()` with an
`IllegalStateException` that points at the keyed wiring above. Use those
unkeyed stages with `ExternalCorpus`.

## `BroadcastCorpus`

Use this when ingest and retrieve should run independently, e.g. retrieve
needs more parallelism than ingest, or you want the read operator's state
to be a self-contained replica.

The framework primitive gives you the per-replica vector memory and the
serializable spec. The **wiring**, turning the ingest stream into a
`BroadcastStream` and connecting it into the read operators, is your job.
Canonical snippet:

```java
MapStateDescriptor<String, VectorEntry> broadcastDescriptor =
    new MapStateDescriptor<>("kb-broadcast", String.class, VectorEntry.class);

BroadcastStream<VectorEntry> updates =
    ingestOutputStream.broadcast(broadcastDescriptor);

DataStream<Answer> answers =
    queries
        .connect(updates)
        .process(new MyBroadcastReader(broadcastDescriptor, corpusSpec));
```

The `MyBroadcastReader extends BroadcastProcessFunction<String, VectorEntry,
Answer>` binds the corpus in `open()` and applies each broadcast `VectorEntry`
via `corpus.upsert(...)`.

## `ExternalCorpus`

When the corpus is too big for Flink state or needs to be shared across
jobs. Wraps any `VectorStore` SPI implementation, pgvector and Qdrant ship
in-box.

```java
CorpusSpec corpus = ExternalCorpus.spec(
    "research-kb",
    "pgvector",
    Map.of(
        "postgres.url", "jdbc:postgresql://localhost:5432/agentic_flink",
        "postgres.user", "flink_user",
        "postgres.password", "flink_password",
        "postgres.dimension", "384",
        "pgvector.table", "research_kb_vectors",
        "pgvector.similarity", "cosine"),
    384);
```

The external store creates its schema lazily; just point the spec at the
right DB and let `initialize(...)` run.

## How the example uses it

[`LiveResearchExample`](examples/live-research.md) uses
`SingleOperatorCorpus.spec("research-kb", FlinkStateHnswVectorMemory.spec(384))`
through the keyed `search(corpus, k, indexed)` stage, so the crawled chunks and
the queries meet on one keyed operator whose vectors checkpoint with the job.
Production deployments would more typically use `ExternalCorpus` so the
corpus survives job restarts and is shared across jobs.
