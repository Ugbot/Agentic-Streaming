# Live-research walkthrough

> **Flink-runtime showcase, design sketch.** A crawler frontier + LLM-steered crawl composed as
> **Flink operators** over an HNSW corpus. Not the portable baseline; for the agent that runs on
> the portable runtimes see [the banking agent on the portable runtimes](banking-everywhere.md).
> This example does not run end to end in the current checkout; see "Running it" below.
> For a working Flink RAG loop use [the RAG walkthrough](rag.md).

> Source: `src/main/java/org/agentic/flink/example/research/LiveResearchExample.java`
> Inline README: `src/main/java/org/agentic/flink/example/research/README.md`

## Why this shape

A research assistant has two operating modes that have to coexist:

1. **Learning**: ingest documents (URLs, PDFs, ...) into a searchable corpus.
2. **Recall**: answer questions against that corpus with citations.

Most production systems split these into two services. We argue that for
Flink-scale workloads they want to be **one job**, same state, same
failure domain, same checkpoint barrier, but the *operators* doing the
work should still be independent. That's what the framework's `Channel`,
`Corpus`, and pipeline-builder primitives buy you.

## The composition

```
                seeds              agent's crawl-url      external producer
              (Static)             (ToolInvocation)        (KafkaChannel)
                  \                       │                       /
                   \                      │                      /
                    ▼                     ▼                     ▼
                          union → CrawlerCore (multi-source frontier)
                                          │
                                          ▼  Fetcher (robots.txt, max-size, UA)
                                          ▼  DocumentExtractor (Jsoup + Tika)
                                          ▼  RecursiveTextChunker(maxChars=512)
                                          ▼  DjlEmbeddingConnection (MiniLM-L6-v2)
                                          ▼  corpus.upsert(id, vec, item)
                                          │
                                          ▼  DataStream<IngestAck>

queries (Static / Kafka / Webhook) ─►  RetrievalPipeline
                                          ▼  embed(query)
                                          ▼  corpus.search(k=6)
                                          ▼  cross-encoder rerank
                                          ▼  ChatConnection answer(citations)
                                          │
                                          ▼  DataStream<Answer>
```

The corpus is `BroadcastCorpus(FlinkStateHnswVectorMemory.spec(384))`.
Ingest is one operator that broadcasts updates; the retrieve operator
holds a per-replica copy. Either side scales independently.

## Targeting the crawler

Three independent inputs feed the frontier today; the framework doesn't
care which you wire in:

- **Seeds**: `StaticSeedChannel<UrlRequest>` for an initial corpus.
- **LLM-driven**: `ToolInvocationChannel.sideOutput("crawl-url", ...)`.
  When the LLM decides it needs a URL it doesn't have, it calls
  `crawl-url(url=...)`. The framework routes via Flink side-output; the
  crawler sees it on its frontier just like any other input.
- **External**: add `KafkaChannel<UrlRequest>` to the frontier to let an
  external service nudge the crawler. Useful for "I just found a new doc;
  please index it" workflows.

The frontier is a union. Adding a channel is one line:

```java
CrawlerCore.builder()
    .frontier(seedChannel, agentCrawlChannel, externalKafkaChannel)
    .options(WebToolkitOptions.defaults())
    .open(env);
```

## Vector index choice

`FlinkStateHnswVectorMemory.spec(384)` is the default. It's a single-layer
NSW graph backed by Flink `MapState`:

- **Vectors live in MapState**: they checkpoint with the job and survive
  restarts.
- **Graph is rebuilt on operator `open()`** by replaying MapState. At
  d=384, ~1 s per 10⁵ vectors.
- For larger corpora drop in a JVector or Lucene-HNSW backed
  `VectorMemorySpec` via the SPI, the `Corpus` interface doesn't change.
- For very large or cross-job corpora, swap to `ExternalCorpus.spec("pgvector",
  ...)` and the vectors live in Postgres + pgvector. Same `Corpus` API.

## Side-output vs Kafka for the LLM tool

The default `ToolInvocationChannel.sideOutput(...)` routes invocations
through Flink's normal network stack, exactly-once with checkpoints,
cross-TM safe within a single job. That's what we want for an in-job
agent + crawler.

When the consumer is a different job (or not a Flink job at all), swap to
`ToolInvocationChannel.via(...)` with a `KafkaChannel<UrlRequest>` and a
producer. The LLM still sees just one tool; the transport choice lives
inside the channel.

## Running it

```bash
bash examples-bin/run-live-research.sh
```

The script exits with status 2 and an explanation. The composition above is the target shape,
but three things stop it from running in this checkout, and they need framework changes rather
than example changes:

1. `IngestionPipeline.into()` and `RetrievalPipeline.search()` are plain (unkeyed) Flink
   operators, while `FlinkStateHnswVectorMemory` binds keyed `MapState`. The job fails in
   `open()` with `Keyed state 'vector.hnsw.entries' ... can only be used on a 'keyed stream'`.
2. Even on keyed streams the ingest operator and the search operator would own separate Flink
   state, so the retrieve side would never see what the ingest side indexed. The "broadcast"
   in `BroadcastCorpus` is not implemented by the pipeline builders yet; `BroadcastCorpus.Spec
   .bind` returns a per-operator `SingleOperatorCorpus`.
3. `ToolInvocationChannel.sideOutput("crawl-url", ...)` opens an unbounded polling source, so
   the job never finishes, and the two static queries are answered before any page has been
   indexed.

Set `AGENTIC_RUN_LIVE_RESEARCH=1` to run the example anyway and observe the first failure. That
path needs JDK 21, the Maven wrapper, Ollama at `OLLAMA_URL` with `qwen2.5:3b`
(`bash examples-bin/run-ollama.sh`), and outbound internet for the Wikipedia seeds and the
first-run DJL downloads of `sentence-transformers/all-MiniLM-L6-v2`,
`cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` and the PyTorch CPU native runtime.

## What the design intends you to see

- Ingest acks, one `ingested <chunk> into research-kb` line per chunk, as the seed URLs are
  fetched, chunked, embedded and indexed.
- `Answer[...]` records with `[1] [2]` citations drawn from the corpus.
- URLs pushed through an external `KafkaChannel<UrlRequest>` joining the same crawler frontier,
  with the corpus growing on the fly.

None of that output is produced today. `tools/smoke-examples.sh` lists this example as a skip
with the reason above.
