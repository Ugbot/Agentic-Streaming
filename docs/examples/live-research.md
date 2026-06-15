# Live-research walkthrough

> **Flink-runtime showcase** — a crawler frontier + LLM-steered crawl composed as **Flink operators**
> over an HNSW corpus. Not the portable baseline; for the agent that runs unchanged on every runtime
> see [the banking agent on every runtime](banking-everywhere.md).

> Source: `src/main/java/org/agentic/flink/example/research/LiveResearchExample.java`
> Inline README: `src/main/java/org/agentic/flink/example/research/README.md`

## Why this shape

A research assistant has two operating modes that have to coexist:

1. **Learning** — ingest documents (URLs, PDFs, …) into a searchable corpus.
2. **Recall** — answer questions against that corpus with citations.

Most production systems split these into two services. We argue that for
Flink-scale workloads they want to be **one job** — same state, same
failure domain, same checkpoint barrier — but the *operators* doing the
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

- **Seeds** — `StaticSeedChannel<UrlRequest>` for an initial corpus.
- **LLM-driven** — `ToolInvocationChannel.sideOutput("crawl-url", ...)`.
  When the LLM decides it needs a URL it doesn't have, it calls
  `crawl-url(url=...)`. The framework routes via Flink side-output; the
  crawler sees it on its frontier just like any other input.
- **External** — add `KafkaChannel<UrlRequest>` to the frontier to let an
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

- **Vectors live in MapState** — they checkpoint with the job and survive
  restarts.
- **Graph is rebuilt on operator `open()`** by replaying MapState. At
  d=384, ~1 s per 10⁵ vectors.
- For larger corpora drop in a JVector or Lucene-HNSW backed
  `VectorMemorySpec` via the SPI — the `Corpus` interface doesn't change.
- For very large or cross-job corpora, swap to `ExternalCorpus.spec("pgvector",
  …)` and the vectors live in Postgres + pgvector. Same `Corpus` API.

## Side-output vs Kafka for the LLM tool

The default `ToolInvocationChannel.sideOutput(...)` routes invocations
through Flink's normal network stack — exactly-once with checkpoints,
cross-TM safe within a single job. That's what we want for an in-job
agent + crawler.

When the consumer is a different job (or not a Flink job at all), swap to
`ToolInvocationChannel.via(...)` with a `KafkaChannel<UrlRequest>` and a
producer. The LLM still sees just one tool; the transport choice lives
inside the channel.

## Prerequisites

```bash
docker compose up -d ollama
docker compose exec ollama ollama pull qwen2.5:3b
```

Add `ai.djl.pytorch:pytorch-native-cpu:0.30.0` to your downstream pom (the
framework deliberately doesn't pull a native binary itself).

## Run

```bash
./examples-bin/run-live-research.sh
```

On first run, DJL caches ~150 MB of model weights into `~/.djl.ai`. Each
query takes a couple of seconds; the LLM is the dominant cost.

## What you should see

- Two ingest streams emit "ingested chunk-XX into research-kb" lines as
  the seed URLs are fetched, chunked, embedded, and indexed.
- Two answer streams emit `Answer[...]` records with `[1] [2]` citations
  drawn from the corpus.
- If you point a Kafka producer at the optional `crawl-requests` topic,
  those URLs join the same crawler frontier and the corpus grows
  on the fly — searchable on the very next query.
