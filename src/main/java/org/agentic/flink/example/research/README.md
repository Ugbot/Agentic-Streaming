# Live research + RAG example

One Flink job, two inputs, one shared corpus. Throw URLs in one side, ask
questions on the other, and let the LLM steer the crawler when it doesn't
know the answer.

```
            seeds (StaticSeedChannel<UrlRequest>)
                       \
                        \
agent's crawl-url ──► ToolInvocationChannel<UrlRequest> ──┐
        (side-output)                                     │
                                                          ▼
                                                  CrawlerCore.builder()
                                                       .frontier(…)
                                                       .open(env)
                                                          │
                                                          ▼  DataStream<CrawledPage>
                                                  IngestionPipeline.from(pages)
                                                       .chunk(RecursiveTextChunker)
                                                       .embed(DjlEmbeddingConnection)
                                                       .into(BroadcastCorpus(HNSW))
                                                          │
                                                          ▼  DataStream<IngestAck>
                                                       print

queries (StaticSeedChannel<String>) ─► RetrievalPipeline.from(queries)
                                            .embed(djlEmbeddings)
                                            .search(corpus, 6)
                                            .rerank(crossEncoder)
                                            .answer(ollama, chatSetup)
                                            │
                                            ▼  DataStream<Answer>
                                          print
```

## What's interesting

| Piece | Framework primitive |
|-------|---------------------|
| Crawler frontier (seeds + LLM + …) | `CrawlerCore.builder().frontier(Channel…)` |
| LLM-driven URL emission | `ToolInvocationChannel.sideOutput(…)` |
| Multi-format extraction (HTML, PDF, …) | `DocumentExtractor` (Jsoup + Tika) |
| robots.txt enforcement | `RobotsCache` inside `Fetcher` |
| HNSW over Flink state | `FlinkStateHnswVectorMemory.spec(dim)` |
| Shared corpus across operators | `BroadcastCorpus.spec(name, vectorSpec)` |
| Chunk → embed → upsert | `IngestionPipeline.from(pages).chunk(…).embed(…).into(corpus)` |
| Embed → search → rerank → answer | `RetrievalPipeline.from(queries).embed(…).search(…).rerank(…).answer(…)` |

The point of this example: **everything heavy is a framework primitive.**
The `main()` is the sentence the verbs let you write.

## Prerequisites

```bash
docker compose up -d ollama
docker compose exec ollama ollama pull qwen2.5:3b
```

Add a DJL native binary to your local pom:

```xml
<dependency>
  <groupId>ai.djl.pytorch</groupId>
  <artifactId>pytorch-native-cpu</artifactId>
  <version>0.30.0</version>
  <scope>runtime</scope>
</dependency>
```

## Run

```bash
./examples-bin/run-live-research.sh
# or
mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.research.LiveResearchExample"
```

## Targeting the crawler from outside the job

Add a Kafka channel to the frontier:

```java
Channel<UrlRequest> external =
    new KafkaChannel<>("kafka:9092", "crawl-requests", "research-agent", UrlRequest.class);

CrawlerCore.builder()
    .frontier(seedChannel, agentCrawlChannel, external)
    .options(WebToolkitOptions.defaults())
    .open(env);
```

Any producer that writes JSON `UrlRequest` records to the `crawl-requests`
topic now nudges the crawler — same operator, no code change to the crawler.

## Cross-job tool invocations

Swap the side-output transport for a Kafka-backed one so a separate crawler
job consumes the LLM's `crawl-url` calls:

```java
KafkaChannel<UrlRequest> crawlReqs =
    new KafkaChannel<>("kafka:9092", "crawl-requests", "research-agent", UrlRequest.class);
KafkaProducer<String, String> producer = ...;  // configured externally

ToolInvocationChannel<UrlRequest> agentCrawl =
    ToolInvocationChannel.via(
        "crawl-url",
        UrlRequest.class,
        params -> new UrlRequest((String) params.get("url"), "agent"),
        crawlReqs,
        req -> producer.send(new ProducerRecord<>("crawl-requests", asJson(req))));
```

The LLM still sees `crawl-url` as one tool; the framework hides the
transport behind the channel.
