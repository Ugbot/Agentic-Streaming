# RAG research assistant walkthrough

> Source: `src/main/java/org/agentic/flink/example/rag/RagResearchExample.java`
> Inline README: `src/main/java/org/agentic/flink/example/rag/README.md`

## Why this shape

A retrieval-augmented assistant has four levers that all matter:

1. **Embedder quality** — bad embeddings ≡ retrieving the wrong passages.
2. **Vector store latency** — every query embeds + searches; cost compounds.
3. **Rerank precision** — embedding similarity is a coarse signal. A
   cross-encoder over the top-k catches "looks similar, is wrong" cases.
4. **Citations** — the LLM has to ground its answer or the system is just a
   confident summarizer.

The example wires all four with the framework's primitives:

```
Query
  │
  ▼  keyBy(topic)       — each topic gets its own KB scope
  │
  ▼  EmbeddingClient.embed(question)
  │
  ▼  VectorMemory.search(queryVec, k=4)   — brute-force over MapState
  │
  ▼  Scorer.scorePair(passage, question)  — cross-encoder rerank
  │
  ▼  LLM answers using top-3 passages, cites [1] [2] [3]
```

## Why Flink-state vector memory?

The framework ships brute-force KNN backed by `MapState<String, VectorEntry>`.
For a few thousand vectors per key (the typical "conversation-local recall"
use case), exact search at d=384 is sub-millisecond. The state itself rides
in Flink checkpoints, so a job restart picks up where it left off — no
"rebuild the index" boot sequence.

When you outgrow that (10⁵+ vectors per key), drop in an HNSW-backed
`VectorMemorySpec` via the SPI. The agent code doesn't change.

## Why per-topic keying?

Topics are independent corpora. Keying by topic gives each topic its own
`VectorMemory` scope, which means:

- A noisy topic doesn't drown a clean one in the same brute-force scan.
- Different topics can use different embedders or dimensions if you want
  (one operator per topic, different specs).
- Eviction policies (TTL, max-items) apply per topic.

## Seeding the corpus

The demo seeds 4 documents per topic on the first event using a
`ValueState<Boolean>` flag. In production you'd either:

- **Hydrate from a `LongTermMemoryStore`** on cold-start — the framework's
  standard pattern.
- **Stream new documents in via a `Channel<KeyedContextItem>`** —
  `KafkaContextChannel`, `PostgresChangeChannel`, `RedisPubSubChannel`, or
  any custom transport. The channel produces `KeyedContextItem` records that
  the agent operator union-connects with its main input.

Both approaches are covered in [`docs/memory.md`](../memory.md). For the
unified channel SPI see [`docs/channels.md`](../channels.md). For a more
ambitious example with two inputs sharing a corpus, see
[live-research](live-research.md).

## Adding an MCP tool source

The demo keeps the runtime self-contained; to let the LLM call out to file or
web tools through Model Context Protocol, register an `McpServerSpec` and
discover its tools at job startup:

```java
McpServerSpec everything = McpServerSpec.stdio(
    "everything", "npx", "-y", "@modelcontextprotocol/server-everything");

Agent agent = Agent.builder()
    .withId("rag")
    .withChatConnection(chat)
    .withChatSetup(chatSetup)
    .withMcpServer(everything)
    .build();

// In the operator's open():
try (McpClient client = new McpClient(everything)) {
    client.initialize();
    for (McpToolExecutor t : McpToolRegistry.discover(everything, client)) {
        toolRegistry.register(t.getToolId(), t);
    }
}
```

Each discovered tool surfaces as a regular `ToolExecutor` — the LLM doesn't
know or care that it's MCP under the hood.

## Performance shape

On a recent laptop with the suggested models:

- Embed: ~12 ms (MiniLM-L6-v2, batch 1, CPU)
- Brute-force search (k=4 over 4 vectors): ~0.1 ms
- Rerank top-4: ~80 ms (cross-encoder, batch 4)
- LLM answer (qwen2.5:3b): ~600 ms

The LLM dominates; everything else is overhead. Scale the embedder + vector
search up to 10k vectors per key before the search side starts to matter.

## Failure modes

- **Cold first run**: first call downloads model weights (~250 MB across the
  three HF models). Subsequent runs are cache-warm.
- **Dimension mismatch**: `FlinkStateVectorMemory.spec(384)` *must* match the
  embedder's actual dimension. The framework throws
  `IllegalArgumentException` at insert time if they disagree.
- **No-op rerank when k=1**: with a single hit, the cross-encoder still runs.
  Skip it conditionally if you care about the savings.
