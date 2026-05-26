# RAG research assistant example

End-to-end retrieval-augmented generation on Flink keyed state. Per inbound
query: embed → top-k recall over Flink-state vector memory → cross-encoder
rerank → LLM answers with citations.

```
Query (topic, question)
  │
  ▼  keyBy(topic)
  │
  ▼  RagProcessFunction.open()
  │       ─► binds embedder, reranker, chat, vector memory
  │       ─► seeds per-topic KB on first event
  ▼  embed(question)  → vector.search(k=4)
  ▼  reranker.scorePair(passage, question) × k  →  sort
  ▼  LLM answers using top-3 passages, cites [1] [2] [3]
  │
  ▼  Answer
```

## What's interesting

| Piece | API used |
|-------|----------|
| Embedder | `DjlEmbeddingConnection` over sentence-transformers/all-MiniLM-L6-v2 (384-d) |
| Vector memory | `FlinkStateVectorMemory.spec(384)` — exact brute-force KNN over `MapState` |
| Reranker | `Scorer` via `DjlInferenceConnection` — cross-encoder/ms-marco-MiniLM-L-6-v2 |
| LLM | vendor-neutral `ChatConnection` (Ollama by default; swap freely) |
| Per-key state | `ValueState<Boolean>` tracks whether the KB has been seeded for the topic |

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
mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.rag.RagResearchExample"
```

Or:

```bash
./examples-bin/run-rag.sh
```

## Adding an MCP tool source

To let the LLM call out to external file/web tools through MCP, register a
server on the agent and let `McpToolRegistry` discover its tools:

```java
McpServerSpec everything = McpServerSpec.stdio(
    "everything", "npx", "-y", "@modelcontextprotocol/server-everything");

Agent agent = Agent.builder()
    .withId("rag")
    .withChatConnection(chat)
    .withChatSetup(chatSetup)
    .withMcpServer(everything)
    .build();

// In open():
try (McpClient client = new McpClient(everything)) {
    client.initialize();
    for (McpToolExecutor t : McpToolRegistry.discover(everything, client)) {
        toolRegistry.register(t.getToolId(), t);
    }
}
```

## Expected output

```
Answer[topic=flink, question=How does Flink guarantee exactly-once state?,
  answer=Flink uses a stream-processing model with exactly-once state guarantees [1],
         and the RocksDB state backend supports incremental checkpoints to S3 or HDFS [2].,
  topScore=8.91]
Answer[topic=agents, question=What is a guardrail?,
  answer=Guardrails are pre/post-LLM classifiers that can block or rewrite a call [1]. ...,
  topScore=9.42]
```

(Exact phrasing depends on the LLM; the citations should always line up with
the numbered sources passed to the model.)
