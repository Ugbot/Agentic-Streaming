# Cookbook

Short recipes for common SPI combinations. Each recipe is self-contained — copy
the snippet, plug in your chat / inference / storage backends, and go.

## Recipes

- [1. Guard the LLM with a sentiment classifier](#1-guard-the-llm-with-a-sentiment-classifier)
- [2. Expose a model as a tool](#2-expose-a-model-as-a-tool)
- [3. Replace heuristic relevancy with a cross-encoder](#3-replace-heuristic-relevancy-with-a-cross-encoder)
- [4. Vector recall over Flink keyed state](#4-vector-recall-over-flink-keyed-state)
- [5. Feed external memories in via Kafka](#5-feed-external-memories-in-via-kafka)
- [6. Reach the underlying LangChain4J model](#6-reach-the-underlying-langchain4j-model)
- [7. Structured output with `OutputSchema`](#7-structured-output-with-outputschema)
- [8. CEP-gated agent runs](#8-cep-gated-agent-runs)
- [9. Audit listener writing to Postgres](#9-audit-listener-writing-to-postgres)
- [10. Skills bundling tools + prompt + facts](#10-skills-bundling-tools--prompt--facts)
- [11. HNSW vector memory over Flink state](#11-hnsw-vector-memory-over-flink-state)
- [12. Swap to pgvector for production](#12-swap-to-pgvector-for-production)
- [13. Web fetch as an LLM tool](#13-web-fetch-as-an-llm-tool)
- [14. LLM-driven channel into a crawler](#14-llm-driven-channel-into-a-crawler)
- [15. Broadcast corpus shared across operators](#15-broadcast-corpus-shared-across-operators)
- [16. PyFlink-native agent with Python decorators](#16-pyflink-native-agent-with-python-decorators)
- [17. Inspect a Python agent plan offline](#17-inspect-a-python-agent-plan-offline)

---

### 1. Guard the LLM with a sentiment classifier

```java
DjlInferenceConnection sentiment = DjlInferenceConnection.classification(
    "djl://ai.djl.huggingface.pytorch/distilbert-base-uncased-finetuned-sst-2-english");
InferenceSetup setup = InferenceSetup.builder()
    .withModelName("sst-2").withModelUri(sentiment.getDefaultModelUri()).build();
ClassifierGuardrail guard = new ClassifierGuardrail(
    "abuse", sentiment, setup, Set.of("NEGATIVE"), /* input */ true, /* output */ false);

Agent agent = Agent.builder()
    .withId("support")
    .withSystemPrompt("…")
    .withGuardrail(guard)
    .build();
```

Pre-LLM blocks short-circuit the chat call entirely; post-LLM blocks rewrite or
suppress the response. Listener hooks fire either way.

### 2. Expose a model as a tool

```java
InferenceToolAdapter intent = new InferenceToolAdapter(
    "ticket-intent",
    "Classify a support ticket into billing/technical/refund/general.",
    DjlInferenceConnection.classification("djl://…/facebook/bart-large-mnli"),
    InferenceSetup.builder().withModelName("bart-mnli").withModelUri("djl://…/bart-mnli").build(),
    InferenceToolAdapter.TaskKind.CLASSIFIER);

Agent agent = Agent.builder()
    .withId("triage")
    .withInferenceTool(intent)
    .build();
```

The LLM sees `ticket-intent` in its allowed tool list. The adapter expects a
`text` argument from the model and returns a map of `{label, score, probabilities}`.

### 3. Replace heuristic relevancy with a cross-encoder

```java
DjlInferenceConnection ranker = DjlInferenceConnection.classification(
    "djl://ai.djl.huggingface.pytorch/cross-encoder/ms-marco-MiniLM-L-6-v2");
InferenceSetup setup = InferenceSetup.builder()
    .withModelName("ms-marco").withModelUri(ranker.getDefaultModelUri()).build();

RelevancyScorer relevancy = new RelevancyScorer(ranker.bind(null).asScorer(), setup);
```

`RelevancyScorer` keeps the same `scoreRelevancy(item, intent)` API — existing
callers don't change. Output is clamped to `[0, 1]`.

### 4. Vector recall over Flink keyed state

```java
DjlEmbeddingConnection embeddings = DjlEmbeddingConnection.of(
    "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2");
VectorMemorySpec memorySpec = FlinkStateVectorMemory.spec(384);

// In a KeyedProcessFunction.open():
EmbeddingClient embedder = embeddings.bind(getRuntimeContext());
VectorMemory vector = memorySpec.bind(getRuntimeContext());

// In processElement:
vector.put(docId, embedder.embed(doc, EmbeddingSetup.of("MiniLM", 384, true)), contextItem);
List<ScoredItem> hits = vector.search(
    embedder.embed(query, EmbeddingSetup.of("MiniLM", 384, true)), 4);
```

Brute-force KNN over `MapState`. Scales to a few thousand vectors per key
trivially; for 10⁵+ per key drop in an HNSW-backed `VectorMemorySpec`.

### 5. Feed external memories in via Kafka

```java
Channel<KeyedContextItem> feed =
    new KafkaContextChannel("kafka:9092", "agent-memories", "research-agent");

Agent agent = Agent.builder()
    .withId("research-agent")
    .withMemoryChannel(feed)
    .build();
```

The channel produces a `DataStream<KeyedContextItem>` that's union-connected
to the agent operator's input. Items land in Flink state via the same write
path as in-band events. Swap `KafkaContextChannel` for `PostgresChangeChannel`
/ `RedisPubSubChannel` / any custom `Channel<KeyedContextItem>` without
changing the agent. See [`docs/channels.md`](channels.md) for the full SPI.

### 6. Reach the underlying LangChain4J model

```java
ChatClient client = chatConnection.bind(getRuntimeContext());
if (client instanceof LangChain4jChatClient lc) {
    // Trigger at least one chat so the model is built.
    lc.chat(List.of(ChatMessage.user("warmup")), warmupSetup);
    ChatLanguageModel raw = lc.getUnderlyingModel();
    // Now use LangChain4J-specific features:
    String reply = raw.generate("System prompt", "User prompt");
}
```

This is the documented escape hatch. Code that uses it is implementation-
coupled, but it lets you opt back into LangChain4J idioms (typed AI services,
custom output parsers) when you need them without leaking into the framework.

### 7. Structured output with `OutputSchema`

```java
@Data @NoArgsConstructor @AllArgsConstructor
class Verdict { boolean accepted; double confidence; String reason; }

ChatSetup setup = ChatSetup.builder()
    .withModel("qwen2.5:7b")
    .withTemperature(0.1)
    .withOutputSchema(OutputSchema.of(Verdict.class))
    .build();

ChatResponse resp = chatClient.chat(messages, setup);
Verdict v = resp.as(OutputSchema.of(Verdict.class));   // typed JSON parse, fence-stripping built in
```

`OutputSchema.of(Class)` infers a minimal JSON Schema by reflection and parses
the LLM's response through Jackson, automatically peeling away markdown
fences.

### 8. CEP-gated agent runs

```java
Pattern<MetricSample, ?> burst = Pattern.<MetricSample>begin("a")
    .where(SimpleCondition.of(s -> s.value() > 900))
    .timesOrMore(3).within(Duration.ofMinutes(5));

CEP.pattern(metrics.keyBy(MetricSample::host), burst)
    .select(m -> new IncidentEvent(...))
    .keyBy(IncidentEvent::host)
    .process(new IncidentAgentFn(chatConn, chatSetup))
    .print();
```

The LLM only fires on patterns that matter. Pair with anomaly detection
upstream (a `GenericInferenceModel`) to filter cheaper signals first.

### 9. Audit listener writing to Postgres

```java
class PostgresAuditListener implements AgentEventListener {
    private final LongTermMemoryStore store;
    PostgresAuditListener(LongTermMemoryStore store) { this.store = store; }

    @Override public void onGuardrailBlock(String agentId, String modelName, String label) {
        ContextItem item = new ContextItem(
            "BLOCKED label=" + label, ContextPriority.MUST, MemoryType.LONG_TERM);
        try { store.addFact(agentId, item.getItemId(), item); }
        catch (Exception ignored) {}
    }
}
```

Register via `AgentBuilder.withListener(...)`. The `MetricsAgentEventListener`
reference impl ships counters for every hook so basic SLOs are one-liner away.

### 10. Skills bundling tools + prompt + facts

```java
Skill research = Skill.builder()
    .withName("research")
    .withTools("web-search", "doc-fetch", "summarize")
    .withSystemPromptFragment("Prefer primary sources. Cite arxiv IDs.")
    .withRequiredFacts("user_research_area")
    .build();

Agent agent = Agent.builder()
    .withId("research-bot")
    .withSystemPrompt("Base prompt …")
    .withSkill(research)        // tools fan out to allowedTools; prompt is concatenated
    .build();
```

Skills are additive over `withTools(...)` — a clean way to package reusable
capability bundles.

### 11. HNSW vector memory over Flink state

```java
VectorMemorySpec hnsw = FlinkStateHnswVectorMemory.spec(
    384, new HnswBuildConfig(16, 100, 50, 1.2f, VectorMemorySpec.Similarity.COSINE));

CorpusSpec corpus = SingleOperatorCorpus.spec("my-kb", hnsw);
```

Vectors live in `MapState`; the HNSW graph is rebuilt from MapState on
operator `open()`. For corpora up to roughly 10⁵ vectors per key this is
sub-millisecond queries with no external infra.

### 12. Swap to pgvector for production

Same agent code; different corpus spec.

```java
CorpusSpec corpus = ExternalCorpus.spec(
    "kb",
    "pgvector",
    Map.of(
        "postgres.url", "jdbc:postgresql://prod-db:5432/agentic",
        "postgres.user", "agent",
        "postgres.password", System.getenv("PG_PASSWORD"),
        "postgres.dimension", "384",
        "pgvector.similarity", "cosine"),
    384);
```

`pgvector` registers via `META-INF/services`. The schema (`agent_vectors`
table + ivfflat index) is created on first use.

### 13. Web fetch as an LLM tool

```java
Agent.builder()
    .withId("research-bot")
    .withSystemPrompt("...")
    .withTools("web-fetch", "extract-links")
    .build();

ToolRegistry registry = ToolRegistry.builder()
    .registerTool("web-fetch",  "Fetch a URL", new WebFetchTool(WebToolkitOptions.defaults()))
    .registerTool("extract-links", "Discover links",
                  new ExtractLinksTool(WebToolkitOptions.defaults()))
    .build();
```

The LLM can call `web-fetch(url=...)` and get back `{title, text, links,
contentType}`; Jsoup handles HTML, Tika handles everything else.

### 14. LLM-driven channel into a crawler

```java
ToolInvocationChannel<UrlRequest> agentCrawl =
    ToolInvocationChannel.sideOutput(
        "crawl-url",
        UrlRequest.class,
        params -> new UrlRequest((String) params.get("url"), "agent"));

CrawlerCore.builder()
    .frontier(seedChannel, agentCrawl)
    .options(WebToolkitOptions.defaults())
    .open(env);
```

LLM calls to `crawl-url` materialize as `UrlRequest` records on a Flink
side-output that union-joins the crawler's frontier. The crawler doesn't
know or care that this channel is LLM-driven — it just consumes URLs.

### 15. Broadcast corpus shared across operators

```java
CorpusSpec corpus =
    BroadcastCorpus.spec("kb", FlinkStateHnswVectorMemory.spec(384));

// Ingest pipeline writes; broadcast its output to a separate read operator:
MapStateDescriptor<String, VectorEntry> bsDescriptor =
    new MapStateDescriptor<>("kb-bs", String.class, VectorEntry.class);
BroadcastStream<VectorEntry> bs = ingestStream.broadcast(bsDescriptor);

queries.connect(bs).process(new MyBroadcastReader(corpus, bsDescriptor));
```

Each query replica holds an independent copy of the corpus; reads scale
independently of ingest.

### 16. PyFlink-native agent with Python decorators

```python
from agentic_flink.pyflink import Agent, ResourceRef, action, environment, tool
from pyflink.datastream import StreamExecutionEnvironment

class TriageAgent(Agent):
    agent_id = "triage"
    chat_connection = ResourceRef(
        "org.agentic.flink.llm.langchain4j.LangChain4jChatConnection",
        {"provider": "OLLAMA", "base_url": "http://localhost:11434"},
    )

    @tool
    def classify(self, text: str) -> str:
        return "billing" if "refund" in text.lower() else "general"

    @action("ticket")
    def reply(self, event, ctx):
        return {"id": event["id"], "label": self.classify(event["body"])}

s_env = StreamExecutionEnvironment.get_execution_environment()
tickets = s_env.from_collection([...])

ae = environment(s_env)
(ae.from_datastream(tickets, key_selector=lambda t: t["id"])
   .apply(TriageAgent())
   .to_datastream()
   .print())
s_env.execute("triage")
```

The agent ships as a JSON plan; the operator runs in the JVM; PEMJA invokes
your Python tool/action on the operator thread. See
[`pyflink-integration.md`](pyflink-integration.md) for the full mechanics.

### 17. Inspect a Python agent plan offline

```python
import json
from agentic_flink.pyflink.plan import build_plan

print(json.dumps(build_plan(TriageAgent()), indent=2))
```

No JVM, no PyFlink — just walks the decorated class and dumps the JSON
that would be sent across the gateway. Handy for diffing schema changes
and writing unit tests against the plan shape.
