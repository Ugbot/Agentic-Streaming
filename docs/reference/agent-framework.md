# Agent framework: architecture reference

A focused reference to how the framework is shaped today: the SPIs, how they
compose, and where you plug in. Written in the present tense, features
documented here are shipped, not planned.

For runnable walkthroughs of these ideas wired together, see
[`docs/examples/`](../examples/).

## Mental model

Agentic Flink is a thin set of SPIs over Apache Flink that lets you describe
an agent declaratively (`Agent.builder()...build()`) and then run it inside a
Flink operator. The framework is **vendor-neutral by default**. LangChain4J
is the default chat backend, DJL the default inference backend, Postgres the
default long-term store, but every one of those is a swap away.

Memory is **Flink-state-first.** Short-term context lives in keyed state and
rides in checkpoints; long-term storage exists for resumption and archival
but isn't on the hot path.

```
                  ┌────────────────────────────────┐
                  │           Agent.builder()       │
                  │  - immutable spec, ships in     │
                  │    the Flink job graph          │
                  └──────────────┬─────────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
  ChatConnection           InferenceConnection      LongTermMemoryStore
   (transport)               (transport)              (optional)
        │                        │                        │
        ▼ bind(RuntimeContext)   ▼ bind(...)              ▼
  ChatClient               InferenceClient           Postgres / Redis / in-mem
   .chat(messages,         .asClassifier()
         ChatSetup)        .asScorer()
                           .asEmbedder()
                           .asGeneric()
```

## The SPIs

Every pluggable surface follows the same three-layer pattern:

| Layer | Role | Lifecycle |
|-------|------|-----------|
| **Connection** | Serializable spec. Vendor / endpoint / credentials. | Ships in the job graph; ServiceLoader-discoverable. |
| **Client** | Runtime handle. Holds live HTTP / native / state resources. | Built once per Flink task via `bind(RuntimeContext)` in `open()`. |
| **Setup** | Per-call config. Model name, temperature, dimension, etc. | Created per call; cheap and immutable. |

Eight SPIs follow this shape today:

| SPI | Package | Default |
|-----|---------|---------|
| `ShortTermMemorySpec` | `memory` | `FlinkStateShortTermMemory` |
| `VectorMemorySpec` | `memory.vector` | `FlinkStateVectorMemory` (brute-force), `FlinkStateHnswVectorMemory` (HNSW) |
| `CorpusSpec` | `corpus` | `SingleOperatorCorpus`, `BroadcastCorpus`, `ExternalCorpus` |
| `LongTermMemoryStore` | `storage` | `PostgresConversationStore` |
| `VectorStore` | `storage` | `PgVectorStore` (opt-in) |
| `Channel<T>` | `channel` | `StaticSeedChannel`, `KafkaChannel<T>`, `WebhookChannel<T>`, `KafkaContextChannel`, `PostgresChangeChannel`, `RedisPubSubChannel`, `ToolInvocationChannel<T>` |
| `ChatConnection` | `llm` | `LangChain4jChatConnection` (Ollama) |
| `EmbeddingConnection` | `embedding` | `OllamaEmbeddingConnection`, `DjlEmbeddingConnection` |
| `InferenceConnection` | `inference` | `DjlInferenceConnection` (opt-in) |
| `AgentEventListener` | `listener` | `LoggingAgentEventListener` |

Tools (`tools.ToolExecutor`) and skills (`skill.Skill`) are simpler, no
Connection/Client split because they don't hold persistent transports.
Guardrails (`inference.Guardrail`) are an interceptor layer over the chat
path.

## Concept index

### Agent

`dsl.Agent` is an immutable specification of behaviour: name, type, prompt,
chat backend, tools, guardrails, memory stores, listeners, skills, MCP servers
and remote A2A peers. Built once via `AgentBuilder` and serialized into the
job graph. The DSL is the supported pure-Flink flavour of the framework; it
sits outside the agentic/v1 conformance path.

```java
Agent agent = Agent.builder()
    .withId("research-bot")
    .withSystemPrompt("...")
    .withChatConnection(LangChain4jChatConnection.ollama("http://localhost:11434"))
    .withChatSetup(ChatSetup.builder()
        .withModel("qwen2.5:7b").withTemperature(0.3).withMaxResponseTokens(2048)
        .build())
    .withTools("search", "fetch")
    .withRequiredTools("search")
    .withToolDefaults("search", Map.of("max_results", 5))
    .withToolTimeout(Duration.ofSeconds(20))
    .withCompensatingTool("fetch", "purge_cache")
    .withShortTermTtl(Duration.ofHours(1))
    .withConversationStore(new InMemoryConversationStore())
    .withLongTermStore(StorageFactory.createLongTermStore("postgres", pgConfig))
    .withMcpServer(McpServerSpec.stdio("calc", "npx", "-y", "mcp-server-calculator"))
    .withInferenceTool(toxicityAdapter)
    .withGuardrail(guardrailInstance)
    .withSkill(researchSkill)
    .withListener(new LoggingAgentEventListener(), new MetricsAgentEventListener())
    .withMaxIterations(10)
    .build();
```

Every `with*` method is optional. The minimum viable agent is
`Agent.builder().withId(...).withSystemPrompt(...).build()`; it gets the
default state machine, chat setup and an in-memory conversation store.

Each builder method is read by an operator of the legacy job graph, and
`src/test/java/org/agentic/flink/dsl/AgentBuilderTest` asserts the effect of
every one of them:

| Builder methods | Consumer |
|-----------------|----------|
| `withId`, `withName`, `withDescription`, `withType` | identity on every emitted event; `withType` picks the default budgets and `ChatSetup` |
| `withSystemPrompt`, `withSkill` | the system message of every LLM call (skill prompt fragments are appended) |
| `withChatConnection`, `withChatSetup`, `withOutputSchema` | the `LLMClient` built in `stream.AgentExecutionFunction.open()` |
| `withTools`, `withRequiredTools`, `withToolDefaults`, `withToolTimeout`, `withMcpServer`, `withRemoteAgent`, `withA2AClientFactory`, `withInferenceTool` | the effective `ToolRegistry` of the execution operator; a missing required tool fails `open()`, defaults are merged under the model's arguments, every call is bounded by the tool timeout |
| `withMaxIterations`, `withTimeout` | loop and async operator bounds |
| `withMaxValidationAttempts`, `withMaxCorrectionAttempts`, `withStateMachine` | the default `AgentStateMachine` (or your own) that drives the CEP pattern |
| `withCompensationEnabled`, `withCompensatingTool` | a failed turn lists the compensating tools of the completed steps; `stream.CompensationFunction` runs them in reverse call order |
| `withShortTermTtl` | TTL of the keyed dispatch state in `AgentTurnDispatcher` |
| `withConversationStore`, `withLongTermStore` | history appended per turn; the turn is archived as a fact and `onLongTermSync` fires |
| `withGuardrail`, `withListener` | installed on the `LLMClient`; blocked calls return the guardrail's reason |

Vector memory, embedding connections, named inference connections and memory
channels are wired on the pipeline builders (`retrieve.RetrievalPipeline`,
`ingest.IngestionPipeline`, `ChannelRegistry`) rather than on `AgentBuilder`.

### Memory

Three layers:

1. **Short-term**: `FlinkStateShortTermMemory` over `ValueState` +
   `MapState` with `StateTtlConfig`. The hot path. Checkpoints provide
   durability; no external HOT tier required.
2. **Vector**: `FlinkStateVectorMemory` over `MapState<String, VectorEntry>`,
   brute-force KNN, configurable scope (per-key or per-operator). Default
   for in-JVM semantic recall; SPI escape hatch for HNSW backends.
3. **Long-term**: `LongTermMemoryStore` (Postgres / Redis / in-memory).
   Used for conversation resumption across job lifetimes and fact archive.
   Write-behind from Flink state.

External memories can be **fed in** via any `Channel<KeyedContextItem>`,
`KafkaContextChannel`, `PostgresChangeChannel`, `RedisPubSubChannel`, or any
custom transport you implement. Items materialize as a `DataStream` and
union-connect into the agent operator.

Detail: [`docs/memory.md`](../memory.md), [`docs/channels.md`](../channels.md),
[`docs/corpus.md`](../corpus.md).

### Chat (LLM)

The `ChatConnection` ↔ `ChatClient` ↔ `ChatSetup` split mirrors upstream
Apache Flink Agents' `BaseChatModelConnection` / `Setup`. One connection per
provider deployment, many setups per agent.

```java
ChatConnection conn = LangChain4jChatConnection.ollama("http://localhost:11434");
ChatClient client = conn.bind(getRuntimeContext());          // in open()
ChatResponse r = client.chat(messages,
    ChatSetup.builder().withModel("qwen2.5:7b").withTemperature(0.3).build());
```

For LangChain4J-specific features the SPI doesn't expose, downcast through
the documented escape hatch:

```java
if (client instanceof LangChain4jChatClient lc) {
    ChatLanguageModel raw = lc.getUnderlyingModel();
    // ... LangChain4J idioms here ...
}
```

Public framework API never returns `dev.langchain4j.*` types.

### Inference (non-LLM DL models)

`InferenceConnection` exposes up to four task surfaces on a single client:

- `Classifier`, text → label + score + probability distribution
- `Scorer`, text → numeric (also pair-scoring for cross-encoders)
- `EmbeddingClient`, the existing embedding SPI, not a parallel hierarchy
- `GenericInferenceModel`, `Map → Map` escape hatch

`DjlInferenceConnection` is the default backend, covering PyTorch, TF, ONNX,
and HuggingFace under one API. DJL is `<optional>true</optional>` in the pom
, users who don't use DL pay nothing transitively.

Inference models slot into the agent in four ways:

| Integration | API |
|-------------|-----|
| Standalone | bind the connection in your operator's `open()`: `InferenceClient client = conn.bind(getRuntimeContext())` |
| As a tool | `withInferenceTool(InferenceToolAdapter)` |
| As a guardrail | `withGuardrail(new ClassifierGuardrail(...))` |
| As `RelevancyScorer`'s backend | `new RelevancyScorer(scorer, setup)` |

Detail: [`docs/inference.md`](../inference.md).

### Tools and skills

`tools.ToolExecutor` is the framework's tool interface:
`execute(Map<String, Object>) → CompletableFuture<Object>`. Tools register
through `ToolRegistry`. Built-in `@Tool`-annotated tools discover via the
LangChain4J annotation registry; MCP servers discover at job startup.

Skills (`skill.Skill`) bundle tools + a system-prompt fragment + required
facts into one named capability. `AgentBuilder.withSkill(...)` fans the
tools out to `allowedTools` and concatenates the prompt fragment.

### Guardrails

`inference.Guardrail` is a small interceptor interface. Implementations are
called from inside `LLMClient.chat(...)`:

- `beforeChat(agentId, messages) → GuardrailDecision`
- `afterChat(agentId, response) → GuardrailDecision`

Returning `BLOCK` short-circuits the chat; `REWRITE` swaps the payload.
Listener hooks fire either way.

The canonical impl is `ClassifierGuardrail`, plug any DL classifier into it
and configure a block-list of labels.

### Observability

`AgentEventListener` has eleven lifecycle hooks:

```
onAgentStart, onChatRequest, onChatResponse,
onToolCallStart, onToolCallEnd,
onCompaction, onLongTermSync, onError,
onInference, onGuardrailBlock, onGuardrailRewrite
```

`CompositeListener` fans out across multiple registrations and isolates
exceptions. Reference impls: `LoggingAgentEventListener` (SLF4J),
`MetricsAgentEventListener` (in-memory counters; wire to Flink's
`MetricGroup` in your operator).

### Orchestration

Two complementary models:

- **CEP-driven**: Flink CEP patterns drive when the agent runs (validation,
  escalation, anomaly confirmation). See `cep/CepSpecTranslator`, which turns
  a declarative `cep:` rule into a native Flink `Pattern`. Pair with the
  incident example.
- **Workflow / ReAct**: `function.ReActProcessFunction` packages the
  canonical Thought / Action / Observation loop on the `ChatClient` SPI,
  bounded by `Agent.getMaxIterations()`. Pair with the RAG example.

The framework also ships `statemachine/AgentStateMachine` for transition-
based workflows when CEP and ReAct don't fit. Saga rollback lives in
`compensation/` and runs inside the legacy job graph through
`stream.CompensationFunction`.

## Putting it together

The four runnable examples in `docs/examples/` exercise different
combinations of the SPIs:

| Example | Combinations exercised |
|---------|------------------------|
| [Support triage](../examples/support-triage.md) | Guardrail + InferenceToolAdapter + Scorer rerank + LangChain4J escape hatch |
| [Content moderation](../examples/moderation.md) | Classifier-as-hard-gate + ProcessFunction side outputs + listener-driven audit |
| [RAG research](../examples/rag.md) | Embedder + FlinkStateVectorMemory + cross-encoder rerank + per-key keying |
| [Incident agent](../examples/incident.md) | GenericInferenceModel + Flink CEP + tools + state machine |

For shorter recipes (one SPI combination per snippet), see
[`docs/cookbook.md`](../cookbook.md).

## Package map

```
src/main/java/org/agentic/flink/
    dsl/             Agent, AgentBuilder
    core/            AgentEvent, AgentEventType (typed event model)
    llm/             ChatConnection / Client / Setup / OutputSchema
        langchain4j/  default LangChain4J chat backend + escape hatch
    embedding/       EmbeddingConnection / Client / Setup
        djl/         optional DJL-backed embedder
    inference/       Classifier, Scorer, EmbeddingClient view, GenericInferenceModel
                     Guardrail + ClassifierGuardrail
                     InferenceToolAdapter (model-as-tool)
                     InferenceModelCache (per-JVM weight cache)
        djl/         optional DJL backend covering classification + embedding
    memory/          FlinkStateShortTermMemory + ShortTermMemorySpec
        vector/      FlinkStateVectorMemory (brute-force) + FlinkStateHnswVectorMemory (HNSW)
                     + VectorMemorySpec + ScoredItem
    channel/         Channel<T> SPI + StaticSeed/Kafka/Webhook/ToolInvocation
                     + KafkaContextChannel / PostgresChangeChannel / RedisPubSubChannel
    corpus/          Corpus + CorpusSpec + SingleOperator/Broadcast/External flavours
    web/             Jsoup + crawler-commons + Tika: Fetcher, RobotsCache,
                     DocumentExtractor, CrawlerCore, WebFetchTool, ExtractLinksTool
    ingest/          Chunker + RecursiveTextChunker + IngestionPipeline
    retrieve/        RetrievalPipeline + RetrievedPassage + Answer
    storage/         LongTermMemoryStore SPI; in-memory + Postgres + Redis impls
                     MemorySet / MemorySetAccessor (typed long-term cohorts)
        vector/      PgVectorStore (pgvector impl of VectorStore)
    listener/        AgentEventListener + Logging / Metrics / Composite
    skill/           Skill + SkillRegistry
    tool/            ToolRegistry
    tools/           ToolExecutor + built-ins + RAG helpers
        mcp/         McpServerSpec + JSON-RPC client + tool adapter
    cep/             CEP pattern builder
    statemachine/    AgentStateMachine + transition DSL
    compensation/    Saga compensation
    function/        Flink ProcessFunction wrappers (ReActProcessFunction, etc.)
    execution/       LLMClient (wraps ChatConnection) + AgentExecutor
    plugins/
        flintagents/ optional bridge to Apache Flink Agents 0.3-SNAPSHOT
```

## Where to put your own code

- **New LLM provider** → implement `ChatConnection` + `ChatClient`. Register
  via `META-INF/services/org.agentic.flink.llm.ChatConnection`.
- **New embedder** → implement `EmbeddingConnection` + `EmbeddingClient`.
- **New DL backend** → implement `InferenceConnection` + `InferenceClient`
  (return `Classifier` / `Scorer` / `EmbeddingClient` / `GenericInferenceModel`
  as your backend supports).
- **New tool source** → implement `ToolExecutor` and register in your
  `ToolRegistry`, or wrap an existing protocol like MCP did.
- **New audit / metrics sink** → implement `AgentEventListener` and register
  via `META-INF/services/org.agentic.flink.listener.AgentEventListener`
  or programmatically with `AgentBuilder.withListener(...)`.
- **New channel transport** → implement `Channel<T>` (or
  `Channel<KeyedContextItem>` for the memory-feed shape). Wire programmatically
  via `ChannelRegistry`.
- **New vector store** → implement `VectorStore` and register via
  `META-INF/services/org.agentic.flink.storage.VectorStore`.
- **New corpus flavour** → implement `CorpusSpec` (and the matching `Corpus`
  runtime view).

The framework's own backends are all examples of the same patterns; reading
`LangChain4jChatConnection`, `OllamaEmbeddingConnection`, or
`DjlInferenceConnection` is the fastest way to learn how the SPI shape works
in practice.
