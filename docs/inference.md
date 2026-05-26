# Inference: traditional DL models as first-class agent components

Agentic Flink treats traditional deep learning models — classifiers,
scorers/regressors, embedders, anomaly detectors, anything that isn't an LLM —
as ordinary, swappable framework components. The contract mirrors the chat-
model SPI: a serializable `InferenceConnection` (transport) ships in the job
graph, `bind(RuntimeContext)` produces a runtime `InferenceClient` in
`open()`, and a per-call `InferenceSetup` carries the model URI, device, batch
size, and threads.

## Why this exists

LLMs are powerful but expensive and slow. Many real workflows want cheap,
deterministic decisions instead:

| Use case | Better with… |
|----------|--------------|
| Intent routing | small classifier (DistilBERT, fastText) |
| Content moderation / safety guardrail | dedicated safety classifier |
| Relevancy scoring | trained cross-encoder ranking model |
| Semantic recall | sentence-transformers embedder |
| Anomaly detection on streaming events | autoencoder / isolation forest |

The framework gives every one of these a slot in `AgentBuilder` without forcing
you to write your own SPI.

## The four task surfaces

A single `InferenceClient` exposes up to four typed views:

| View | Method | Input → Output |
|------|--------|----------------|
| `Classifier` | `classify(String, InferenceSetup)` | text → label + score + probabilities |
| `Scorer` | `score(String, InferenceSetup)` / `scorePair(String, String, InferenceSetup)` | text → numeric score |
| `EmbeddingClient` | `embed(String, EmbeddingSetup)` | text → `float[]` |
| `GenericInferenceModel` | `infer(Map, InferenceSetup)` | map → map (escape hatch) |

Implementations only need to support the surfaces they cover. Calls to
unsupported views throw `UnsupportedOperationException`; probe with
`client.supports(TaskKind.X)` first.

Note that the **embedder view is the same `EmbeddingClient` the chat layer
uses** — there is no parallel hierarchy. Anything that registers as an
`EmbeddingConnection` (e.g. `DjlEmbeddingConnection`) works as the agent's
embedder out of the box.

## DJL: the default backend

DJL (Deep Java Library) is the primary backend. One API covers PyTorch,
TensorFlow, ONNX, MXNet, and HuggingFace tokenizers.

The DJL artifacts are **optional** dependencies in the project's pom. Users
who don't run DL pay nothing transitively. To opt in, add:

```xml
<dependency>
  <groupId>ai.djl</groupId>
  <artifactId>api</artifactId>
  <version>0.30.0</version>
</dependency>
<dependency>
  <groupId>ai.djl.huggingface</groupId>
  <artifactId>tokenizers</artifactId>
  <version>0.30.0</version>
</dependency>
<dependency>
  <groupId>ai.djl.pytorch</groupId>
  <artifactId>pytorch-engine</artifactId>
  <version>0.30.0</version>
</dependency>
```

Plus the right native binary for your platform:

| Platform | Native artifact |
|----------|-----------------|
| Linux / macOS CPU | `ai.djl.pytorch:pytorch-native-cpu` |
| Linux CUDA 11.8 | `ai.djl.pytorch:pytorch-native-cu118` |
| macOS Apple Silicon | `ai.djl.pytorch:pytorch-native-cpu` (auto-uses MPS where available) |

The framework deliberately does **not** pull a native binary itself — pick
the one matching your deployment.

## Wiring it up

### Classifier as a guardrail

```java
DjlInferenceConnection safety =
    DjlInferenceConnection.classification(
        "djl://ai.djl.huggingface.pytorch/protectai/deberta-v3-base-prompt-injection-v2");

InferenceSetup setup =
    InferenceSetup.builder()
        .withModelName("prompt-injection-v2")
        .withModelUri(safety.getDefaultModelUri())
        .build();

ClassifierGuardrail guardrail =
    new ClassifierGuardrail(
        "prompt-injection",
        safety,
        setup,
        java.util.Set.of("INJECTION"),
        /* checkInput= */ true,
        /* checkOutput= */ false);

Agent agent = Agent.builder()
    .withId("research-bot")
    .withSystemPrompt("You are a research assistant.")
    .withGuardrail(guardrail)
    .build();
```

### Classifier as a tool

```java
InferenceToolAdapter sentiment =
    new InferenceToolAdapter(
        "sentiment",
        "Classify text sentiment as positive/negative",
        DjlInferenceConnection.classification(
            "djl://ai.djl.huggingface.pytorch/distilbert-base-uncased-finetuned-sst-2-english"),
        InferenceSetup.builder()
            .withModelName("sst-2")
            .withModelUri("djl://ai.djl.huggingface.pytorch/distilbert-base-uncased-finetuned-sst-2-english")
            .build(),
        InferenceToolAdapter.TaskKind.CLASSIFIER);

Agent agent = Agent.builder()
    .withId("review-bot")
    .withSystemPrompt("Summarize reviews. Use the sentiment tool when unsure.")
    .withInferenceTool(sentiment)
    .build();
```

### Sentence-transformers as the embedder

```java
Agent agent = Agent.builder()
    .withId("retrieval-bot")
    .withSystemPrompt("Answer using retrieved passages only.")
    .withEmbeddingConnection(
        DjlEmbeddingConnection.of(
            "djl://ai.djl.huggingface.pytorch/sentence-transformers/all-MiniLM-L6-v2"))
    .withVectorMemory(FlinkStateVectorMemory.spec(384))
    .build();
```

### Trained scorer for relevancy

```java
DjlInferenceConnection ranker =
    DjlInferenceConnection.classification(
        "djl://ai.djl.huggingface.pytorch/cross-encoder/ms-marco-MiniLM-L-6-v2");

InferenceSetup setup =
    InferenceSetup.builder()
        .withModelName("ms-marco-MiniLM-L-6-v2")
        .withModelUri(ranker.getDefaultModelUri())
        .build();

Scorer rankerScorer = ranker.bind(null).asScorer();

RelevancyScorer relevancy = new RelevancyScorer(rankerScorer, setup);
// Pass this into your context-management pipeline in place of the heuristic
// `new RelevancyScorer(config)`.
```

### Standalone access on the Agent

```java
Agent agent = Agent.builder()
    .withId("…")
    .withInferenceConnection("ner", DjlInferenceConnection.classification(
        "djl://ai.djl.huggingface.pytorch/dslim/bert-base-NER"))
    .withInferenceConnection("toxicity", DjlInferenceConnection.classification(
        "djl://ai.djl.huggingface.pytorch/unitary/toxic-bert"))
    .build();

// In your operator's open():
InferenceConnection ner = agent.getInferenceConnection("ner");
InferenceClient nerClient = ner.bind(getRuntimeContext());
```

## Listener hooks

The agent listener interface gained three hooks for observability:

- `onInference(agentId, modelName, task, durationMs)` — fires on every
  classifier / scorer / embedder / generic call.
- `onGuardrailBlock(agentId, modelName, label)` — fires when a guardrail
  short-circuits a chat.
- `onGuardrailRewrite(agentId, modelName, reason)` — fires when a guardrail
  swaps the payload.

Register listeners via `AgentBuilder.withListener(...)`; the
`MetricsAgentEventListener` reference impl ships counters for each new hook.

## Caching

`InferenceModelCache.global()` keys loaded model handles on
`(modelUri, deviceType)` so multiple operators in the same task slot share
weights. Entries are held by `SoftReference` so the JVM can reclaim under
memory pressure without explicit cleanup.

## Adding another backend

Implement `InferenceConnection` and (typically) a private `InferenceClient`
that returns the relevant `Classifier` / `Scorer` / `EmbeddingClient` /
`GenericInferenceModel`. Register at
`META-INF/services/org.agentic.flink.inference.InferenceConnection`
if you want `ServiceLoader` discovery, or pass it explicitly via
`AgentBuilder.withInferenceConnection(name, conn)`.

ONNX Runtime and DL4J would each follow this pattern. Their dependencies stay
out of the default build the same way DJL's do — mark them `<optional>true</optional>`.
