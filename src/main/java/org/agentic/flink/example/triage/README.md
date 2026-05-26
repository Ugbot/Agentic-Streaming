# Support triage example

Triage one inbound customer-support ticket through a five-stage pipeline that
exercises the full inference SPI plus the LangChain4J escape hatch.

```
Ticket
  │
  ▼  ClassifierGuardrail (sentiment, SST-2)
  │      ─► strongly-negative → route to human, stop
  ▼  InferenceToolAdapter (intent, BART-MNLI)
  │      ─► billing / technical / refund / general
  ▼  Three LLM drafts (temperature ↑)
  │
  ▼  Scorer (cross-encoder MS-MARCO)
  │      ─► reranks drafts against the ticket body
  ▼  Tone-rewrite via LangChain4jChatClient.getUnderlyingModel()
  │
  ▼  Final reply
```

## What's interesting

| Stage | API used |
|-------|----------|
| Guardrail | `ClassifierGuardrail` over a DJL HuggingFace classifier |
| Intent tool | `InferenceToolAdapter` — model behind the `ToolExecutor` interface |
| Draft loop | `ChatConnection` + `ChatSetup` — vendor-neutral SPI |
| Reranker | `Scorer` via `InferenceClient.asScorer()` (binary classifier's positive-class probability) |
| Tone polish | `LangChain4jChatClient#getUnderlyingModel()` — the documented escape hatch |
| Observability | `MetricsAgentEventListener` counts chat / inference / guardrail events |

## Prerequisites

```bash
docker compose up -d ollama
docker compose exec ollama ollama pull qwen2.5:3b
```

Add a DJL native binary to your local pom (the framework's pom marks DJL deps
optional and does not pull a native build by itself):

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
mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.triage.SupportTriageExample"
```

Or via the wrapper script:

```bash
./examples-bin/run-support-triage.sh
```

## Expected output

The first run downloads model weights (~250 MB for the three HuggingFace
models) into DJL's cache. After that:

```
Triaging Ticket[T-1023 from Alex Morgan]: Where is my refund?
Intent: refund (score=0.81)
Picked draft (score=8.42)
=== Final reply ===
Hi Alex,

Thanks for reaching out — I'm sorry to hear about the delay with your refund...

Metrics — chatRequests=4 toolCalls=1 inferences=4 guardrailBlocks=0
```

(Exact numbers depend on the LLM and model versions.)
