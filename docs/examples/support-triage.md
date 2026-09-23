# Support triage walkthrough

> **Flink-runtime showcase**, composes guardrails + inference + a scorer over **Flink keyed state**.
> The guardrail/inference/scorer pieces are portable SPIs; this walkthrough wires them on the Flink
> runtime. For the runs-everywhere baseline see
> [the banking agent on every runtime](banking-everywhere.md).

> Source: `src/main/java/org/agentic/flink/example/triage/SupportTriageExample.java`
> Inline README: `src/main/java/org/agentic/flink/example/triage/README.md`

This walkthrough is the long-form companion to the inline README. It explains
why each stage is there and what the design alternatives were.

## Why this shape

Customer-support triage has three pressures pulling in different directions:

1. **Latency**: replies should land in seconds, not after a 30-second LLM call.
2. **Cost**: calling a 70B model on every ticket is wasteful when 80% of
   tickets fall into four well-known intents.
3. **Tone consistency**: replies have to sound like *your* company, not the
   foundation model's default voice.

The example threads all three: tiny models do the deterministic work
(sentiment + intent + reranking), the LLM only writes prose, and a tone-pass
through LangChain4J's two-arg `generate` enforces voice.

## Pipeline

```
Ticket
  │
  ▼  1. ClassifierGuardrail - sentiment
  │      • SST-2 DistilBERT, ~50 ms CPU
  │      • Label NEGATIVE → route to human queue, skip LLM
  │
  ▼  2. InferenceToolAdapter - intent
  │      • BART-MNLI zero-shot classifier
  │      • Returns {label: refund, score: 0.81, probabilities: {...}}
  │      • Result wrapped as ToolExecutor; LLM "called" it through the tool path
  │
  ▼  3. Draft loop - 3 candidates at temperature 0.85
  │      • SPI-only: ChatConnection + ChatSetup
  │      • Vendor-neutral; switch to OpenAI by changing the connection
  │
  ▼  4. Scorer - cross-encoder rerank
  │      • cross-encoder/mmarco-mMiniLMv2-L12-H384-v1
  │      • Pair-scores (candidate, ticket-body); pick the best
  │
  ▼  5. Tone rewrite - LangChain4J escape hatch
  │      • Downcast to LangChain4jChatClient
  │      • Use LC4j's generate(system, user) convenience the SPI doesn't expose
  │
  ▼  Final reply
```

## Design choices

### Why a guardrail for sentiment?

The blunt rule "if the customer sounds furious, escalate to a human" prevents
the LLM from drafting a tone-deaf reply to a complaint. The DJL classifier
gives us this for ~50 ms of CPU instead of an extra LLM call.

### Why an MNLI model as a tool, not a guardrail?

Intent is an input to the LLM's draft prompt, not a gate. We want the LLM to
see "this is a refund ticket" so it can structure the reply accordingly. The
`InferenceToolAdapter` is the right wrapper because the LLM could in principle
re-call it on a follow-up message.

### Why three draft candidates?

A single sample from a 3B-parameter model at temperature 0.85 is noisy. Three
samples + reranker gives roughly best-of-3 with linear cost. The reranker is
cheap (~30 ms each on CPU) so the total is dominated by the LLM calls.

### Why downcast to LangChain4jChatClient for the tone pass?

The framework SPI exposes `chat(List<ChatMessage>, ChatSetup) → ChatResponse`.
That's deliberately generic. LangChain4J's `ChatLanguageModel.generate(system,
user)` is a one-shot convenience for prompts that don't need a multi-message
chat history. Rather than push that into our `ChatClient` (where it would
constrain every backend), the escape hatch lets *one specific* call do the
LangChain4J-idiomatic thing.

## Running it

```bash
bash examples-bin/run-ollama.sh      # Ollama in Podman on 127.0.0.1:11434, pulls qwen2.5:3b
bash examples-bin/run-support-triage.sh
```

Prerequisites: JDK 21, the Maven wrapper, Podman (for Ollama), and outbound internet on the
first run: DJL downloads `distilbert-base-uncased-finetuned-sst-2-english` (sentiment),
`facebook/bart-large-mnli` (zero-shot intent, about 1.6 GB),
`cross-encoder/mmarco-mMiniLMv2-L12-H384-v1` (reranker) and the PyTorch CPU native runtime
into its cache directory. No API key. The script installs `ports/jagentic-core` when missing,
resolves the provided-scope Flink dependencies and runs the example in a forked JVM. The second
run is cache-warm and finishes in a few seconds.

The example processes one synthetic ticket and prints `Triaging ...`, then either
`Routed to human queue: ...` (negative sentiment) or `Intent: <label> (score=...)`,
`Picked draft (score=...)` and the polished text under `=== Final reply ===`.

## Wiring it into a streaming job

The example processes one synthetic ticket. To run it on a real `DataStream`,
follow the pattern in `ContentModerationExample`, extract the per-ticket
logic into a `KeyedProcessFunction`, bind the chat / inference clients once in
`open()`, and emit `Reply` records on the main output (with `route-to-human`
on a side output).
