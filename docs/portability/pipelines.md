# Declarative pipelines — build the agentic system of your choice, deploy it anywhere

Agentic Streaming lets you define an agent **declaratively in a `pipeline.yaml`**, pick a
**backend**, and the rest falls into place. The same YAML builds the same
`router → path → verifier` agent — with prompts, tools, calls to other agents, retrieval,
guardrails, and a hot-swappable durable store — on **any** backend and in **any** of the
three languages (Python, JVM, Go). Nothing is locked to Flink.

```bash
# Python
PYTHONPATH=ports/agentic-pipeline:ports/pyagentic \
  python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"

# JVM (jagentic-core)
mvn -q -f ports/jagentic-core/pom.xml exec:java \
  -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
  -Dexec.args="../../examples/pipelines/banking.yaml --text 'what is my balance?'"

# Go
cd ports/go && go run ./cmd/pipeline ../../examples/pipelines/banking.yaml --text "what is my balance?"
```

Swap the engine with `--backend` (or the YAML `backend:` key) — **nothing else changes**:

```bash
python -m agentic_pipeline run examples/pipelines/banking.yaml --backend celery --text "card types?"
python -m agentic_pipeline run examples/pipelines/banking.yaml --backend nats   --text "what is my balance?"
```

## The schema

```yaml
backend: local            # which engine runs it (see the parity matrix for what each supports)

llm:                      # OPTIONAL — omit for model-free rule brains
  provider: ollama        # ollama | openai | stub  (stub = deterministic, offline)
  model: qwen2.5:3b
  base_url: http://localhost:11434     # connection link (or env)
  # script: [...]         # only for provider: stub — a scripted ReAct trace

agent:
  router:
    kind: keyword         # keyword rules (the portable router)
    default: general      # path when no rule matches
    rules:
      cards:    [card, crypto, cash-back]
      payments: [balance, transfer, dispute]
  paths:
    cards:
      brain: rule          # rule (model-free) | llm (ReAct loop over the ChatClient)
      prompt: "You answer card questions."
      tools: [get_balance] # tools this path's LLM brain may call
      tool_triggers: {balance: get_balance}   # rule-brain: keyword -> tool
    payments: { brain: llm, prompt: "You are a payments specialist.", tools: [get_balance] }
    general:  { brain: rule, prompt: "You answer general questions." }
  verifier:
    kind: prefix           # prefix | none

tools:
  - {id: get_balance, kind: constant, value: 1234.56}          # a fixed value
  - {id: web, kind: http, url: "http://localhost:8000/fetch"}  # POST params as JSON
  - {id: ask_cs, kind: agent, url: "http://cs-agent:8080/agent"}  # CALL ANOTHER AGENT (A2A-as-tool)

retrieval:
  embedder: hashing        # deterministic, model-free (swap for a real embedder via the SPI)
  dim: 256
  kb:
    - {id: kb_cards, text: "We offer classic, gold, and platinum cards."}

guardrails:
  - {kind: regex, deny: ["ignore (all|previous)"], reason: "prompt injection"}        # input screen
  - {kind: regex, deny: ['\d{16}'], reason: "leaked PAN", check_outputs: true}        # output screen

stores:                    # OPTIONAL — hot-swap the durable backing (default: memory)
  conversation:
    kind: redis            # memory | redis (Redis/Valkey)
    url: "${AGENTIC_REDIS_URL}"   # connection link; ${ENV} is expanded

backend_config:            # OPTIONAL — engine connection links
  url: "nats://localhost:4222"
```

### Calling other agents

A `tool` of `kind: agent` (alias of `http`) POSTs `{conversation_id, text, user_id}` to
another agent's HTTP gateway (`/agent`) and returns its reply — so a path's brain can
delegate to a peer agent. That's portable multi-agent / A2A with no engine lock-in: the
peer can be any backend's gateway. See [`examples/pipelines/multiagent.yaml`](../../examples/pipelines/multiagent.yaml).

### Prompts & LLM

`brain: llm` runs a bounded ReAct loop (thought → tool → observation → final) over the
`ChatClient` chosen in `llm:` (`ollama`/`openai` are real HTTP clients; `stub` is a
scripted, offline client for tests/demos). `brain: rule` is model-free (keyword + tool
triggers + retrieval) and needs no `llm:` section — the default everywhere.

## Hot-swappable externals

Every external service sits **behind an interface**, selected by a **connection link**:

- **Durable store** — `stores.conversation.{kind,url}` swaps the `ConversationStore`
  implementation (`memory` → `redis`/Valkey) with no agent-code change. Postgres/Fluss
  slot in the same way (a class implementing the SPI + a registry entry).
- **Backend transport** — `backend: nats|celery|kafka-streams|…` + `backend_config.url`
  point at the broker/cluster.
- Bring the services up with Compose and point the links at them:

  ```bash
  podman compose -f examples/compose/externals.yml up -d valkey nats
  AGENTIC_REDIS_URL=redis://localhost:6379/0 \
    python -m agentic_pipeline run examples/pipelines/banking.yaml --backend local
  ```

## What each backend supports

A backend can only run what its substrate allows (e.g. Dask/Airflow are batch, not a
live keyed stream). See the **[parity matrix](parity-matrix.md)** for the per-backend
feature/limitation table and **[choosing a backend](choosing-a-backend.md)** for the
decision guide. The pipeline loader builds the *same* agent everywhere; the backend
decides how it runs (online turn, streamed, or batch).

## Loaders

| Language | Builder | Loader / CLI | YAML |
|----------|---------|--------------|------|
| Python | `pyagentic.builder.build` | `agentic_pipeline.load` / `python -m agentic_pipeline run` | PyYAML |
| JVM | `org.jagentic.core.pipeline.GraphBuilder` | `PipelineLoader.load` / `PipelineCli` | Jackson-YAML |
| Go | `pipeline.Build` | `pipeline.Load` / `go run ./cmd/pipeline` | yaml.v3 |

All three consume the **same schema** and the **same example files** under
`examples/pipelines/`.
