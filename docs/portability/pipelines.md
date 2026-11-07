# Declarative pipelines: build the agentic system of your choice

Agentic Streaming lets you define an agent **declaratively in a `pipeline.yaml`**. The same YAML
builds the same `router → path → verifier` agent, with prompts, tools, calls to other agents,
retrieval, guardrails, and a hot-swappable durable store, from Python or the JVM. Nothing is
locked to Flink.

```bash
# Python
PYTHONPATH=ports/agentic-pipeline:ports/pyagentic \
  python -m agentic_pipeline run examples/pipelines/banking.yaml --text "what is my balance?"

# JVM (jagentic-core)
mvn -q -f ports/jagentic-core/pom.xml exec:java \
  -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
  -Dexec.args="../../examples/pipelines/banking.yaml --text 'what is my balance?'"
```

The Python loader has one backend, `local`, and raises `ValueError` for any other name. The JVM
loader ships `local` and resolves other names through the `BackendProvider` SPI, as
[`agentic-pekko`](../../agentic-pekko/) does for `backend: pekko`.

## The schema

```yaml
backend: local            # which engine runs it (Python: local; JVM: local, pekko)

llm:                      # OPTIONAL - omit for model-free rule brains
  provider: ollama        # ollama | openai | litellm | langchain4j | stub
  model: qwen2.5:3b
  base_url: http://localhost:11434     # connection link (or env)
  # script: [...]         # only for provider: stub - a scripted ReAct trace

embeddings:               # OPTIONAL - default is the FNV hashing embedder (offline)
  provider: hashing       # hashing | litellm | ollama | openai
  model: nomic-embed-text
  dim: 256
  # base_url: http://localhost:11434

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
      skills: [card_help]  # expand into extra tools + a prompt fragment
      output_schema:       # OPTIONAL - enforce a schema-validated final answer
        {type: object, properties: {answer: {type: string}}, required: [answer]}
    payments: { brain: llm, prompt: "You are a payments specialist.", tools: [get_balance] }
    general:  { brain: rule, prompt: "You answer general questions." }
  verifier:
    kind: prefix           # prefix | none

skills:                    # OPTIONAL - reusable bundles a path can pull in by name
  - name: card_help
    prompt: "Prefer the knowledge base over guessing."
    tools: [get_balance]
    facts: [account_tier]

tools:
  - {id: get_balance, kind: constant, effect: read, value: 1234.56}  # a fixed value
  - {id: web, kind: http, effect: read, url: "http://localhost:8000/fetch"}  # POST params as JSON
  - {id: ask_cs, kind: agent, url: "http://cs-agent:8080/agent"}  # CALL ANOTHER AGENT (A2A-as-tool)

retrieval:
  dim: 256
  vector_store:            # OPTIONAL cold tier - default is the in-memory hot tier only
    kind: hnsw             # memory | hnsw (in-process ANN) | duckdb (Python) | qdrant | pgvector (JVM)
    m: 16                  # hnsw graph degree
    ef_search: 64
    # url: http://localhost:6333   # qdrant connection link
    # pgvector: url (a jdbc:postgresql link), user, password and table; use ${ENV} for the password
  kb:
    - {id: kb_cards, text: "We offer classic, gold, and platinum cards."}

context:                   # OPTIONAL - compact the replayed transcript before the model
  max_tokens: 512          # (or max_items: N)
  compaction: moscow

guardrails:
  - {kind: regex, deny: ["ignore (all|previous)"], reason: "prompt injection"}        # input screen
  - {kind: regex, deny: ['\d{16}'], reason: "leaked PAN", check_outputs: true}        # output screen
  - kind: classifier       # a learned/lexicon screen instead of a regex catalogue
    classifier: lexicon    # lexicon | embedding (nearest-centroid over the Embedder)
    blocked: [toxic]
    threshold: 0.3
    lexicon: {toxic: [idiot, stupid, hate], ok: [please, thanks, help]}

mcp:                       # OPTIONAL - register an external MCP server's tools
  - {name: fs, transport: stdio, command: [python, mcp_server.py]}   # tools become fs_<name>

a2a:                       # OPTIONAL - register a peer agent as a tool (peer-as-tool)
  - {id: specialist, url: "${SPECIALIST_URL}", retries: 2}

stores:                    # OPTIONAL - hot-swap the durable backing (default: memory)
  conversation:
    kind: redis            # memory | redis (Redis/Valkey)
    url: "${AGENTIC_REDIS_URL}"   # connection link; ${ENV} is expanded
  long_term:
    kind: postgres         # memory | postgres - resumption + fact archive (LongTermStore SPI)
    url: "${AGENTIC_PG_URL}"
```

See [`examples/pipelines/banking-rag.yaml`](../../examples/pipelines/banking-rag.yaml) for a
runnable spec that uses the HNSW cold tier, a classifier guardrail, skills, context-window
management and a long-term store, all on model-free defaults, identical routing on
each loader. [`banking-mcp.yaml`](../../examples/pipelines/banking-mcp.yaml) shows the
MCP + A2A sections.

### Calling other agents

A `tool` of `kind: agent` (alias of `http`) POSTs `{conversation_id, text, user_id}` to
another agent's HTTP gateway (`/agent`) and returns its reply, so a path's brain can
delegate to a peer agent. That's portable multi-agent / A2A with no engine lock-in: the
peer can be any backend's gateway. See [`examples/pipelines/multiagent.yaml`](../../examples/pipelines/multiagent.yaml).

### Prompts & LLM

`brain: llm` runs a bounded ReAct loop (thought → tool → observation → final) over the
`ChatClient` chosen in `llm:` (`ollama`/`openai` are real HTTP clients; `stub` is a
scripted, offline client for tests/demos). `brain: rule` is model-free (keyword + tool
triggers + retrieval) and needs no `llm:` section, the default everywhere.

## Hot-swappable externals

Every external service sits **behind an interface**, selected by a **connection link**:

- **Durable store**: `stores.conversation.{kind,url}` swaps the `ConversationStore`
  implementation (`memory` → `redis`/Valkey) with no agent-code change. A Flink job ignores `stores:`: its log is
  keyed state.
- Bring the services up with Compose and point the links at them:

  ```bash
  podman compose -f examples/compose/externals.yml up -d valkey
  AGENTIC_REDIS_URL=redis://localhost:6379/0 \
    python -m agentic_pipeline run examples/pipelines/banking.yaml --backend local
  ```

## What each backend supports

See the **[conformance results](../runtimes/README.md#conformance-results)** for what each runtime passes and
**[choosing a backend](choosing-a-backend.md)** for the decision guide.

## Loaders

| Language | Builder | Loader / CLI | YAML |
|----------|---------|--------------|------|
| Python | `pyagentic.builder.build` | `agentic_pipeline.load` / `python -m agentic_pipeline run` | PyYAML |
| JVM | `org.jagentic.core.pipeline.GraphBuilder` | `PipelineLoader.load` / `PipelineCli` | Jackson-YAML |

Both consume the **same schema** and the **same example files** under
`examples/pipelines/`.
