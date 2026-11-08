# Using a real model (chat + embeddings): per language

Every core ships **model-free defaults** so the test suites run offline: the `stub` provider
(a scripted ReAct trace), the FNV hashing embedder (deterministic), and a lexicon classifier. To go
live, swap the provider, the SPIs are the same, so nothing else in your agent changes.

Secrets stay in a gitignored `.env`; never commit keys. Live tests run only when a key/server
is present and **skip** otherwise.

## Bring up a local model (no API key)

```bash
podman run -d --name ollama -p 11434:11434 ollama/ollama
podman exec ollama ollama pull qwen2.5:3b          # a small chat model
podman exec ollama ollama pull nomic-embed-text    # embeddings
```

## In a `pipeline.yaml` (Python and the JVM)

```yaml
llm:        {provider: ollama, model: qwen2.5:3b, base_url: http://localhost:11434}
embeddings: {provider: ollama, model: nomic-embed-text, base_url: http://localhost:11434}
```

Switch to a hosted provider by changing two fields and exporting the key:

```yaml
llm:        {provider: openai, model: gpt-6-luna}
embeddings: {provider: openai, model: text-embedding-3-small}   # the JVM; Python embeds with hashing
```
```bash
export OPENAI_API_KEY=sk-...          # read from the environment, never the YAML
```

Claude and Gemini are declared the same way, on both cores:

```yaml
llm: {provider: anthropic}                      # model defaults to claude-sonnet-5-5
llm: {provider: gemini, model: gemini-3-flash}  # gemini needs a model
```
```bash
export ANTHROPIC_API_KEY=...          # or GEMINI_API_KEY; each goes in a header, never the URL
```

`temperature` and `max_tokens` are sent only when declared. Every model call goes through the egress
policy and the effect journal, a tool result is marked as untrusted before the model reads it, and a
key never reaches the log, the journal or an error message.

## In code

### Python (the `agentic` core)

```python
from agentic import Agent

spec = (Agent("assistant")
        .path("main", brain="llm", prompt="You are helpful.", tools=["get_balance"])
        .use_tool("get_balance", lambda user: 1234.56, effect="read")
        .with_llm("anthropic")                       # or "openai", "gemini", "ollama" with model=...
        .build())
result = spec.run(runtime="local", text="what is my balance?", conversation_id="c1", turn_id="t1")
```
`model` is required for `ollama`, `openai` and `gemini`; an empty or missing one raises
`ValidationError` before any request is made. Name a model you have actually pulled or been granted
access to. The clients are `urllib` and `json`, with no SDK; `pip install "mcp<2"` is the one optional
extra, for `mcp:` servers.

### Java (`jagentic-core`): LangChain4J behind the ChatClient SPI

```java
var chat  = new LangChain4jChatClient("ollama", "qwen2.5:3b", "http://localhost:11434");
var embed = Embedders.make(Map.of("provider", "ollama", "model", "nomic-embed-text"));
var brain = new LlmBrain(chat, "assistant", "You are helpful.", null, 6, null);
```
Add the LangChain4J provider modules (`langchain4j-ollama` / `-open-ai`) as `optional`
dependencies, the lean core builds without them; they activate when present.

## Real embeddings → real retrieval

The `Embedder` SPI feeds both the two-tier retriever and the embedding classifier. With a
real embedder, the in-process **HNSW** cold tier becomes a real semantic index:

```yaml
embeddings: {provider: ollama, model: nomic-embed-text}
retrieval:  {vector_store: {kind: hnsw, m: 16, ef_search: 64}, kb: [...]}
```

## Verify it's live

```bash
# Python: a model-driven path
python -m agentic_pipeline run examples/pipelines/banking-llm.yaml --text "what is my balance?"
```
Swap the `llm` block of `banking-llm.yaml` for a real provider first. Real embeddings are a JVM
feature: the Python core refuses `embeddings.provider` other than `hashing` rather than fall back.
