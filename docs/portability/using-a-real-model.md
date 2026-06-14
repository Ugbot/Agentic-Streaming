# Using a real model (chat + embeddings) — per language

Every core ships **model-free defaults** so the test suites run offline: the `StubChatClient`
(scripted ReAct), the FNV `HashingEmbedder` (deterministic), and a lexicon classifier. To go
live, swap the provider — the SPIs are the same, so nothing else in your agent changes.

Secrets stay in a gitignored `.env`; never commit keys. Live tests run only when a key/server
is present and **skip** otherwise.

## Bring up a local model (no API key)

```bash
podman run -d --name ollama -p 11434:11434 ollama/ollama
podman exec ollama ollama pull qwen2.5:3b          # a small chat model
podman exec ollama ollama pull nomic-embed-text    # embeddings
```

## In a `pipeline.yaml` (all three languages)

```yaml
llm:        {provider: ollama, model: qwen2.5:3b, base_url: http://localhost:11434}
embeddings: {provider: ollama, model: nomic-embed-text, base_url: http://localhost:11434}
```

Switch to a hosted provider by changing two fields and exporting the key:

```yaml
llm:        {provider: openai, model: gpt-5.4-mini}
embeddings: {provider: openai, model: text-embedding-3-small}
```
```bash
export OPENAI_API_KEY=sk-...          # read from the environment, never the YAML
```

## In code

### Python (`pyagentic`) — unified via litellm, or native SDKs

```python
from pyagentic.llm import LiteLLMChatClient, OllamaChatClient, LlmBrain
from pyagentic.embeddings import LiteLLMEmbedder, make_embedder

chat = LiteLLMChatClient(model="ollama/qwen2.5:3b")          # one API across providers
# chat = LiteLLMChatClient(model="gpt-5.4-mini")             # OpenAI (OPENAI_API_KEY)
# chat = LiteLLMChatClient(model="anthropic/claude-...")     # Anthropic (ANTHROPIC_API_KEY)
embed = make_embedder({"provider": "ollama", "model": "nomic-embed-text"})
brain = LlmBrain(chat, name="assistant", system_prompt="You are helpful.")
```
`pip install litellm` (chat+embeddings across providers); `qdrant-client`, `psycopg`,
`duckdb`, `mcp` are optional extras for the matching backends.

### Go (`goagentic`) — langchaingo, native go-openai where needed

```go
chat := core.NewOllamaChatClient("qwen2.5:3b", "http://localhost:11434")
emb, _ := core.NewLangChainGoEmbedder("ollama", "nomic-embed-text", "")
brain := core.NewLlmBrain(chat, "assistant", "You are helpful.", nil, 6)
```
`go get github.com/tmc/langchaingo` is already wired; `qdrant`/`pgx`/`mark3labs/mcp-go`
back the optional stores/MCP.

### Java (`jagentic-core`) — LangChain4J behind the ChatClient SPI

```java
var chat  = new LangChain4jChatClient("ollama", "qwen2.5:3b", "http://localhost:11434");
var embed = Embedders.make(Map.of("provider", "ollama", "model", "nomic-embed-text"));
var brain = new LlmBrain(chat, "assistant", "You are helpful.", null, 6, null);
```
Add the LangChain4J provider modules (`langchain4j-ollama` / `-open-ai`) as `optional`
dependencies — the lean core builds without them; they activate when present.

## Real embeddings → real retrieval

The `Embedder` SPI feeds both the two-tier retriever and the embedding classifier. With a
real embedder, the in-process **HNSW** cold tier becomes a real semantic index:

```yaml
embeddings: {provider: ollama, model: nomic-embed-text}
retrieval:  {vector_store: {kind: hnsw, m: 16, ef_search: 64}, kb: [...]}
```

## Verify it's live

```bash
# Python
PYTHONPATH=ports/agentic-pipeline:ports/pyagentic python -m agentic_pipeline run \
  examples/pipelines/banking-rag.yaml --text "how do I dispute a charge?"
```
Swap `embeddings.provider` to `ollama` in the YAML first; with Ollama down it falls back to
the hashing embedder (still routes, just lexical recall).
