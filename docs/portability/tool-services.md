# Tool Services — the toolkit as framework-agnostic, one-shot tools

The agent toolkit (web scraping, **Tika** document extraction, RAG/ingestion, inference,
utilities) is decomposed into standalone, self-describing tools that **any LLM or framework
can run** over standard protocols — without depending on Flink or any of the agent cores.
The decoupling boundary is the **protocol**: you build a tool pack once (Java/Quarkus-first),
and every framework consumes it the same way.

Lives under [`tool-services/`](../../tool-services/) (standalone Maven modules, kept out of the
core reactor like `a2a-gateway`). The deployed service is **Flink-free** — it depends on the
pure-Java `jagentic-core` (the tool model + the `ToolServer` MCP-server engine), not the Flink
runtime.

## Why

A tool like "fetch a URL and Tika-extract its text" shouldn't require being inside a Flink job
or one of the cores. Exposed behind a protocol, the *same* service is callable from a Python
agent, a JVM agent, a Go agent, a shell script, or any third-party MCP-capable client. One
implementation, every consumer.

## Architecture

| Layer | What |
|-------|------|
| `jagentic-core` | `ToolRegistry` (tools carry an optional input JSON-schema) + `mcp/server/ToolServer` — "expose any ToolRegistry as an MCP server", the byte-for-byte reverse of the core's `McpStdioClient`. |
| `tool-services-packs` | Flink-free library of `ToolPack`s — each registers self-describing tools into a `ToolRegistry`. |
| `tool-services-app` | One configurable Quarkus app serving the selected packs over every transport. |

## Packs

| Pack | `name` | Tools |
|------|--------|-------|
| Utility | `util` | calculator + string ops (from LangChain4j `@Tool` methods, reflected into schemas) |
| Web + document | `web` | `web_fetch`, `web_links`, `web_crawl` (bounded), `doc_extract` (Tika: PDF/DOCX/PPTX/HTML/…) |
| RAG / ingestion | `rag` | `ingest_document` (chunk→embed→store), `semantic_search`, `rag_answer` (extractive) |
| Inference | `inference` | `classify_text`, `score_text`, `guardrail_check` (lexicon or fitted nearest-centroid) |

Pick which to serve with `TOOL_PACKS=util,web` (CSV; empty = all). The web/Tika tools are
**lifted** from the Flink `web/` package (they were already Flink-free); RAG/inference are
**rebuilt** on `jagentic-core`'s `Embedder`/`VectorStore`/`Classifier` SPIs. Everything has a
model-free default (hashing embedder, in-memory store, lexicon classifier) so the service runs
with no infra; swap in Ollama/Qdrant/HNSW/DJL via config.

## Transports

| Transport | Endpoint / entrypoint | Notes |
|-----------|----------------------|-------|
| **MCP** (Streamable-HTTP) | `POST /mcp` (JSON-RPC 2.0) | the LLM-native protocol |
| **MCP** (stdio) | `org.jagentic.tools.app.mcp.StdioMain` (subprocess) | what an LLM/agent spawns |
| **REST / OpenAPI** | `GET /tools`, `POST /tools/{name}`, spec at `/q/openapi` | universal; curl/any framework |
| **gRPC** | `ToolService` (`ListTools`/`CallTool`); rides the HTTP port | reliable typed RPC |
| **Kafka pub-sub** | `tool-requests` → `tool-results` | `-Dtools.kafka.enabled=true`; decoupled/at-scale |
| **Redis pub-sub** | request → reply channels | `-Dtools.redis.enabled=true` |

Kafka/Redis bridges are **build-time gated off by default** so the service boots with no broker.
Pub-sub payloads: `{id, tool, args}` in → `{id, ok, result|error}` out, correlated by `id`.

## Build & run

```bash
mvn -q -f ports/jagentic-core/pom.xml install -DskipTests
mvn -q -f tool-services/tool-services-packs/pom.xml install -DskipTests
mvn -f tool-services/tool-services-app/pom.xml package

# HTTP (MCP-HTTP + REST + OpenAPI + gRPC on one port)
TOOL_PACKS=util,web TOOL_SERVICES_PORT=8081 \
  java -jar tool-services/tool-services-app/target/quarkus-app/quarkus-run.jar
bash examples/tool-services/call-tools.sh          # REST + MCP-HTTP demo calls

# or as an MCP stdio server (what an agent spawns)
TOOL_PACKS=util,web java -cp '.../quarkus-app/app/*:.../quarkus-app/lib/main/*' \
  org.jagentic.tools.app.mcp.StdioMain
```

## The closed loop: an agent consuming the tool services

The agent cores already ship an MCP **client** and a declarative `mcp:` pipeline section, so an
agent pulls these packs in directly — see [`examples/pipelines/tools-mcp.yaml`](../../examples/pipelines/tools-mcp.yaml):

```yaml
mcp:
  - name: tools
    transport: stdio
    command: [java, "-cp", ".../quarkus-app/...", "org.jagentic.tools.app.mcp.StdioMain", "util,web"]
```

The tools arrive as `tools_util_add`, `tools_web_fetch`, … and the LLM brain calls them like any
other tool. Tool packs are MCP **servers**; agents are MCP **clients** — both built here, proven
to interoperate by `ToolServerTest` (a `StdioToolServer` subprocess served to the existing
`McpStdioClient`).

## Status

All four packs and all six transports are implemented and tested (offline packs tests +
`@QuarkusTest` REST/MCP-HTTP/gRPC; Kafka/Redis round-trips skip cleanly when no broker is up).
Python/Go MCP-server helpers (mirroring the Java `ToolServer`) are a future addition — today the
services are Java/Quarkus-first and consumed cross-language via the protocols.
