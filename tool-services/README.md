# Agentic Tool Services

Standalone, **framework-agnostic** tool services: the toolkit (web scraping, Tika document
extraction, RAG, inference, utilities) decomposed into self-describing "one-shot" tools that
**any LLM or framework can run** over standard protocols — without depending on Flink or any
of the agent cores. Build a pack once (Java/Quarkus-first); every framework consumes it via
the protocol.

Flink-free: the services depend on the pure-Java `jagentic-core` (tool model + the
`ToolServer` MCP-server engine), not the Flink runtime.

## Modules

| Module | What it is |
|--------|-----------|
| `tool-services-packs` | Flink-free library of `ToolPack`s (each registers self-describing tools, with input JSON-schemas, into a `jagentic-core` `ToolRegistry`). |
| `tool-services-app` | Standalone Quarkus app that serves the selected packs over the transports below. |

## Transports (what's wired)

| Transport | Endpoint / entrypoint | Status |
|-----------|----------------------|--------|
| MCP (Streamable-HTTP) | `POST /mcp` (JSON-RPC 2.0) | ✅ |
| MCP (stdio) | `org.jagentic.tools.app.mcp.StdioMain` (subprocess) | ✅ |
| REST / OpenAPI | `GET /tools`, `POST /tools/{name}`, spec at `/q/openapi` | ✅ |
| gRPC | `ToolService` (ListTools/CallTool) | _Phase 3_ |
| Kafka / Redis pub-sub | request→result topics | _Phase 3_ |

## Packs

| Pack | name | Tools | Status |
|------|------|-------|--------|
| Utility | `util` | calculator + string ops (from `@Tool` methods) | ✅ |
| Web + document | `web` | `web_fetch`, `web_links`, `doc_extract` (Tika), `web_crawl` | _Phase 2_ |
| RAG / ingestion | `rag` | `ingest_document`, `semantic_search`, `rag_answer` | _Phase 4_ |
| Inference | `inference` | `classify_text`, `score_text`, `guardrail_check` | _Phase 4_ |

## Build & run

```bash
# 1. install the Flink-free core, then the packs library, then build the app
mvn -q -f ports/jagentic-core/pom.xml install -DskipTests
mvn -q -f tool-services/tool-services-packs/pom.xml install -DskipTests
mvn -f tool-services/tool-services-app/pom.xml package

# 2a. run as an HTTP service (MCP-HTTP + REST + OpenAPI)
TOOL_PACKS=util TOOL_SERVICES_PORT=8081 \
  java -jar tool-services/tool-services-app/target/quarkus-app/quarkus-run.jar
curl localhost:8081/tools
curl -XPOST localhost:8081/tools/util_add -H 'content-type: application/json' -d '{"a":40,"b":2}'

# 2b. run as an MCP stdio server (what an LLM/agent spawns)
TOOL_PACKS=util \
  java -cp tool-services/tool-services-app/target/quarkus-app/app/*:... \
  org.jagentic.tools.app.mcp.StdioMain
```

`TOOL_PACKS` selects which packs to serve (CSV; empty = all). Any MCP-capable client — incl.
our own `mcp:` pipeline section and `McpStdioClient` — can consume the stdio or HTTP server.
