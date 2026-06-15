#!/usr/bin/env bash
# Calling the standalone Tool Services from anything — no Flink, no agent core, just HTTP.
# Start the service first:
#   mvn -q -f ports/jagentic-core/pom.xml install -DskipTests
#   mvn -q -f tool-services/tool-services-packs/pom.xml install -DskipTests
#   mvn -f tool-services/tool-services-app/pom.xml package
#   TOOL_PACKS= TOOL_SERVICES_PORT=8081 \
#     java -jar tool-services/tool-services-app/target/quarkus-app/quarkus-run.jar
set -euo pipefail
BASE="${TOOL_SERVICES_URL:-http://localhost:8081}"

echo "== REST: list tools (with input schemas) =="
curl -s "$BASE/tools" | head -c 2000; echo

echo "== REST: invoke a tool =="
curl -s -XPOST "$BASE/tools/util_add" -H 'content-type: application/json' \
  -d '{"a":40,"b":2}'; echo

echo "== REST: Tika document extraction (inline HTML) =="
curl -s -XPOST "$BASE/tools/doc_extract" -H 'content-type: application/json' \
  -d '{"text":"<html><head><title>Q3</title></head><body>Revenue grew 12%.</body></html>","content_type":"text/html"}'; echo

echo "== MCP over HTTP: tools/list =="
curl -s -XPOST "$BASE/mcp" -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | head -c 2000; echo

echo "== MCP over HTTP: tools/call =="
curl -s -XPOST "$BASE/mcp" -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"util_add","arguments":{"a":40,"b":2}}}'; echo

echo "== OpenAPI spec =="
echo "  open $BASE/q/openapi  (or $BASE/q/swagger-ui)"

# gRPC (needs grpcurl + reflection or the proto):
#   grpcurl -plaintext -d '{}' localhost:8081 jagentic.tools.ToolService/ListTools
#   grpcurl -plaintext -d '{"name":"util_add","argsJson":"{\"a\":40,\"b\":2}"}' \
#     localhost:8081 jagentic.tools.ToolService/CallTool
