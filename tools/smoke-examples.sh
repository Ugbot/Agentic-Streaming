#!/usr/bin/env bash
# Smoke test for examples/pipelines/ in no-key mode: each spec runs one turn on the Python core,
# the Java core and a Flink MiniCluster and, for banking.yaml, on Pekko, with a per-example timeout. It stops on the first
# failure. An example that cannot run here is reported as SKIP with the reason, never silently.
#
#   bash tools/smoke-examples.sh
#
# Knobs (all optional):
#   SMOKE_TIMEOUT   seconds per example (default 300)
#   SMOKE_PEER_PORT port for the A2A peer the multi-agent examples call (default 8085)
#   PYTHON          interpreter with pyagentic and agentic_pipeline installed (default python3)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The peer the multi-agent examples call is on this machine.
export AGENTIC_EGRESS_ALLOW_PRIVATE=true
# The one MCP server the examples start. A document can start nothing the operator did not list.
export AGENTIC_MCP_ALLOWED_COMMANDS="python3 examples/pipelines/mcp_util_server.py"
export AGENTIC_PEER_TOKEN="${AGENTIC_PEER_TOKEN:-smoke-peer-$RANDOM$RANDOM$RANDOM$RANDOM}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-300}"
PEER_PORT="${SMOKE_PEER_PORT:-8085}"
PYTHON="${PYTHON:-python3}"
LOG_DIR="target/smoke-logs"
TEXT="what is my balance?"
mkdir -p "$LOG_DIR"

PASSED=()
SKIPPED=()
PEER_PID=""

cleanup() { [ -z "$PEER_PID" ] || kill "$PEER_PID" 2>/dev/null || true; }
trap cleanup EXIT

summary() {
  printf '\n==== smoke summary ====\nPASS (%d):\n' "${#PASSED[@]}"
  for p in ${PASSED[@]+"${PASSED[@]}"}; do printf '  %s\n' "$p"; done
  printf 'SKIP (%d):\n' "${#SKIPPED[@]}"
  for s in ${SKIPPED[@]+"${SKIPPED[@]}"}; do printf '  %s\n' "$s"; done
}

skip() { printf 'SKIP %-34s %s\n' "$1" "$2"; SKIPPED+=("$1: $2"); }

# run_example <name> <expected regex> <command...>
run_example() {
  local name="$1" expect="$2" log="$LOG_DIR/$1.log" rc=0
  shift 2
  printf 'RUN  %-34s ' "$name"
  # timeout execs a program, not a shell function, so the helpers below run through a child bash.
  timeout "$SMOKE_TIMEOUT" bash -c '"$@"' smoke-example "$@" >"$log" 2>&1 || rc=$?
  if [ "$rc" -ne 0 ] || { [ -n "$expect" ] && ! grep -Eq -- "$expect" "$log"; }; then
    printf 'FAIL (exit %s, wanted /%s/)\n' "$rc" "$expect"
    tail -n 40 "$log"
    summary
    exit 1
  fi
  printf 'PASS\n'
  PASSED+=("$name")
}

on_python() { "$PYTHON" -m agentic_pipeline run "examples/pipelines/$1.yaml" --text "$2"; }
on_jvm() {
  ./mvnw -q -f ports/jagentic-core/pom.xml exec:java -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
    -Dexec.args="examples/pipelines/$1.yaml --text '$2'"
}
on_pekko() {
  ./mvnw -q -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
    -Dexec.args="examples/pipelines/$1.yaml --text '$2'"
}
on_flink() {
  ./mvnw -q -P examples compile exec:exec -Dexec.mainClass=org.agentic.flink.pipeline.FlinkPipelineRunner \
    -Dexec.args="examples/pipelines/$1.yaml --text '$2'"
}
export -f on_python on_jvm on_pekko on_flink
export PYTHON

"$PYTHON" -c 'import pyagentic, agentic_pipeline' \
  || { echo "error: $PYTHON cannot import pyagentic and agentic_pipeline; pip install -e ports/pyagentic -e ports/agentic-pipeline" >&2; exit 1; }
HAVE_MCP=0
python3 -c 'import mcp' 2>/dev/null && HAVE_MCP=1

run_example "build.jagentic-core" "" ./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
run_example "spec.validate" "0 failure" "$PYTHON" spec/tools/validate_spec.py

for y in banking banking-llm banking-rag; do
  run_example "pipeline.$y.python" 'path=payments ok=True' on_python "$y" "$TEXT"
  run_example "pipeline.$y.jvm" 'path=payments ok=true' on_jvm "$y" "$TEXT"
  run_example "pipeline.$y.flink" 'path=payments \| ok=true' on_flink "$y" "$TEXT"
done
run_example "pipeline.incident.python" 'path=monitor ok=True' on_python incident "$TEXT"
run_example "pipeline.incident.jvm" 'path=monitor ok=true' on_jvm incident "$TEXT"
run_example "pipeline.incident.flink" 'path=monitor \| ok=true' on_flink incident "$TEXT"
run_example "pipeline.banking.pekko" 'path=payments ok=true' on_pekko banking "$TEXT"
run_example "pipeline.multiagent.python.no-peer" 'path=triage ok=True' on_python multiagent "what time do you open?"

if [ "$HAVE_MCP" = "1" ]; then
  run_example "pipeline.tools-mcp.python" 'ok=True' on_python tools-mcp "what is 40 plus 2?"
  run_example "pipeline.tools-mcp.jvm" 'ok=true' on_jvm tools-mcp "what is 40 plus 2?"
  run_example "pipeline.tools-mcp.flink" 'ok=true' on_flink tools-mcp "what is 40 plus 2?"
else
  skip "pipeline.tools-mcp.*" "python3 on PATH has no MCP SDK (pip install \"mcp<2\")"
fi

# The multi-agent examples call a peer: the Pekko HTTP front door.
peer_up() { curl -fsS "http://127.0.0.1:$PEER_PORT/.well-known/agent-card.json" >/dev/null 2>&1; }
if ! peer_up; then
  # The peer is behind the gateway, with a local-only token the examples name as env:AGENTIC_PEER_TOKEN.
  AGENTIC_GATEWAY_PORT="$PEER_PORT" AGENTIC_GATEWAY_ALLOW_STATIC_TOKENS=true \
  AGENTIC_GATEWAY_STATIC_TOKENS="$AGENTIC_PEER_TOKEN=agent|local" \
  ./mvnw -q -f agentic-pekko/pom.xml compile exec:java \
    -Dexec.mainClass=org.jagentic.pekko.http.HttpMain >"$LOG_DIR/a2a.peer.log" 2>&1 &
  PEER_PID=$!
  for _ in $(seq 1 120); do peer_up && break; sleep 1; done
fi
if peer_up; then
  export AGENTIC_PEER_AGENT_URL="http://127.0.0.1:$PEER_PORT"
  run_example "pipeline.multiagent.python" 'path=escalations ok=True' on_python multiagent "please escalate this"
  run_example "pipeline.multiagent.jvm" 'path=escalations ok=true' on_jvm multiagent "please escalate this"
  if [ "$HAVE_MCP" = "1" ]; then
    run_example "pipeline.banking-mcp.python" 'path=support ok=True' on_python banking-mcp "$TEXT"
    run_example "pipeline.banking-mcp.jvm" 'path=support ok=true' on_jvm banking-mcp "$TEXT"
  else
    skip "pipeline.banking-mcp.*" "python3 on PATH has no MCP SDK (pip install \"mcp<2\")"
  fi
else
  skip "pipeline.multiagent.*" "the Pekko peer did not answer on :$PEER_PORT"
fi

summary
