#!/usr/bin/env bash
# Smoke test for every example under examples-bin/ and examples/pipelines/ in no-key mode.
#
# Runs each example that works without a paid API key, with Podman providing the services
# (Ollama, Redis, and the A2A peer as a plain JVM process), applies a per-example timeout, and
# exits non-zero on the first failure. Examples that need a paid key, outbound market data, or
# container images this box cannot pull are reported as SKIP with the exact reason; nothing is
# skipped silently. The summary at the end lists every PASS and SKIP.
#
# Intended CI job (owned by the CI session, not wired here):
#   name: smoke-examples
#   runs-on: ubuntu-latest (Podman preinstalled), JDK 21 (temurin), Python 3.12
#   steps:
#     - actions/checkout
#     - pip install -e ports/pyagentic -e ports/agentic-pipeline mcp
#     - MVNW_REPOURL=https://maven-central.storage-download.googleapis.com/maven2
#       bash tools/smoke-examples.sh
#   The job needs about 25 minutes on a 4 vCPU runner with warm Maven and DJL caches; the first
#   run also pulls the Ollama image, the qwen2.5:3b weights (2 GB) and the DJL models (2.5 GB),
#   so cache ~/.m2, ~/.djl.ai and the ollama_data volume. Set SMOKE_DJL=0 and SMOKE_OLLAMA=0 on
#   runners without room for those downloads; the affected examples are then listed as SKIP.
#
# Usage:
#   bash tools/smoke-examples.sh              # everything that can run here
#   SMOKE_TIMEOUT=900 bash tools/smoke-examples.sh
#
# Knobs (all optional):
#   SMOKE_TIMEOUT   seconds per example (default 600)
#   SMOKE_BUILD     1 (default) builds the jars first; 0 reuses target/ as is
#   SMOKE_OLLAMA    1 (default) starts Ollama in Podman for the LLM examples; 0 skips them
#   SMOKE_DJL       1 (default) runs the examples that download DJL models; 0 skips them
#   SMOKE_REDIS     1 (default) starts Redis in Podman for run-banking-local.sh; 0 skips it
#   SMOKE_MARKETS   0 (default) skips the Kafka/Flink session cluster stack; 1 brings it up
#   SMOKE_STACKS    0 (default) skips run-rag-stack.sh and run-session-cluster.sh; 1 runs them
#   PYTHON          interpreter with pyagentic and agentic_pipeline installed (default python3)
#   MAVEN_ARGS      extra flags for every ./mvnw call
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../examples-bin/_common.sh
source "$HERE/../examples-bin/_common.sh"

SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-600}"
SMOKE_BUILD="${SMOKE_BUILD:-1}"
SMOKE_OLLAMA="${SMOKE_OLLAMA:-1}"
SMOKE_DJL="${SMOKE_DJL:-1}"
SMOKE_REDIS="${SMOKE_REDIS:-1}"
SMOKE_MARKETS="${SMOKE_MARKETS:-0}"
SMOKE_STACKS="${SMOKE_STACKS:-0}"
LOG_DIR="$REPO_ROOT/target/smoke-logs"
mkdir -p "$LOG_DIR"
cd "$REPO_ROOT"

PASSED=()
SKIPPED=()
CLEANUP_PIDS=()
STARTED_REDIS=0

cleanup() {
  [ ${#CLEANUP_PIDS[@]} -eq 0 ] || stop_pids "${CLEANUP_PIDS[@]}"
  if [ "$STARTED_REDIS" = "1" ]; then
    podman rm -f agentic-smoke-redis >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

summary() {
  printf '\n==== smoke summary ====\n'
  printf 'PASS (%d):\n' "${#PASSED[@]}"
  for p in ${PASSED[@]+"${PASSED[@]}"}; do printf '  %s\n' "$p"; done
  printf 'SKIP (%d):\n' "${#SKIPPED[@]}"
  for s in ${SKIPPED[@]+"${SKIPPED[@]}"}; do printf '  %s\n' "$s"; done
}

# skip <name> <reason>
skip() {
  printf 'SKIP %-40s %s\n' "$1" "$2"
  SKIPPED+=("$1: $2")
}

# coreutils timeout execs a program, not a shell function, so the Maven helpers from _common.sh
# are exported and every command runs through a child bash that sees them.
export -f mvn_run mvn_q
export REPO_ROOT MVNW MAVEN_ARGS

# run_example <name> <expected-regex or ""> <command...>
# Runs the command with the timeout, logs to target/smoke-logs/<name>.log, checks the exit code
# and (when given) that the log matches the regex, and aborts the whole run on failure.
run_example() {
  local name="$1" expect="$2"
  shift 2
  local log="$LOG_DIR/$name.log" start rc=0
  start=$(date +%s)
  printf 'RUN  %-40s ' "$name"
  timeout --kill-after=20 "$SMOKE_TIMEOUT" bash -c '"$@"' smoke-example "$@" >"$log" 2>&1 || rc=$?
  local secs=$(( $(date +%s) - start ))
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
    printf 'FAIL (timeout after %ss)\n' "$SMOKE_TIMEOUT"
    tail -n 40 "$log"
    summary
    die "$name exceeded SMOKE_TIMEOUT=$SMOKE_TIMEOUT (log: $log)"
  fi
  if [ "$rc" -ne 0 ]; then
    printf 'FAIL (exit %s after %ss)\n' "$rc" "$secs"
    tail -n 40 "$log"
    summary
    die "$name failed (log: $log)"
  fi
  if [ -n "$expect" ] && ! grep -Eq -- "$expect" "$log"; then
    printf 'FAIL (no match for /%s/ after %ss)\n' "$expect" "$secs"
    tail -n 40 "$log"
    summary
    die "$name produced no line matching /$expect/ (log: $log)"
  fi
  printf 'PASS (%ss)\n' "$secs"
  PASSED+=("$name (${secs}s)")
}

# run_bg <name> <command...> : starts a long-running helper (A2A peer) and remembers its pid.
run_bg() {
  local name="$1"
  shift
  "$@" >"$LOG_DIR/$name.log" 2>&1 &
  CLEANUP_PIDS+=("$!")
}

python_ready() {
  command -v "$PYTHON" >/dev/null 2>&1 && "$PYTHON" -c 'import pyagentic, agentic_pipeline' >/dev/null 2>&1
}

# ---------------------------------------------------------------- prerequisites and builds
require_java21
require_mvnw
require_curl
require_cmd timeout "coreutils timeout is required."
HAVE_PODMAN=0
if command -v podman >/dev/null 2>&1; then HAVE_PODMAN=1; fi

if [ "$SMOKE_BUILD" = "1" ]; then
  run_example "build.jagentic-core" "" mvn_q -f ports/jagentic-core/pom.xml install -DskipTests
  run_example "build.root-install" "" mvn_q install -DskipTests
  run_example "build.tool-services-packs" "" mvn_q -f tool-services/tool-services-packs/pom.xml install -DskipTests
  run_example "build.tool-services-app" "" mvn_q -f tool-services/tool-services-app/pom.xml package -DskipTests
  run_example "build.a2a-gateway" "" mvn_q -f a2a-gateway/pom.xml package -DskipTests
  run_example "build.banking-job" "" mvn_q -f banking-job/pom.xml package -DskipTests
else
  ensure_jagentic_core
fi

run_example "spec.validate" "0 failure" "$PYTHON" spec/tools/validate_spec.py

# ---------------------------------------------------------------- portable pipelines
PIPELINE_TEXT="what is my balance?"
for y in banking banking-llm banking-rag; do
  run_example "pipeline.$y.jvm" 'path=payments ok=true' \
    bash examples-bin/run-pipeline.sh "examples/pipelines/$y.yaml" --runtime jvm --text "$PIPELINE_TEXT"
done
run_example "pipeline.incident.jvm" 'path=monitor ok=true' \
  bash examples-bin/run-pipeline.sh examples/pipelines/incident.yaml --runtime jvm --text "$PIPELINE_TEXT"
run_example "pipeline.banking.pekko" 'path=payments ok=true' \
  bash examples-bin/run-pipeline.sh examples/pipelines/banking.yaml --runtime pekko --text "$PIPELINE_TEXT"

if python_ready; then
  for y in banking banking-llm banking-rag; do
    run_example "pipeline.$y.python" 'path=payments ok=True' \
      bash examples-bin/run-pipeline.sh "examples/pipelines/$y.yaml" --runtime python --text "$PIPELINE_TEXT"
  done
  run_example "pipeline.incident.python" 'path=monitor ok=True' \
    bash examples-bin/run-pipeline.sh examples/pipelines/incident.yaml --runtime python --text "$PIPELINE_TEXT"
  run_example "pipeline.multiagent.python.no-peer" 'path=triage ok=True' \
    bash examples-bin/run-pipeline.sh examples/pipelines/multiagent.yaml --runtime python --text "what time do you open?"
else
  skip "pipeline.*.python" "$PYTHON cannot import pyagentic and agentic_pipeline; run: $PYTHON -m pip install -e ports/pyagentic -e ports/agentic-pipeline"
fi

TOOL_SERVICES_APP="tool-services/tool-services-app/target/quarkus-app/quarkus-run.jar"
if [ -f "$TOOL_SERVICES_APP" ]; then
  run_example "pipeline.tools-mcp.jvm" 'path=assistant ok=true' \
    bash examples-bin/run-pipeline.sh examples/pipelines/tools-mcp.yaml --runtime jvm --text "what is 40 plus 2?"
  if python_ready && "$PYTHON" -c 'import mcp' >/dev/null 2>&1; then
    run_example "pipeline.tools-mcp.python" 'path=assistant ok=True' \
      bash examples-bin/run-pipeline.sh examples/pipelines/tools-mcp.yaml --runtime python --text "what is 40 plus 2?"
  else
    skip "pipeline.tools-mcp.python" "needs $PYTHON with pyagentic, agentic_pipeline and the mcp SDK (pip install mcp)"
  fi
else
  skip "pipeline.tools-mcp.*" "Tool Services not built: ./mvnw -f tool-services/tool-services-packs/pom.xml install -DskipTests && ./mvnw -f tool-services/tool-services-app/pom.xml package -DskipTests"
fi

# A2A peer for multiagent.yaml and banking-mcp.yaml: the Agentic Pekko HTTP front door.
PEER_PORT="${SMOKE_PEER_PORT:-8085}"
if [ -n "${AGENTIC_PEER_AGENT_URL:-}" ]; then
  info "using the A2A peer from AGENTIC_PEER_AGENT_URL=$AGENTIC_PEER_AGENT_URL"
  PEER_UP=1
elif (exec 3<>"/dev/tcp/127.0.0.1/$PEER_PORT") 2>/dev/null; then
  if curl -fsS "http://127.0.0.1:$PEER_PORT/.well-known/agent-card.json" >/dev/null 2>&1; then
    info "reusing the A2A peer already answering on :$PEER_PORT"
    PEER_UP=1
  else
    skip "a2a.peer" "port $PEER_PORT is in use by something that is not an A2A peer; set SMOKE_PEER_PORT to a free port"
    PEER_UP=0
  fi
else
  info "starting the Agentic Pekko HTTP front door on :$PEER_PORT as the A2A peer"
  AGENTIC_PEKKO_HTTP_PORT="$PEER_PORT" run_bg "a2a.peer" mvn_q -f agentic-pekko/pom.xml compile exec:java \
    -Dexec.mainClass=org.jagentic.pekko.http.HttpMain
  PEER_UP=0
  for _ in $(seq 1 180); do
    if curl -fsS "http://127.0.0.1:$PEER_PORT/.well-known/agent-card.json" >/dev/null 2>&1; then PEER_UP=1; break; fi
    sleep 1
  done
  [ "$PEER_UP" = "1" ] || { tail -n 30 "$LOG_DIR/a2a.peer.log"; summary; die "the A2A peer did not answer on :$PEER_PORT within 180s"; }
  ok "A2A peer answering on :$PEER_PORT"
fi
if [ "$PEER_UP" = "1" ]; then
  export AGENTIC_PEER_AGENT_URL="${AGENTIC_PEER_AGENT_URL:-http://127.0.0.1:$PEER_PORT}"
  run_example "pipeline.multiagent.jvm" 'path=escalations ok=true' \
    bash examples-bin/run-pipeline.sh examples/pipelines/multiagent.yaml --runtime jvm --text "please escalate this"
  if python_ready; then
    run_example "pipeline.multiagent.python" 'path=escalations ok=True' \
      bash examples-bin/run-pipeline.sh examples/pipelines/multiagent.yaml --runtime python --text "please escalate this"
  fi
  if [ -f "$TOOL_SERVICES_APP" ]; then
    run_example "pipeline.banking-mcp.jvm" 'path=support ok=true' \
      bash examples-bin/run-pipeline.sh examples/pipelines/banking-mcp.yaml --runtime jvm --text "$PIPELINE_TEXT"
    if python_ready && "$PYTHON" -c 'import mcp' >/dev/null 2>&1; then
      run_example "pipeline.banking-mcp.python" 'path=support ok=True' \
        bash examples-bin/run-pipeline.sh examples/pipelines/banking-mcp.yaml --runtime python --text "$PIPELINE_TEXT"
    else
      skip "pipeline.banking-mcp.python" "needs $PYTHON with pyagentic, agentic_pipeline and the mcp SDK (pip install mcp)"
    fi
  fi
else
  skip "pipeline.multiagent.{jvm,python}" "no A2A peer (AGENTIC_PEER_AGENT_URL)"
  skip "pipeline.banking-mcp.{jvm,python}" "no A2A peer (AGENTIC_PEER_AGENT_URL)"
fi

# ---------------------------------------------------------------- Podman network and Ollama
OLLAMA_UP=0
if [ "$HAVE_PODMAN" = "1" ]; then
  run_example "setup-network" "podman network" bash examples-bin/setup-network.sh
  if [ "$SMOKE_OLLAMA" = "1" ]; then
    run_example "run-ollama" "model .* ready" bash examples-bin/run-ollama.sh
    OLLAMA_UP=1
  else
    skip "run-ollama" "SMOKE_OLLAMA=0"
  fi
else
  skip "setup-network" "podman is not installed"
  skip "run-ollama" "podman is not installed"
fi
# shellcheck disable=SC2153
if [ "$OLLAMA_UP" = "0" ] && curl -sf --max-time 3 "$OLLAMA_URL/api/tags" >/dev/null 2>&1; then
  OLLAMA_UP=1
  ok "using the Ollama already running at $OLLAMA_URL"
fi

# ---------------------------------------------------------------- Flink-runtime showcases
if [ "$OLLAMA_UP" = "1" ]; then
  run_example "run-incident" 'incident#1 ticket=INC-1' bash examples-bin/run-incident.sh
  run_example "run-banking.once" 'personal agent answered' bash examples-bin/run-banking.sh --once "$PIPELINE_TEXT"
else
  skip "run-incident" "Ollama is not reachable at $OLLAMA_URL"
  skip "run-banking" "Ollama is not reachable at $OLLAMA_URL"
fi

if [ "$SMOKE_DJL" = "1" ]; then
  run_example "run-djl-embed" 'mean embed latency' bash examples-bin/run-djl-embed.sh
  if [ "$OLLAMA_UP" = "1" ]; then
    run_example "run-moderation" 'BLOCKED p-00[24] by' bash examples-bin/run-moderation.sh
    run_example "run-rag" 'Answer\[topic=' bash examples-bin/run-rag.sh
    run_example "run-support-triage" '=== Final reply ===|Routed to human queue' bash examples-bin/run-support-triage.sh
  else
    for s in run-moderation run-rag run-support-triage; do
      skip "$s" "Ollama is not reachable at $OLLAMA_URL"
    done
  fi
else
  for s in run-djl-embed run-moderation run-rag run-support-triage; do
    skip "$s" "SMOKE_DJL=0 (downloads DJL models)"
  done
fi
# run-live-research.sh refuses by design (exit 2); check that it does so and says why.
run_example "run-live-research.refuses" 'design sketch' bash -c '! bash examples-bin/run-live-research.sh 2>&1'
skip "run-live-research" "LiveResearchExample does not run end to end (unkeyed operators over keyed Flink state); see docs/examples/live-research.md"

# ---------------------------------------------------------------- production-shaped banking
if [ "$HAVE_PODMAN" = "1" ] && [ "$SMOKE_REDIS" = "1" ] && [ "$OLLAMA_UP" = "1" ]; then
  if ! (exec 3<>"/dev/tcp/127.0.0.1/6379") 2>/dev/null; then
    info "starting Redis in Podman for run-banking-local.sh"
    podman run -d --rm --name agentic-smoke-redis -p 127.0.0.1:6379:6379 docker.io/library/redis:7 >/dev/null
    STARTED_REDIS=1
    wait_for_port 127.0.0.1 6379 60 "Redis"
  fi
  run_example "run-banking-local.once" 'personal agent answered' bash examples-bin/run-banking-local.sh --once "$PIPELINE_TEXT"
else
  skip "run-banking-local" "needs Podman (Redis) and Ollama; SMOKE_REDIS=$SMOKE_REDIS podman=$HAVE_PODMAN ollama=$OLLAMA_UP"
fi

# ---------------------------------------------------------------- markets and infra stacks
if [ "$SMOKE_MARKETS" = "1" ] && [ "$HAVE_PODMAN" = "1" ]; then
  run_example "run-markets-stack" "Flink REST reachable" bash examples-bin/run-markets-stack.sh
  run_example "run-bond-market" "flink run -c" bash examples-bin/run-bond-market.sh
  skip "run-bond-market --submit" "long-running streaming job; submit by hand with the printed flink run command"
  skip "run-crypto-market" "needs outbound access to the Coinbase WebSocket feed"
else
  skip "run-markets-stack" "SMOKE_MARKETS=0 (pulls docker.io/library/flink and cp-kafka; unauthenticated Docker Hub pulls are rate limited)"
  skip "run-bond-market" "needs Kafka from run-markets-stack.sh"
  skip "run-crypto-market" "needs Kafka from run-markets-stack.sh and outbound Coinbase access"
fi
if [ "$SMOKE_STACKS" = "1" ] && [ "$HAVE_PODMAN" = "1" ]; then
  run_example "run-rag-stack" "Fluss" bash examples-bin/run-rag-stack.sh
  run_example "run-session-cluster" "Flink REST" bash examples-bin/run-session-cluster.sh
  run_example "down-all" "" bash examples-bin/down-all.sh
else
  skip "run-rag-stack" "SMOKE_STACKS=0 (pulls Postgres, Redis, Fluss images; needs POSTGRES_PASSWORD and REDIS_PASSWORD)"
  skip "run-session-cluster" "SMOKE_STACKS=0 (pulls docker.io/library/flink and Fluss images)"
  skip "down-all" "only meaningful after SMOKE_STACKS=1 or SMOKE_MARKETS=1"
fi

summary
printf '\nall runnable examples passed\n'
