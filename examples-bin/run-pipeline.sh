#!/usr/bin/env bash
# Runs one examples/pipelines/*.yaml turn on a chosen portable runtime with the prerequisites
# checked first. This is the executable form of docs/examples/banking-everywhere.md.
#
#   bash examples-bin/run-pipeline.sh <pipeline.yaml> [--runtime python|jvm|pekko] [--text "..."]
#
# Runtimes:
#   python  pure Python core (ports/pyagentic + ports/agentic-pipeline). Needs $PYTHON (default
#           python3, set PYTHON to a venv interpreter) with both packages installed:
#           python3 -m pip install -e ports/pyagentic -e ports/agentic-pipeline
#   jvm     jagentic-core PipelineCli (default). Needs JDK 21 and the Maven wrapper; installs
#           ports/jagentic-core into the local Maven repository when it is missing.
#   pekko   Agentic Pekko PipelineMain (backend forced to pekko). Needs JDK 21 and the wrapper.
#
# Pipelines that declare external prerequisites fail fast with the runtime's own message when
# those are missing: banking-mcp.yaml and tools-mcp.yaml need the Tool Services stdio server
# (./mvnw -f tool-services/tool-services-packs/pom.xml install -DskipTests, then
# ./mvnw -f tool-services/tool-services-app/pom.xml package -DskipTests) and, for the Python runtime, the mcp
# SDK (pip install mcp); banking-mcp.yaml and multiagent.yaml need an A2A peer at
# AGENTIC_PEER_AGENT_URL. The headers of the YAML files list the exact requirements.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "$HERE/_common.sh"

usage() {
  die "usage: bash examples-bin/run-pipeline.sh <pipeline.yaml> [--runtime python|jvm|pekko] [--text \"...\"]"
}

[ $# -ge 1 ] || usage
PIPELINE="$1"
shift
RUNTIME="${PIPELINE_RUNTIME:-jvm}"
TEXT="what is my balance?"
while [ $# -gt 0 ]; do
  case "$1" in
    --runtime) [ $# -ge 2 ] || usage; RUNTIME="$2"; shift 2 ;;
    --text)    [ $# -ge 2 ] || usage; TEXT="$2"; shift 2 ;;
    *) usage ;;
  esac
done

case "$PIPELINE" in
  /*) ;;
  *) PIPELINE="$REPO_ROOT/$PIPELINE" ;;
esac
require_file "$PIPELINE" "Pass a path such as examples/pipelines/banking.yaml."
cd "$REPO_ROOT"

case "$RUNTIME" in
  python)
    require_python3
    "$PYTHON" -c 'import pyagentic, agentic_pipeline' 2>/dev/null \
      || die "The Python pipeline CLI is not importable from $PYTHON. Run: $PYTHON -m pip install -e ports/pyagentic -e ports/agentic-pipeline (or set PYTHON to a venv interpreter)"
    info "python ($PYTHON): $PIPELINE"
    exec "$PYTHON" -m agentic_pipeline run "$PIPELINE" --text "$TEXT"
    ;;
  jvm)
    require_java21
    require_mvnw
    ensure_jagentic_core
    info "jvm-core PipelineCli: $PIPELINE"
    mvn_q -f ports/jagentic-core/pom.xml exec:java \
      -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
      -Dexec.args="'$PIPELINE' --text '$TEXT'"
    ;;
  pekko)
    require_java21
    require_mvnw
    ensure_jagentic_core
    info "agentic-pekko PipelineMain: $PIPELINE"
    mvn_q -f agentic-pekko/pom.xml compile exec:java \
      -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
      -Dexec.args="'$PIPELINE' --text '$TEXT'"
    ;;
  *)
    die "unknown runtime '$RUNTIME' (python, jvm or pekko)"
    ;;
esac
