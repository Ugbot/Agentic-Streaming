# Shared body for run-bond-market.sh and run-crypto-market.sh.
#
# Both market examples are long-running Flink streaming jobs fed from Kafka, so they are
# submitted to a Flink cluster instead of an embedded MiniCluster. This helper checks the
# prerequisites, builds the uber jar when it is missing, and either prints the exact producer
# and `flink run` commands or, with --submit, submits the job through the flink CLI.
#
# Prerequisites: JDK 21, the Maven wrapper, python3, Kafka at KAFKA_BOOTSTRAP
# (bash examples-bin/run-markets-stack.sh brings up Kafka plus a Flink 2.2.1 session cluster
# in Podman). ANTHROPIC_API_KEY is optional: without it the LLM tier is disabled and the
# rule-based tiers still emit alerts.

# markets_main <main class> <"--submit" or ""> <producer instructions...>
markets_main() {
  local main_class="$1" submit=0
  case "$2" in
    --submit) submit=1 ;;
    "") ;;
    *) die "unknown argument '$2' (the only flag is --submit)" ;;
  esac
  shift 2

  KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
  require_java21
  require_mvnw
  require_python3
  require_port "$KAFKA_BOOTSTRAP" "Kafka" \
    "Start it with: bash examples-bin/run-markets-stack.sh"

  JAR="$REPO_ROOT/target/agentic-flink-1.0.0-SNAPSHOT-uber.jar"
  if [ ! -f "$JAR" ]; then
    ensure_jagentic_core
    info "building $JAR (first run, a few minutes)"
    mvn_q -DskipTests package
  fi
  require_file "$JAR" "Run: ./mvnw -DskipTests package"
  ok "uber jar at $JAR"

  if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    ok "ANTHROPIC_API_KEY is set: the LLM tier is enabled"
  else
    info "ANTHROPIC_API_KEY is not set: the LLM tier is disabled, rule tiers still alert"
  fi

  printf '\nProducers (each in its own shell, from the repository root, after: pip install kafka-python websockets):\n'
  printf '  %s\n' "$@"
  printf '\nSubmit the Flink job (flink CLI 2.2.x, or POST the jar to the REST API on http://localhost:8081):\n'
  printf '  flink run -c %s "%s"\n' "$main_class" "$JAR"
  printf '\nAlerts print to the TaskManager stdout log.\n'

  if [ "$submit" = "1" ]; then
    require_cmd flink "Install the Flink 2.2.x CLI or submit the jar through the REST API instead."
    info "submitting $main_class"
    exec flink run -c "$main_class" "$JAR"
  fi
}
