# Shared bootstrap for examples-bin/*.sh.
#
# Source this file first. It resolves the repository root from its own location, defines
# the logging helpers, and provides the prerequisite checks every example uses (JDK 21,
# the Maven wrapper, Podman, python3, Ollama). Each check fails fast with the exact
# command that satisfies the missing prerequisite. No absolute paths are assumed.
#
# Environment knobs (all optional):
#   OLLAMA_URL     Ollama base URL              (default http://localhost:11434)
#   OLLAMA_MODEL   chat model to make sure of   (default qwen2.5:3b)
#   MAVEN_ARGS     extra flags for every ./mvnw call, for example "-o" for offline
#   PYTHON         Python interpreter to use     (default python3; point it at a venv)

set -euo pipefail

AGENTIC_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$AGENTIC_COMMON_DIR/.." && pwd)"
MVNW="$REPO_ROOT/mvnw"
# shellcheck source=jvm-opts.sh
source "$AGENTIC_COMMON_DIR/jvm-opts.sh"

OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
OLLAMA_MODEL="${OLLAMA_MODEL:-qwen2.5:3b}"
OLLAMA_CONTAINER="${OLLAMA_CONTAINER:-agentic-flink-ollama}"
PODMAN_NETWORK="${PODMAN_NETWORK:-agentic-flink-network}"
PYTHON="${PYTHON:-python3}"

err()  { printf 'error: %s\n' "$*" >&2; }
ok()   { printf 'ok: %s\n' "$*"; }
info() { printf '.. %s\n' "$*"; }
die()  { err "$@"; exit 1; }

# require_cmd <command> <hint>
require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is not on PATH. $2"
}

require_curl()    { require_cmd curl "Install curl with your package manager."; }
require_python3() { require_cmd "$PYTHON" "Install Python 3.9 or newer, or set PYTHON to an interpreter."; }

require_mvnw() {
  [ -x "$MVNW" ] || die "Maven wrapper $MVNW is missing or not executable (run: chmod +x mvnw)."
}

# mvn_run <args...> : runs the Maven wrapper from the repository root, with MAVEN_ARGS split
# into words in front of the given arguments. mvn_q is the same with -q.
mvn_run() {
  local -a extra=()
  # shellcheck disable=SC2206
  [ -z "${MAVEN_ARGS:-}" ] || extra=($MAVEN_ARGS)
  ( cd "$REPO_ROOT" && "$MVNW" ${extra[@]+"${extra[@]}"} "$@" )
}
mvn_q() { mvn_run -q "$@"; }

require_java21() {
  require_cmd java "Install a JDK 21, for example from https://adoptium.net, and set JAVA_HOME."
  local version major
  version="$(java -version 2>&1 | head -n 1 | sed -E 's/.*"([^"]+)".*/\1/')"
  major="${version%%.*}"
  case "$major" in
    1) major="${version#1.}"; major="${major%%.*}" ;;
  esac
  [[ "$major" =~ ^[0-9]+$ ]] || die "Could not parse the Java version from: $version"
  (( major >= 21 )) || die "JDK 21 or newer is required; found $version. Set JAVA_HOME to a JDK 21."
  ok "java $version"
}

require_podman() {
  require_cmd podman "Install Podman: https://podman.io/docs/installation (Ubuntu: sudo apt install podman; macOS: brew install podman)."
}

# Prints the compose command to use: `podman compose` when the subcommand exists, else the
# standalone `podman-compose`. Fails with an install hint when neither works.
compose_cmd() {
  require_podman
  if podman compose version >/dev/null 2>&1; then
    printf 'podman compose'
  elif command -v podman-compose >/dev/null 2>&1; then
    printf 'podman-compose'
  else
    die "Neither 'podman compose' nor 'podman-compose' is available. Install one: pip install podman-compose"
  fi
}

# podman_compose <compose args...> : runs from the repository root.
podman_compose() {
  local cmd
  cmd="$(compose_cmd)"
  # shellcheck disable=SC2086
  ( cd "$REPO_ROOT" && $cmd "$@" )
}

# ensure_podman_network : creates the external network every compose file expects.
ensure_podman_network() {
  require_podman
  if podman network exists "$PODMAN_NETWORK" >/dev/null 2>&1; then
    ok "podman network $PODMAN_NETWORK exists"
  else
    info "creating podman network $PODMAN_NETWORK"
    podman network create "$PODMAN_NETWORK" >/dev/null
    ok "podman network $PODMAN_NETWORK created"
  fi
}

# wait_for_port <host> <port> <seconds> <label>
wait_for_port() {
  local host="$1" port="$2" secs="$3" label="$4" i
  for ((i = 0; i < secs; i++)); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      ok "$label reachable on $host:$port"
      return 0
    fi
    sleep 1
  done
  die "$label did not answer on $host:$port within ${secs}s"
}

# require_port <host:port> <label> <hint>
require_port() {
  local host="${1%:*}" port="${1#*:}"
  if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
    ok "$2 reachable at $1"
  else
    die "$2 is not reachable at $1. $3"
  fi
}

# stop_pids <pid...> : TERM, wait up to 10s, then KILL whatever is still alive (embedded Flink
# MiniClusters can take longer than that to unwind their shutdown hooks).
stop_pids() {
  [ $# -gt 0 ] || return 0
  kill "$@" 2>/dev/null || true
  local i pid
  for ((i = 0; i < 10; i++)); do
    local alive=0
    for pid in "$@"; do kill -0 "$pid" 2>/dev/null && alive=1; done
    [ "$alive" = "1" ] || return 0
    sleep 1
  done
  kill -9 "$@" 2>/dev/null || true
}

# require_env <VAR> <hint>
require_env() {
  [ -n "${!1:-}" ] || die "$1 is not set. $2"
}

# require_file <path> [hint]
require_file() {
  [ -f "$1" ] || die "missing file $1. ${2:-}"
}

# require_dir <path> [hint]
require_dir() {
  [ -d "$1" ] || die "missing directory $1. ${2:-}"
}

# Makes sure the shared JVM core is in the local Maven repository (the root module depends on
# it but it is outside the root reactor). Installs it when absent.
ensure_jagentic_core() {
  require_java21
  require_mvnw
  local repo
  repo="$(mvn_q help:evaluate -Dexpression=settings.localRepository -DforceStdout 2>/dev/null || true)"
  [ -n "$repo" ] || repo="${MAVEN_REPO_LOCAL:-$HOME/.m2/repository}"
  if compgen -G "$repo/org/jagentic/jagentic-core/0.1.0/jagentic-core-0.1.0.jar" >/dev/null; then
    ok "jagentic-core 0.1.0 is installed"
  else
    info "installing ports/jagentic-core (first run only)"
    mvn_q -f ports/jagentic-core/pom.xml install -DskipTests
    ok "jagentic-core installed"
  fi
}

# check_ollama : Ollama must answer and OLLAMA_MODEL must be present (pulled on demand).
check_ollama() {
  require_curl
  info "checking Ollama at $OLLAMA_URL"
  if ! curl -sf --max-time 3 "$OLLAMA_URL/api/tags" >/dev/null; then
    err "Ollama is not reachable at $OLLAMA_URL"
    die "start it with: bash examples-bin/run-ollama.sh"
  fi
  ok "Ollama reachable"
  if ! curl -sf "$OLLAMA_URL/api/tags" | grep -q "\"$OLLAMA_MODEL\""; then
    info "pulling $OLLAMA_MODEL (one time, about 2 GB)"
    local last
    last="$(curl -sf -X POST "$OLLAMA_URL/api/pull" -d "{\"name\":\"$OLLAMA_MODEL\"}" | tail -n 1)"
    printf '%s' "$last" | grep -q '"success"' || die "pull of $OLLAMA_MODEL failed: $last"
  fi
  ok "model $OLLAMA_MODEL ready"
}

# example_classpath : prints the classpath for running the Flink showcase mains from
# src/main/java. Flink is `provided` in the root pom, so a plain `exec:java` run lacks
# org.apache.flink.streaming.*; this compiles the module once and resolves the full (test scope,
# which includes provided) dependency classpath into target/example-classpath.txt, refreshed
# whenever pom.xml changes.
example_classpath() {
  ensure_jagentic_core >&2
  local cp_file="$REPO_ROOT/target/example-classpath.txt"
  if [ ! -f "$cp_file" ] || [ "$REPO_ROOT/pom.xml" -nt "$cp_file" ] || [ ! -d "$REPO_ROOT/target/classes" ]; then
    info "compiling the Flink module and resolving its classpath (first run takes a few minutes)" >&2
    mvn_q compile dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile="$cp_file"
  else
    mvn_q compile
  fi
  printf '%s:%s' "$REPO_ROOT/target/classes" "$(cat "$cp_file")"
}

# run_flink_example <main class> [program args...]
# Replaces the shell with a JVM running the given showcase main with the add-opens Flink needs.
run_flink_example() {
  local main_class="$1"
  shift
  local cp
  cp="$(example_classpath)"
  info "running $main_class"
  cd "$REPO_ROOT"
  # shellcheck disable=SC2086
  exec java $AGENTIC_ADD_OPENS -cp "$cp" "$main_class" "$@"
}
