#!/usr/bin/env bash
# Launch check for the shaded jar under JDK 21.
#
# Proves two things the reactor build cannot: that the uber jar starts under a plain `java` on
# JDK 21 with the project's --add-opens flags and no illegal-access failure, and that every copy of
# that flag list agrees with .mvn/jvm.config.
#
# It runs target/agentic-flink-<version>-uber.jar on a Flink runtime classpath resolved from the
# reactor's flink.version (the uber jar excludes the `provided` Flink runtime by design, so a bare
# `java -jar` of it is not a supported entry point). It runs
# org.agentic.flink.pipeline.FlinkPipelineRunner on examples/pipelines/banking.yaml with a single
# --text turn: that boots a MiniCluster, serializes the routed graph through Flink's Kryo path and
# exits, with no LLM, key or service needed. The check fails when the process exits non-zero,
# prints no `ok=true` result line, or logs an InaccessibleObjectException, IllegalAccessError,
# "does not \"opens\"" or "Unrecognized option" line.
#
# Usage:
#   bash tools/check-jar-launch.sh            # needs the jar: ./mvnw -f reactor/pom.xml -DskipTests install
#   LAUNCH_TIMEOUT=300 bash tools/check-jar-launch.sh
# Knobs:
#   LAUNCH_TIMEOUT  seconds per launch (default 180)
#   PYTHON          interpreter used to read the reactor version (default python3)
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

err()  { printf 'error: %s\n' "$*" >&2; }
ok()   { printf 'ok: %s\n' "$*"; }
info() { printf '.. %s\n' "$*"; }
die()  { err "$@"; exit 1; }

LAUNCH_TIMEOUT="${LAUNCH_TIMEOUT:-180}"
PYTHON="${PYTHON:-python3}"
LOG_DIR="$REPO_ROOT/target/jar-launch-check"
mkdir -p "$LOG_DIR"

java -version 2>&1 | head -n 1 | grep -q '"21\.' || die "JDK 21 is required on PATH"

# ---- 1. every copy of the --add-opens list matches .mvn/jvm.config ---------------------------
canonical="$(sort -u .mvn/jvm.config)"
[ -n "$canonical" ] || die ".mvn/jvm.config is empty"
AGENTIC_ADD_OPENS="$(printf '%s ' $canonical)"

check_flags() {
  local label="$1" file="$2" found
  found="$(grep -o -- '--add-opens=[^ "<]*' "$file" | sort -u)"
  if [ "$found" != "$canonical" ]; then
    err "$label ($file) does not carry the same --add-opens list as .mvn/jvm.config"
    diff <(printf '%s\n' "$canonical") <(printf '%s\n' "$found") >&2 || true
    exit 1
  fi
  ok "$label carries the shared --add-opens list"
}
check_flags "surefire argLine" "pom.xml"
check_flags "session cluster compose file" "docker-compose-session.yml"

# ---- 2. locate the jar --------------------------------------------------------------------
UBER_JAR="$(ls -1 "$REPO_ROOT"/target/agentic-flink-*-uber.jar 2>/dev/null | head -n 1 || true)"
[ -n "$UBER_JAR" ] && [ -f "$UBER_JAR" ] \
  || die "target/agentic-flink-*-uber.jar is missing. Run: ./mvnw -f reactor/pom.xml -DskipTests install"

# ---- 3. a Flink runtime classpath for the uber jar ----------------------------------------
# The uber jar keeps flink-streaming-java/flink-clients `provided`; a Flink cluster's lib/
# supplies them. Resolve the same artifacts at the reactor's flink.version through a
# throwaway pom that inherits the reactor parent.
reactor_version="$("$PYTHON" - <<'EOF'
import xml.etree.ElementTree as ET
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
print(ET.parse("reactor/pom.xml").getroot().find("m:version", ns).text)
EOF
)"
CP_POM_DIR="$LOG_DIR/flink-runtime"
mkdir -p "$CP_POM_DIR"
cat > "$CP_POM_DIR/pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.jagentic</groupId>
    <artifactId>agentic-streaming-reactor</artifactId>
    <version>${reactor_version}</version>
    <relativePath>../../../reactor/pom.xml</relativePath>
  </parent>
  <artifactId>jar-launch-check-flink-runtime</artifactId>
  <packaging>pom</packaging>
  <dependencies>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>\${flink.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients</artifactId>
      <version>\${flink.version}</version>
    </dependency>
  </dependencies>
</project>
EOF
FLINK_CP_FILE="$CP_POM_DIR/classpath.txt"
info "resolving the Flink runtime classpath through the reactor ${reactor_version} parent"
./mvnw -q -f "$CP_POM_DIR/pom.xml" org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile="$FLINK_CP_FILE" >"$LOG_DIR/flink-classpath.log" 2>&1 \
  || { cat "$LOG_DIR/flink-classpath.log" >&2; die "could not resolve the Flink runtime classpath"; }
FLINK_CP="$(cat "$FLINK_CP_FILE")"
[ -n "$FLINK_CP" ] || die "empty Flink runtime classpath in $FLINK_CP_FILE"

# ---- 4. launch -----------------------------------------------------------------------------
PIPELINE="$REPO_ROOT/examples/pipelines/banking.yaml"
MAIN="org.agentic.flink.pipeline.FlinkPipelineRunner"
FAIL_PATTERN='InaccessibleObjectException|IllegalAccessError|does not "opens"|Unrecognized option|UnsupportedClassVersionError'

launch() {
  local label="$1" cp="$2" log="$LOG_DIR/$3.log" status=0
  info "launching $label"
  # shellcheck disable=SC2086
  timeout "$LAUNCH_TIMEOUT" java $AGENTIC_ADD_OPENS -cp "$cp" "$MAIN" "$PIPELINE" \
    --text "what is my balance?" >"$log" 2>&1 || status=$?
  if [ "$status" -eq 124 ]; then
    tail -n 40 "$log" >&2
    die "$label did not finish within ${LAUNCH_TIMEOUT}s (log: $log)"
  fi
  if grep -Eq "$FAIL_PATTERN" "$log"; then
    grep -E "$FAIL_PATTERN" "$log" | head -n 5 >&2
    die "$label hit a reflective-access or launch failure under $(java -version 2>&1 | head -n 1) (log: $log)"
  fi
  [ "$status" -eq 0 ] || { tail -n 40 "$log" >&2; die "$label exited with status $status (log: $log)"; }
  grep -q 'ok=true' "$log" || { tail -n 40 "$log" >&2; die "$label produced no ok=true result line (log: $log)"; }
  ok "$label ran the banking pipeline turn (log: $log)"
}

launch "agentic-flink uber jar ($(basename "$UBER_JAR")) + Flink runtime" "$UBER_JAR:$FLINK_CP" "uber-jar"
ok "jar launch check passed"
