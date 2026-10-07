#!/usr/bin/env bash
# Launch check for the shaded jars under JDK 21.
#
# Proves two things the reactor build cannot: that the runtime jars start under a plain
# `java` on JDK 21 with the project's --add-opens flags and no illegal-access failure, and
# that every copy of that flag list agrees with examples-bin/jvm-opts.sh.
#
# What runs (each with the flags from jvm-opts.sh, each bounded by a timeout):
#   1. target/agentic-flink-<version>-uber.jar on a Flink runtime classpath resolved from the
#      reactor's flink.version (the uber jar excludes the `provided` Flink runtime by design,
#      so a bare `java -jar` of it is not a supported entry point; see docs/getting-started.md).
#   2. banking-job/target/banking-job.jar on its own (it shades the Flink runtime).
# Both run org.agentic.flink.pipeline.FlinkPipelineRunner on examples/pipelines/banking.yaml
# with a single --text turn: that boots a MiniCluster, serializes the routed graph through
# Flink's Kryo path and exits, with no LLM, key or service needed. The check fails when the
# process exits non-zero, prints no `ok=true` result line, or logs an InaccessibleObjectException,
# IllegalAccessError, "does not \"opens\"" or "Unrecognized option" line.
#
# Usage:
#   bash tools/check-jar-launch.sh            # needs the jars: ./mvnw -f reactor/pom.xml -DskipTests install
#   LAUNCH_TIMEOUT=300 bash tools/check-jar-launch.sh
# Knobs:
#   LAUNCH_TIMEOUT  seconds per launch (default 180)
#   MAVEN_ARGS      extra flags for the ./mvnw classpath resolution
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../examples-bin/_common.sh
source "$HERE/../examples-bin/_common.sh"
cd "$REPO_ROOT"

LAUNCH_TIMEOUT="${LAUNCH_TIMEOUT:-180}"
LOG_DIR="$REPO_ROOT/target/jar-launch-check"
mkdir -p "$LOG_DIR"

require_java21
require_mvnw
require_python3

# ---- 1. every copy of the --add-opens list matches examples-bin/jvm-opts.sh -----------------
# shellcheck disable=SC2086
canonical="$(printf '%s\n' $AGENTIC_ADD_OPENS | sort)"
[ -n "$canonical" ] || die "AGENTIC_ADD_OPENS is empty after sourcing examples-bin/jvm-opts.sh"

check_flags() {
  local label="$1" file="$2" found
  found="$(grep -o -- '--add-opens=[^ "<]*' "$file" | sort -u)"
  if [ "$found" != "$canonical" ]; then
    err "$label ($file) does not carry the same --add-opens list as examples-bin/jvm-opts.sh"
    diff <(printf '%s\n' "$canonical") <(printf '%s\n' "$found") >&2 || true
    exit 1
  fi
  ok "$label carries the shared --add-opens list"
}
check_flags "Maven JVM config" ".mvn/jvm.config"
check_flags "surefire argLine" "pom.xml"
check_flags "banking container entrypoint" "docker/banking-entrypoint.sh"
check_flags "session cluster compose file" "docker-compose-session.yml"

python_flags="$("$PYTHON" - <<'EOF'
import re, pathlib
src = pathlib.Path("python/agentic_flink/_jvm.py").read_text()
block = re.search(r"_ADD_OPENS = tuple\(.*?\n\)\n", src, re.S).group(0)
for pkg in re.findall(r'"([a-z.]+)"', block):
    print(f"--add-opens=java.base/{pkg}=ALL-UNNAMED")
EOF
)"
if [ "$(printf '%s\n' "$python_flags" | sort)" != "$canonical" ]; then
  err "python/agentic_flink/_jvm.py does not carry the same --add-opens list as examples-bin/jvm-opts.sh"
  diff <(printf '%s\n' "$canonical") <(printf '%s\n' "$python_flags" | sort) >&2 || true
  exit 1
fi
ok "python facade carries the shared --add-opens list"

# ---- 2. locate the jars -------------------------------------------------------------------
UBER_JAR="$(ls -1 "$REPO_ROOT"/target/agentic-flink-*-uber.jar 2>/dev/null | head -n 1 || true)"
BANKING_JAR="$REPO_ROOT/banking-job/target/banking-job.jar"
[ -n "$UBER_JAR" ] && [ -f "$UBER_JAR" ] \
  || die "target/agentic-flink-*-uber.jar is missing. Run: ./mvnw -f reactor/pom.xml -DskipTests install"
require_file "$BANKING_JAR" "Run: ./mvnw -f reactor/pom.xml -DskipTests install"

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
mvn_q -f "$CP_POM_DIR/pom.xml" org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile="$FLINK_CP_FILE" >"$LOG_DIR/flink-classpath.log" 2>&1 \
  || { cat "$LOG_DIR/flink-classpath.log" >&2; die "could not resolve the Flink runtime classpath"; }
FLINK_CP="$(cat "$FLINK_CP_FILE")"
[ -n "$FLINK_CP" ] || die "empty Flink runtime classpath in $FLINK_CP_FILE"

# ---- 4. launch -----------------------------------------------------------------------------
PIPELINE="$REPO_ROOT/examples/pipelines/banking.yaml"
require_file "$PIPELINE" "The banking pipeline spec is part of the checkout."
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
launch "banking-job.jar (self-contained)" "$BANKING_JAR" "banking-job"
ok "jar launch check passed"
