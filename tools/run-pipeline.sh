#!/usr/bin/env bash
# Runs a pipeline.yaml as a Flink job on a local cluster, in its own JVM. `exec:java` cannot do this:
# Flink deserializes the job with the system classloader, which under Maven does not have it.
#
#   bash tools/run-pipeline.sh examples/pipelines/banking.yaml --text "what is my balance?"
#
# Any FlinkPipelineRunner flag works, including --input-topic for the Kafka job.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

mkdir -p target
CLASSPATH_FILE=target/run-pipeline.classpath
./mvnw -q -f reactor/pom.xml -pl .. -am test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile="$PWD/$CLASSPATH_FILE" >&2

OPENS=()
for package in java.util java.lang java.lang.invoke java.lang.reflect java.io java.net java.nio \
  java.text java.time java.util.concurrent java.util.concurrent.atomic; do
  OPENS+=("--add-opens=java.base/$package=ALL-UNNAMED")
done

exec java "${OPENS[@]}" \
  -cp "target/classes:ports/jagentic-core/target/classes:$(cat "$CLASSPATH_FILE")" \
  org.agentic.flink.pipeline.FlinkPipelineRunner "$@"
