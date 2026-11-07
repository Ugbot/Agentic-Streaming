#!/usr/bin/env bash
# End-to-end proof of the supported way to run an agent: examples/pipelines/banking.yaml (rule
# brain, no model, no key) as a job on a real Flink 2.2.1 session cluster, between real Kafka
# topics. Needs Docker or Podman, and ports 8081 and 9092 free.
#
#   bash tools/kafka-flink-e2e.sh              # builds the uber jar, runs, tears everything down
#   BUILD=0 bash tools/kafka-flink-e2e.sh      # reuse target/agentic-flink-*-uber.jar
#   KEEP=1 bash tools/kafka-flink-e2e.sh       # leave the stack up to look around
#
# It checks that turns come back, that a poison record goes to the failures topic and the job
# carries on, that a redelivered turn is a duplicate, and that the conversation survives losing
# the TaskManager, by restoring from a checkpoint.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source tools/container-engine.sh
use_container_engine

REST=http://localhost:8081
NETWORK=agentic-flink-network
KAFKA=agentic-flink-kafka
TASK_MANAGER=agentic-flink-tm
INPUT=agentic.turns
RESULTS="$INPUT.results"
FAILURES="$RESULTS.failures"
JAR=target/agentic-flink-1.0.0-SNAPSHOT-uber.jar
COMPOSE=(-f docker-compose-kafka.yml -f docker-compose-session.yml)
NETWORK_CREATED=""

say() { printf '\n== %s\n' "$*"; }

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    say "FAILED; last JobManager log lines"
    "$ENGINE" logs --tail 40 agentic-flink-jm 2>&1 | cut -c1-220 || true
  fi
  if [ "${KEEP:-0}" = "1" ]; then
    say "KEEP=1: stack left up. $REST"
    return
  fi
  compose "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  [ -z "$NETWORK_CREATED" ] || "$ENGINE" network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_until() { # description, timeout seconds, command...
  local what=$1 deadline=$((SECONDS + $2))
  shift 2
  until "$@" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for $what" >&2
      return 1
    fi
    sleep 2
  done
}

json() { # expression over `d`, reading a JSON document from stdin
  python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"
}

topics() { "$ENGINE" exec "$KAFKA" kafka-topics --bootstrap-server localhost:9092 "$@"; }

produce() { # topic; records on stdin as key|value
  "$ENGINE" exec -i "$KAFKA" kafka-console-producer --bootstrap-server localhost:9092 --topic "$1" \
    --property parse.key=true --property key.separator='|'
}

consume() { # topic, count
  "$ENGINE" exec "$KAFKA" kafka-console-consumer --bootstrap-server localhost:9092 --topic "$1" \
    --from-beginning --max-messages "$2" --timeout-ms 120000 2>/dev/null
}

turn() { # conversation, turn, text
  printf '%s|{"conversation_id":"%s","turn_id":"%s","user_id":"alice","text":"%s"}\n' "$1" "$1" "$2" "$3"
}

job_state() { curl -sf "$REST/jobs/$JOB" | json 'd["state"]'; }
job_running() {
  curl -sf "$REST/jobs/$JOB" | json 'd["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in d["vertices"])' | grep -q True
}
completed_checkpoints() { curl -sf "$REST/jobs/$JOB/checkpoints" | json 'd["counts"]["completed"]'; }
checkpoint_after() { [ "$(completed_checkpoints)" -gt "$1" ]; }

if [ "${BUILD:-1}" = "1" ]; then
  say "building the uber jar"
  ./mvnw -q -f reactor/pom.xml -pl .. -am package -DskipTests
fi
[ -f "$JAR" ] || { echo "$JAR not found; run with BUILD=1" >&2; exit 1; }

say "starting Kafka and a Flink session cluster with $ENGINE"
"$ENGINE" network inspect "$NETWORK" >/dev/null 2>&1 || NETWORK_CREATED=1
ensure_network "$NETWORK"
compose "${COMPOSE[@]}" up -d
wait_until "Kafka" 120 topics --list
wait_until "the Flink REST API" 120 curl -sf "$REST/overview"
wait_until "a TaskManager" 120 sh -c "curl -sf $REST/overview | grep -q '\"taskmanagers\":[1-9]'"
for topic in "$INPUT" "$RESULTS" "$FAILURES"; do
  topics --create --if-not-exists --topic "$topic" --partitions 2 --replication-factor 1
done

say "submitting banking.yaml over REST; the job reads KAFKA_BOOTSTRAP from compose"
JAR_ID=$(curl -sf -X POST -H "Expect:" -F "jarfile=@$JAR" "$REST/jars/upload" \
  | json 'd["filename"].split("/")[-1]')
JOB=$(curl -sf -X POST -H 'Content-Type: application/json' "$REST/jars/$JAR_ID/run" -d "{
  \"entryClass\": \"org.agentic.flink.pipeline.FlinkPipelineRunner\",
  \"programArgsList\": [\"/opt/agentic/examples/pipelines/banking.yaml\", \"--input-topic\", \"$INPUT\"]
}" | json 'd["jobid"]')
wait_until "the job to run" 120 job_running
echo "job $JOB is running"

say "turns, a poison record and a redelivery"
{
  turn c1 t1 "what is my balance?"
  echo 'bad|not json at all'
  turn c1 t1 "what is my balance?"
  turn c2 t1 "tell me about card types"
} | produce "$INPUT"

check_results() { # count, then a Python body over `r`, the decoded results by order per conversation
  consume "$RESULTS" "$1" | python3 -c "
import json, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
assert len(rows) == $1, f'expected $1 results, got {len(rows)}: {rows}'
r = {}
for row in rows:
    r.setdefault(row['conversation_id'], []).append(row)
$2
print('ok:', [(x['conversation_id'], x['turn_id'], x['status']) for x in rows])
"
}

check_results 3 "
assert r['c1'][0]['status'] == 'completed', r['c1'][0]
assert r['c1'][0]['path'] == 'payments', r['c1'][0]
assert '1234.56' in r['c1'][0]['reply'], r['c1'][0]
assert r['c1'][1]['status'] == 'duplicate', r['c1'][1]
assert r['c2'][0]['status'] == 'completed', r['c2'][0]
"
consume "$FAILURES" 1 | python3 -c "
import json, sys
row = json.loads(sys.stdin.readline())
assert row['kind'] == 'malformed_record', row
assert row['record'] == 'not json at all', row
print('ok: dead letter:', row['reason'][:60])
"
[ "$(job_state)" = RUNNING ] || { echo "the poison record stopped the job" >&2; exit 1; }

say "losing the TaskManager after a checkpoint"
BEFORE=$(completed_checkpoints)
wait_until "a checkpoint" 120 checkpoint_after "$BEFORE"
"$ENGINE" restart "$TASK_MANAGER" >/dev/null
sleep 5
wait_until "the job to recover" 240 job_running
LOST=$(curl -sf "$REST/jobs/$JOB/exceptions" | json 'len(d["exceptionHistory"]["entries"])')
[ "$LOST" -ge 1 ] || { echo "the job saw no failure, so this proves nothing" >&2; exit 1; }
echo "job recovered from $LOST recorded failure(s), state $(job_state)"
{
  turn c1 t1 "what is my balance?"
  turn c1 t2 "what is my balance?"
} | produce "$INPUT"

check_results 5 "
assert r['c1'][2]['status'] == 'duplicate', r['c1'][2]
assert r['c1'][3]['status'] == 'completed', r['c1'][3]
assert r['c1'][3]['state']['turn_count'] == 2, r['c1'][3]['state']
assert r['c1'][3]['events'][0]['sequence'] > 0, 'the log was restored, not restarted'
"
[ "$(job_state)" = RUNNING ]

say "PASS: pipeline.yaml from Kafka on a real Flink cluster, restored after losing the TaskManager"
