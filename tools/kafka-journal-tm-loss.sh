#!/usr/bin/env bash
# The effect journal across a lost TaskManager: tools/e2e/pay-once.yaml as a job on a real Flink
# 2.2.1 session cluster, its journal on a compacted Kafka topic, a real HTTP bank as the tool, and
# the TaskManager killed. No checkpoint is taken, so each loss rolls the log back to nothing and the
# source reads every turn again. Needs Docker or Podman, and ports 8081, 9092 and 18099 free.
#
#   bash tools/kafka-journal-tm-loss.sh            # builds the uber jar, runs, tears everything down
#   BUILD=0 bash tools/kafka-journal-tm-loss.sh    # reuse target/agentic-flink-*-uber.jar
#   KEEP=1 bash tools/kafka-journal-tm-loss.sh     # leave the stack up to look around
#
# It checks that a payment the journal recorded is not repeated, that the payment in flight when the
# TaskManager died is never repeated and waits for a person on the failures topic, and that the
# person's answer survives a second loss.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source tools/container-engine.sh
use_container_engine

REST=http://localhost:8081
BANK=http://localhost:18099
NETWORK=agentic-flink-network
KAFKA=agentic-flink-kafka
TASK_MANAGER=agentic-flink-tm
INPUT=agentic.pay
RESULTS="$INPUT.results"
FAILURES="$RESULTS.failures"
JAR=target/agentic-flink-1.0.0-SNAPSHOT-uber.jar
COMPOSE=(-f docker-compose-kafka.yml -f docker-compose-session.yml -f docker-compose-journal-e2e.yml)
NETWORK_CREATED=""

say() { printf '\n== %s\n' "$*"; }

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    say "FAILED; last JobManager and TaskManager log lines"
    "$ENGINE" logs --tail 30 agentic-flink-jm 2>&1 | cut -c1-220 || true
    "$ENGINE" logs --tail 30 "$TASK_MANAGER" 2>&1 | cut -c1-220 || true
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
    sleep 1
  done
}

json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
topics() { "$ENGINE" exec "$KAFKA" kafka-topics --bootstrap-server localhost:9092 "$@"; }
produce() {
  "$ENGINE" exec -i "$KAFKA" kafka-console-producer --bootstrap-server localhost:9092 --topic "$1" \
    --property parse.key=true --property key.separator='|'
}
consume() {
  "$ENGINE" exec "$KAFKA" kafka-console-consumer --bootstrap-server localhost:9092 --topic "$1" \
    --from-beginning --max-messages "$2" --timeout-ms 120000 2>/dev/null
}
turn() { printf '%s|{"conversation_id":"%s","turn_id":"%s","user_id":"alice","text":"%s"}\n' "$1" "$1" "$2" "$3"; }

seen() { curl -sf "$BANK/seen" | json "len([s for s in d if s[0] == '$1'])"; }
bank_saw() { [ "$(seen "$1")" = "$2" ]; }
job_running() {
  curl -sf "$REST/jobs/$JOB" | json 'd["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in d["vertices"])' | grep -q True
}
failures_recorded() { curl -sf "$REST/jobs/$JOB/exceptions" | json 'len(d["exceptionHistory"]["entries"])'; }

lose_the_task_manager() {
  local before
  before=$(failures_recorded)
  "$ENGINE" kill "$TASK_MANAGER" >/dev/null
  wait_until "Flink to notice" 120 sh -c "[ \"\$(curl -sf $REST/jobs/$JOB/exceptions | python3 -c 'import json,sys; print(len(json.load(sys.stdin)[\"exceptionHistory\"][\"entries\"]))')\" -gt $before ]"
  "$ENGINE" start "$TASK_MANAGER" >/dev/null
  wait_until "the job to recover" 240 job_running
  echo "job recovered, $(failures_recorded) failure(s) recorded, $(curl -sf "$REST/jobs/$JOB/checkpoints" | json 'd["counts"]["completed"]') checkpoint(s) completed"
}

results() { # count, then a Python body over `rows`
  consume "$RESULTS" "$1" | python3 -c "
import json, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
assert len(rows) == $1, f'expected $1 results, got {len(rows)}: {rows}'
$2
print('ok:', [(x['turn_id'], x['status']) for x in rows])
"
}

if [ "${BUILD:-1}" = "1" ]; then
  say "building the uber jar"
  ./mvnw -q -f reactor/pom.xml -pl .. -am package -DskipTests
fi
[ -f "$JAR" ] || { echo "$JAR not found; run with BUILD=1" >&2; exit 1; }

say "starting Kafka, a Flink session cluster and the bank with $ENGINE"
"$ENGINE" network inspect "$NETWORK" >/dev/null 2>&1 || NETWORK_CREATED=1
ensure_network "$NETWORK"
compose "${COMPOSE[@]}" up -d
wait_until "Kafka" 120 topics --list
wait_until "the Flink REST API" 120 curl -sf "$REST/overview"
wait_until "a TaskManager" 120 sh -c "curl -sf $REST/overview | grep -q '\"taskmanagers\":[1-9]'"
wait_until "the bank" 60 curl -sf "$BANK/seen"
for topic in "$INPUT" "$RESULTS" "$FAILURES"; do
  topics --create --if-not-exists --topic "$topic" --partitions 2 --replication-factor 1
done

say "submitting pay-once.yaml over REST"
JAR_ID=$(curl -sf -X POST -H "Expect:" -F "jarfile=@$JAR" "$REST/jars/upload" | json 'd["filename"].split("/")[-1]')
JOB=$(curl -sf -X POST -H 'Content-Type: application/json' "$REST/jars/$JAR_ID/run" -d "{
  \"entryClass\": \"org.agentic.flink.pipeline.FlinkPipelineRunner\",
  \"programArgsList\": [\"/opt/agentic/e2e/pay-once.yaml\", \"--input-topic\", \"$INPUT\"]
}" | json 'd["jobid"]')
wait_until "the job to run" 120 job_running
echo "job $JOB is running"

say "a payment completes, and another is in flight when the TaskManager dies"
turn c1 t1 "pay the invoice" | produce "$INPUT"
results 1 "assert rows[0]['status'] == 'completed', rows[0]"
turn c1 t2 "hold the invoice" | produce "$INPUT"
wait_until "the bank to see the held payment" 60 bank_saw /hold 1
lose_the_task_manager

say "the log is gone and the source reads both turns again"
results 3 "
assert rows[1]['turn_id'] == 't1' and rows[1]['status'] == 'completed', rows[1]
assert rows[1]['events'][0]['sequence'] == 0, 'the log was rolled back, or this proves nothing'
assert rows[2]['turn_id'] == 't2' and rows[2]['status'] == 'suspended', rows[2]
assert any(e['type'] == 'unknown_outcome' for e in rows[2]['events']), rows[2]
"
bank_saw /pay 1 || { echo "the recorded payment was repeated: $(curl -s "$BANK/seen")" >&2; exit 1; }
bank_saw /hold 1 || { echo "the payment in flight was repeated: $(curl -s "$BANK/seen")" >&2; exit 1; }
consume "$FAILURES" 1 | python3 -c "
import json, sys
row = json.loads(sys.stdin.readline())['result']
assert row['status'] == 'suspended' and row['turn_id'] == 't2', row
print('ok: the unknown outcome is on the failures topic for a person')
"

say "a person says it landed, and the answer survives a second loss"
printf 'c1|{"conversation_id":"c1","turn_id":"t2","user_id":"ops","signal":{"resolution":"applied","result":"paid by hand"}}\n' | produce "$INPUT"
results 4 "assert rows[3]['status'] == 'completed', rows[3]"
lose_the_task_manager
results 7 "
assert rows[4]['status'] == 'completed' and rows[5]['status'] == 'completed', rows[4:]
assert 'paid by hand' in rows[5]['reply'], rows[5]
assert rows[6]['status'] == 'duplicate', rows[6]
"
bank_saw /pay 1 || { echo "repeated after the second loss: $(curl -s "$BANK/seen")" >&2; exit 1; }
bank_saw /hold 1 || { echo "repeated after the second loss: $(curl -s "$BANK/seen")" >&2; exit 1; }
curl -sf "$BANK/seen" | json 'len({s[1] for s in d}) == 2' | grep -q True

say "PASS: the Kafka journal across two lost TaskManagers: one request per payment, and a person decided the unknown one"
