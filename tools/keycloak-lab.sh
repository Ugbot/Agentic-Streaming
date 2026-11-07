#!/usr/bin/env bash
# The gateway against a real Keycloak: real tokens, in front of a Pekko system over the banking graph.
# It starts its own containers and gateway, proves who gets in and who does not, and stops them.
#
#   bash tools/keycloak-lab.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source tools/container-engine.sh
use_container_engine

FILE=examples/compose/keycloak-lab.yml
PORT="${GATEWAY_PORT:-18090}"
KC=http://127.0.0.1:18080
export KEYCLOAK_ADMIN_PASSWORD="${KEYCLOAK_ADMIN_PASSWORD:-lab-admin-$RANDOM}"
PID=""
cleanup() {
  [ -z "$PID" ] || kill "$PID" 2>/dev/null || true
  compose -f "$FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose -f "$FILE" up -d >/dev/null
for _ in $(seq 1 120); do
  curl -fsS "$KC/realms/agentic/.well-known/openid-configuration" >/dev/null 2>&1 && break
  sleep 2
done

token() { # client user password
  curl -fsS "$KC/realms/agentic/protocol/openid-connect/token" -d grant_type=password \
    -d "client_id=$1" -d "username=$2" -d "password=$3" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}
ALICE=$(token agentic-lab alice lab-alice)
OPS=$(token agentic-lab ops lab-ops)
BOB=$(token agentic-lab bob lab-bob)
WRONG_AUDIENCE=$(token some-other-api alice lab-alice)

AGENTIC_OIDC_ISSUER="$KC/realms/agentic" AGENTIC_OIDC_AUDIENCE=agentic-gateway AGENTIC_GATEWAY_PORT="$PORT" \
  ./mvnw -q -f agentic-pekko/pom.xml compile exec:java -Dexec.mainClass=org.jagentic.pekko.http.HttpMain \
  ${MAVEN_LAB_ARGS:-} >target/keycloak-lab.log 2>&1 &
PID=$!
for _ in $(seq 1 120); do curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && break; sleep 1; done

status() { # token method path [body]
  curl -s -o /dev/null -w '%{http_code}' -H "authorization: Bearer $1" -X "$2" "http://127.0.0.1:$PORT$3" ${4:+-d "$4"}
}
check() { # want got what
  if [ "$1" != "$2" ]; then echo "FAIL $3: wanted $1, got $2"; exit 1; fi
  echo "ok   $3 ($2)"
}

TURN='{"conversation_id":"lab","turn_id":"t1","text":"what is my balance?","user_id":"mallory"}'
check 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/agent" -d "$TURN")" "no token"
check 401 "$(status not.a.token POST /agent "$TURN")" "rubbish token"
check 401 "$(status "$WRONG_AUDIENCE" POST /agent "$TURN")" "token for another audience"
check 200 "$(status "$ALICE" POST /agent "$TURN")" "alice runs a turn"
check 200 "$(status "$ALICE" GET /conversations/lab)" "alice reads her conversation"
check 404 "$(status "$OPS" GET /conversations/lab)" "ops, same tenant, is not the owner"
check 404 "$(status "$BOB" GET /conversations/lab)" "bob is in another tenant"
LOG=$(curl -s -H "authorization: Bearer $ALICE" "http://127.0.0.1:$PORT/conversations/lab")
case "$LOG" in
  *'"tenant":"acme"'*) echo "ok   the log carries the tenant from the token" ;;
  *) echo "FAIL no tenant on the log: $LOG"; exit 1 ;;
esac
case "$LOG" in
  *mallory*) echo "FAIL the body's user_id reached the log"; exit 1 ;;
  *) echo "ok   the body's user_id reached nothing" ;;
esac
echo "keycloak lab passed"
