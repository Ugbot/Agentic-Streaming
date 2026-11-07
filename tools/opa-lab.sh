#!/usr/bin/env bash
# The tool boundary against a real OPA and the policy in examples/compose/opa/tools.rego.
#
#   bash tools/opa-lab.sh
#   PYTHON=/path/to/python bash tools/opa-lab.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source tools/container-engine.sh
use_container_engine

FILE=examples/compose/opa-lab.yml
PYTHON="${PYTHON:-python3}"
trap 'compose -f "$FILE" down -v >/dev/null 2>&1 || true' EXIT
compose -f "$FILE" up -d >/dev/null
for _ in $(seq 1 60); do curl -fsS http://127.0.0.1:18181/health >/dev/null 2>&1 && break; sleep 1; done

AGENTIC_OPA_URL=http://127.0.0.1:18181 "$PYTHON" - <<'PY'
from agentic.events import Turn
from agentic.runtime import LocalRuntime
from agentic.spec import Agent

spec = (Agent("opa-lab").path("main", tools=["pay", "look"], tool_triggers={"pay": "pay", "look": "look"})
        .tool("pay", "constant", value="paid", effect="idempotent")
        .tool("look", "constant", value="seen", effect="read")
        .verifier("none").build())


def turn(i, text, **who):
    rt = LocalRuntime()
    rt.deploy(spec)
    return rt.submit(Turn(f"c{i}", "t", text, principal={"subject": "s", "tenant": "acme", **who}))


read = turn(1, "look")
assert read["status"] == "completed", read
print("ok   a read is allowed")
refused = turn(2, "pay")
assert refused["error"]["class"] == "policy" and refused["tool_calls"] == [], refused
print("ok   a payment without the role is refused:", refused["error"]["message"])
paid = turn(3, "pay", roles=["payments"])
assert paid["error"]["class"] == "policy", paid  # the rule brain sends {user}, so no amount: the limit refuses
print("ok   a payment with no amount under the limit is refused")
print("opa lab passed")
PY
