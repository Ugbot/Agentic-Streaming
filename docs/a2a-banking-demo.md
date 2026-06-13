# Rho-Bank A2A demo — safe two-agent customer service

A first-class Agentic-Flink demo of the [A2A Hackathon](https://hackathon.a2anet.com) banking track
(tau2-bench): a **personal-agent** (`:9001`) and a **cs-agent** (`:9002`) talk over A2A JSON-RPC
while a simulated-user harness scores them. The stock template is two thin LLM agents with no
guardrails; this rebuild wraps them in the framework's **escalation / protection / screening /
bounded-routing** stack so they can't be run into the harness timeouts, bypass identity
verification, or be hijacked by prompt injection.

## Architecture

```
 tau2 harness ──A2A JSON-RPC──▶ personal-agent :9001 ──ask_customer_service (A2A)──▶ cs-agent :9002
  simulated user                  │                                                    │
        ▲  env API :8090          ▼  per inbound turn (keyed by contextId)             ▼  RAG (kb_search)
        └───── env tools ───── SCREEN ─▶ bounded ReAct brain ─▶ reply
                                 (InjectionDetector +            (RoutingBudget caps iterations +
                                  Repeat/Velocity)                personal↔cs round-trips; deadline)
                                 AuthorizationToolGuard on env tool calls (verify-before-act, no placeholders)
```

Each turn runs the same path whether hosted in the standalone gateway (default, in-process) or a
Flink operator (`BankingTurnFunction`): **screen → bounded ReAct brain → reply**, keyed by the A2A
`contextId` for free per-session isolation.

## The four protections

| Concern | Component | Behavior |
|---|---|---|
| Anti-explosion | `RoutingBudget` | caps personal↔cs round-trips + per-turn iterations + a soft deadline under the harness 5-min limit; dedupes repeats — the agent answers from what it has instead of looping into a timeout |
| Authorization | `AuthorizationToolGuard` | refuses high-risk env tools until identity is verified; refuses placeholder args |
| Threat screening | `BankingScreening` (`InjectionDetector` + Repeat + Velocity) | prompt-injection / identity-bypass / impersonation / exfiltration → BLOCK before the LLM; loop/flood → REVIEW |
| Escalation | ReAct brain + bank policy (`kb/policy.md`) | ALLOW / REVIEW / BLOCK; the policy's transfer-to-human guidance is the escalation path |

## Run it locally (OpenAI for dev)

```bash
export OPENAI_API_KEY=sk-...
bash examples-bin/run-banking.sh                 # builds + starts personal:9001 + cs:9002
curl http://localhost:9001/.well-known/agent-card.json

# conformance + benchmark (separate terminal; starts the harness env API):
cd hackathons/a2a-hackathon
uv run a2a-hack smoke --personal-url http://localhost:9001 --cs-url http://localhost:9002
uv run a2a-hack run --personal-url http://localhost:9001 --cs-url http://localhost:9002 \
    --tasks train --save-to results/dev --auto-resume
```

Set `ENV_API_URL`/`ENV_API_TOKEN` (and `CS_AGENT_URL` to the harness's `/cs-agent` recording
gateway) to exercise the environment tools and have the harness record the personal↔cs leg.

## One-shot (Compose / podman)

```bash
mvn -q install -DskipTests
mvn -q -f a2a-gateway/pom.xml package -DskipTests -Da2a.mode=banking
OPENAI_API_KEY=sk-... podman compose -f docker-compose-a2a-banking.yml up --build
```

## Swapping to the hackathon model

The model is config-only (`BankingModel.fromEnv`): for marked runs set `LLM_PROVIDER=gemini` and
`GOOGLE_API_KEY=...` (Vertex). No code change — the framework's Gemini `ChatConnection`
(`gemini-3.5-flash`) is already wired.

## Hosting inside Flink (the second "swap")

The brain (`ReActTurnBrain`) and the Flink operator (`BankingTurnFunction`, keyed by `contextId`)
are already split. The gateway runs the brain in-process today; moving execution into a Flink job
behind the A2A bridge is a wiring change (gateway → `A2ABridge` → `BankingTurnFunction`), not a
rewrite — the bounded-loop guarantee is proven in `BankingTurnFunctionTest` on a MiniCluster.

## Config reference

| Env var | Default | Meaning |
|---|---|---|
| `LLM_PROVIDER` | `openai` | `openai` / `gemini` / `ollama` |
| `OPENAI_API_KEY` / `GOOGLE_API_KEY` | — | model credential |
| `MODEL` | per-provider | chat model id |
| `A2A_BANKING_ROLE` | `personal` | `personal` / `cs` |
| `CS_AGENT_URL` | — | personal agent's CS endpoint (the harness `/cs-agent` gateway in marked runs) |
| `ENV_API_URL` / `ENV_API_TOKEN` | — | harness env-tools API (optional; chat/RAG only without it) |
| `KB_PATH` / `KB_POLICY_PATH` | `kb/documents`, `kb/policy.md` | CS knowledge base + policy |
| `A2A_MAX_ROUND_TRIPS` / `A2A_MAX_ITERATIONS` / `A2A_TURN_DEADLINE_MS` | 4 / 12 / 240000 | routing budget caps |
