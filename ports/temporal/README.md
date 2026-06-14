# agentic-temporal — Agentic-Flink on Temporal

The agent essence as **Temporal durable workflows**, reusing the Flink-free
`org.jagentic:jagentic-core`. See the design in
[`../../docs/portability/temporal.md`](../../docs/portability/temporal.md).

**Why it fits so well:** **one entity workflow per conversation**, with
`workflowId == conversationId`. Temporal guarantees exactly one running execution per
id (single-writer — **C2**), makes the workflow's in-memory state durable and
fault-tolerant via its event-sourced history (**C1+C3**, replayed on crash/restart),
and delivers each turn as a synchronous **Update** applied serially. With Pekko and
Pulsar Functions it's one of only three engines besides Flink to give C1+C2+C3
natively — and the strongest durability of all (event-sourced replay + activity
retries are the whole point of the engine).

| File | Role |
|------|------|
| `ConversationWorkflow.java` | `@WorkflowInterface`: `run()` entity + `turn()` Update + `close()` Signal + `messageCount()` Query |
| `ConversationWorkflowImpl.java` | runs `Banking.buildGraph().handle(...)` over the durable in-workflow `ConversationStore`. Injectable graph/tools/retriever |
| `TurnMessages.java` | Jackson-serializable Update request/reply payloads |
| `LocalDemo.java` | runnable demo on an in-memory `TestWorkflowEnvironment` (no external Temporal server) |

The model-free banking graph is deterministic, so it runs *inside* the workflow; a real
LLM/A2A/tool call would move into an `@ActivityMethod` (its result recorded in history).

## Run

```bash
mvn -f ports/temporal/pom.xml compile            # BUILD SUCCESS
mvn -f ports/temporal/pom.xml test               # 2 tests (banking + extended-graph via worker factory)
mvn -f ports/temporal/pom.xml -q compile exec:java   # runs the banking demo on an in-memory Temporal service
# ->
# [c1] turn=1 path=cards    ok=true reply=[cards] We offer three card types...
# [c2] turn=1 path=payments ok=true reply=[payments] Your balance is 1234.56.
# [c1] turn=2 path=cards    ok=true reply=[cards] Crypto cash-back can be redeemed...
# [c3] turn=1 path=general  ok=true reply=[general] To dispute a charge...
# c1 durable message count = 4 (event-sourced workflow state)
```

In production, point a `WorkflowClient` at a real Temporal service and run the same
`ConversationWorkflowImpl` on a `Worker`; `UpdateWithStart` routes a turn to the
running-or-new workflow for its `conversationId`.
