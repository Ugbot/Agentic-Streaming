# agentic-temporal: Agentic-Flink on Temporal

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

The agent essence as **Temporal durable workflows**, reusing the Flink-free
`org.jagentic:jagentic-core`. See the design in
[`../../../docs/portability/temporal.md`](../../../docs/portability/temporal.md).

**Why it fits so well:** **one entity workflow per conversation**, with
`workflowId == conversationId`. Temporal guarantees exactly one running execution per
id (single-writer, **C2**), makes the workflow's in-memory state durable and
fault-tolerant via its event-sourced history (**C1+C3**, replayed on crash/restart),
and delivers each turn as a synchronous **Update** applied serially. With Pekko and
Pulsar Functions it's one of only three engines besides Flink to give C1+C2+C3
natively, and the strongest durability of all (event-sourced replay + activity
retries are the whole point of the engine).

| File | Role |
|------|------|
| `ConversationWorkflow.java` | `@WorkflowInterface`: `run()` entity + `turn()` Update + `close()` Signal + `messageCount()` Query |
| `ConversationWorkflowImpl.java` | runs `Banking.buildGraph().handle(...)` over the durable in-workflow `ConversationStore`. Injectable graph/tools/retriever |
| `TurnMessages.java` | Jackson-serializable Update request/reply payloads |
| `LocalDemo.java` | runnable demo on an in-memory `TestWorkflowEnvironment` (no external Temporal server) |

The model-free banking graph is deterministic, so it runs *inside* the workflow; a real
LLM/A2A/tool call would move into an `@ActivityMethod` (its result recorded in history).

## Build and test

The adapter builds against the reactor-installed `org.jagentic:jagentic-core:1.0.0-SNAPSHOT`,
so install the reactor once first. The pom declares the same Maven Enforcer gate as the
reactor (JDK 21 or newer, Maven 3.9 or newer); use the committed wrapper.

```bash
./mvnw -f reactor/pom.xml -DskipTests install
./mvnw -f ports/experimental/temporal/pom.xml test
./mvnw -f ports/experimental/temporal/pom.xml -q compile exec:java   # runs the banking demo on an in-memory Temporal service
# ->
# [c1] turn=1 path=cards    ok=true reply=[cards] We offer three card types...
# [c2] turn=1 path=payments ok=true reply=[payments] Your balance is 1234.56.
# [c1] turn=2 path=cards    ok=true reply=[cards] Crypto cash-back can be redeemed...
# [c3] turn=1 path=general  ok=true reply=[general] To dispute a charge...
# c1 durable message count = 4 (event-sourced workflow state)
```

Tests (JUnit 5, no external Temporal server; `TestWorkflowEnvironment` is in-memory):

| Test | What it proves |
|------|----------------|
| `ConversationWorkflowTest.routesPersistsStateAndCallsToolAcrossTurns` | routing, durable transcript across two Updates on one workflow id, tool call on the payments path |
| `ConversationWorkflowTest.extendedCoreGraphFlowsThroughTheWorkflow` | an extended core graph (new path and tool) injected through a worker factory with no change to the workflow class |
| `EventBuilderTest` | the adapter's named `EventBuilder` puts random `conversationId`, `userId` and `text` values on the matching `Event` accessors and rejects missing fields |

`ConversationWorkflowImpl` builds the core `Event` through `EventBuilder` rather than the
positional constructor, because the Java and Go cores take `userId` before `text` while the
Python core takes `text` first; no call site in this adapter depends on that order.

In production, point a `WorkflowClient` at a real Temporal service and run the same
`ConversationWorkflowImpl` on a `Worker`; `UpdateWithStart` routes a turn to the
running-or-new workflow for its `conversationId`.
