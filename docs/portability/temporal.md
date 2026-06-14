# Agentic-Flink on Temporal

> Per the keystone [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md).
> Temporal is a **durable execution** engine: workflow code whose state and progress
> survive process crashes via an event-sourced history, with activities for the
> non-deterministic / I/O work. A working port lives in
> [`../../ports/temporal/`](../../ports/temporal/) (compiles, runs, and is tested
> against an in-memory Temporal service). A pure-Go peer (entity workflow per
> conversation, same design) lives in
> [`../../ports/go/engines/temporal/`](../../ports/go/engines/temporal/).

## 1. Verdict

Temporal is among the strongest fits in the series — and has arguably the **strongest
durability and correctness story of any engine here, Flink included**, for
long-running, multi-turn, possibly human-in-the-loop conversations. The mapping is
clean: **one entity workflow per conversation**, with `workflowId == conversationId`.
Temporal guarantees exactly one running execution per workflowId (single-writer — C2),
the workflow's in-memory state is made durable and fault-tolerant by its event-sourced
history (C1+C3 — replayed to rebuild state on any worker crash/restart), and a
synchronous **Update** delivers each turn and returns its reply, applied serially on
the workflow's single thread. You keep the whole Flink-free `jagentic-core` verbatim;
the deterministic banking graph runs *inside* the workflow, and a real LLM/A2A/tool
call would move into an Activity (its result recorded in history). The trade-off vs a
stream processor is latency and ops weight: Temporal is request/response orchestration
with durable state, not a low-latency keyed stream — but for durable agentic workflows
(retries, timers, waits-for-human, sagas) it is exactly the right engine.

## 2. Capability mapping (C1..C12)

| Cap | How Temporal supplies it |
|-----|---------------------------|
| **C1** durable keyed state | **N** — the workflow's fields are the per-conversation state; Temporal persists the event history and replays it to rebuild state. workflowId = conversationId keys it. |
| **C2** per-key ordered processing | **N** — exactly one running execution per workflowId; Updates/Signals are applied serially on the workflow's single thread, in arrival order. |
| **C3** fault tolerance / durability | **N** — event-sourced history survives worker crashes; automatic activity retries, timers, and replay recovery. The strongest durability guarantee in the series. |
| **C4** async I/O | **N** — activities run off the workflow thread (async, retried, timed out); the workflow `await`s their futures without blocking a partition. |
| **C5** backpressure | **L** — task-queue + worker concurrency limits; rate-limit activities. |
| **C6** connectors | **L** — activities call anything; Nexus / schedules / signals are the integration surface (no built-in stream connectors). |
| **C7** side outputs | **L** — start child workflows / send signals to other entities. |
| **C8** broadcast state | **L** — a shared activity-backed store, or signal fan-out. |
| **C9** event-time / windows | **—** — not a stream/event-time engine (timers give wall-clock waits). |
| **C10** CEP | **L** — workflow logic / await conditions; not a pattern engine. |
| **C11** distributed scale | **N** — workers scale horizontally; the service shards workflows by id. |
| **C12** topology builder | **N** — workflow code *is* the orchestration graph (child workflows, activities, signals). |

Temporal is one of only **three** engines besides Flink here (with Pekko and Pulsar
Functions) to supply **C1+C2+C3 all natively** — and the one whose durability is
strongest, because event-sourced replay + activity retries are the entire point of the
engine.

## 3. The core abstractions on Temporal

- **Agent / keyed state (C1+C2).** The per-conversation agent is an entity workflow;
  its captured `ConversationStore` is the durable keyed state. One execution per id:

  ```java
  @WorkflowInterface
  public interface ConversationWorkflow {
    @WorkflowMethod void run();                 // entity lives until close()
    @UpdateMethod   TurnReply turn(TurnRequest r);  // one turn, synchronous
    @SignalMethod   void close();
    @QueryMethod    int messageCount();
  }

  public final class ConversationWorkflowImpl implements ConversationWorkflow {
    private final RoutedGraph graph = Banking.buildGraph();        // reused from jagentic-core
    private final ConversationStore store = new ConversationStore.InMemory();  // durable via history
    private final String cid = Workflow.getInfo().getWorkflowId(); // == conversationId

    public TurnReply turn(TurnRequest r) {
      var ctx = new AgentContext(cid, r.userId, store, state, tools, retriever);
      TurnResult res = graph.handle(new Event(cid, r.userId, r.text), ctx);  // the portable essence
      return new TurnReply(res.reply, res.path, res.ok, store.messageCount(cid));
    }
  }
  ```

- **Determinism.** Workflow code must be deterministic, so Temporal can replay it. The
  model-free banking graph is — rule routing, a constant `get_balance` tool, pure-math
  hashing-embedder retrieval; no clock, randomness, or I/O — so it runs in-workflow with
  no Activity. A *real* LLM/A2A/tool call is non-deterministic and moves into an
  `@ActivityMethod` (result recorded in history); the workflow then orchestrates
  router → path-activity → verifier.
- **ConversationStore.** The workflow's in-memory `ConversationStore.InMemory` is the
  durable state — Temporal's history *is* the persistence layer. (For very long
  transcripts use continue-as-new or write through to a long-term store.)
- **Single-writer per conversation (C2).** Temporal's contract: one running execution
  per workflowId, updates applied serially. No locks, no partitions.
- **Async / tools (C4).** Activities are async, retried, and timed out by the engine —
  the natural home for the blocking external calls a real agent makes.
- **Inbound edge.** `UpdateWithStart` (or signal-with-start) routes a turn to the
  running-or-new workflow for its conversationId.

## 4. Worked example — banking router→path→verifier

[`LocalDemo`](../../ports/temporal/src/main/java/org/jagentic/ports/temporal/LocalDemo.java)
runs the workflows with **no external server** — an in-memory `TestWorkflowEnvironment`
hosts the worker:

```
[c1] turn=1 path=cards    ok=true reply=[cards] We offer three card types...
[c2] turn=1 path=payments ok=true reply=[payments] Your balance is 1234.56.
[c1] turn=2 path=cards    ok=true reply=[cards] Crypto cash-back can be redeemed...
[c3] turn=1 path=general  ok=true reply=[general] To dispute a charge...

c1 durable message count = 4 (event-sourced workflow state)
```

`c1`'s two turns hit the *same* durable execution (4 messages = user+assistant ×2),
read back via a Query — proving C1+C3. In production, point a `WorkflowClient` at a
Temporal service and run the same `ConversationWorkflowImpl` on a `Worker`.

## 5. What doesn't fit

- **Low-latency keyed streaming.** Each turn is a workflow task round-trip through the
  service; throughput/latency are well below an in-process stream operator. For a
  high-rate keyed stream, Faust/Kafka Streams/Flink are the home.
- **Event-time / windows / CEP.** Temporal has wall-clock timers, not event-time or
  windowing; streaming-analytics flows are out of scope.
- **Determinism constraints.** All non-determinism (LLM calls, clocks, randomness,
  external reads) must be in activities — a real porting cost for an LLM agent, though a
  natural fit (the model call is the activity).
- **Long transcripts / history size.** Unbounded workflow state bloats history; bound
  the transcript (this port does) or continue-as-new / offload to a long-term store.

## 6. When to choose Temporal

Choose Temporal when **durability and correctness of long-running agentic workflows**
matter more than per-turn latency: multi-step tool/agent orchestration, sagas with
compensation, human-in-the-loop waits, scheduled or long-lived conversations that must
survive restarts and resume exactly. The entity-workflow-per-conversation pattern gives
the keyed-stateful essence with the strongest durability guarantee here, and activities
are the ideal home for the blocking LLM/A2A calls. If you need a low-latency keyed
stream, pair it with (or prefer) Kafka Streams/Flink; Temporal then orchestrates the
durable, retried, long-horizon parts.
