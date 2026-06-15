# Agentic Streaming on Apache Pulsar Functions

> Per the keystone [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md).
> Pulsar Functions is Apache Pulsar's lightweight serverless compute: a function
> consumes from input topic(s), processes, and publishes to an output topic, with a
> built-in durable **state store**. A working port lives in
> [`../../ports/pulsar/`](../../ports/pulsar/) (compiles, runs, and is tested).

## 1. Verdict

Pulsar Functions is one of the strongest fits in the series, and the **closest of the
native-C1+C2+C3 engines to Flink's own topic-in/topic-out shape**. A function is a
keyed stream processor with state: consume the request topic with a `Key_Shared`
subscription keyed by `conversationId`, and Pulsar delivers each conversation to one
function instance, in order (single-writer — C2); the function's **state store**
(BookKeeper-backed, replicated) is durable keyed state (C1 *and* C3 — the runtime
persists and recovers it, no external database); chained functions/topics are the
topology (C12); effectively-once processing covers fault tolerance. You keep the whole
Flink-free `jagentic-core` verbatim — the function body just builds an `AgentContext`
over a state-backed `ConversationStore` and calls `Banking.buildGraph().handle(...)`.
The one caveat is C4: a blocking LLM/A2A call on the function thread stalls the
instance, so the non-blocking pattern is the same response-topic split as Kafka
Streams. Operationally it is lighter than a Pekko cluster (Pulsar manages the state
store and rebalancing for you).

## 2. Capability mapping (C1..C12)

| Cap | How Pulsar Functions supplies it |
|-----|------------------------------------|
| **C1** durable keyed state | **N** — the function **state store** (`Context.putState/getState`), BookKeeper-backed and replicated; the conversation envelope is one state value per `conversationId`. |
| **C2** per-key ordered processing | **N** — a `Key_Shared` subscription with the message key = `conversationId` routes one key to one instance, processed in order. |
| **C3** fault tolerance / durability | **N** — effectively-once processing guarantee + acks; state survives instance restart/rebalance via BookKeeper. |
| **C4** async I/O | **L** — `process` is synchronous on the function thread; go non-blocking by publishing the reply to a response topic keyed by the same id (the Kafka-Streams-style split). |
| **C5** backpressure | **L** — Pulsar's flow control / receiver queue + ack pacing. |
| **C6** connectors | **N** — Pulsar IO connectors (sources/sinks) feed/drain the function. |
| **C7** side outputs | **N** — `context.newOutputMessage(topic, schema)` publishes to any topic. |
| **C8** broadcast state | **L** — a compacted topic the function reads, or shared state. |
| **C9** event-time / windows | **L** — windowing via the Windowed Functions wrapper; not a full event-time engine. |
| **C10** CEP | **L** — custom in the function. |
| **C11** distributed scale | **N** — instances scale per partition; the broker rebalances key ownership. |
| **C12** topology builder | **N** — chained functions and topics form the DAG. |

With Pekko and Temporal, Pulsar Functions is one of only three engines besides Flink
here to give **C1+C2+C3 all natively** — and the only one of them that does it in
Flink's topic-in/topic-out streaming shape.

## 3. The core abstractions on Pulsar Functions

- **Agent / keyed state (C1+C2).** The function is the keyed agent. The
  `conversationId` is the message key; with `Key_Shared` it owns that conversation
  single-writer. Per-conversation state lives in the state store:

  ```java
  public final class BankingFunction implements Function<String, String> {
    private final RoutedGraph graph = Banking.buildGraph();      // reused from jagentic-core
    private final ToolRegistry tools = Banking.defaultTools();
    private final Retrieval.TwoTierRetriever retriever = Banking.retriever();

    public String process(String text, Context context) {
      String cid = context.getCurrentRecord().getKey().orElse("anon");
      StateBytes sb = stateBytes(context);                       // Context.getState/putState
      AgentContext ctx = new AgentContext(cid, userIdOf(context),
          new PulsarStateConversationStore(sb),                  // C1: durable, BookKeeper-backed
          new PulsarStateKeyedStore(sb), tools, retriever);
      return graph.handle(new Event(cid, userIdOf(context), text), ctx).reply;  // the portable essence
    }
  }
  ```

- **ConversationStore.** [`PulsarStateConversationStore`](../../ports/pulsar/src/main/java/org/jagentic/ports/pulsar/PulsarStateConversationStore.java)
  serializes the per-conversation envelope (bounded transcript + attributes + owner)
  into one state value under `conv/<cid>`, with a `user/<userId>` reverse index — the
  same SPI as every other port, now backed by Pulsar's durable state instead of Redis.
- **KeyedStateStore.** [`PulsarStateKeyedStore`](../../ports/pulsar/src/main/java/org/jagentic/ports/pulsar/PulsarStateKeyedStore.java)
  maps each `(key,name)` scalar slot to a state key — the analogue of Flink keyed
  `ValueState`, persisted by the runtime.
- **Single-writer per conversation (C2).** The `Key_Shared` subscription contract:
  one key → one consumer instance, in order. Read-modify-write of the envelope is safe.
- **Tools / async (C4).** The rule-based banking graph never blocks. A real LLM/A2A
  call publishes to a response topic keyed by `conversationId` and re-enters the
  function to resume — the state store carries the pending turn between the two halves.
- **Inbound edge.** A Pulsar IO source or any producer onto the request topic, with
  the message key set to the `conversationId`.

## 4. Worked example — banking router→path→verifier

[`LocalDemo`](../../ports/pulsar/src/main/java/org/jagentic/ports/pulsar/LocalDemo.java)
runs the function with **no cluster** — an [`InMemoryContext`](../../ports/pulsar/src/main/java/org/jagentic/ports/pulsar/InMemoryContext.java)
(a dynamic proxy over the state API) stands in for the broker + BookKeeper:

```
[c1] turn=1 reply=[cards] We offer three card types: classic, gold, and platinum...
[c2] turn=1 reply=[payments] Your balance is 1234.56.
[c1] turn=2 reply=[cards] Crypto cash-back can be redeemed to a linked wallet...
[c3] turn=1 reply=[general] To dispute a charge, open the transaction and tap Dispute...

c1 persisted message count = 4 (state survives across turns)
```

`c1`'s two turns hit the *same* persisted envelope (4 messages = user+assistant ×2),
recovered straight from the state store — proving C1. In production:

```
pulsar-admin functions create --jar agentic-pulsar.jar \
  --classname org.jagentic.ports.pulsar.BankingFunction \
  --inputs banking-requests --output banking-responses \
  --subscription-type Key_Shared
```

## 5. What doesn't fit

- **Blocking async I/O.** Like Kafka Streams, a blocking LLM/A2A call on the function
  thread stalls the instance. Use the response-topic split for the non-blocking path.
- **Event-time / windowing / CEP.** Windowed Functions exist but there's no
  watermark/event-time engine; streaming-analytics flows are a weak fit.
- **State value size.** Function state values are best kept small; a very long
  transcript should be bounded (this port bounds it) or offloaded to a long-term store.
- **Client binding on the classpath.** A deployed function bundles the Pulsar client;
  the local demo adds `pulsar-client` at runtime scope so the `Context` proxy resolves.

## 6. When to choose Pulsar Functions

Choose Pulsar Functions when you already run **Pulsar** and want the keyed-stateful
agent essence with **native durable state and effectively-once**, without standing up
Flink — a serverless function per stage, state managed for you, scaling per partition.
It is the closest non-Flink engine to the topic-in/topic-out streaming model, and the
operationally lightest of the native-C1+C2+C3 options. If you need event-time analytics
or windowed CEP, that remains Flink's home; for a Kafka-native (not Pulsar) stack the
peer choice is Kafka Streams.
