# Agentic Streaming on Apache Pekko

> Per the keystone [`00-essence-and-core-abstractions.md`](00-essence-and-core-abstractions.md).
> Apache Pekko is the open-source fork of Akka — a JVM actor/streams/cluster toolkit.
>
> **Pekko is now a first-class runtime: [`agentic-pekko/`](../../agentic-pekko/)** — an event-sourced,
> cluster-sharded entity per conversation with HTTP + Kafka front doors, pluggable durability
> (memory · Postgres · Cassandra · Redis), `backend: pekko` for any `pipeline.yaml`, and a
> durability/recovery demo. This doc is the design rationale (C1..C12); the original
> [`../../ports/pekko/`](../../ports/pekko/) proof-of-concept it was written against is superseded by
> that module.

## 1. Verdict

Pekko is, alongside Kafka Streams, the **best JVM fit** — and for the *stateful agent*
essence specifically it may be the most natural of all: **one actor per conversation**
is Flink's keyed operator expressed in the actor model. An actor processes its mailbox
one message at a time (single-writer, ordered — C2) and owns private fields (its state
— C1); **Cluster Sharding** distributes one entity per `conversationId` across a
cluster with location transparency and single-instance-per-key guarantees; **Pekko
Persistence** (event sourcing) makes that state durable and replayable (C3 — natively,
not bolted on); the **ask** pattern makes turns async (C4); **Pekko Streams** supplies
the dataflow with built-in backpressure (C5). You keep the entire Flink-free Java core
(`jagentic-core`) verbatim and write only the actor seam. The one thing it is *not* is
a Kafka-topology streaming engine — ingress is messages/streams you wire yourself —
but everything load-bearing maps cleanly and natively.

## 2. Capability mapping (C1..C12)

| Cap | How Pekko supplies it |
|-----|------------------------|
| **C1** durable keyed state | **N** — actor fields = per-entity state; **Cluster Sharding** keys it by `conversationId`; **Persistence/EventSourcedBehavior** makes it durable. |
| **C2** per-key ordered processing | **N** — an actor's mailbox is processed sequentially; sharding guarantees one live entity per id ⇒ single-writer-per-conversation. |
| **C3** fault tolerance / durability | **N** — event-sourced persistence (snapshots + journal) recovers entity state on restart/rebalance; supervision restarts failed actors. |
| **C4** async I/O | **N** — actors are async by construction; `ask` returns a `CompletionStage`; pipe LLM/A2A futures back as messages (`pipeToSelf`). |
| **C5** backpressure | **N** (Pekko Streams) / **L** (mailbox bounds, work-pulling) for the actor path. |
| **C6** connectors | **L** — Pekko Connectors (formerly Alpakka): Kafka, etc.; or drive entities from any ingress. |
| **C7** side outputs | **L** — just send to another actor / stream branch. |
| **C8** broadcast state | **L** — Distributed Data (CRDTs) or a pub/sub topic. |
| **C9** event-time / windows | **L** — Pekko Streams has time/throttle/group operators; not a full event-time engine. |
| **C10** CEP | **L** — actor FSM (`Behaviors`) or streams; custom. |
| **C11** distributed scale | **N** — cluster + sharding rebalances entities across nodes. |
| **C12** topology builder | **N** — the actor graph / Pekko Streams graph DSL. |

The standout: Pekko is one of only **three engines besides Flink** in this series
(with **Pulsar Functions** and **Temporal**) that give **C1 + C2 + C3 all natively**
(Kafka Streams does too, in the streaming paradigm; Ray does C1+C2 in memory but pushes
C3 to an external store). Pekko gets durability for free via Persistence — the
actor-model peer of Temporal's event-sourced workflows.

## 3. The core abstractions on Pekko

- **Agent / keyed state (C1+C2).** The per-conversation agent is a typed actor; its
  captured stores are the keyed state. One instance per `conversationId`:

  ```java
  public static Behavior<Command> create(String conversationId) {
    return Behaviors.setup(ctx -> {
      ConversationStore store = new ConversationStore.InMemory();   // this entity's state
      RoutedGraph graph = Banking.buildGraph();                     // reused from jagentic-core
      var tools = Banking.defaultTools(); var retriever = Banking.retriever();
      return Behaviors.receive(Command.class)
        .onMessage(ProcessTurn.class, msg -> {
          var actx = new AgentContext(conversationId, msg.event.userId(), store,
                                      new KeyedStateStore.InMemory(), tools, retriever);
          TurnResult r = graph.handle(msg.event, actx);             // the portable essence
          msg.replyTo.tell(new Reply(r.reply, r.path, r.ok));
          return Behaviors.same();
        }).build();
    });
  }
  ```

- **ConversationStore.** For a single sharded entity the actor's fields *are* the store
  (one writer). For durability use `EventSourcedBehavior` (the transcript/attributes are
  the persisted state) or write through to a Redis/Fluss `ConversationStore` — the same
  SPI as every other port.
- **Single-writer per conversation (C2).** Cluster Sharding's contract: at most one live
  entity per id. No locks, no partitions to reason about — the toolkit guarantees it.
- **Tools / async (C4).** A tool/LLM call returns a `CompletionStage`; `ctx.pipeToSelf`
  feeds the result back as a message so the actor never blocks its thread.
- **Routed graph.** Reused verbatim — `Banking.buildGraph().handle(event, ctx)` inside
  the actor. (You *could* model router/path/verifier as separate actors, but reusing the
  core graph keeps the logic identical across ports.)
- **A2A.** A peer call is an async `ask` to a remote entity (same cluster) or an HTTP
  call via Pekko HTTP + the resilient client; supervision/backoff give retry.
- **Inbound edge.** Pekko HTTP route, or a Pekko Connectors Kafka source, feeding
  `ProcessTurn` messages to the sharded entities.

## 4. Worked example — banking router→path→verifier

The first-class module's `Main` (single-node) and `ConversationSharding` (cluster) run the banking
graph as one event-sourced entity per conversation:

```
[c1] path=cards    reply=[cards] We offer three card types: classic, gold, and platinum...
[c2] path=payments reply=[payments] Your balance is 1234.56.
[c1] path=cards    reply=[cards] Crypto cash-back can be redeemed to a linked wallet ...
[c3] path=general  reply=[general] ...
```

`c1`'s two turns hit the *same* entity (its event-sourced state persists between turns, and survives
restart — see `RecoveryDemo`); `c2`/`c3` are separate entities, distributed across the cluster by
`ConversationSharding` and addressed by `entityRefFor(TYPE_KEY, conversationId).ask(...)`. Any shared
`pipeline.yaml` runs unchanged via `PipelineMain` (`backend: pekko`), including a declarative `cep:`
section (see [`stream-stateful-core.md`](stream-stateful-core.md)). See
[`agentic-pekko/README.md`](../../agentic-pekko/README.md).

## 5. What doesn't fit

- **Kafka-topology streaming.** Pekko isn't a "topic-in, topic-out" stream processor;
  you wire ingress (Pekko Connectors Kafka, HTTP) yourself. Fine for agents, but if your
  mental model is a Kafka Streams `Topology`, that's Kafka Streams' or Flink's shape.
- **Event-time / windowing.** Pekko Streams has time operators but not Flink's
  event-time+watermarks+windows engine; the streaming-analytics flows (markets) are a weak fit.
  (Complex event processing itself is *not* a gap — the portable `cep:` weave runs on Pekko via the
  core loader; only event-time-with-watermarks stays Flink-native.)
- **Operational weight.** A cluster (seed nodes, split-brain resolver, serialization
  bindings, persistence journal) is real ops surface — more than a single Faust/Kafka
  Streams app for small deployments. Single-node (as in `LocalDemo`) avoids it but gives
  up distribution/durability.
- **Scala-versioned artifacts.** Pekko publishes `_2.13`/`_3` artifacts; pick one
  consistently (this port uses `_2.13`, fine for a pure-Java consumer).

## 6. When to choose Pekko

Choose Pekko when you want the **stateful-agent essence on the JVM with native
durability and clustering**, and you think in actors: one durable, supervised,
location-transparent entity per conversation, event-sourced for free, scaling by
sharding. It's the JVM peer of Ray's actor-per-conversation model — but with built-in
persistence (C3) and cluster sharding (C1+C2+C11) rather than an external store. If you
also need a Kafka-native streaming topology with exactly-once, pair it with (or prefer)
Kafka Streams; if you need event-time analytics, that's Flink's home.
