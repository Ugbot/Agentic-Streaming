# agentic-pekko — Agentic-Flink on Apache Pekko

The agent essence on the **actor model** (Pekko, the Apache fork of Akka), reusing
the Flink-free `org.jagentic:jagentic-core`. See the design in
[`../../docs/portability/pekko.md`](../../docs/portability/pekko.md).

**Why it fits so well:** a Pekko actor has a mailbox (one message at a time =
single-writer, ordered — **C2**) and private fields (its state — **C1**). One actor
per `conversationId` via **Cluster Sharding** is exactly Flink's keyed operator, and
**Pekko Persistence** (event sourcing) makes that state durable (**C3**) — so Pekko
gives the C1+C2+C3 heart *natively*, in the actor paradigm, the way Kafka Streams
does in the streaming paradigm. The `ask` pattern makes turns async (**C4**).

| File | Role |
|------|------|
| `ConversationActor.java` | the per-conversation typed actor; runs `Banking.buildGraph().handle(...)` over its private keyed state |
| `BankingSharding.java` | Cluster Sharding wiring — one entity per `conversationId` (keyed state + single-writer across the cluster) + an async `ask` |
| `LocalDemo.java` | runnable single-node demo (guardian spawns a child actor per conversation; no cluster/broker needed) |

## Run

```bash
mvn -f ports/pekko/pom.xml compile          # BUILD SUCCESS
mvn -f ports/pekko/pom.xml -q exec:java     # runs the banking demo on real Pekko actors
# ->
# [c1] path=cards    reply=[cards] We offer three card types: classic, gold, and platinum...
# [c2] path=payments reply=[payments] Your balance is 1234.56.
# [c1] path=cards    reply=[cards] Crypto cash-back can be redeemed to ...
# [c3] path=general  reply=[general] ...
```

`LocalDemo` runs in-process (provider `local`). The production path uses
`BankingSharding` (`pekko.actor.provider=cluster` + seed nodes) so conversation
entities distribute across the cluster, and an `EventSourcedBehavior` (or
write-through to a Redis/Fluss `ConversationStore`) for durable state.
