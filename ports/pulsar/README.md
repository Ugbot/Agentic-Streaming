# agentic-pulsar — Agentic-Flink as an Apache Pulsar Function

The agent essence as a **Pulsar Function**, reusing the Flink-free
`org.jagentic:jagentic-core`. See the design in
[`../../docs/portability/pulsar.md`](../../docs/portability/pulsar.md).

**Why it fits so well:** a Pulsar Function is a keyed stream processor *with durable
state*. Consume the request topic with a `Key_Shared` subscription keyed by
`conversationId` and Pulsar delivers one conversation to one instance, in order
(**C2**); the built-in **state store** (BookKeeper-backed, replicated) is durable keyed
state (**C1** + **C3**) — supplied by the runtime, no external database. With Pekko it
is one of only two engines besides Flink that give C1+C2+C3 natively, and the closest
of the two to Flink's topic-in/topic-out shape.

| File | Role |
|------|------|
| `BankingFunction.java` | the Pulsar `Function<String,String>`; runs `Banking.buildGraph().handle(...)` over Pulsar-state-backed stores. Injectable with any core graph/tools/retriever. |
| `PulsarStateConversationStore.java` | `ConversationStore` over the Pulsar state API — durable per-conversation transcript + attributes + user index (C1) |
| `PulsarStateKeyedStore.java` | `KeyedStateStore` over the Pulsar state API — the Flink `ValueState` analogue |
| `StateBytes.java` | the narrow byte-keyed seam onto `Context.getState/putState` (keeps the stores testable + Context-decoupled) |
| `InMemoryContext.java` | an in-memory `Context`/`Record` (dynamic proxies) so the function runs with no cluster |
| `LocalDemo.java` | runnable single-node demo (state persists across turns, proving C1) |

## Run

```bash
mvn -f ports/pulsar/pom.xml compile          # BUILD SUCCESS
mvn -f ports/pulsar/pom.xml test              # 2 tests (banking + extended-graph through the seam)
mvn -f ports/pulsar/pom.xml -q exec:java      # runs the banking demo on an in-memory Pulsar Context
# ->
# [c1] turn=1 reply=[cards] We offer three card types: classic, gold, and platinum...
# [c2] turn=1 reply=[payments] Your balance is 1234.56.
# [c1] turn=2 reply=[cards] Crypto cash-back can be redeemed to a linked wallet...
# [c3] turn=1 reply=[general] To dispute a charge, open the transaction and tap Dispute...
# c1 persisted message count = 4 (state survives across turns)
```

Deploy to a real cluster (state then lives in BookKeeper; `Key_Shared` keying by
`conversationId` gives single-writer ordering across instances):

```bash
pulsar-admin functions create --jar agentic-pulsar.jar \
  --classname org.jagentic.ports.pulsar.BankingFunction \
  --inputs banking-requests --output banking-responses \
  --subscription-type Key_Shared
```
