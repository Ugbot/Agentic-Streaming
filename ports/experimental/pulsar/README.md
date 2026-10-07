# agentic-pulsar: Agentic-Flink as an Apache Pulsar Function

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

The agent essence as a **Pulsar Function**, reusing the Flink-free
`org.jagentic:jagentic-core`. See the design in
[`../../../docs/portability/pulsar.md`](../../../docs/portability/pulsar.md).

**Why it fits so well:** a Pulsar Function is a keyed stream processor *with durable
state*. Consume the request topic with a `Key_Shared` subscription keyed by
`conversationId` and Pulsar delivers one conversation to one instance, in order
(**C2**); the built-in **state store** (BookKeeper-backed, replicated) is durable keyed
state (**C1** + **C3**), supplied by the runtime, no external database. With Pekko it
is one of only two engines besides Flink that give C1+C2+C3 natively, and the closest
of the two to Flink's topic-in/topic-out shape.

| File | Role |
|------|------|
| `BankingFunction.java` | the Pulsar `Function<String,String>`; runs `Banking.buildGraph().handle(...)` over Pulsar-state-backed stores. Injectable with any core graph/tools/retriever. |
| `PulsarStateConversationStore.java` | `ConversationStore` over the Pulsar state API, durable per-conversation transcript + attributes + user index (C1) |
| `PulsarStateKeyedStore.java` | `KeyedStateStore` over the Pulsar state API, the Flink `ValueState` analogue |
| `StateBytes.java` | the narrow byte-keyed seam onto `Context.getState/putState` (keeps the stores testable + Context-decoupled) |
| `src/test/.../InMemoryContext.java` | test-only in-memory `Context`/`Record` (dynamic proxies) so the function runs with no cluster; not part of the shipped jar |
| `src/test/.../LocalDemo.java` | test-scoped single-node demo on that fake (state persists across turns in a heap map) |

## Build and test

The adapter builds against the reactor-installed `org.jagentic:jagentic-core:1.0.0-SNAPSHOT`,
so install the reactor once first. The pom declares the same Maven Enforcer gate as the
reactor (JDK 21 or newer, Maven 3.9 or newer); use the committed wrapper.

```bash
./mvnw -f reactor/pom.xml -DskipTests install
./mvnw -f ports/experimental/pulsar/pom.xml test
./mvnw -f ports/experimental/pulsar/pom.xml -DskipTests package   # target/agentic-pulsar-0.1.0-function.jar
./mvnw -f ports/experimental/pulsar/pom.xml -q test-compile exec:java  # runs the banking demo on the test-only in-memory Context
# ->
# [c1] turn=1 reply=[cards] We offer three card types: classic, gold, and platinum...
# [c2] turn=1 reply=[payments] Your balance is 1234.56.
# [c1] turn=2 reply=[cards] Crypto cash-back can be redeemed to a linked wallet...
# [c3] turn=1 reply=[general] To dispute a charge, open the transaction and tap Dispute...
# c1 persisted message count = 4 (state survives across turns)
```

Tests (JUnit 5):

| Test | Needs | What it proves |
|------|-------|----------------|
| `BankingFunctionTest` | nothing | routing and state across turns, and an extended core graph, through the in-memory `Context` proxy |
| `EventBuilderTest` | nothing | the adapter's named `EventBuilder` puts random `conversationId`, `userId` and `text` on the matching `Event` accessors and rejects missing fields |
| `BankingFunctionStandaloneTest` (tagged `integration`) | Podman, or an external standalone | the function deployed to a real Pulsar standalone: replies on the output topic, forwarded `userId` property, transcript and user index read back from the BookKeeper state store, transcript intact after a function instance restart |

`BankingFunctionStandaloneTest` starts `docker.io/apachepulsar/pulsar:3.3.1` with
`podman run ... -e PULSAR_STANDALONE_USE_ZOOKEEPER=1 bin/pulsar standalone` on ports 6650 and
8080, waits for the broker and the functions worker, builds a function package from the compiled
adapter classes plus `jagentic-core`, deploys it over the admin REST API with
`retainKeyOrdering` (a `Key_Shared` subscription keyed by `conversationId`), drives a random
number of randomized banking turns for several conversations, and removes the container
afterwards. The ZooKeeper flag matters: the default ZooKeeper-less standalone in Pulsar 3.x does
not start the BookKeeper stream storage, so `Context.getState` fails with
`State public/default/<name> is not enabled`. To run against a standalone you already have, set
`AGENTIC_PULSAR_SERVICE_URL` (for example `pulsar://localhost:6650`) and
`AGENTIC_PULSAR_ADMIN_URL` (for example `http://localhost:8080`); the test then deploys the
function there and does not start a container. Override the image with
`-Dagentic.pulsar.image=...`.

When neither Podman nor those variables are available the test is skipped and prints
`SKIPPED BankingFunctionStandaloneTest: ...` with the reason; Surefire reports it as skipped,
not passed. Any other problem (image pull failure, broker not healthy, function not running,
missing reply, wrong state) fails the test. The state keys contain a slash (`conv/<id>`,
`user/<id>`), so the test reads them with the slash URL-encoded
(`/admin/v3/functions/public/default/<name>/state/conv%2F<id>`); `pulsar-admin functions
querystate --key conv/<id>` returns 404 for the same reason.

`BankingFunction` builds the core `Event` through `EventBuilder` rather than the positional
constructor, because the Java and Go cores take `userId` before `text` while the Python core
takes `text` first; no call site in this adapter depends on that order.

Deploy to a real cluster (state then lives in BookKeeper; `Key_Shared` keying by
`conversationId` gives single-writer ordering across instances):

```bash
./mvnw -f ports/experimental/pulsar/pom.xml -DskipTests package
pulsar-admin functions create --jar ports/experimental/pulsar/target/agentic-pulsar-0.1.0-function.jar \
  --classname org.jagentic.ports.pulsar.BankingFunction \
  --inputs banking-requests --output banking-responses \
  --retain-key-ordering
```
