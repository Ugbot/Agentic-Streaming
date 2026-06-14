# Agentic-Flink on Quarkus (JVM, reactive)

> Per-engine doc in the `docs/portability/` series. Read
> [`00-essence-and-core-abstractions.md`](./00-essence-and-core-abstractions.md)
> first — this doc is written against its essence (§2), capability inventory
> (§3, C1..C12), Engine SPI (§4c), matrix (§6, the **Quarkus** column), ranked
> fit (§7, #4), and the six-section template (§9). It also leans hard on what
> **already exists**: the `a2a-gateway/` Quarkus module is *already* our inbound
> A2A proxy (`A2AResource`) and our live RAG proxy (`RagResource`).

---

## 1. Verdict

**Quarkus is already half of this story shipped.** The inbound edge (essence §2.5)
is *not a sketch* — `a2a-gateway/` is a running Quarkus app: `A2AResource` serves
the Agent Card, JSON-RPC `message/send`, SSE `message/stream`, the full `tasks/*`
lifecycle, and `tasks/pushNotificationConfig/*`; `PushDispatcher` fires webhooks;
`RagResource` serves `/rag/ingest` + `/rag/query` over the same `HotVectorIndex`
+ `VectorStore` + `TwoTierRetriever` the Flink job uses. Today that gateway
**bridges into** an embedded Flink job (`A2AGatewayConnector` →
`A2ARequestBridge`). The porting question is: *delete Flink, keep Quarkus —
where does the keyed-stateful core go?*

The answer is an honest one. Quarkus gives you the I/O-bound async half of the
essence **natively and beautifully**: SmallRye Reactive Messaging (`@Incoming`/
`@Outgoing`, the Kafka connector) is C6; Mutiny `Uni`/`Multi` + Loom virtual
threads are C4 and C5 reactive backpressure; the whole thing is a CDI bean graph
you already know how to build. What it does **not** give you is an engine keyed-
state primitive. There is no `ValueState`, no `keyBy`, no checkpoint barrier. So
C1 (durable keyed state) comes from the **`ConversationStore` SPI** (Redis/Fluss
backend) and C2 (single-writer-per-conversation) comes from **Kafka partition
assignment + consumer groups** — key the request topic by `conversationId` and a
partition is owned by exactly one consumer in the group, which *is* your single
writer. You **assemble** the streaming-stateful core from Kafka + Redis/Fluss
rather than receiving it as one engine primitive.

**Keep, verbatim:** the entire pure core (§4a) — `ConversationStore`,
`VectorStore`/`HotVectorIndex`/`TwoTierRetriever`, `ToolRegistry`, embeddings/
inference SPIs, the A2A protocol types + `ResilientA2AClient`/`SdkA2AClient`, the
ReAct loop — and the gateway resources, which need *zero* change. **Replace:** the
Flink operator layer with reactive-messaging beans; the `A2AGatewayConnector`
bridge collapses from "Quarkus → bridge → Flink" to "Quarkus → bridge → Kafka →
Quarkus bean" (or even an in-JVM `MutinyEmitter` for embedded). **Drop:** native
checkpointing, event-time/windows, native CEP — re-derived from Kafka + idempotency.

Fit rank (§7): **#4.** Unbeatable for the proxy + per-message agent + A2A gateway;
the keyed-streaming core rides on Kafka + the state SPIs, which is solid but is
*infrastructure you operate*, not a primitive you inherit.

---

## 2. Capability mapping (C1..C12 — §6 Quarkus column)

| Cap | §6 | Mechanism on Quarkus |
|-----|:--:|----------------------|
| **C1** durable keyed state | **X** | No engine state. `ConversationStore` SPI (the §4a interface, unchanged) backed by **Redis** (`RedisConversationStore`) or **Fluss** PK table; short-term per-turn working state becomes a `KeyedStateStore` SPI over the same backend. Durability = the store, not a checkpoint. |
| **C2** per-key ordered processing | **L** | **Kafka partition assignment.** Topic keyed by `conversationId`; one partition → one consumer in the group → single writer, in-order. SmallRye consumer concurrency stays at one-per-partition. No locks needed *if* you keep keying honest; for in-JVM/embedded fall back to a per-key Mutiny serializing executor. |
| **C3** fault tolerance / EOS | **X** | Broker durability + **Kafka transactions** (exactly-once producer) where it matters; otherwise at-least-once + **idempotency** (the `ResilientA2AClient` already assumes idempotent sends; `ConversationStore` is the source of truth). Consumer offsets commit after the store write. |
| **C4** async I/O | **N** | **Mutiny `Uni`/`Multi`** + **virtual threads** (`@RunOnVirtualThread`). LLM and A2A calls are non-blocking by construction; bounded in-flight via `Multi` concurrency / a `Semaphore`. This is C4 *better* than Flink Async I/O. |
| **C5** backpressure | **N** | Reactive Streams backpressure end-to-end (Mutiny is a Reactive Streams impl); the Kafka connector pauses partitions when downstream is slow. Native. |
| **C6** connectors | **N** | **SmallRye Reactive Messaging** connectors: Kafka (primary), plus HTTP/AMQP/MQTT/Pulsar. `Channel<T>` maps to a named `@Incoming`/`@Outgoing` channel. Webhook out = the JDK `HttpClient` already in `PushDispatcher`. |
| **C7** side outputs | **L** | Extra `@Outgoing` channels / topics. The tool-invocation channel and debug stream become their own named channels — same shape as Flink side outputs, different wiring. |
| **C8** broadcast state | **L** | A control-plane Kafka topic consumed by *every* instance (unique group per instance, or a compacted topic replayed on start) into a `@ApplicationScoped` holder. No `GlobalKTable`, but a compacted topic + in-memory map is the idiom. |
| **C9** event-time / windows | **L** | No engine windowing. Roll your own with Mutiny `group`/`window`/`onItem().transformToUniAndConcatenate` over timestamps, or push windowed analytics to Flink/Kafka-Streams and keep Quarkus for serving. Honest gap for the analytics flows. |
| **C10** CEP | **L** | Custom — a stateful bean over the keyed stream, or a small rules lib. No native CEP. |
| **C11** distributed scale | **N** | Native on Kubernetes: scale the Deployment, Kafka rebalances partitions across pods (which is also how C2 stays correct under scale-out). Quarkus native-image gives fast cold-start. |
| **C12** topology builder | **L** | The DAG is a graph of CDI beans wired by channel names (`@Incoming("requests")` → `@Channel("path.x")` emitter → `@Incoming("path.x")`). Declarative-ish, but the topology lives in annotations + a wiring config, not a single fluent builder. |

The shape of the Quarkus column is "two N's where Flink leans hardest on I/O
(C4/C5/C6), two X's where Flink's heart is (C1/C3)." You trade the streaming-state
engine for a reactive I/O engine and *buy back* the state from SPIs you already own.

---

## 3. The core abstractions on Quarkus

### 3a. The Engine SPI (§4c) realized

```java
// EngineRuntime for Quarkus: channels are SmallRye, keyedAgent is a partitioned
// consumer bean, asyncStage is Mutiny. There is no single execute() — the CDI
// container + the Kafka connector ARE the runtime; you wire beans, not a graph.
public final class QuarkusEngine {                 // mostly conceptual: CDI does the work
  // source(spec)  -> @Incoming(spec.name())  on a bean method
  // sink(stream)  -> @Outgoing(spec.name())  / MutinyEmitter
  // keyedAgent(.) -> a partitioned @Incoming bean + KeyedStateStore over the SPI
  // asyncStage(.) -> Uni/Multi pipelines (virtual threads for blocking SDKs)
  // route(.)      -> a router @Incoming that emits to per-path @Channel emitters
}
```

### 3b. `KeyedStateStore` (the portable C1) over Redis/Fluss

The §4b Flink-bound `FlinkStateShortTermMemory` is replaced by an SPI that reads/
writes the same backend as `ConversationStore`. No checkpoint — the store *is* the
durable copy, written before the offset commits.

```java
public interface KeyedStateStore {                 // portable C1, §4c
  <V> Optional<V> get(String key, String name, Class<V> type);
  <V> void put(String key, String name, V value);
  void clear(String key);
}

@ApplicationScoped
public class RedisKeyedStateStore implements KeyedStateStore {
  @Inject RedisDataSource redis;                   // quarkus-redis-client
  public <V> Optional<V> get(String key, String name, Class<V> type) {
    String raw = redis.hash(String.class).hget("st:" + key, name);
    return raw == null ? Optional.empty() : Optional.of(Json.decode(raw, type));
  }
  public <V> void put(String key, String name, V v) {
    redis.hash(String.class).hset("st:" + key, name, Json.encode(v));
  }
  public void clear(String key) { redis.key().del("st:" + key); }
}
```

### 3c. An Agent = a reactive, partition-keyed message bean

An agent is a CDI bean that consumes from a `conversationId`-keyed Kafka channel,
loads per-conversation state via the SPIs, runs the (unchanged) ReAct loop, saves
state, and emits. Kafka's partition ownership gives C2; the store gives C1.

```java
@ApplicationScoped
public class BankingAgent {
  @Inject ConversationStore conversations;          // §4a — verbatim, Redis-backed
  @Inject KeyedStateStore shortTerm;                // §3b
  @Inject ToolRegistry tools;                       // §4a — verbatim
  @Inject TurnBrain brain;                           // §4a ReAct loop — verbatim

  // C2: requests topic is keyed by conversationId, so this partition's consumer
  // is the single writer for that conversation. C4/C5: Multi gives async + backpressure.
  @Incoming("requests")
  @Outgoing("replies")
  @RunOnVirtualThread                                // blocking LLM/tool SDKs are fine here
  public Message<AgentReply> onTurn(Message<AgentRequest> in) {
    AgentRequest req = in.getPayload();
    String cid = req.conversationId();               // == Kafka message key

    var transcript = conversations.recent(cid, 32);  // C1 read (cross-operator memory)
    var working = shortTerm.get(cid, "budget", RoutingBudget.class)
                           .orElseGet(RoutingBudget::fresh);

    TurnResult r = brain.run(req.text(), transcript, working, tools); // unchanged core

    conversations.append(cid, ChatMessage.user(req.text()));
    conversations.append(cid, ChatMessage.assistant(r.text()));        // C1 write …
    shortTerm.put(cid, "budget", r.budget());
    // … BEFORE the ack: offset commits after this returns, so the store leads the offset.
    return in.withPayload(new AgentReply(req.taskId(), cid, r.text()));
  }
}
```

### 3d. Tools, async, A2A — what changes (almost nothing)

- **Tools** (`ToolExecutor`/`ToolRegistry`, §4a) are unchanged. `execute(...)`
  returns `CompletableFuture<Object>`; wrap with `Uni.createFrom().completionStage(...)`
  to compose into a Mutiny pipeline, or just call it on a virtual thread.
- **Async I/O** is native: a fan-out of tool calls is `Multi.createFrom().iterable(calls)
  .onItem().transformToUniAndMerge(c -> Uni.createFrom().completionStage(c.run()))`
  with a concurrency cap — C4 + C5 in one expression.
- **A2A outbound** is *already done*: build `RemoteAgentSpec` → `A2AClientFactory`
  → `SdkA2AClient` wrapped in `ResilientA2AClient` (retry/backoff/jitter +
  per-peer `CircuitBreaker` + deadline, all unchanged). The `A2AStep` *pattern*
  (router-fixed delegation) ports; its Flink `applyTo*` methods (which call
  `keyBy`/`AsyncDataStream`) are replaced by a Mutiny stage that calls the
  resilient client — see §4. Run the (blocking) `send` on a virtual thread or
  wrap in `Uni`.

```java
@Inject A2AClientFactory a2a;                        // discovering() → SdkA2AClient
Uni<String> delegate(RemoteAgentSpec peer, String prompt) {
  return Uni.createFrom().item(() ->
      a2a.clientFor(peer)                            // ResilientA2AClient under the hood
         .sendAndAwait(A2AMessage.userText(UUID.randomUUID().toString(), prompt))
         .latestArtifactText())
    .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()); // off the event loop
}
```

### 3e. RAG and the inbound proxy — already shipped

`RagResource` (`/rag/ingest`, `/rag/query`) and `A2AResource` (JSON-RPC + SSE +
`tasks/*` + push) need **no change** for a Flink-free port — they never depended
on Flink, only on the §4a SPIs and the bridge connector. The *only* thing that
changes is the **bridge target**: instead of `A2AGatewayConnector` publishing to
an embedded Flink job, a `KafkaA2AGatewayConnector` publishes the `A2ARequest` to
the `requests` topic and correlates `A2AResponse`s by `taskId` off a `replies`
topic — the same `onResponse(listener)` contract the SSE path in `rpcStream` and
`PushDispatcher` already consume. For embedded single-JVM, an `InProcConnector`
hands the request straight to a `MutinyEmitter` and the agent bean's reply comes
back through the same listener registry. **`A2ARequestBridge`,
`CollectingGatewayEmitter`, and the SSE/push code are transport-agnostic and
survive verbatim.**

---

## 4. Worked example — banking `router → path → verifier`

The canonical routed graph (essence §2.3) becomes three reactive beans wired by
Kafka channel names, all keyed by `conversationId` so each conversation is a
single writer at every hop. The phase lives as a `ConversationStore` *attribute*
(`putAttribute`/`getAttribute`) so the graph progresses across turns and across
beans — exactly what the `ConversationStore` Javadoc describes as the layer
"between per-operator short-term state and the long-term store."

```
A2AResource (message/send)  ──► requests topic (key=conversationId)
        │                              │
        │  KafkaA2AGatewayConnector    ▼
        │                        ┌───────────┐   classify
        │                        │  Router   │───────────────┐
        │                        └───────────┘               │ @Channel emitters
        │                          emits to one of:           ▼
        │                  path.balance / path.transfer / path.dispute
        │                              │ (each a keyed @Incoming bean)
        │                              ▼
        │                        ┌───────────┐
        │                        │ Verifier  │  (@Incoming consumes all paths)
        │                        └───────────┘
        ▼                              │
  reply (taskId-correlated) ◄──── replies topic ◄── verified result
```

```java
@ApplicationScoped
public class Router {
  @Inject Classifier classifier;                     // inference SPI — verbatim
  @Inject ConversationStore conv;
  @Inject @Channel("path.balance")  MutinyEmitter<Routed> balance;
  @Inject @Channel("path.transfer") MutinyEmitter<Routed> transfer;
  @Inject @Channel("path.dispute")  MutinyEmitter<Routed> dispute;

  @Incoming("requests")                              // C2: keyed by conversationId
  public Uni<Void> route(AgentRequest req) {
    String cid = req.conversationId();
    String intent = classifier.classify(req.text()); // "balance" | "transfer" | "dispute"
    conv.append(cid, ChatMessage.user(req.text()));
    conv.putAttribute(cid, "phase", "ROUTED:" + intent); // cross-operator state, C1
    Routed r = new Routed(req.taskId(), cid, intent, req.text());
    return switch (intent) {                          // C7: per-path channels
      case "transfer" -> transfer.send(r);
      case "dispute"  -> dispute.send(r);
      default         -> balance.send(r);
    };
  }
}

@ApplicationScoped
public class TransferPath {
  @Inject ToolRegistry tools;                         // verbatim
  @Inject A2AClientFactory a2a;                        // ResilientA2AClient — verbatim
  @Inject ConversationStore conv;

  // Path may delegate to the remote fraud-check peer (the A2AStep pattern, §3d).
  @Incoming("path.transfer")
  @Outgoing("verify")
  @RunOnVirtualThread
  public Verified handle(Routed r) {
    var peer = RemoteAgentSpec.builder().name("fraud").url(fraudUrl).build();
    String risk = a2a.clientFor(peer)                 // resilient: retry+breaker+deadline
        .sendAndAwait(A2AMessage.userText(UUID.randomUUID().toString(), r.text()))
        .latestArtifactText();
    Object move = tools.get("transfer_funds").execute(parse(r.text())).join();
    conv.putAttribute(r.conversationId(), "phase", "PATH_DONE:transfer");
    return new Verified(r.taskId(), r.conversationId(), move + " (risk=" + risk + ")");
  }
}

@ApplicationScoped
public class Verifier {
  @Inject Scorer guardrail;                            // inference SPI — verbatim
  @Inject ConversationStore conv;

  @Incoming("verify")
  @Outgoing("replies")                                 // correlated back to the A2A task by taskId
  public AgentReply verify(Verified v) {
    boolean ok = guardrail.score(v.text()) >= 0.5;
    String cid = v.conversationId();
    conv.append(cid, ChatMessage.assistant(v.text()));
    conv.putAttribute(cid, "phase", ok ? "VERIFIED" : "REJECTED");
    return new AgentReply(v.taskId(), cid,
        ok ? v.text() : "I can't complete that — failed verification.");
  }
}
```

The `replies` channel feeds the `KafkaA2AGatewayConnector`'s listener registry;
`A2ARequestBridge` (unchanged) matches by `taskId`, drives the
`CollectingGatewayEmitter`, and `A2AResource.messageSend` returns the JSON-RPC
Message — *the same code path that runs against Flink today.* SSE
(`rpcStream`) and push (`PushDispatcher`) work identically because they consume
the same connector contract.

**Why this is correct without Flink:** every hop keys by `conversationId`, so
Kafka guarantees one writer per conversation per hop (C2). State that must cross
hops lives in `ConversationStore` (C1), not operator-local memory — which is
precisely the gap that interface was created to fill. Offsets commit after the
store write, so at-least-once + idempotent tools/A2A (C3) give effective
once-processing without an EOS engine.

---

## 5. What doesn't fit

- **No keyed-state primitive.** This is *the* gap (§3 C1). You don't get
  `ValueState`; you get Redis/Fluss + a `KeyedStateStore` SPI. The danger is a
  read-modify-write race if you ever process the same `conversationId` on two
  consumers. **Mitigation:** keep keying honest (one partition = one writer); for
  the embedded in-JVM mode, a per-key serializing executor; if you must, add a
  Redis `WATCH`/Lua CAS on the state hash. Don't pretend it's free.
- **No checkpointing / exactly-once-by-construction (C3).** State durability is
  "wrote it to Redis before committing the offset," not a consistent global
  snapshot. A crash between two `ConversationStore` writes can leave a half-
  applied turn — which is why the turn must be **idempotent** and the store the
  single source of truth. Use Kafka transactions only where a duplicated emit is
  genuinely unacceptable.
- **No event-time / watermarks / windowing (C9) and no native CEP (C10).** The
  streaming-analytics flows (feature aggregation, CEP timing) have no clean home.
  Hand-rolled Mutiny windowing is possible but fiddly. **Recommendation:** if you
  need real windowed/event-time analytics, *don't* do it on Quarkus — keep a
  Flink/Kafka-Streams job for that flow and let Quarkus serve and proxy. This is a
  hybrid, and that's fine.
- **The topology is implicit (C12).** There's no single place that says
  "router → path → verifier"; it's distributed across `@Incoming`/`@Outgoing`
  annotations and channel config. Comprehension and global reasoning are weaker
  than a fluent `AgentBuilder` DAG. Mitigate with a documented channel map.
- **Per-key state is not co-located with compute.** Every turn pays a Redis round
  trip Flink would have served from local RocksDB. For chatty multi-tool turns
  this adds latency. Fluss or a local near-cache helps; it's a real cost.

---

## 6. When to choose Quarkus

Choose Quarkus when:

- **You already run the gateway** (you do) and want to collapse the stack to *one*
  JVM technology — the inbound proxy, RAG, A2A client, and the agent core all live
  in the same reactive app, reusing the §4a core verbatim. No second runtime.
- **The workload is I/O-bound, request/response or moderate-throughput streaming.**
  LLM/A2A latency dominates; Mutiny + virtual threads + Kafka backpressure (C4/C5)
  are exactly the right tools, and arguably cleaner than Flink Async I/O.
- **You want Kubernetes-native scale and fast cold-start** (native image), with
  Kafka rebalancing giving you C2-under-scale for free.
- **You already operate Kafka + Redis/Fluss** and are comfortable assembling the
  streaming-stateful core from them rather than inheriting it.

Choose **something else** (Flink, or Kafka Streams — §7 #2) when the *center of
gravity* is the live keyed-stateful stream: heavy windowed/event-time analytics,
CEP, exactly-once-by-construction across many operators, or very high-throughput
stateful processing where a per-turn external state round trip is too costly. In
those cases the strongest pattern is **hybrid**: Quarkus stays the inbound A2A +
RAG proxy and the per-message/per-conversation agent (which it already is), and a
dedicated streaming engine owns the analytics flows behind it.

> Design intent, not a migration plan. A real port extracts the §4c Engine SPI
> (`KeyedStateStore`, the channel seam, the async stage) in the Java core, then
> writes one Quarkus adapter — most of which (`a2a-gateway/`) is already written.
