# Agentic Streaming on Spring (Boot / Cloud Stream / AI / Integration / StateMachine)

> Per-engine portability doc. Read `00-essence-and-core-abstractions.md` first — this
> doc is written against its essence (§2), capability inventory (§3, C1..C12), Engine
> SPI (§4c), matrix (§6, the **Spring** column), and ranked fit (§7, rank 5). It
> follows the §9 six-section template.

## 1. Verdict

Spring is the **enterprise-wiring** target, not a streaming-analytics engine. Almost
everything in the *agentic* half of the essence (§2: agents-as-stateful-processors,
tiered memory, tools, routing, A2A, RAG) lands cleanly — and several pieces land
*better* than hand-rolled, because Spring already ships first-class equivalents:
**Spring AI** gives you `ChatClient` + advisors + `@Tool` function-calling +
`VectorStore` + `ChatMemory`; **Spring Integration** gives you the routed graph as a
literal EIP topology (`router → service activators → aggregator` ≈ our
`router → path → verifier`); **Spring StateMachine** gives you the agent phase FSM as
a declarative config; **Spring Cloud Stream** gives you Kafka I/O as functional beans.

What you *give up* is the streaming runtime itself. There is no native
keyed-checkpointed state (C1), no `keyBy`-derived single-writer (C2), no event-time /
windowing (C9), and no CEP (C10). The dataflow is **message channels and integration
flows**, not a keyed streaming topology, so the "one durable processor per
conversation, processed in order, checkpointed" guarantee that Flink hands you for
free must be *assembled* from **Kafka partitioning + an external state store**
(Redis via Spring Data Redis, or JPA/Postgres) behind the existing `ConversationStore`
SPI. You keep the entire pure core (§4a) byte-for-byte; you re-implement the Engine
SPI (§4c) as Spring beans; and for chat/tools/vectors/FSM you may *swap* the project's
own implementations for Spring AI / Spring StateMachine equivalents to inherit the
ecosystem (Micrometer tracing, Boot autoconfig, actuator health).

**Keep:** ConversationStore SPI, TwoTierRetriever, ToolExecutor, A2A protocol types,
ReAct/TurnBrain logic, RAG pipeline logic.
**Swap (optional):** ChatConnection→Spring AI `ChatClient`; VectorStore→Spring AI
`VectorStore`; agent FSM→Spring StateMachine; CircuitBreaker/ResilientA2AClient→
Resilience4j annotations.
**Drop:** event-time/windows, native CEP, checkpoint-managed keyed state.

## 2. Capability mapping (C1..C12 — the §6 Spring column)

| Cap | §6 | Spring mechanism (concrete) |
|----|:--:|------------------------------|
| **C1** keyed durable state | **X** | **External.** `ConversationStore` backed by Spring Data Redis (`RedisTemplate`/`ReactiveRedisTemplate`) or JPA/Postgres. Spring AI `ChatMemory` (`MessageWindowChatMemory` + `JdbcChatMemoryRepository`/`CassandraChatMemoryRepository`) can *be* the transcript tier. No checkpoint-managed state — durability = the store's durability. |
| **C2** per-key ordered processing | **L** | Kafka partition assignment gives single-consumer-per-partition; key the producer on `conversationId` so one consumer instance owns a conversation. Within a JVM, serialize per-key work with a striped lock or a per-key single-thread executor. *Not* an engine primitive — an idiom you enforce. |
| **C3** fault tolerance / EOS | **X** | Broker (Kafka) + external store. Spring Cloud Stream Kafka binder supports `transaction` for producer EOS; consumers lean on **idempotency** + the ConversationStore as source of truth (essence §8). At-least-once + idempotent tools is the realistic posture. |
| **C4** async I/O | **L** | Reactor (`Mono`/`Flux`) is native across WebFlux + Spring AI's reactive `ChatClient`; `@Async` thread pools or **Java 21 virtual threads** (`spring.threads.virtual.enabled=true`) for blocking calls. Bounded in-flight via `Flux.flatMap(fn, concurrency)` or a `Semaphore`. |
| **C5** backpressure | **L** | Reactor backpressure end-to-end on the WebFlux/`Flux` path; Kafka binder consumer `max.poll.records` + pause/resume for the messaging path. |
| **C6** connectors | **N** | **Spring Cloud Stream** binders (Kafka, RabbitMQ, Pulsar) as functional `Supplier`/`Function`/`Consumer` beans. `Channel<T>` maps to a binding. First-class. |
| **C7** side outputs | **L** | Multiple output bindings (`Function<In, Tuple2<...>>` → multiple destinations) or Spring Integration `PublishSubscribeChannel` / `WireTap`. The debug stream and tool-invocation channel are extra channels. |
| **C8** broadcast state | **L** | Control-plane directives via a compacted Kafka topic consumed into a shared `@Component` (an in-JVM map refreshed on message), or Spring Cloud Config / `@RefreshScope`. No `GlobalKTable` primitive but the pattern is straightforward. |
| **C9** event-time / windows | **L** (effectively —) | No native windowing. You'd reach for Kafka Streams *under* Spring (binder supports it) or do micro-batch aggregation by hand. For the agentic core this is rarely needed; the streaming-analytics flows don't port. |
| **C10** CEP | **L** | No native CEP. The agent *phase* FSM is far better served by **Spring StateMachine** (declarative states + transitions + guards + actions); genuine multi-event pattern detection would be hand-rolled or delegated to Kafka Streams. |
| **C11** distributed scale | **N** | Spring Boot on Kubernetes; scale = replicas + Kafka partitions. Mature. |
| **C12** topology builder | **L** | **Spring Integration** `IntegrationFlow` DSL *is* a topology builder (channels + endpoints), and the bean graph wires the rest. It's a message-flow topology, not a dataflow-operator graph, but it expresses router→path→verifier directly. |

The honest read of the matrix: **C1 and C2 — the heart (§3) — are not native.** That
is the whole story. Spring hosts the essence well *because the pure core was already
SPI-shaped*, but the "durable thing per key, processed in order" substrate is bolted
on from Kafka + Redis/JPA rather than given by the runtime.

## 3. The core abstractions on Spring

### 3a. The agent as a per-conversation processor (C1+C2)

Flink gives you `keyBy(conversationId).process(...)` and a checkpointed `ValueState`.
On Spring the equivalent is: a Kafka-keyed consumer (single-writer per partition) +
an external `ConversationStore` for the durable per-key state. The `AgentLogic` from
the Engine SPI (§4c) is a plain bean.

```java
@Component
public class AgentConsumer {

  private final ConversationStore store;     // §4a pure SPI — Redis/JPA-backed bean
  private final TurnBrain brain;             // §4a pure ReAct loop, unchanged
  private final StreamBridge out;            // Spring Cloud Stream programmatic send
  private final PerKeyExecutor keyed;        // single-writer-per-conversation guard

  // Spring Cloud Stream functional binding: turns-in -> consumed here.
  @Bean
  public Consumer<Message<AgentEvent>> turns() {
    return msg -> {
      AgentEvent ev = msg.getPayload();
      String convId = ev.getCorrelationId();           // the C2 key
      keyed.runFor(convId, () -> {                      // serialize per conversation
        store.append(convId, ChatMessage.user(ev.text()));
        TurnResult r = brain.run(ev, store.recent(convId, 20)); // §4a logic
        store.append(convId, ChatMessage.assistant(r.text()));
        out.send("responses-out-0", r.toEvent());       // emit
      });
    };
  }
}
```

`PerKeyExecutor` is the C2 idiom: a bounded set of single-thread executors striped by
`convId.hashCode()`, so concurrent turns of the *same* conversation never interleave,
matching Flink's per-key ordering. Combined with Kafka keying on `conversationId`
(so the same conversation lands on the same consumer instance), this reproduces
single-writer-per-conversation without `keyBy`.

### 3b. ConversationStore (C1, external)

The SPI is already Flink-free (`get/append/history/attributes`, keyed by string). A
Spring backend is a thin `@Component`. Two natural choices:

```java
@Component
public class RedisConversationStore implements ConversationStore {
  private final ReactiveRedisTemplate<String, ChatMessage> tmpl;

  @Override public void append(String convId, ChatMessage m) {
    if (convId == null) return;
    tmpl.opsForList().rightPush(key(convId), m).block();         // ordered transcript
    tmpl.expire(key(convId), Duration.ofHours(24)).subscribe();  // TTL = short-term tier
  }
  @Override public List<ChatMessage> history(String convId) {
    return tmpl.opsForList().range(key(convId), 0, -1).collectList().block();
  }
  @Override public void putAttribute(String c, String k, String v) {
    tmpl.opsForHash().put(attrKey(c), k, v).block();             // scalar workflow attrs
  }
  // associateUser/conversationsForUser -> a Redis set per userId; clear -> DEL keys.
  private String key(String c){ return "conv:"+c+":msgs"; }
  private String attrKey(String c){ return "conv:"+c+":attrs"; }
}
```

Equivalently a JPA `@Entity Message(convId, seq, role, content)` + repository, or —
if you adopt Spring AI — `MessageWindowChatMemory` over a `JdbcChatMemoryRepository`
*is* the transcript tier, and you write a thin adapter from `ConversationStore` onto
it so the rest of the core is unaware.

### 3c. Tools (C7-adjacent; pure)

`ToolExecutor` (`Map<String,Object> → CompletableFuture<Object>`) is unchanged. On
Spring you can keep `ToolRegistry`, *or* expose tools to a Spring AI `ChatClient` so
the model does the calling:

```java
// Keep the pure executor...
public class BalanceTool implements ToolExecutor {
  public CompletableFuture<Object> execute(Map<String,Object> p) {
    return cs.balance((String) p.get("accountId"));
  }
  public String getToolId() { return "account.balance"; }
  public String getDescription() { return "Look up an account balance"; }
}

// ...and bridge it to Spring AI function-calling (model-selectable) with one adapter:
ToolCallback cb = FunctionToolCallback.builder("account.balance",
        (BalanceArgs a) -> tool.execute(Map.of("accountId", a.accountId())).join())
    .description("Look up an account balance")
    .inputType(BalanceArgs.class)
    .build();
String answer = chatClient.prompt(userText).toolCallbacks(cb).call().content();
```

So `ToolRegistry`/`@Tool` discovery becomes Spring AI `ToolCallback`s; the executor
bodies (the real work) stay in the pure core.

### 3d. Async, retries, A2A (C4)

The A2A protocol types + `A2AClient` SPI are pure and stay. The project deliberately
**hand-rolled** `CircuitBreaker` + `ResilientA2AClient` (retry/backoff/jitter/deadline)
to avoid a dependency. On Spring you would idiomatically **adopt Resilience4j** — its
`@CircuitBreaker`/`@Retry`/`@TimeLimiter` annotations (or the functional decorators)
do exactly what `ResilientA2AClient.guarded(...)` does, with metrics wired into
Micrometer for free:

```java
@Component
public class SpringA2AClient {
  private final WebClient web;                  // replaces SdkA2AClient transport
  @Retry(name = "a2a")                          // exp backoff + jitter via config
  @CircuitBreaker(name = "a2a", fallbackMethod = "fail") // per-peer breaker
  @TimeLimiter(name = "a2a")                     // request deadline
  public CompletableFuture<A2ATask> send(RemoteAgentSpec peer, A2AMessage m) {
    return web.post().uri(peer.url()).bodyValue(jsonRpc("message/send", m))
        .retrieve().bodyToMono(A2ATask.class).toFuture();
  }
  CompletableFuture<A2ATask> fail(RemoteAgentSpec p, A2AMessage m, Throwable t) {
    return CompletableFuture.failedFuture(new A2AClientException("peer down: "+p.name(), t));
  }
}
```

**Trade noted:** you trade ~270 lines of self-contained, dependency-free
`ResilientA2AClient`+`CircuitBreaker` for a battle-tested library with config-driven
policy and observability — the right call *on Spring*, where Resilience4j is already
in the room. Keep the SPI seam (`A2AClient`) so either implementation drops in.

### 3e. The routed graph as a Spring Integration flow (C12)

This is where Spring shines. `router → path → verifier` (BankingAgentGraph) maps onto
EIP almost 1:1: a **Content-Based Router** = our `BankingRouterFunction`; **channels**
= the paths; **service activators** = the path brains; an **aggregator/filter** = the
verifier.

```java
@Bean
public IntegrationFlow bankingFlow(BankingRouter router, /* path beans */, Verifier v) {
  return IntegrationFlow.from("requests.in")            // Kafka inbound (Cloud Stream)
      .enrichHeaders(h -> h.headerExpression("convId", "payload.contextId"))
      .<A2ARequest, BankingPath>route(router::classify, // CBR = the router (rule-based)
          mapping -> mapping
              .channelMapping(BankingPath.TRANSFER,    "path.transfer")
              .channelMapping(BankingPath.BALANCE,     "path.balance")
              .channelMapping(BankingPath.REFUSE,      "path.refuse"))
      .get();
}

@Bean public IntegrationFlow transferPath(TransferBrain brain) {
  return IntegrationFlow.from("path.transfer")
      .handle((A2ARequest r, MessageHeaders h) -> brain.runTurn(r))  // service activator
      .channel("verify.in").get();
}
// ...balancePath, refusePath identical shape, each its own channel + brain...

@Bean public IntegrationFlow verifyFlow(BankingVerifier verifier) {
  return IntegrationFlow.from("verify.in")
      .handle((BankingTurn t, MessageHeaders h) -> verifier.advance(t))  // verifier
      .handle((A2AResponse resp, MessageHeaders h) -> resp)
      .channel("responses.out").get();                 // Kafka outbound
}
```

Keying by `convId` (header) on the Kafka bindings preserves per-conversation ordering
across the whole flow — the EIP analog of the Flink graph's "all operators keyed by
contextId." Cross-turn chaining (multi-step) lives in the shared `ConversationStore`
(transcript + a `phase` attribute), exactly as the Flink version uses `PhaseStore`.

### 3f. The agent phase FSM (C10 → Spring StateMachine)

`AgentStateMachine` (INITIALIZED→VALIDATING→EXECUTING→SUPERVISOR_REVIEW→COMPLETED,
with CORRECTING/FAILED/COMPENSATING/PAUSED) is currently compiled to Flink CEP
patterns. On Spring it becomes a **Spring StateMachine** config — a far more natural
home, since it's a workflow FSM, not event-pattern detection:

```java
@Configuration @EnableStateMachineFactory
public class AgentFsmConfig extends StateMachineConfigurerAdapter<AgentState, AgentEventType> {
  @Override public void configure(StateMachineStateConfigurer<AgentState, AgentEventType> s) throws Exception {
    s.withStates().initial(AgentState.INITIALIZED)
        .states(EnumSet.allOf(AgentState.class))
        .end(AgentState.COMPLETED).end(AgentState.FAILED).end(AgentState.COMPENSATED);
  }
  @Override public void configure(StateMachineTransitionConfigurer<AgentState, AgentEventType> t) throws Exception {
    t.withExternal().source(VALIDATING).target(EXECUTING).event(VALIDATION_PASSED)
         .guard(ctx -> score(ctx) > 0.8)                 // == AgentTransition.when(...)
         .action(ctx -> log("validation passed"))        // == AgentTransition.action(...)
     .and().withExternal().source(VALIDATING).target(CORRECTING).event(VALIDATION_FAILED)
     .and().withExternal().source(EXECUTING).target(SUPERVISOR_REVIEW).event(REVIEW_REQUESTED)
     .and().withExternal().source(EXECUTING).target(FAILED).event(MAX_ITERATIONS_REACHED)
     .and().withExternal().source(FAILED).target(COMPENSATING).event(COMPENSATION_TRIGGERED);
    // timeouts: StateMachine timer triggers (per AgentState.getTypicalTimeoutSeconds()).
  }
}
```

The `AgentState` enum, `AgentTransition` (guard `when` + `action`), and timeout
durations map directly onto Spring StateMachine's states/transitions/guards/actions
and timed triggers. Persist the machine (`StateMachinePersister` → Redis/JPA) keyed by
`conversationId` to survive restarts — the C1 story again.

### 3g. RAG / retrieval (pure)

`TwoTierRetriever` (hot in-window + cold durable, dedupe-by-id, degrade-on-failure) is
pure and unchanged. The *cold* `ColdSearch` seam is implemented over a Spring AI
`VectorStore` (`PgVectorStore`, `RedisVectorStore`, `QdrantVectorStore` — all Boot
auto-configured); the *hot* tier stays the in-process index. Embeddings come from a
Spring AI `EmbeddingModel` behind the `Embedding` SPI. Ingestion/chunking logic is
unchanged; only the sink (`VectorStore.add(documents)`) is Spring AI.

### 3h. Inbound proxy (the A2A gateway)

The Quarkus inbound gateway becomes a Spring MVC/WebFlux `@RestController` (+ SSE) that
speaks A2A JSON-RPC and drops requests onto the `requests.in` channel via `StreamBridge`
— the same bridge pattern as `A2ABridge`, expressed as a controller publishing to Kafka.

## 4. Worked example — banking router→path→verifier

End-to-end on Spring, faithful to `BankingAgentGraph`:

1. **Inbound.** `@RestController POST /a2a` (JSON-RPC `message/send`) parses an
   `A2ARequest`, sets header `convId = contextId`, and `streamBridge.send("requests.in", msg)`.
   Kafka keys the record on `convId` → single-writer per conversation (C2).
2. **Router (rule-based, no LLM).** `IntegrationFlow` Content-Based Router calls
   `BankingRouter.classify(req)` → `BankingPath` (screen + classify). Same logic as
   `BankingRouterFunction`, now a `@Component`. Routes to `path.transfer` /
   `path.balance` / `path.refuse` channels.
3. **Path (LLM brain).** The selected path's service-activator runs a `TurnBrain`
   (`ReActTurnBrain`, unchanged): reads `store.recent(convId, 20)`, drives the ReAct
   loop, tools execute via `ToolExecutor` (or Spring AI `ToolCallback`s if model-driven),
   appends the turn to the `ConversationStore`. Slow model calls run on virtual threads
   or Reactor so the consumer isn't blocked (C4). `REFUSE` is a pass-through with no brain.
4. **Verifier (rule-based).** `verifyFlow` service-activator runs `BankingVerifier`:
   reads/advances the `phase` attribute in the `ConversationStore` (the cross-turn
   `BankingPhase`), validates the path output, builds the `A2AResponse`.
5. **A2A delegation.** If the personal agent must consult the CS agent, the path or
   verifier calls `SpringA2AClient.send(csPeer, msg)` (§3d) — Resilience4j wraps
   retry/breaker/deadline; the remote `contextId` continuity is persisted in the
   `ConversationStore` attribute (the Spring analog of `A2AStep.applyToStateful`'s
   keyed pre/post operators sharing the store across the async hop).
6. **Outbound.** Verifier emits to `responses.out`; the Kafka binder publishes; the
   controller's SSE stream (or a response topic the controller tails) returns it to the
   caller.

One turn = Router → Path → Verifier (a clean flow, no cycle). Multi-step *chaining* is
across turns via the shared store — identical to the Flink design, different substrate.

## 5. What doesn't fit (honest gaps)

- **No checkpoint-managed keyed state (C1).** Flink snapshots conversation state
  atomically with stream position. Spring has nothing equivalent: durability is the
  store's (Redis/Postgres), recovery is "replay from Kafka offset + idempotency + the
  ConversationStore as source of truth." Acceptable, but it's *your* correctness
  argument, not the runtime's. **Don't** pretend an in-memory `@Component` map is
  durable state.
- **No `keyBy` (C2).** Single-writer-per-conversation is *convention* (Kafka keying +
  per-key executor), not enforced by the engine. A misconfigured partition count or a
  rebalance mid-turn can break the invariant; design tools to be idempotent.
- **Event-time, watermarks, windowing (C9)** — absent. The streaming-analytics /
  feature-aggregation flows do not port; push them to Kafka Streams (binder-hosted) if
  you truly need them. Don't emulate windows by hand in Integration flows.
- **Native CEP (C10)** — absent. The *phase FSM* is fine (Spring StateMachine); genuine
  multi-event temporal pattern detection is not Spring's job.
- **Exactly-once across tool side effects (C3)** — at-least-once is the realistic
  posture; lean on idempotency. Kafka transactional producers help for the
  emit-once-downstream case but not for external tool calls.
- **Backpressure is partial (C5)** — clean on the Reactor path, coarse (poll-size /
  pause-resume) on the Kafka consumer path; protect the model endpoint with an explicit
  `Semaphore`/bulkhead rather than relying on flow control.

## 6. When to choose Spring

Choose Spring when the *organization* is already a Spring shop and the agentic system
is one service among many enterprise services — when you value Boot autoconfiguration,
actuator health, Micrometer/OpenTelemetry tracing, Spring Security, and the ability to
hand the agent FSM to **Spring StateMachine**, chat/tools/vectors to **Spring AI**, and
the routed graph to **Spring Integration** without writing those primitives yourself.
It is an excellent fit for **request/response and multi-turn conversational agents** at
moderate scale, the **A2A inbound gateway**, **tool-rich** and **RAG-backed** agents,
and any deployment where "fits the existing JVM/Spring platform" outweighs "best
streaming runtime."

Do **not** choose Spring as the home for the **live keyed-stateful streaming core** if
you need real checkpointed state, event-time, windowing, or CEP — that is what Flink
(or Kafka Streams, rank 2) is for. The honest framing (§7): Spring gives you
**messaging + integration + DI + Spring AI**, and you assemble durability from
**Kafka + Redis/JPA**. You reuse the pure Java core wholesale and, uniquely among the
JVM targets, you get the *option* to swap several core pieces for mature Spring
ecosystem equivalents — at the cost of those new dependencies.
