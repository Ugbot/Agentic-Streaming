# A2A (Agent2Agent) support

Agentic-Flink speaks the **A2A protocol v1.0** in both directions:

- **Outbound** — a Flink agent calls a remote A2A agent as a step in its workflow, either as an
  LLM-selectable tool or as an explicit, deterministic pipeline step.
- **Inbound** — a standalone Quarkus **gateway** exposes the agents running in a Flink job to any
  external A2A client (Agent Card discovery + JSON-RPC/SSE, gRPC, and HTTP/REST bindings).

The two sides are connected to the Flink job by a pluggable **bridge** (`inproc` / `zeromq` /
`redis`). A2A is built on the official [`a2a-java` SDK](https://github.com/a2aproject/a2a-java)
(`io.github.a2asdk`, pinned `1.0.0.Alpha3`), isolated behind our own `A2AClient` SPI so all SDK
usage is confined to one adapter and the gateway.

```
 EXTERNAL A2A CLIENT                                   REMOTE A2A AGENT (peer)
        │ JSON-RPC/gRPC/REST + SSE                              ▲
        ▼                                                       │ a2a-java SDK client (outbound)
 ┌──────────────────────┐      bridge (Channel)        ┌────────┴───────────────────────┐
 │  Quarkus A2A Gateway  │  inproc / zeromq / redis     │       Flink Agent Job          │
 │  AgentCard + Executor │ ── A2ARequest ─────────────▶ │  A2AToolExecutor (tool)        │
 │  SSE + push           │ ◀─ A2AResponse ───────────── │  A2AStep (explicit step)       │
 └──────────────────────┘                              └────────────────────────────────┘
```

The SDK is an **optional** dependency of the core library; `mvn clean test` builds and passes
without it. The outbound path needs `a2a-java-sdk-client` on the runtime classpath; the gateway
module pulls everything it needs.

---

## Outbound: chaining A2A steps into workflows

### As a tool (LLM-selected)

`AgentBuilder.withRemoteAgent(...)` registers a peer as a synthetic tool `a2a:<name>` and adds a
generated `Skill` so the model knows when to delegate:

```java
RemoteAgentSpec planner =
    RemoteAgentSpec.card("planner", "https://planner.example.com/.well-known/agent-card.json");
RemoteAgentSpec quoter =
    RemoteAgentSpec.endpoint("quoter", "https://quoter.example.com/a2a", A2ATransport.JSONRPC);

Agent agent =
    Agent.builder()
        .withId("coordinator")
        .withSystemPrompt("Coordinate planning and pricing.")
        .withRemoteAgent(planner, quoter)         // -> tools a2a:planner, a2a:quoter
        .build();

// At job-assembly time, register the executors into the tool registry:
ToolRegistry.ToolRegistryBuilder b = ToolRegistry.builder();
A2AToolRegistry.registerInto(b, agent);
ToolRegistry tools = b.build();
```

When the LLM calls `a2a:planner`, the `A2AToolExecutor` builds an A2A message from the tool
parameters (`input`/`prompt` → text part, `data` → data part), sends it, awaits a terminal task
(streaming where supported, otherwise `message/send` + `tasks/get` polling), and returns the peer's
artifacts as the tool result. Because `ToolExecutor.execute` returns a `CompletableFuture`, this
slots straight into the async agent loop.

### As an explicit pipeline step (deterministic)

When delegation is part of the topology rather than a model choice, use `A2AStep`:

```java
A2AStep enrich =
    A2AStep.builder()
        .withName("enrich")
        .withSpec(RemoteAgentSpec.endpoint("enricher", "https://enricher/a2a", A2ATransport.JSONRPC))
        .withOutputKey("a2a.enrich")
        .build();

DataStream<AgentEvent> out = enrich.applyTo(inputStream);   // keyBy(contextId) -> delegate -> emit
```

`A2AStep` keys the stream by A2A `contextId` (default: event `correlationId`/`flowId`) and keeps the
remote context in Flink state for conversation continuity. Record steps on a job with
`AgentJobBuilder.withA2AStep(...)`.

### Picking a client

Outbound uses the official SDK via `SdkA2AClient` (JSON-RPC binding), discovered through
`A2AClientFactory.discovering()` (ServiceLoader). Override with
`AgentBuilder.withA2AClientFactory(...)` — tests pass an in-memory fake.

---

## Inbound: the Quarkus gateway

A standalone module under `a2a-gateway/` (built separately — see its `README.md`). It serves the
Agent Card at `/.well-known/agent-card.json` and all three transport bindings, bridging each request
into the Flink job and driving SSE + push from the job's responses.

```bash
mvn -q install -DskipTests                 # install core
mvn -f a2a-gateway/pom.xml package         # build the gateway
java -jar a2a-gateway/target/quarkus-app/quarkus-run.jar
curl http://localhost:9999/.well-known/agent-card.json
```

The Flink job participates by unioning the bridge request channel into its agent input and writing
results to the bridge response sink:

```java
A2ABridge bridge = A2ABridgeFactory.create(config);            // zeromq by default
DataStream<A2ARequest> requests = bridge.requestChannel().open(env);
DataStream<A2AResponse> responses = /* run agents on requests, emit A2AResponse */;
responses.addSink(bridge.responseSink());
```

---

## Bridge transports

| Transport | When | Notes |
|---|---|---|
| `inproc` | embedded mode / tests | gateway + minicluster in one JVM; shared queues, no broker |
| `zeromq` | localhost / single host (**default**) | PULL/PUSH sockets, no broker; needs JeroMQ |
| `redis`  | distributed-light | Redis pub/sub; needs Jedis + a Redis server |

Envelopes (`A2ARequest`/`A2AResponse`) cross both Flink operators and the wire as JSON via the
shared `A2AJson` mapper (`A2AJsonTypeInfo` makes them first-class Flink stream elements). The
gateway-side connector's `awaitFinal` buffers final responses, so a publish-then-await never misses
a fast response.

---

## Task persistence

`A2ATaskStore` persists A2A task lifecycle + push-notification configs gateway-side. Backends:
`memory` (default/embedded), `postgres` (`MERGE` upserts, JSON columns), `redis` (optional Jedis).
Selected via `a2a.task.store`; discovered through `A2ATaskStoreFactory` / ServiceLoader.

---

## Configuration

All keys resolve via `AgenticFlinkConfig` (explicit > `AGENTIC_FLINK_*` env > system props >
default). See `config/ConfigKeys.java`.

| Key | Default | Meaning |
|---|---|---|
| `a2a.protocol.version` | `1.0` | advertised protocol version |
| `a2a.client.default.transport` | `JSONRPC` | outbound default binding |
| `a2a.gateway.public.url` | `http://localhost:9999` | Agent Card `url` |
| `a2a.bridge.transport` | `zeromq` | `inproc` / `zeromq` / `redis` |
| `a2a.bridge.request.endpoint` | `tcp://127.0.0.1:5760` | gateway → job |
| `a2a.bridge.response.endpoint` | `tcp://127.0.0.1:5761` | job → gateway |
| `a2a.task.store` | `memory` | gateway task store backend |

---

## Versioning note

The A2A 1.0 protocol line is published on Maven Central only as alphas (`1.0.0.Alpha3`); the newest
stable `Final` is `0.3.x`. We pin `1.0.0.Alpha3` to match the v1.0 spec and isolate all SDK types
behind the `A2AClient` SPI / gateway, so a later `Final` bump (or 0.3 fallback) is contained to
`SdkA2AClient` + the gateway pom. The SDK `groupId` is `io.github.a2asdk` (not the
`org.a2aproject.sdk` some 2026 blog posts mention).
