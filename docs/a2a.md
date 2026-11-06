# A2A (Agent2Agent) support

The Flink framework's legacy DSL has outbound A2A: a Flink agent calls a remote A2A agent as a step in
its workflow, either as an LLM-selectable tool or as an explicit, deterministic pipeline step.

The inbound Quarkus gateway moved out of this repository; the code as it stood is at
[tag `pre-cornerstone`](https://github.com/Ugbot/Agentic-Streaming/tree/pre-cornerstone/a2a-gateway). The `a2a` package still holds the **bridge**
(`inproc` / `zeromq` / `redis`) a gateway connects through. A2A is built on the official [`a2a-java` SDK](https://github.com/a2aproject/a2a-java)
(`io.github.a2asdk`, pinned `1.0.0.Alpha3`), isolated behind our own `A2AClient` SPI so all SDK
usage is confined to one adapter.

The SDK is an **optional** dependency of the core library; `mvn clean test` builds and passes
without it. The outbound path needs `a2a-java-sdk-client` on the runtime classpath.

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
`AgentBuilder.withA2AClientFactory(...)`, tests pass an in-memory fake.

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
| `a2a.gateway.public.url` | `http://localhost:9999` | Agent Card `url` and its only interface (JSON-RPC) |
| `a2a.gateway.agent.skills` | one generic skill | Agent Card `skills`, `id:name:description,...` |
| `a2a.bridge.transport` | `zeromq` | `inproc` / `zeromq` / `redis` |
| `a2a.bridge.request.endpoint` | `tcp://127.0.0.1:5760` | gateway → job |
| `a2a.bridge.response.endpoint` | `tcp://127.0.0.1:5761` | job → gateway |
| `a2a.task.store` | `memory` | gateway task store backend |

---

## Versioning note

`pom.xml` pins the SDK at `io.github.a2asdk` `1.0.0.Alpha3`, an alpha. Upstream has since moved to the
`org.a2aproject.sdk` coordinates. All SDK types sit behind the `A2AClient` SPI, so an upgrade is
contained to `SdkA2AClient`.
