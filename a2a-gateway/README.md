# Agentic Flink A2A Gateway

A [Quarkus](https://quarkus.io) service that exposes agents running in an Agentic-Flink job over the
**A2A (Agent2Agent) protocol v1.0**, so any A2A-compliant client can discover and call them.

It serves the Agent Card and the A2A JSON-RPC surface (`message/send`, `message/stream` over SSE,
and the task methods) and bridges each request into the running Flink job over a pluggable
transport, holding A2A task lifecycle state on behalf of disconnected callers.

JSON-RPC and SSE are the only inbound A2A transports implemented. There is no gRPC server and no
A2A REST binding in this module: the wire is served by one hand-rolled JAX-RS resource
(`A2AResource`), not by the a2a-java SDK reference servers, because the SDK's JSON-RPC server
registers proto method names instead of the spec's `message/send`. `GatewayConfig.grpcUrl()` is
configuration metadata only and nothing listens on it. The separate `rag/RagResource` is a plain
HTTP resource for RAG ingest and query; it is not an A2A binding.

```
 external A2A client ──JSON-RPC/SSE──▶ a2a-gateway (Quarkus) ──A2ABridge──▶ Flink agent job
                                       AgentCard + A2AResource     (inproc/zeromq/redis)
```

## How it fits together

- **`A2AResource`**, the JAX-RS resource that builds the Agent Card served at
  `/.well-known/agent-card.json` from `GatewayConfig` and handles JSON-RPC `POST /` for
  `message/send` and `message/stream` (SSE).
- **`A2ARequestBridge`**, transport-agnostic core: publishes an `A2ARequest` over the
  `A2AGatewayConnector` and pumps the job's `A2AResponse`s to the `GatewayEmitter` (driving SSE
  and push via `PushDispatcher`). Unit-tested without booting Quarkus.
- **`BridgeProducer`**, opens the gateway-side `A2ABridge` connector chosen by
  `a2a.bridge.transport` (`inproc` | `zeromq` | `redis`).

The Flink job consumes requests by unioning `bridge.requestChannel()` into its agent input and writes
results to `bridge.responseSink()` (see `docs/a2a.md` and the example).

## Build & run

This module is **not** part of the core Maven reactor (kept separate to avoid converting the root to
`pom` packaging, mirrors how `plugins/flintagents` is excluded). Install the core artifact first,
then build the gateway:

```bash
# 1. install jagentic-core, then the core library, into the local repo
./mvnw -q -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -q install -DskipTests

# 2. build the gateway (from repo root)
./mvnw -f a2a-gateway/pom.xml package

# 3. run it
java -jar a2a-gateway/target/quarkus-app/quarkus-run.jar
#    or for live reload during development:
./mvnw -f a2a-gateway/pom.xml quarkus:dev
```

Configuration is read from `AgenticFlinkConfig` (`AGENTIC_FLINK_*` env vars / system properties) and
`application.properties`. Key settings:

| Setting (env var) | Default | Meaning |
|---|---|---|
| `QUARKUS_HTTP_PORT` | `9999` | JSON-RPC + SSE port |
| `AGENTIC_FLINK_A2A_GATEWAY_PUBLIC_URL` | `http://localhost:9999` | Agent Card `url` |
| `AGENTIC_FLINK_A2A_BRIDGE_TRANSPORT` | `zeromq` | `inproc` / `zeromq` / `redis` |
| `AGENTIC_FLINK_A2A_BRIDGE_REQUEST_ENDPOINT` | `tcp://127.0.0.1:5760` | gateway→job |
| `AGENTIC_FLINK_A2A_BRIDGE_RESPONSE_ENDPOINT` | `tcp://127.0.0.1:5761` | job→gateway |
| `AGENTIC_FLINK_A2A_GATEWAY_AGENT_SKILLS` | _(one generic skill)_ | `id:name:desc,...` |

## Verify

```bash
curl http://localhost:9999/.well-known/agent-card.json        # discovery document
```

`A2ARequestBridgeTest` covers the gateway↔Flink bridging deterministically (in-process bridge + a
minicluster echo job) without a Quarkus boot.
