# Agentic Flink A2A Gateway

A [Quarkus](https://quarkus.io) service that exposes agents running in an Agentic-Flink job over the
**A2A (Agent2Agent) protocol v1.0**, so any A2A-compliant client can discover and call them.

It serves the Agent Card and the A2A RPC surface (JSON-RPC + SSE primary; gRPC and HTTP/REST
bindings are included via the SDK reference servers) and bridges each request into the running Flink
job over a pluggable transport, holding A2A task lifecycle state on behalf of disconnected callers.

```
 external A2A client ──JSON-RPC/SSE──▶ a2a-gateway (Quarkus) ──A2ABridge──▶ Flink agent job
                                       AgentCard + AgentExecutor   (inproc/zeromq/redis)
```

## How it fits together

- **`AgentCardProducer`** — produces the `AgentCard` served at `/.well-known/agent-card.json`, from
  `GatewayConfig`.
- **`AgenticFlinkAgentExecutor`** — the server-side `AgentExecutor` the SDK reference servers call
  for every `message/send` / `message/stream`. Backs all three transport bindings with one bean.
- **`A2ARequestBridge`** — transport-agnostic core: publishes an `A2ARequest` over the
  `A2AGatewayConnector` and pumps the job's `A2AResponse`s into the SDK `AgentEmitter` (driving SSE
  + push). Unit-tested without booting Quarkus.
- **`BridgeProducer`** — opens the gateway-side `A2ABridge` connector chosen by
  `a2a.bridge.transport` (`inproc` | `zeromq` | `redis`).

The Flink job consumes requests by unioning `bridge.requestChannel()` into its agent input and writes
results to `bridge.responseSink()` (see `docs/a2a.md` and the example).

## Build & run

This module is **not** part of the core Maven reactor (kept separate to avoid converting the root to
`pom` packaging — mirrors how `plugins/flintagents` is excluded). Install the core artifact first,
then build the gateway:

```bash
# 1. install the core library into the local repo
mvn -q install -DskipTests

# 2. build the gateway (from repo root)
mvn -f a2a-gateway/pom.xml package

# 3. run it
java -jar a2a-gateway/target/quarkus-app/quarkus-run.jar
#    or for live reload during development:
mvn -f a2a-gateway/pom.xml quarkus:dev
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
