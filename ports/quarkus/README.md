# agentic-quarkus

A minimal, standalone Quarkus (reactive) port of the Agentic-Flink essence onto the
shared pure-Java core `org.jagentic:jagentic-core` — **no Flink dependency**. It maps the
engine SPIs from [`docs/portability/quarkus.md`](../../docs/portability/quarkus.md) onto
idiomatic Quarkus: the engine-agnostic `RoutedGraph` (`Banking.buildGraph()`,
`router -> path -> verifier`) runs verbatim; **C1 durable keyed state** comes from the
`ConversationStore`/`KeyedStateStore` SPIs (in-memory here, swappable for Redis/Fluss);
**C2 single-writer-per-conversation** comes from Kafka partition assignment (the `requests`
topic is keyed by `conversationId`, so one partition = one consumer = one writer);
**C4/C5 async + backpressure** come from Mutiny `Uni` and SmallRye Reactive Messaging.
`AgentResource` is the synchronous inbound REST edge returning a `Uni`; `BankingStream` is
the `@Incoming("requests")`/`@Outgoing("replies")` streaming agent over Kafka. This module
**complements** the existing `a2a-gateway/` Quarkus module (the inbound A2A/RAG proxy) — it
is a separate, self-contained demonstration of the agent-on-Quarkus pattern and does not
touch that gateway.

## Build / compile

```
mvn -f ports/quarkus/pom.xml compile
```

## Run

```
mvn -f ports/quarkus/pom.xml quarkus:dev
```

REST turn (no Kafka required):

```
curl -s -X POST localhost:8080/agent \
  -H 'content-type: application/json' \
  -d '{"conversationId":"c1","userId":"u1","text":"what is my balance"}'
```

Streaming turn: produce a JSON `AgentRequest` to the `agent.requests` topic
(key = `conversationId`); the verified `AgentReply` lands on `agent.replies`. Point
`kafka.bootstrap.servers` at your broker in `application.properties` (defaults to
`localhost:9092`).
