# agentic-quarkus

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

A minimal, standalone Quarkus (reactive) port of the Agentic-Flink essence onto the
shared pure-Java core `org.jagentic:jagentic-core:1.0.0-SNAPSHOT`. **no Flink dependency**. It maps the
engine SPIs from [`docs/portability/quarkus.md`](../../../docs/portability/quarkus.md) onto
idiomatic Quarkus: the engine-agnostic `RoutedGraph` (`Banking.buildGraph()`,
`router -> path -> verifier`) runs verbatim; **C1 durable keyed state** comes from the
`ConversationStore`/`KeyedStateStore` SPIs (in-memory here, swappable for Redis/Fluss);
**C2 single-writer-per-conversation** comes from Kafka partition assignment (the `requests`
topic is keyed by `conversationId`, so one partition = one consumer = one writer);
**C4/C5 async + backpressure** come from Mutiny `Uni` and SmallRye Reactive Messaging.
`AgentResource` is the synchronous inbound REST edge returning a `Uni`; `BankingStream` is
the `@Incoming("requests")`/`@Outgoing("replies")` streaming agent over Kafka. This module
**complements** the existing `a2a-gateway/` Quarkus module (the inbound A2A/RAG proxy), it
is a separate, self-contained demonstration of the agent-on-Quarkus pattern and does not
touch that gateway. Both edges build the core `Event` through the adapter's `EventBuilder`
(named fields), so no call site depends on the positional order of `userId` and `text` in
the core constructors, and neither edge writes to the `ConversationStore` itself: the core
appends the user and assistant messages of a completed turn, so one turn adds exactly two
transcript messages.

## Build, test and package

```
./mvnw -f reactor/pom.xml -DskipTests install      # installs jagentic-core 1.0.0-SNAPSHOT into ~/.m2
./mvnw -f ports/experimental/quarkus/pom.xml test    # @QuarkusTest suites, no broker needed
./mvnw -f ports/experimental/quarkus/pom.xml verify  # plus the Quarkus build and the packaging check
```

The module is not part of the reactor, so it carries its own enforcer rule (JDK 21 or newer,
Maven 3.9 or newer). `quarkus-maven-plugin` builds the runnable application into
`target/quarkus-app/` in the `package` phase, and an enforcer `requireFilesExist` rule in the
`verify` phase fails the build unless `target/quarkus-app/quarkus-run.jar` and the bundled
`jagentic-core` jar exist. Run the packaged application with
`java -jar ports/experimental/quarkus/target/quarkus-app/quarkus-run.jar`.

Tests boot the real application with `@QuarkusTest`. `AgentResourceTest` posts randomized
banking turns to `POST /agent` with RestAssured and checks the route, tool call and reply, that
each turn appends exactly one user and one assistant message, and the `EventBuilder` field
mapping. `BankingStreamTest` pushes requests into the `requests` channel and reads the replies
from the `replies` channel; the `%test` profile in `application.properties` binds both channels
to the SmallRye in-memory connector, so the same `BankingStream` bean runs without Kafka. Outside
the test profile the channels use the Kafka connector as configured below; the `requests` value
deserializer is `AgentRequestDeserializer`, a concrete `ObjectMapperDeserializer<AgentRequest>`
(the abstract class itself cannot be instantiated by the Kafka client), and
`AgentRequestDeserializerTest` checks that the Kafka client can instantiate it and that it
round-trips a request.

## Run

```
./mvnw -f ports/experimental/quarkus/pom.xml quarkus:dev
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
