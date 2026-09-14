# agentic-spring

> Status: experimental adapter, not conformance tested. This adapter predates the `agentic/v1`
> spec, does not run the fixtures under `spec/conformance/v1`, and is not on the acceptance path
> (define a workflow once, select a runtime, get the same observable behavior). It runs the
> banking worked example on its engine and shares the conformance tested core it is built on,
> nothing more. It may be removed. See [`../README.md`](../README.md) for the full list of
> experimental adapters and how each one runs.

The **Spring** port of the Agentic-Flink essence. It reuses the pure-Java
`org.jagentic:jagentic-core:0.1.0` (no Flink dependency) byte-for-byte and supplies the
enterprise wiring: Spring exposes the inbound REST edge and expresses the core
`router -> path -> verifier` graph as a Spring Integration EIP topology. Concretely,
`AgentController` (`POST /agent {conversationId,userId,text}`) builds a per-turn
`AgentContext` over a singleton `ConversationStore.InMemory` + `Banking.retriever()` and
runs `Banking.buildGraph().handle(...)`, returning the verified reply; `RoutedFlow` shows
the equivalent integration wiring, a Content-Based Router (`Banking.router`) dispatching
to per-path channels (`cards|payments|general`), each a service activator that delegates
the turn to the shared `RoutedGraph` and forwards to a final verify endpoint, while
`AgentPhaseFsm` maps the agent phase FSM onto Spring StateMachine (the durable, external
state story replaces Flink's checkpointed keyed state). See
`docs/portability/spring.md` for the full design.

## Build

```
mvn -f ports/experimental/spring/pom.xml compile
```

(Dependencies download online. `jagentic-core` must be installed in the local `~/.m2`.)

## Run

```
mvn -f ports/experimental/spring/pom.xml spring-boot:run
```

Then drive one turn:

```
curl -s -XPOST localhost:8080/agent \
  -H 'Content-Type: application/json' \
  -d '{"conversationId":"c1","userId":"u1","text":"what is my balance?"}'
```

The reply, the path that handled it, and the tool calls made are returned. Repeated calls
with the same `conversationId` resume the conversation (transcript persists in the
`ConversationStore`).
