# agentic-spring

The **Spring** port of the Agentic-Flink essence. It reuses the pure-Java
`org.jagentic:jagentic-core:0.1.0` (no Flink dependency) byte-for-byte and supplies the
enterprise wiring: Spring exposes the inbound REST edge and expresses the core
`router -> path -> verifier` graph as a Spring Integration EIP topology. Concretely,
`AgentController` (`POST /agent {conversationId,userId,text}`) builds a per-turn
`AgentContext` over a singleton `ConversationStore.InMemory` + `Banking.retriever()` and
runs `Banking.buildGraph().handle(...)`, returning the verified reply; `RoutedFlow` shows
the equivalent integration wiring — a Content-Based Router (`Banking.router`) dispatching
to per-path channels (`cards|payments|general`), each a service activator that delegates
the turn to the shared `RoutedGraph` and forwards to a final verify endpoint — while
`AgentPhaseFsm` maps the agent phase FSM onto Spring StateMachine (the durable, external
state story replaces Flink's checkpointed keyed state). See
`docs/portability/spring.md` for the full design.

## Build

```
mvn -f ports/spring/pom.xml compile
```

(Dependencies download online. `jagentic-core` must be installed in the local `~/.m2`.)

## Run

```
mvn -f ports/spring/pom.xml spring-boot:run
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
