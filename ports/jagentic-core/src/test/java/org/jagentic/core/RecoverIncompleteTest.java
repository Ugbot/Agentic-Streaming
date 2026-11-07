package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.jagentic.core.pipeline.GraphBuilder;
import org.junit.jupiter.api.Test;

/** A restart finishes what the log shows as started, with nobody redelivering it. */
class RecoverIncompleteTest {

  private static final class Died extends Error {
    private static final long serialVersionUID = 1L;
  }

  @Test
  void anIncompleteSagaIsFinishedFromTheLogWithoutRepeatingAStep() {
    Map<String, Object> workflow = Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
            "paths", Map.of("p", Map.of("brain", "rule", "tools", List.of("first", "second"))), "verifier", Map.of("kind", "none")),
        "tools", List.of(Map.of("id", "first", "kind", "counting", "effect", "idempotent", "x-world", "recover"),
            Map.of("id", "second", "kind", "counting", "effect", "idempotent", "x-world", "recover")),
        "saga", Map.of("steps", List.of(Map.of("tool", "first"), Map.of("tool", "second"))));
    GraphBuilder.Built built = GraphBuilder.build(workflow, null);
    ConversationLog log = new ConversationLog.InMemory();
    LocalRuntime dying = runtime(built, log).withProbe(new TurnProbe() {
      private static final long serialVersionUID = 1L;

      @Override
      public void beforeCall(String toolId) {
        if ("second".equals(toolId)) {
          throw new Died();
        }
      }
    });
    assertThrows(Died.class, () -> dying.submit(Event.turn("c", "t", "alice", "go")));

    List<TurnResult> recovered = runtime(built, log).recoverIncomplete();

    assertEquals(1, recovered.size());
    TurnResult r = recovered.get(0);
    assertEquals(TurnStatus.COMPLETED, r.status);
    assertEquals(1, r.calls.get(0).result(), "first ran once");
    assertEquals(1, r.calls.get(1).result(), "second ran once");
    assertEquals(List.of(), runtime(built, log).recoverIncomplete());
  }

  private static LocalRuntime runtime(GraphBuilder.Built built, ConversationLog log) {
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), built.retriever(), log).withJournal(built.journal());
  }
}
