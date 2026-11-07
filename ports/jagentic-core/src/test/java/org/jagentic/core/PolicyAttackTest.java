package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

/** Attacks on the tool boundary that found something. Each test failed before its fix. */
class PolicyAttackTest {

  /** An OPA, or anything else outside this process: it allows whatever it is asked about. */
  private static final ToolPolicy OUTSIDE = new ToolPolicy() {
    @Override
    public Decision decide(Request request) {
      return Decision.ALLOW;
    }

    @Override
    public boolean outsideThisProcess() {
      return true;
    }
  };

  /**
   * With a policy outside the process, a call already begun is not asked about again, and the
   * document's own policy was skipped with it. A re-run whose journal is gone asks the model again;
   * the model can now want another tool at the same index, and that tool ran without any policy:
   * the document's deny-by-default included.
   */
  @Test
  void aReRunCannotCallAToolTheDocumentDeniesAtAnIndexAnotherToolBegan() {
    List<String> called = new ArrayList<>();
    ToolRegistry tools = new ToolRegistry()
        .registerKeyed("look", "", null, ToolEffect.IDEMPOTENT, (a, c) -> {
          called.add("look");
          return "ok";
        })
        .registerKeyed("wire", "", null, ToolEffect.IDEMPOTENT, (a, c) -> {
          called.add("wire");
          return "sent";
        });
    ConversationLog log = new ConversationLog.InMemory();
    ToolPolicy policy = ToolPolicy.of(Map.of("main", Set.of("look")), Set.of()).and(OUTSIDE);

    AgentContext first = new AgentContext("c", "t", "u", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), tools, null, log, Policies.DEFAULTS);
    first.policy = policy;
    first.path = "main";
    first.callTool("look", Map.of());
    assertEquals(List.of("look"), called);

    // The process is gone, and the journal with it; the log kept the turn.
    AgentContext rerun = new AgentContext("c", "t", "u", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), tools, null, log, Policies.DEFAULTS);
    rerun.policy = policy;
    rerun.path = "main";
    rerun.replayOver(log.events("c"));
    assertThrows(RuntimeException.class, () -> rerun.callTool("wire", Map.of()));
    assertEquals(List.of("look"), called, "wire is not in the path's tools and must never run");
  }
}
