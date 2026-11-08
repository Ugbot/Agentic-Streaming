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
   * An approval named the tool and its index, not what the call would do. A resume after the
   * journal is gone asks the model again, and the call it then makes at that index ran on the
   * approval given for another: an approver who allowed 10 sent 10000.
   */
  @Test
  void anApprovalIsForTheCallTheApproverWasShown() {
    List<Object> sent = new ArrayList<>();
    ToolRegistry tools = new ToolRegistry().registerKeyed("wire", "", null, ToolEffect.NON_IDEMPOTENT, (a, c) -> {
      sent.add(a.get("amount"));
      return "sent";
    });
    int[] amount = {10};
    Brain brain = (text, ctx) -> String.valueOf(ctx.callTool("wire", Map.of("amount", amount[0])));
    RoutedGraph graph = new RoutedGraph((e, c) -> "main", Map.of("main", new Agent("main", "p", brain)), null,
        List.of(), List.of())
        .withToolPolicy(ToolPolicy.of(Map.of("main", Set.of("wire")), Set.of(), Set.of("wire"), Set.of()))
        .withApprovers(Approvers.ofRoles("approver"));
    ConversationLog log = new ConversationLog.InMemory();
    LocalRuntime first = new LocalRuntime(graph, new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        tools, null, log).withJournal(new EffectJournal.InMemory());
    assertEquals(TurnStatus.SUSPENDED, first.submit(Event.turn("c", "t", "u", "pay 10")).status);

    amount[0] = 10_000; // the model, asked again, wants something else
    LocalRuntime restarted = new LocalRuntime(graph, new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        tools, null, log).withJournal(new EffectJournal.InMemory());
    TurnResult r = restarted.submit(new Event("c", "t", "ops", "", Map.of(), Map.of("approved", true),
        Principal.of("ops", null, "approver")));
    assertEquals(List.of(), sent, "nobody approved 10000");
    assertEquals(TurnStatus.SUSPENDED, r.status, "the new call waits for its own approval");
  }

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
