package org.jagentic.core.suspend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.TurnResult;

/** Phase 6: human-in-the-loop suspend/resume. A turn needing approval suspends; resume replays it
 * (approved) or denies; un-actioned suspensions escalate on timeout. */
class HumanGateTest {

  private static LocalRuntime banking() {
    return new LocalRuntime(Banking.buildGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), Banking.defaultTools(), Banking.retriever());
  }

  // approval required when the event carries a "needs_approval" metadata flag
  private static HumanGate gate(SuspensionService s, long timeout) {
    return new HumanGate(banking(), s, e -> e.metadata().containsKey("needs_approval"), timeout);
  }

  private static Event needsApproval() {
    return new Event("c1", "alice", "what is my balance?", Map.of("needs_approval", "true"));
  }

  @Test
  void approvalRequiredTurnSuspends() {
    SuspensionService s = new SuspensionService.InMemory();
    TurnResult r = gate(s, 0).submit(needsApproval(), 0);
    assertEquals("awaiting-approval", r.path);
    assertFalse(r.ok);
    assertTrue(s.isSuspended("c1"));
  }

  @Test
  void resumeApprovedReplaysTheHeldTurn() {
    SuspensionService s = new SuspensionService.InMemory();
    HumanGate g = gate(s, 0);
    g.submit(needsApproval(), 0);
    TurnResult resumed = g.resume("c1", true, 10);
    assertEquals("payments", resumed.path, "held 'what is my balance?' now routes normally");
    assertTrue(resumed.reply.contains("1234.56"));
    assertFalse(s.isSuspended("c1"), "suspension cleared");
  }

  @Test
  void resumeDeniedReportsAndClears() {
    SuspensionService s = new SuspensionService.InMemory();
    HumanGate g = gate(s, 0);
    g.submit(needsApproval(), 0);
    TurnResult denied = g.resume("c1", false, 10);
    assertEquals("denied", denied.path);
    assertFalse(s.isSuspended("c1"));
  }

  @Test
  void normalTurnsPassThrough() {
    SuspensionService s = new SuspensionService.InMemory();
    TurnResult r = gate(s, 0).submit(new Event("c1", "alice", "what is my balance?"), 0);
    assertEquals("payments", r.path);
    assertTrue(r.ok);
    assertFalse(s.isSuspended("c1"));
  }

  @Test
  void timeoutEscalatesUnactionedSuspensions() {
    SuspensionService s = new SuspensionService.InMemory();
    HumanGate g = gate(s, 1000); // 1s approval window
    g.submit(needsApproval(), 0);
    assertTrue(g.checkTimeouts(500).isEmpty(), "within the window");
    List<TurnResult> escalated = g.checkTimeouts(2000);
    assertEquals(1, escalated.size());
    assertEquals("escalated", escalated.get(0).path);
    assertFalse(s.isSuspended("c1"), "escalation cleared the suspension");
  }

  @Test
  void resumeWithNothingPendingIsANoop() {
    TurnResult r = gate(new SuspensionService.InMemory(), 0).resume("nope", true, 0);
    assertEquals("resume", r.path);
    assertFalse(r.ok);
  }
}
