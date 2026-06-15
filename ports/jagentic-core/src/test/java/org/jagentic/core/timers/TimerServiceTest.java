package org.jagentic.core.timers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.TurnResult;
import org.jagentic.core.stream.StreamRuntime;

/** Phase 2: portable timers — logical-time advance, cancel, durable restore, and firing through the
 * stream runtime as turns. */
class TimerServiceTest {

  private static Event ev(String id) {
    return new Event("c1", "u", id);
  }

  @Test
  void advanceFiresDueTimersAscendingByDeadline() {
    InMemoryTimerService t = new InMemoryTimerService();
    t.schedule("a", 100, ev("a"));
    t.schedule("b", 50, ev("b"));
    t.schedule("c", 200, ev("c"));

    assertEquals(50L, t.nextDeadline().orElseThrow());
    List<Timer> due = t.advanceTo(150);
    assertEquals(List.of("b", "a"), due.stream().map(Timer::id).toList(), "ascending by fireAt");
    assertEquals(200L, t.nextDeadline().orElseThrow(), "only c remains");
    assertTrue(t.advanceTo(150).isEmpty(), "nothing new due");
  }

  @Test
  void equalDeadlinesKeepScheduleOrder() {
    InMemoryTimerService t = new InMemoryTimerService();
    t.schedule("x", 100, ev("x"));
    t.schedule("y", 100, ev("y"));
    t.schedule("z", 100, ev("z"));
    assertEquals(List.of("x", "y", "z"), t.advanceTo(100).stream().map(Timer::id).toList());
  }

  @Test
  void cancelRemovesPendingTimer() {
    InMemoryTimerService t = new InMemoryTimerService();
    t.schedule("a", 100, ev("a"));
    assertTrue(t.cancel("a"));
    assertFalse(t.cancel("a"));
    assertTrue(t.advanceTo(1000).isEmpty());
  }

  @Test
  void durableTimersSurviveRestore() {
    KeyedStateStore store = new KeyedStateStore.InMemory();
    DurableTimerService first = new DurableTimerService(store);
    first.schedule("escalate", 500, new Event("c9", "alice", "what is my balance?"));

    // "restart": a brand-new service over the same store, then restore.
    DurableTimerService recovered = new DurableTimerService(store);
    recovered.restore();
    assertEquals(500L, recovered.nextDeadline().orElseThrow());
    List<Timer> due = recovered.advanceTo(500);
    assertEquals(1, due.size());
    assertEquals("c9", due.get(0).payload().conversationId());
    assertEquals("what is my balance?", due.get(0).payload().text());
  }

  @Test
  void timersFireThroughTheStreamRuntimeAsTurns() {
    LocalRuntime rt = new LocalRuntime(Banking.buildGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), Banking.defaultTools(), Banking.retriever());
    StreamRuntime stream = new StreamRuntime(rt);
    InMemoryTimerService timers = new InMemoryTimerService();
    timers.schedule("followup", 1000, new Event("c1", "alice", "what is my balance?"));

    assertTrue(stream.fireDueTimers(timers, 999).isEmpty(), "not due yet");
    List<TurnResult> fired = stream.fireDueTimers(timers, 1000);
    assertEquals(1, fired.size());
    assertEquals("payments", fired.get(0).path);
    assertTrue(fired.get(0).reply.contains("1234.56"));
  }
}
