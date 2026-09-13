package org.jagentic.pekko.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * Durable timers: a scheduled timer is a journal event, survives passivation/restart, fires exactly
 * once, and resumes a suspended turn through the ordinary turn path.
 */
class DurableTimerTest {

  private CountingGraph graph;
  private PekkoSystem sys;
  private PekkoRuntime rt;

  @BeforeEach
  void boot() {
    graph = new CountingGraph();
    sys = new PekkoSystem(graph.deps());
    rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
  }

  @AfterEach
  void shutdown() {
    sys.close();
  }

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private static void awaitUntil(java.util.function.BooleanSupplier cond, Duration max) throws InterruptedException {
    long deadline = System.nanoTime() + max.toNanos();
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("condition not met within " + max);
      }
      Thread.sleep(25);
    }
  }

  @Test
  void timerResumesASuspendedTurnAfterRestart() throws InterruptedException {
    String cid = rnd("c");
    String t1 = rnd("t");
    String timerId = rnd("timer");
    TurnResult suspended = rt.submit(Event.turn(cid, t1, "hal", "refund please"));
    assertEquals(TurnStatus.SUSPENDED, suspended.status);

    long delayMs = ThreadLocalRandom.current().nextLong(400, 900);
    Event fire = Event.resume(cid, t1, Map.of("kind", "approval", "approved", true, "via", "timer"));
    ConversationEntity.TimerAck ack = rt.scheduleTimer(cid, timerId, Duration.ofMillis(delayMs), fire);
    assertFalse(ack.alreadyScheduled());
    ConversationEntity.TimerAck again = rt.scheduleTimer(cid, timerId, Duration.ofMillis(delayMs), fire);
    assertTrue(again.alreadyScheduled(), "re-scheduling the same timer id is idempotent");
    assertEquals(ack.fireAt(), again.fireAt());

    ConversationEntity.StateSnapshot armed = rt.state(cid);
    assertTrue(armed.pendingTimers().containsKey(timerId));
    assertEquals(1, armed.events().stream().filter(e -> e.is(EventType.TIMER_SCHEDULED)).count(),
        "the timer is exactly one journal event");

    rt.passivate(cid);
    ConversationEntity.StateSnapshot recovered = rt.state(cid);
    assertTrue(recovered.pendingTimers().containsKey(timerId), "the pending timer is rebuilt from the journal");
    assertEquals(0, graph.brainCalls.get());

    awaitUntil(() -> rt.state(cid).suspendedTurnIds().isEmpty(), Duration.ofSeconds(10));
    ConversationEntity.StateSnapshot done = rt.state(cid);
    List<LogEvent> events = done.events();
    assertTrue(done.pendingTimers().isEmpty());
    assertEquals(1, events.stream().filter(e -> e.is(EventType.TIMER_FIRED)).count(), "fires exactly once");
    assertTrue(events.stream().anyMatch(e -> e.is(EventType.TURN_RESUMED)));
    assertTrue(events.stream().anyMatch(e -> e.is(EventType.TURN_COMPLETED)));
    assertEquals(1, graph.brainCalls.get());
    for (int i = 0; i < events.size(); i++) {
      assertEquals(i, events.get(i).sequence());
    }

    TurnResult dup = rt.submit(Event.resume(cid, t1, Map.of("kind", "approval", "approved", true)));
    assertEquals(TurnStatus.DUPLICATE, dup.status);
    assertEquals("[approval] refund approved", dup.reply);

    ConversationEntity.TimerAck late = rt.scheduleTimer(cid, timerId, Duration.ZERO, fire);
    assertTrue(late.alreadyScheduled(), "a fired timer id cannot be re-armed");
    rt.passivate(cid);
    Thread.sleep(200);
    assertEquals(1, rt.state(cid).events().stream().filter(e -> e.is(EventType.TIMER_FIRED)).count(),
        "recovery does not re-fire a consumed timer");
  }

  @Test
  void timerFiresAcrossPassivationWhenItExpiresWhileTheEntityIsDown() throws InterruptedException {
    String cid = rnd("c");
    String t1 = rnd("t");
    rt.submit(Event.turn(cid, t1, "ivy", "refund this"));
    rt.scheduleTimer(cid, rnd("timer"), Duration.ofMillis(150),
        Event.resume(cid, t1, Map.of("kind", "approval", "approved", true)));
    rt.passivate(cid);
    Thread.sleep(400);
    // the entity was passivated before the timer could fire; recovery re-arms it with zero remaining delay
    awaitUntil(() -> rt.state(cid).suspendedTurnIds().isEmpty(), Duration.ofSeconds(10));
    assertEquals(1, rt.state(cid).events().stream().filter(e -> e.is(EventType.TIMER_FIRED)).count());
    assertEquals(1, graph.brainCalls.get());
  }
}
