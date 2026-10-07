package org.jagentic.core.timers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.LogEvent;
import org.jagentic.core.LogicalClock;
import org.jagentic.core.TimerSpec;
import org.jagentic.core.TimerState;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.core.pipeline.WorkflowValidator.WorkflowValidationException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Section 8 of the spec (timers, the two clocks, checkpoint recovery) on the local runtime, with
 * randomized deadlines: the timer fires on the first turn at or past its deadline, once, with its
 * tool call first; event timers read the watermark and late turns never move it back; a restart
 * over the same log rebuilds pending timers and the clock without re-scheduling or double-firing.
 */
class WorkflowTimersTest {

  private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

  private static String rnd() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  private static Map<String, Object> workflow(List<Map<String, Object>> timers, String toolId) {
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "a-" + rnd());
    agent.put("router", Map.of("kind", "keyword", "default", "main"));
    agent.put("paths", Map.of("main", Map.of("brain", "rule", "prompt", "You chat.")));
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("spec_version", "agentic/v1");
    s.put("backend", "local");
    s.put("agent", agent);
    s.put("timers", timers);
    s.put("tools", List.of(Map.of("id", toolId, "kind", "constant", "value", "done")));
    return s;
  }

  private static Map<String, Object> timer(String id, long afterMs, String clock, String tool,
                                           Map<String, Object> payload) {
    Map<String, Object> t = new LinkedHashMap<>();
    t.put("id", id);
    t.put("after_ms", afterMs);
    if (clock != null) {
      t.put("clock", clock);
    }
    t.put("tool", tool);
    t.put("payload", payload);
    return t;
  }

  private static LocalRuntime runtime(GraphBuilder.Built built, ConversationLog log, LogicalClock clock) {
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), built.retriever(), log, clock);
  }

  private static List<String> types(List<LogEvent> events) {
    return events.stream().map(LogEvent::type).toList();
  }

  private static Event turnAt(String cid, String tid, long eventTimeMs) {
    return Event.turn(cid, tid, "u", "t", Map.of("event_time_ms", Long.toString(eventTimeMs)));
  }

  @RepeatedTest(5)
  void processingTimerFiresOnceOnTheFirstTurnAtOrPastItsDeadlineWithItsToolCallFirst() {
    long after = RND.nextLong(100, 5_000);
    Map<String, Object> payload = Map.of("channel", "ch-" + rnd());
    GraphBuilder.Built built = GraphBuilder.build(
        workflow(List.of(timer("followup", after, null, "nudge", payload)), "nudge"), null);
    AtomicInteger runs = new AtomicInteger();
    built.tools().register("nudge", "counting", args -> "run" + runs.incrementAndGet());
    LogicalClock.Manual clock = new LogicalClock.Manual();
    LocalRuntime rt = runtime(built, new ConversationLog.InMemory(), clock);
    String cid = "c-" + rnd();

    TurnResult first = rt.submit(Event.turn(cid, "t1", "u", "start"));
    List<String> firstTypes = types(first.events);
    assertEquals(List.of("turn_received", "timer_scheduled", "routed"), firstTypes.subList(0, 3));
    assertEquals("turn_completed", firstTypes.get(firstTypes.size() - 1));
    assertTrue(first.calls.isEmpty());
    assertEquals(after, rt.replay(cid).timers().pending().get("followup").dueMs());
    assertNull(first.state.get("fired_timers"));

    // Several turns strictly before the deadline: nothing fires.
    int early = RND.nextInt(1, 4);
    long elapsed = 0;
    for (int i = 0; i < early; i++) {
      long step = (after - 1 - elapsed) / (early - i);
      clock.advance(step);
      elapsed += step;
      TurnResult r = rt.submit(Event.turn(cid, "e" + i, "u", "waiting"));
      assertFalse(types(r.events).contains("timer_fired"), "fired at " + clock.nowMs() + " before " + after);
      assertTrue(r.calls.isEmpty());
    }
    assertTrue(clock.nowMs() < after);

    clock.advance(after - clock.nowMs() + RND.nextLong(0, 50));
    TurnResult fired = rt.submit(Event.turn(cid, "t-fire", "u", "now?"));
    assertEquals(TurnStatus.COMPLETED, fired.status);
    List<String> t = types(fired.events);
    assertEquals(List.of("timer_fired", "tool_called", "turn_received"), t.subList(0, 3));
    assertEquals(1, fired.calls.size());
    assertEquals("nudge", fired.calls.get(0).tool());
    assertEquals(0, fired.calls.get(0).index());
    assertEquals(payload, fired.calls.get(0).args());
    assertEquals(List.of("followup"), fired.state.get("fired_timers"));
    assertEquals(1, runs.get());

    clock.advance(RND.nextLong(0, 100_000));
    TurnResult later = rt.submit(Event.turn(cid, "t-later", "u", "again"));
    assertFalse(types(later.events).contains("timer_fired"));
    assertTrue(later.calls.isEmpty());
    assertEquals(List.of("followup"), later.state.get("fired_timers"));
    assertEquals(1, runs.get(), "a timer fires once");
    assertTrue(rt.replay(cid).timers().pending().isEmpty());
  }

  @Test
  void dueTimersFireInDueMsThenTimerIdOrder() {
    long base = RND.nextLong(10, 1000);
    List<Map<String, Object>> timers = List.of(
        timer("b", base, null, "tool", Map.of("k", "b")),
        timer("a", base, null, "tool", Map.of("k", "a")),
        timer("c", base / 2, null, "tool", Map.of("k", "c")));
    GraphBuilder.Built built = GraphBuilder.build(workflow(timers, "tool"), null);
    LogicalClock.Manual clock = new LogicalClock.Manual();
    LocalRuntime rt = runtime(built, new ConversationLog.InMemory(), clock);
    rt.submit(Event.turn("c", "t1", "u", "x"));
    clock.advance(base + RND.nextLong(0, 10));
    TurnResult r = rt.submit(Event.turn("c", "t2", "u", "y"));
    assertEquals(List.of("c", "a", "b"), r.state.get("fired_timers"));
    assertEquals(List.of("c", "a", "b"), r.calls.stream().map(c -> c.args().get("k")).toList());
    assertEquals(List.of(0, 1, 2), r.calls.stream().map(c -> c.index()).toList());
    assertEquals(List.of("timer_fired", "tool_called", "timer_fired", "tool_called", "timer_fired",
        "tool_called", "turn_received"), types(r.events).subList(0, 7));
  }

  @RepeatedTest(5)
  void eventTimerReadsTheWatermarkWhichLateTurnsNeverMoveBack() {
    long start = RND.nextLong(1_000, 100_000);
    long after = RND.nextLong(100, 10_000);
    GraphBuilder.Built built = GraphBuilder.build(
        workflow(List.of(timer("sla", after, "event", "escalate", Map.of("reason", "sla"))), "escalate"), null);
    LogicalClock.Manual clock = new LogicalClock.Manual();
    LocalRuntime rt = runtime(built, new ConversationLog.InMemory(), clock);
    String cid = "c-" + rnd();

    TurnResult first = rt.submit(turnAt(cid, "t1", start));
    assertEquals(start, first.state.get("watermark_ms"));
    assertEquals(start + after, rt.replay(cid).timers().pending().get("sla").dueMs());

    long high = start + RND.nextLong(1, after);
    TurnResult second = rt.submit(turnAt(cid, "t2", high));
    assertEquals(high, second.state.get("watermark_ms"));
    assertFalse(types(second.events).contains("timer_fired"));

    long late = RND.nextLong(0, high);
    TurnResult third = rt.submit(turnAt(cid, "t3", late));
    assertEquals(TurnStatus.COMPLETED, third.status, "late turns are processed in arrival order");
    assertEquals(high, third.state.get("watermark_ms"), "the watermark never moves backwards");
    assertEquals(3L, third.state.get("turn_count"));
    assertFalse(types(third.events).contains("timer_fired"));

    // Processing time is irrelevant to an event timer.
    clock.advance(RND.nextLong(0, 1_000_000));
    TurnResult noEventTime = rt.submit(Event.turn(cid, "t-none", "u", "no clock"));
    assertFalse(types(noEventTime.events).contains("timer_fired"));
    assertEquals(high, noEventTime.state.get("watermark_ms"));

    TurnResult fired = rt.submit(turnAt(cid, "t4", start + after + RND.nextLong(0, 100)));
    assertEquals(List.of("timer_fired", "tool_called", "turn_received"), types(fired.events).subList(0, 3));
    assertEquals(Map.of("reason", "sla"), fired.calls.get(0).args());
    assertEquals(List.of("sla"), fired.state.get("fired_timers"));
  }

  @RepeatedTest(5)
  void restartRebuildsPendingTimersAndTheClockFromTheLogAndFiresExactlyOnce() {
    long after = RND.nextLong(200, 5_000);
    GraphBuilder.Built built = GraphBuilder.build(
        workflow(List.of(timer("expiry", after, null, "expire", Map.of())), "expire"), null);
    AtomicInteger runs = new AtomicInteger();
    built.tools().register("expire", "counting", args -> "run" + runs.incrementAndGet());
    ConversationLog log = new ConversationLog.InMemory();
    LogicalClock.Manual clock = new LogicalClock.Manual();
    LocalRuntime rt = runtime(built, log, clock);
    String cid = "c-" + rnd();

    rt.submit(Event.turn(cid, "t1", "u", "add item"));
    long before = RND.nextLong(1, after);
    clock.advance(before);
    TurnResult t2 = rt.submit(Event.turn(cid, "t2", "u", "add another"));
    assertFalse(types(t2.events).contains("timer_fired"));

    // Restart: only the log survives. The clock is recovered from it.
    int restarts = RND.nextInt(1, 4);
    for (int i = 0; i < restarts; i++) {
      LogicalClock.Manual recovered = LogicalClock.Manual.recoveredFrom(log);
      assertEquals(clock.nowMs(), recovered.nowMs(), "the clock survives a restart");
      clock = recovered;
      rt = runtime(built, log, clock);
      TimerState timers = rt.replay(cid).timers();
      assertEquals(1, timers.pending().size(), "pending timers are rebuilt, not re-scheduled");
      assertEquals(after, timers.pending().get("expiry").dueMs());
      assertTrue(timers.fired().isEmpty());
    }

    clock.advance(after - before + RND.nextLong(0, 100));
    TurnResult t3 = rt.submit(Event.turn(cid, "t3", "u", "checkout?"));
    List<String> types = types(t3.events);
    assertEquals(List.of("timer_fired", "tool_called", "turn_received"), types.subList(0, 3));
    assertFalse(types.contains("timer_scheduled"), "a restart never re-schedules");
    assertEquals(1, t3.calls.size());
    assertEquals(List.of("expiry"), t3.state.get("fired_timers"));
    assertEquals(3L, t3.state.get("turn_count"));

    // Another restart after firing: the fired timer stays fired.
    rt = runtime(built, log, LogicalClock.Manual.recoveredFrom(log));
    ((LogicalClock.Manual) rt.clock()).advance(RND.nextLong(0, 100_000));
    TurnResult t4 = rt.submit(Event.turn(cid, "t4", "u", "anything else?"));
    assertFalse(types(t4.events).contains("timer_fired"));
    assertFalse(types(t4.events).contains("timer_scheduled"));
    assertTrue(t4.calls.isEmpty());
    assertEquals(List.of("expiry"), t4.state.get("fired_timers"));
    assertEquals(1, runs.get(), "the timer's tool ran exactly once across restarts");
    assertEquals(1, log.events(cid).stream().filter(e -> e.is(EventType.TIMER_SCHEDULED)).count());
    assertEquals(1, log.events(cid).stream().filter(e -> e.is(EventType.TIMER_FIRED)).count());
  }

  @Test
  void timerStateFoldIsPureAndOrderedAndTheClockNeverMovesBackwards() {
    ConversationLog log = new ConversationLog.InMemory();
    String cid = "c-" + rnd();
    int n = RND.nextInt(2, 8);
    List<String> ids = new ArrayList<>();
    log.append(cid, "t1", EventType.TURN_RECEIVED, Map.of("turn_id", "t1", "text", "x",
        "event_time_ms", 500L, LogicalClock.PROCESSING_TIME_KEY, 40L));
    for (int i = 0; i < n; i++) {
      String id = "timer-" + i;
      ids.add(id);
      Map<String, Object> p = new LinkedHashMap<>();
      p.put("timer_id", id);
      p.put("clock", i % 2 == 0 ? "processing" : "event");
      p.put("due_ms", RND.nextLong(1, 1_000_000));
      log.append(cid, "t1", EventType.TIMER_SCHEDULED, p);
    }
    TimerState scheduled = log.state(cid).timers();
    assertEquals(ids, new ArrayList<>(scheduled.pending().keySet()));
    assertEquals(500L, scheduled.watermarkMs());
    assertEquals(40L, scheduled.processingTimeMs());
    assertEquals(500L, scheduled.watermarkAfter(RND.nextLong(0, 500)));
    assertEquals(900L, scheduled.watermarkAfter(900L));

    String firedId = ids.get(RND.nextInt(n));
    log.append(cid, "t2", EventType.TIMER_FIRED, Map.of("timer_id", firedId, "due_ms", 1L));
    log.append(cid, "t2", EventType.TIMER_FIRED, Map.of("timer_id", firedId, "due_ms", 1L));
    TimerState after = log.state(cid).timers();
    assertEquals(n - 1, after.pending().size());
    assertFalse(after.pending().containsKey(firedId));
    assertEquals(List.of(firedId), after.fired(), "a duplicate timer_fired for an id already fired is ignored");
    assertEquals(List.of(firedId), log.state(cid).reduced().get("fired_timers"));
    assertEquals(500L, log.state(cid).reduced().get("watermark_ms"));

    assertEquals(40L, LogicalClock.Manual.recoveredFrom(log).nowMs());
    LogicalClock.Manual clock = new LogicalClock.Manual(RND.nextLong(0, 1000));
    assertThrows(IllegalArgumentException.class, () -> clock.advance(-1));
    assertThrows(IllegalArgumentException.class, () -> new LogicalClock.Manual(-RND.nextLong(1, 1000)));
  }

  @Test
  void validatorAcceptsTheClockKeyAndRejectsUnknownClocks() {
    GraphBuilder.build(workflow(List.of(timer("t", 1, "event", "x", Map.of())), "x"), null);
    GraphBuilder.build(workflow(List.of(timer("t", 1, "processing", "x", Map.of())), "x"), null);
    assertThrows(WorkflowValidationException.class, () ->
        GraphBuilder.build(workflow(List.of(timer("t", 1, "wall-" + rnd(), "x", Map.of())), "x"), null));
    assertEquals(TimerSpec.Clock.EVENT,
        GraphBuilder.build(workflow(List.of(timer("t", 1, "event", "x", Map.of())), "x"), null)
            .graph().timers().get(0).clock());
  }

  @Test
  void timersNeedAClockOnTheContext() {
    GraphBuilder.Built built = GraphBuilder.build(
        workflow(List.of(timer("t", RND.nextLong(1, 100), null, "x", Map.of())), "x"), null);
    LocalRuntime rt = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), built.retriever(), new ConversationLog.InMemory(), null);
    // The default clock is the wall clock, which is a valid processing clock.
    TurnResult r = rt.submit(Event.turn("c", "t1", "u", "x"));
    assertTrue(types(r.events).contains("timer_scheduled"));
    assertTrue(rt.replay("c").timers().pending().get("t").dueMs() > 0);
  }
}
