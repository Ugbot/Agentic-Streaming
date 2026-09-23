package org.jagentic.pekko.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.LogicalClock;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;

/**
 * Workflow {@code timers} (spec section 8) on the entity: scheduled on the first turn against the
 * clock in {@link AgentDeps}, fired at the head of the first later turn at or past the deadline, the
 * tool invoked with the declared payload, event timers following the conversation watermark, and a
 * pending timer surviving passivation (journal recovery) to fire exactly once.
 */
class WorkflowTimerEntityTest {

  private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

  private LogicalClock.Manual clock;
  private PekkoSystem sys;
  private PekkoRuntime rt;

  @BeforeEach
  void boot() {
    clock = new LogicalClock.Manual();
  }

  @AfterEach
  void shutdown() {
    if (sys != null) {
      sys.close();
    }
  }

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private void start(String timerClock, long afterMs, Map<String, Object> payload) {
    Map<String, Object> path = new LinkedHashMap<>();
    path.put("brain", "rule");
    path.put("prompt", "You answer.");
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "timers");
    agent.put("router", Map.of("kind", "keyword", "default", "main", "rules", Map.of("main", List.of("hello"))));
    agent.put("paths", Map.of("main", path));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> timer = new LinkedHashMap<>();
    timer.put("id", "followup");
    timer.put("after_ms", afterMs);
    timer.put("clock", timerClock);
    timer.put("tool", "notify");
    timer.put("payload", payload);
    Map<String, Object> wf = new LinkedHashMap<>();
    wf.put("spec_version", "agentic/v1");
    wf.put("backend", "local");
    wf.put("agent", agent);
    wf.put("tools", List.of(Map.of("id", "notify", "kind", "constant", "description", "Notify", "value", "sent")));
    wf.put("timers", List.of(timer));
    GraphBuilder.Built built = GraphBuilder.build(wf, llm -> {
      throw new IllegalStateException("no llm");
    });
    sys = new PekkoSystem(new AgentDeps(built.graph(), built.tools(), built.retriever(), clock));
    rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(20));
  }

  private static List<String> types(List<LogEvent> events) {
    return events.stream().map(LogEvent::type).toList();
  }

  private static Event timed(String cid, String tid, long eventTimeMs) {
    return Event.turn(cid, tid, "u", "hello", Map.of("event_time_ms", Long.toString(eventTimeMs)));
  }

  @RepeatedTest(3)
  void processingTimerFiresOnceAtTheHeadOfTheFirstDueTurn() {
    long after = RND.nextLong(100L, 100_000L);
    Map<String, Object> payload = Map.of("channel", rnd("ch"));
    start("processing", after, payload);
    String cid = rnd("c");
    clock.advance(RND.nextLong(0L, 10_000L));
    long start = clock.nowMs();

    TurnResult first = rt.submit(Event.turn(cid, "t1", "u", "hello"));
    assertEquals(List.of("turn_received", "timer_scheduled"), types(first.events).subList(0, 2));
    assertEquals(start + after, ((Number) first.events.get(1).payload().get("due_ms")).longValue());
    assertEquals(start + after, ((Number) rt.state(cid).events().get(1).payload().get("due_ms")).longValue(),
        "the journal carries the scheduled deadline");

    clock.advance(after - 1);
    TurnResult early = rt.submit(Event.turn(cid, "t2", "u", "hello"));
    assertTrue(early.calls.isEmpty());
    assertNull(early.state.get("fired_timers"));

    clock.advance(1 + RND.nextLong(0L, 1_000L));
    TurnResult fired = rt.submit(Event.turn(cid, "t3", "u", "hello"));
    assertEquals(List.of("timer_fired", "tool_called", "turn_received"), types(fired.events).subList(0, 3));
    assertEquals("notify", fired.calls.get(0).tool());
    assertEquals(payload, fired.calls.get(0).args());
    assertEquals(List.of("followup"), fired.state.get("fired_timers"));

    clock.advance(after);
    TurnResult later = rt.submit(Event.turn(cid, "t4", "u", "hello"));
    assertTrue(later.calls.isEmpty(), "a timer fires exactly once");
    assertEquals(TurnStatus.COMPLETED, later.status);
  }

  @RepeatedTest(3)
  void eventTimerFollowsTheWatermarkAndLateTurnsDoNotMoveItBack() {
    long t0 = RND.nextLong(1_000L, 1_000_000L);
    long after = RND.nextLong(100L, 10_000L);
    start("event", after, Map.of("channel", "email"));
    String cid = rnd("c");

    TurnResult first = rt.submit(timed(cid, "t1", t0));
    assertEquals(t0 + after, ((Number) first.events.get(1).payload().get("due_ms")).longValue());
    long ahead = t0 + RND.nextLong(1L, after);
    assertEquals(ahead, ((Number) rt.submit(timed(cid, "t2", ahead)).state.get("watermark_ms")).longValue());

    TurnResult late = rt.submit(timed(cid, "t3", t0 - RND.nextLong(1L, t0)));
    assertEquals(TurnStatus.COMPLETED, late.status);
    assertEquals(ahead, ((Number) late.state.get("watermark_ms")).longValue());
    assertTrue(late.calls.isEmpty());

    clock.advance(after * 10);
    assertTrue(rt.submit(Event.turn(cid, "t4", "u", "hello")).calls.isEmpty(),
        "processing time does not fire an event timer");

    TurnResult fired = rt.submit(timed(cid, "t5", t0 + after));
    assertEquals("timer_fired", fired.events.get(0).type());
    assertEquals(List.of("followup"), fired.state.get("fired_timers"));
  }

  @RepeatedTest(3)
  void pendingTimerSurvivesPassivationAndFiresExactlyOnce() {
    long after = RND.nextLong(200L, 100_000L);
    long beforeRestart = RND.nextLong(1L, after);
    start("processing", after, Map.of("channel", "email"));
    String cid = rnd("c");

    rt.submit(Event.turn(cid, "t1", "u", "hello"));
    clock.advance(beforeRestart);
    assertTrue(rt.submit(Event.turn(cid, "t2", "u", "hello")).calls.isEmpty());
    assertEquals(1, rt.state(cid).events().stream().filter(e -> e.type().equals("timer_scheduled")).count());

    rt.passivate(cid);
    clock.advance(after - beforeRestart);
    TurnResult fired = rt.submit(Event.turn(cid, "t3", "u", "hello"));
    assertEquals(List.of("timer_fired", "tool_called", "turn_received"), types(fired.events).subList(0, 3));
    assertEquals(after, ((Number) fired.events.get(0).payload().get("due_ms")).longValue());
    assertEquals(List.of("followup"), fired.state.get("fired_timers"));
    assertEquals(3L, ((Number) fired.state.get("turn_count")).longValue());

    rt.passivate(cid);
    clock.advance(after);
    TurnResult again = rt.submit(Event.turn(cid, "t4", "u", "hello"));
    assertTrue(again.calls.isEmpty(), "a fired timer never fires again after recovery");
    List<LogEvent> journal = rt.state(cid).events();
    assertEquals(1, journal.stream().filter(e -> e.type().equals("timer_scheduled")).count(), "never re-scheduled");
    assertEquals(1, journal.stream().filter(e -> e.type().equals("timer_fired")).count(), "never double-fired");
    assertTrue(rt.state(cid).pendingTimers().isEmpty(), "workflow timers arm no entity timer");
  }
}
