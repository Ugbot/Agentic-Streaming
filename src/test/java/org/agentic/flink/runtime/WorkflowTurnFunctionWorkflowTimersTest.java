package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.agentic.flink.runtime.testkit.FixtureProcessingClock;
import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Operator-harness tests for the spec's workflow {@code timers} (section 8 of
 * {@code spec/v1/primitives.md}) on the Flink runtime: the processing clock is the operator's
 * processing time (driven here through the harness), the event clock is the conversation watermark
 * folded from {@code metadata.event_time_ms}, and a pending timer survives snapshot and restore
 * without being re-scheduled or fired twice.
 */
class WorkflowTurnFunctionWorkflowTimersTest {

  private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

  private static String rnd(String p) {
    return p + "-" + UUID.randomUUID();
  }

  private static Map<String, Object> timerWorkflow(String clock, long afterMs, Map<String, Object> payload) {
    Map<String, Object> wf = new LinkedHashMap<>(Workflows.billing());
    Map<String, Object> timer = new LinkedHashMap<>();
    timer.put("id", "followup");
    timer.put("after_ms", afterMs);
    timer.put("clock", clock);
    timer.put("tool", "lookup_charge");
    timer.put("payload", payload);
    wf.put("timers", List.of(timer));
    return wf;
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> harness(
      Map<String, Object> wf, FlinkRuntimeOptions options) throws Exception {
    KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new WorkflowTurnFunction(wf, options)), Event::conversationId, Types.STRING);
    h.setup(TurnResultTypeInfo.INSTANCE.createSerializer(h.getExecutionConfig().getSerializerConfig()));
    return h;
  }

  private static List<TurnResult> outputs(KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h) {
    List<TurnResult> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((TurnResult) r.getValue());
      }
    }
    return out;
  }

  private static TurnResult last(KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h) {
    List<TurnResult> out = outputs(h);
    return out.get(out.size() - 1);
  }

  private static List<String> types(List<LogEvent> events) {
    return events.stream().map(LogEvent::type).toList();
  }

  private static Event eventTimed(String cid, String tid, long eventTimeMs) {
    return Event.turn(cid, tid, "u", "hello", Map.of("event_time_ms", Long.toString(eventTimeMs)));
  }

  @RepeatedTest(5)
  void processingTimerFiresOnceOnTheFirstTurnAtOrPastItsDeadline() throws Exception {
    long start = RND.nextLong(1_000L, 1_000_000L);
    long after = RND.nextLong(100L, 50_000L);
    Map<String, Object> payload = Map.of("channel", rnd("ch"));
    Map<String, Object> wf = timerWorkflow("processing", after, payload);
    String cid = rnd("c");
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf, FlinkRuntimeOptions.DEFAULTS)) {
      h.open();
      h.setProcessingTime(start);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "hello")));
      TurnResult first = last(h);
      assertEquals(List.of("turn_received", "timer_scheduled"), types(first.events).subList(0, 2));
      Map<String, Object> scheduled = first.events.get(1).payload();
      assertEquals("followup", scheduled.get("timer_id"));
      assertEquals("processing", scheduled.get("clock"));
      assertEquals(start + after, ((Number) scheduled.get("due_ms")).longValue());
      assertEquals(start, ((Number) first.events.get(0).payload().get("processing_time_ms")).longValue());

      h.setProcessingTime(start + after - 1);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t2", "u", "hello")));
      TurnResult early = last(h);
      assertTrue(early.calls.isEmpty(), "nothing fires before the deadline");
      assertNull(early.state.get("fired_timers"));

      h.setProcessingTime(start + after + RND.nextLong(0L, 1_000L));
      h.processElement(new StreamRecord<>(Event.turn(cid, "t3", "u", "hello")));
      TurnResult fired = last(h);
      assertEquals(List.of("timer_fired", "tool_called", "turn_received"), types(fired.events).subList(0, 3));
      assertEquals(1, fired.calls.size());
      assertEquals("lookup_charge", fired.calls.get(0).tool());
      assertEquals(payload, fired.calls.get(0).args());
      assertEquals(List.of("followup"), fired.state.get("fired_timers"));

      h.setProcessingTime(start + after * 3);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t4", "u", "hello")));
      TurnResult later = last(h);
      assertTrue(later.calls.isEmpty(), "a timer fires exactly once");
      assertEquals(List.of("followup"), later.state.get("fired_timers"));
      assertEquals(TurnStatus.COMPLETED, later.status);
    }
  }

  @RepeatedTest(5)
  void eventTimerFollowsTheConversationWatermarkAndIgnoresLateTurns() throws Exception {
    long t0 = RND.nextLong(1_000L, 1_000_000L);
    long after = RND.nextLong(100L, 10_000L);
    Map<String, Object> wf = timerWorkflow("event", after, Map.of("channel", "email"));
    String cid = rnd("c");
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf, FlinkRuntimeOptions.DEFAULTS)) {
      h.open();
      h.processElement(new StreamRecord<>(eventTimed(cid, "t1", t0)));
      TurnResult first = last(h);
      assertEquals(t0 + after, ((Number) first.events.get(1).payload().get("due_ms")).longValue());
      assertEquals(t0, ((Number) first.state.get("watermark_ms")).longValue());

      long ahead = t0 + RND.nextLong(1L, after);
      h.processElement(new StreamRecord<>(eventTimed(cid, "t2", ahead)));
      assertEquals(ahead, ((Number) last(h).state.get("watermark_ms")).longValue());
      assertTrue(last(h).calls.isEmpty());

      long late = t0 - RND.nextLong(1L, t0);
      h.processElement(new StreamRecord<>(eventTimed(cid, "t3", late)));
      TurnResult lateTurn = last(h);
      assertEquals(TurnStatus.COMPLETED, lateTurn.status, "late turns are processed in arrival order");
      assertEquals(ahead, ((Number) lateTurn.state.get("watermark_ms")).longValue(), "watermark never moves backwards");
      assertTrue(lateTurn.calls.isEmpty());

      h.setProcessingTime(RND.nextLong(1L, 1_000_000_000L));
      h.processElement(new StreamRecord<>(Event.turn(cid, "t4", "u", "hello")));
      assertTrue(last(h).calls.isEmpty(), "processing time does not fire an event timer");

      h.processElement(new StreamRecord<>(eventTimed(cid, "t5", t0 + after)));
      TurnResult fired = last(h);
      assertEquals("timer_fired", fired.events.get(0).type());
      assertEquals(List.of("followup"), fired.state.get("fired_timers"));
      assertEquals(t0 + after, ((Number) fired.state.get("watermark_ms")).longValue());
    }
  }

  @RepeatedTest(5)
  void pendingTimerSurvivesSnapshotAndFiresExactlyOnceAfterRestore() throws Exception {
    long start = RND.nextLong(1_000L, 1_000_000L);
    long after = RND.nextLong(200L, 50_000L);
    long beforeRestart = RND.nextLong(1L, after);
    Map<String, Object> wf = timerWorkflow("processing", after, Map.of("channel", "email"));
    String cid = rnd("c");
    OperatorSubtaskState snapshot;
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf, FlinkRuntimeOptions.DEFAULTS)) {
      h.open();
      h.setProcessingTime(start);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "hello")));
      h.setProcessingTime(start + beforeRestart);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t2", "u", "hello")));
      assertTrue(last(h).calls.isEmpty());
      snapshot = h.snapshot(1L, start + beforeRestart);
    }
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf, FlinkRuntimeOptions.DEFAULTS)) {
      h.initializeState(snapshot);
      h.open();
      h.setProcessingTime(start + after);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t3", "u", "hello")));
      TurnResult fired = last(h);
      assertEquals(List.of("timer_fired", "tool_called", "turn_received"), types(fired.events).subList(0, 3));
      assertEquals(start + after, ((Number) fired.events.get(0).payload().get("due_ms")).longValue());
      assertEquals(List.of("followup"), fired.state.get("fired_timers"));
      assertEquals(3L, ((Number) fired.state.get("turn_count")).longValue());
      assertTrue(types(fired.events).stream().noneMatch("timer_scheduled"::equals), "restore never re-schedules");
      snapshot = h.snapshot(2L, start + after);
    }
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf, FlinkRuntimeOptions.DEFAULTS)) {
      h.initializeState(snapshot);
      h.open();
      h.setProcessingTime(start + after * 2);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t4", "u", "hello")));
      TurnResult after2 = last(h);
      assertTrue(after2.calls.isEmpty(), "a fired timer never fires again after a second restore");
      assertEquals(List.of("followup"), after2.state.get("fired_timers"));
      assertTrue(types(after2.events).stream().noneMatch(t -> t.equals("timer_scheduled") || t.equals("timer_fired")));
    }
  }

  @Test
  void fixtureClockReplacesOperatorProcessingTime() throws Exception {
    long after = RND.nextLong(100L, 10_000L);
    Map<String, Object> wf = timerWorkflow("processing", after, Map.of("channel", "sms"));
    String cid = rnd("c");
    try (FixtureProcessingClock clock = new FixtureProcessingClock();
         KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h =
             harness(wf, FlinkRuntimeOptions.DEFAULTS.withProcessingClock(clock))) {
      h.open();
      h.setProcessingTime(RND.nextLong(1_000_000L, 2_000_000L));
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "hello")));
      TurnResult first = last(h);
      assertEquals(0L, ((Number) first.events.get(0).payload().get("processing_time_ms")).longValue());
      assertEquals(after, ((Number) first.events.get(1).payload().get("due_ms")).longValue());

      h.setProcessingTime(3_000_000L);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t2", "u", "hello")));
      assertTrue(last(h).calls.isEmpty(), "operator time does not move the fixture clock");

      clock.advance(after);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t3", "u", "hello")));
      assertEquals(List.of("followup"), last(h).state.get("fired_timers"));
      assertEquals(Map.of("channel", "sms"), last(h).calls.get(0).args());
    }
  }
}
