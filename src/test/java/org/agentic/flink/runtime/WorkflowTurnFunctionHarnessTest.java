package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Test;

/**
 * Operator-harness tests for what needs controlled time: event-time timers registered for
 * suspended turns, and snapshot/restore of the keyed log together with its pending timers.
 */
class WorkflowTurnFunctionHarnessTest {

  private static String rnd(String p) {
    return p + "-" + UUID.randomUUID();
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> harness(
      Map<String, Object> wf) throws Exception {
    KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(new WorkflowTurnFunction(wf)), Event::conversationId, Types.STRING);
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

  @Test
  void eventTimeTimerResumesSuspendedTurnWhenWatermarkPasses() throws Exception {
    long delay = 1_000 + ThreadLocalRandom.current().nextInt(50_000);
    long t0 = ThreadLocalRandom.current().nextLong(1_000_000L);
    Map<String, Object> wf = Workflows.withFlink(Workflows.approval(),
        Map.of("resume_after_ms", delay, "timer_domain", "event_time"));
    String cid = rnd("c");
    String tid = rnd("t");
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf)) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund please"), t0));
      List<TurnResult> afterTurn = outputs(h);
      assertEquals(1, afterTurn.size());
      assertEquals(TurnStatus.SUSPENDED, afterTurn.get(0).status);
      Map<String, Object> scheduled = afterTurn.get(0).events.get(afterTurn.get(0).events.size() - 1).payload();
      assertEquals("event_time", scheduled.get("domain"));
      assertEquals(t0 + delay, ((Number) scheduled.get("fire_at")).longValue());

      h.processWatermark(t0 + delay - 1);
      assertEquals(1, outputs(h).size(), "timer must not fire before its timestamp");

      h.processWatermark(t0 + delay);
      List<TurnResult> all = outputs(h);
      assertEquals(2, all.size());
      TurnResult resumed = all.get(1);
      assertEquals(tid, resumed.turnId);
      assertEquals(TurnStatus.COMPLETED, resumed.status);
      assertEquals("timer_fired", resumed.events.get(0).type());
      assertEquals(WorkflowTurnFunction.TIMER_SIGNAL_KIND,
          ((Map<?, ?>) resumed.events.stream().filter(e -> "turn_resumed".equals(e.type())).findFirst()
              .orElseThrow().payload().get("signal")).get("kind"));
    }
  }

  @Test
  void eventTimeDomainRequiresTimestamps() throws Exception {
    Map<String, Object> wf = Workflows.withFlink(Workflows.approval(),
        Map.of("resume_after_ms", 10, "timer_domain", "event_time"));
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf)) {
      h.open();
      assertThrows(IllegalStateException.class,
          () -> h.processElement(new StreamRecord<>(Event.turn(rnd("c"), rnd("t"), "u", "refund"))));
    }
  }

  @Test
  void snapshotRestoresLogAndPendingTimer() throws Exception {
    long delay = 5_000;
    long t0 = 10_000;
    Map<String, Object> wf = Workflows.withFlink(Workflows.approval(),
        Map.of("resume_after_ms", delay, "timer_domain", "event_time"));
    String cid = rnd("c");
    String tid = rnd("t");
    OperatorSubtaskState snapshot;
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf)) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund please"), t0));
      snapshot = h.snapshot(1L, t0);
    }
    try (KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = harness(wf)) {
      h.initializeState(snapshot);
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund please"), t0 + 1));
      List<TurnResult> out = outputs(h);
      assertEquals(1, out.size());
      assertEquals(TurnStatus.DUPLICATE, out.get(0).status, "the restored log remembers the suspended turn");
      assertTrue(out.get(0).events.isEmpty());

      h.processWatermark(t0 + delay);
      out = outputs(h);
      assertEquals(2, out.size(), "the pending timer was restored and fired");
      assertEquals(TurnStatus.COMPLETED, out.get(1).status);
      assertEquals(1L, ((Number) out.get(1).state.get("turn_count")).longValue(),
          "one turn, suspended then completed");
    }
  }
}
