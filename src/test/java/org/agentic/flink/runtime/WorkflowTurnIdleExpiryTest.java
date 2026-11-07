package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.runtime.testkit.Workflows;
import org.agentic.flink.typeinfo.JsonTypeInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@code state_ttl_ms} bounds how long an idle conversation is kept. It used to be Flink state TTL,
 * which tracks list elements one by one: the oldest events expired under a live conversation, the
 * log stopped folding, and the key was dead (review finding C3).
 */
class WorkflowTurnIdleExpiryTest {

  private static final long TTL = 1_000;

  private static String rnd(String p) {
    return p + "-" + UUID.randomUUID();
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> harness(
      Map<String, Object> wf) throws Exception {
    var h =
        new KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult>(
            new KeyedProcessOperator<>(new WorkflowTurnFunction(wf)),
            Event::conversationId,
            Types.STRING);
    h.setup(
        TurnResultTypeInfo.INSTANCE.createSerializer(h.getExecutionConfig().getSerializerConfig()));
    return h;
  }

  private static List<TurnResult> outputs(
      KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h) {
    List<TurnResult> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((TurnResult) r.getValue());
      }
    }
    return out;
  }

  private static TurnResult last(
      KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h) {
    List<TurnResult> out = outputs(h);
    return out.get(out.size() - 1);
  }

  private static void clockAt(
      KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h, long ms)
      throws Exception {
    h.setStateTtlProcessingTime(ms);
    h.setProcessingTime(ms);
  }

  private static void useBackend(
      KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h, String backend) {
    if ("rocksdb".equals(backend)) {
      h.setStateBackend(new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend());
    }
  }

  private static Map<String, Object> workflow() {
    return Workflows.withFlink(Workflows.billing(), Map.of("state_ttl_ms", TTL));
  }

  @Test
  void anActiveConversationOutlivesTheTtl() throws Exception {
    String cid = rnd("c");
    int turns = 8 + ThreadLocalRandom.current().nextInt(8);
    try (var h = harness(workflow())) {
      h.open();
      long expected = 0;
      for (int i = 0; i < turns; i++) {
        clockAt(h, i * (TTL * 6 / 10));
        h.processElement(new StreamRecord<>(Event.turn(cid, "t" + i, "u", "hello " + i), i));
        TurnResult r = last(h);
        assertEquals(TurnStatus.COMPLETED, r.status, "turn " + i + ": " + r.error);
        assertEquals(i + 1L, ((Number) r.state.get("turn_count")).longValue());
        assertEquals(expected, r.events.get(0).sequence(), "the log stays dense");
        expected = r.events.get(r.events.size() - 1).sequence() + 1;
      }
      h.processElement(new StreamRecord<>(Event.turn(cid, "t0", "u", "hello 0"), turns));
      assertEquals(TurnStatus.DUPLICATE, last(h).status, "the earliest turn is still remembered");
    }
  }

  @Test
  void aConversationIdleForTheTtlIsForgottenWhole() throws Exception {
    String cid = rnd("c");
    try (var h = harness(workflow())) {
      h.open();
      clockAt(h, 0);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "hello"), 0));
      clockAt(h, TTL - 1);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t2", "u", "hello"), 1));
      clockAt(h, 2 * TTL - 2);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "hello"), 2));
      assertEquals(TurnStatus.DUPLICATE, last(h).status, "each turn restarts the idle clock");

      clockAt(h, 3 * TTL);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "hello"), 3));
      TurnResult after = last(h);
      assertEquals(TurnStatus.COMPLETED, after.status, "the retained window is over");
      assertEquals(0L, after.events.get(0).sequence(), "log and counter go together");
      assertEquals(1L, ((Number) after.state.get("turn_count")).longValue());
    }
  }

  @Test
  void aSuspendedTurnIsNeverExpired() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    Map<String, Object> wf = Workflows.withFlink(Workflows.approval(), Map.of("state_ttl_ms", TTL));
    try (var h = harness(wf)) {
      h.open();
      clockAt(h, 0);
      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund please"), 0));
      assertEquals(TurnStatus.SUSPENDED, last(h).status);

      clockAt(h, 50 * TTL);
      h.processElement(new StreamRecord<>(Event.resume(cid, tid, Map.of("approved", true)), 1));
      assertEquals(TurnStatus.COMPLETED, last(h).status, "the approval must not be lost");
    }
  }

  /**
   * The state as a job built before this change wrote it: the same state names, with Flink TTL.
   * Restoring it needs Flink's TTL-to-plain state migration, so this test pins that it works.
   */
  private static final class OldTtlWriter extends KeyedProcessFunction<String, Event, TurnResult> {
    private transient ListState<LogEvent> log;
    private transient ValueState<Long> next;

    @Override
    public void open(OpenContext ctx) {
      StateTtlConfig ttl =
          StateTtlConfig.newBuilder(java.time.Duration.ofDays(30))
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .build();
      ListStateDescriptor<LogEvent> l =
          new ListStateDescriptor<>(
              WorkflowTurnFunction.LOG_STATE, JsonTypeInfo.of(LogEvent.class));
      ValueStateDescriptor<Long> s =
          new ValueStateDescriptor<>(WorkflowTurnFunction.SEQUENCE_STATE, Types.LONG);
      MapStateDescriptor<Long, String> t =
          new MapStateDescriptor<>(WorkflowTurnFunction.TIMER_STATE, Types.LONG, Types.STRING);
      l.enableTimeToLive(ttl);
      s.enableTimeToLive(ttl);
      t.enableTimeToLive(ttl);
      log = getRuntimeContext().getListState(l);
      next = getRuntimeContext().getState(s);
      getRuntimeContext().getMapState(t);
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<TurnResult> out) throws Exception {
      long n = 0;
      for (EventType type : List.of(EventType.TURN_RECEIVED, EventType.TURN_COMPLETED)) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("turn_id", e.turnId());
        p.put("text", e.text());
        p.put("reply", "[general] old reply");
        log.add(new LogEvent(e.conversationId(), n++, e.turnId(), type.wire(), p));
      }
      next.update(n);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "rocksdb"})
  void aSavepointTakenWithFlinkTtlRestoresIntoTheIdleTimer(String backend) throws Exception {
    String cid = rnd("c");
    OperatorSubtaskState savepoint;
    try (var old =
        new KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult>(
            new KeyedProcessOperator<>(new OldTtlWriter()), Event::conversationId, Types.STRING)) {
      old.setup(
          TurnResultTypeInfo.INSTANCE.createSerializer(
              old.getExecutionConfig().getSerializerConfig()));
      useBackend(old, backend);
      old.open();
      old.processElement(new StreamRecord<>(Event.turn(cid, "t-old", "u", "hello"), 1));
      savepoint = old.snapshot(1, 1);
    }
    try (var h = harness(workflow())) {
      useBackend(h, backend);
      h.initializeState(savepoint);
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, "t-old", "u", "hello"), 2));
      assertEquals(TurnStatus.DUPLICATE, last(h).status, "the restored log is read");
      h.processElement(new StreamRecord<>(Event.turn(cid, "t-new", "u", "hello"), 3));
      TurnResult next = last(h);
      assertEquals(TurnStatus.COMPLETED, next.status);
      assertEquals(2L, next.events.get(0).sequence());
      assertTrue(((Number) next.state.get("turn_count")).longValue() == 2L);
    }
  }

  /**
   * A suspended turn whose log lost its head, with its resume timer still pending: the state an old
   * job's element-wise TTL can leave in a savepoint.
   */
  private static final class GappedLogWriter
      extends KeyedProcessFunction<String, Event, TurnResult> {
    private final long fireAt;
    private transient ListState<LogEvent> log;
    private transient ValueState<Long> next;
    private transient org.apache.flink.api.common.state.MapState<Long, String> timers;

    GappedLogWriter(long fireAt) {
      this.fireAt = fireAt;
    }

    @Override
    public void open(OpenContext ctx) {
      log =
          getRuntimeContext()
              .getListState(
                  new ListStateDescriptor<>(
                      WorkflowTurnFunction.LOG_STATE, JsonTypeInfo.of(LogEvent.class)));
      next =
          getRuntimeContext()
              .getState(
                  new ValueStateDescriptor<>(WorkflowTurnFunction.SEQUENCE_STATE, Types.LONG));
      timers =
          getRuntimeContext()
              .getMapState(
                  new MapStateDescriptor<>(
                      WorkflowTurnFunction.TIMER_STATE, Types.LONG, Types.STRING));
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<TurnResult> out) throws Exception {
      Map<String, Object> p = new LinkedHashMap<>();
      p.put("turn_id", e.turnId());
      p.put("path", "general");
      p.put("text", e.text());
      log.add(new LogEvent(e.conversationId(), 6, e.turnId(), EventType.TURN_SUSPENDED.wire(), p));
      next.update(7L);
      timers.put(fireAt, e.turnId());
      ctx.timerService().registerProcessingTimeTimer(fireAt);
    }
  }

  @Test
  void aResumeTimerOnALogThatNoLongerFoldsFailsThatTurnNotTheJob() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    long fireAt = 5_000;
    OperatorSubtaskState savepoint;
    try (var old =
        new KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult>(
            new KeyedProcessOperator<>(new GappedLogWriter(fireAt)),
            Event::conversationId,
            Types.STRING)) {
      old.setup(
          TurnResultTypeInfo.INSTANCE.createSerializer(
              old.getExecutionConfig().getSerializerConfig()));
      old.open();
      old.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund please"), 1));
      savepoint = old.snapshot(1, 1);
    }
    Map<String, Object> wf =
        Workflows.withFlink(Workflows.approval(), Map.of("resume_after_ms", 1000));
    try (var h = harness(wf)) {
      h.initializeState(savepoint);
      h.open();
      clockAt(h, fireAt);
      TurnResult r = last(h);
      assertEquals(TurnStatus.FAILED, r.status);
      assertEquals("fatal", r.error.errorClass().wire());
      assertEquals(tid, r.turnId);
      assertEquals(
          1,
          h.getSideOutput(WorkflowTurnFunction.TURN_FAILURES).size(),
          "it reaches the dead letter stream");
    }
  }
}
