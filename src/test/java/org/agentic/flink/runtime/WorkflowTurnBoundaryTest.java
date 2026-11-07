package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.runtime.testkit.MiniClusterWorkflowDriver;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The turn boundary never throws. A per-key failure is a terminal {@code turn_failed} on the
 * conversation log, a result on the main output and a record on {@link
 * WorkflowTurnFunction#TURN_FAILURES}; the job keeps running and every other conversation with it.
 * These are the failures the architecture review proved as whole-job poison pills (C2, H3).
 */
class WorkflowTurnBoundaryTest {

  @TempDir java.nio.file.Path savepoints;

  private static String rnd(String p) {
    return p + "-" + UUID.randomUUID();
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> harness(
      Map<String, Object> wf) throws Exception {
    KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h =
        new KeyedOneInputStreamOperatorTestHarness<>(
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

  private static List<TurnResult> sideOutput(
      KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h) {
    List<TurnResult> out = new ArrayList<>();
    var side = h.getSideOutput(WorkflowTurnFunction.TURN_FAILURES);
    if (side != null) {
      for (StreamRecord<TurnResult> r : side) {
        out.add(r.getValue());
      }
    }
    return out;
  }

  private static final Map<String, Object> LOOKUP =
      Map.of("id", "lookup_charge", "kind", "constant", "effect", "read", "value", 42.5);

  private static List<String> types(TurnResult r) {
    return r.events.stream().map(LogEvent::type).toList();
  }

  /** A saga whose second step always fails and whose undo for the first always fails too. */
  private static Map<String, Object> sagaWithFailingUndo(Map<String, Object> firstStepTool) {
    Map<String, Object> wf =
        new LinkedHashMap<>(
            Workflows.onEveryPath(Workflows.billing(), "reserve", "post", "release"));
    wf.put(
        "tools",
        List.of(
            LOOKUP,
            firstStepTool,
            Map.of("id", "post", "kind", "failing", "effect", "idempotent"),
            Map.of("id", "release", "kind", "failing", "effect", "idempotent")));
    wf.put(
        "saga",
        Map.of(
            "steps",
            List.of(
                Map.of("name", "reserve", "tool", "reserve"),
                Map.of("name", "post", "tool", "post"))));
    return wf;
  }

  private static Map<String, Object> constantReserve() {
    return Map.of(
        "id",
        "reserve",
        "kind",
        "constant",
        "effect",
        "read",
        "value",
        1,
        "compensation",
        "release");
  }

  @Test
  void aFailingCompensationTerminatesTheTurnInsteadOfEscaping() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    try (var h = harness(sagaWithFailingUndo(constantReserve()))) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund"), 1));
      List<TurnResult> out = outputs(h);
      assertEquals(1, out.size());
      TurnResult r = out.get(0);
      assertEquals(TurnStatus.FAILED, r.status);
      assertEquals("fatal", r.error.errorClass().wire());
      assertTrue(r.error.message().contains("release"), r.error.message());
      assertTrue(types(r).contains("compensation_started"), types(r).toString());
      assertFalse(types(r).contains("compensation_completed"), types(r).toString());
      assertEquals("turn_failed", types(r).get(types(r).size() - 1));
      assertEquals(1, sideOutput(h).size());
      assertEquals(tid, sideOutput(h).get(0).turnId);

      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund"), 2));
      TurnResult again = outputs(h).get(1);
      assertEquals(TurnStatus.DUPLICATE, again.status, "the failure is recorded, not re-run");
      assertEquals(1, sideOutput(h).size(), "a duplicate is not a new failure");
    }
  }

  @Test
  void aSagaTurnWithNoUserIdRunsTheSaga() throws Exception {
    Map<String, Object> wf =
        new LinkedHashMap<>(Workflows.onEveryPath(Workflows.billing(), "reserve"));
    wf.put(
        "tools",
        List.of(LOOKUP, Map.of("id", "reserve", "kind", "constant", "effect", "read", "value", 1)));
    wf.put("saga", Map.of("steps", List.of(Map.of("name", "reserve", "tool", "reserve"))));
    try (var h = harness(wf)) {
      h.open();
      h.processElement(
          new StreamRecord<>(new Event(rnd("c"), rnd("t"), null, "refund", Map.of(), null), 1));
      TurnResult r = outputs(h).get(0);
      assertEquals(TurnStatus.COMPLETED, r.status);
      assertTrue(sideOutput(h).isEmpty());
    }
  }

  @Test
  void aFailingTimerToolFailsThatTurnAndTheNextOneRuns() throws Exception {
    Map<String, Object> wf = new LinkedHashMap<>(Workflows.billing());
    wf.put(
        "tools", List.of(LOOKUP, Map.of("id", "nudge", "kind", "failing", "effect", "idempotent")));
    wf.put("timers", List.of(Map.of("id", "followup", "after_ms", 10, "tool", "nudge")));
    wf.put("policies", Map.of("unattended_tools", List.of("nudge")));
    String clock = rnd("clock");
    FlinkRuntimeOptions options = FlinkRuntimeOptions.fromSpec(wf).withManualClock(clock);
    String cid = rnd("c");
    var h =
        new KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult>(
            new KeyedProcessOperator<>(new WorkflowTurnFunction(wf, options)),
            Event::conversationId,
            Types.STRING);
    h.setup(
        TurnResultTypeInfo.INSTANCE.createSerializer(h.getExecutionConfig().getSerializerConfig()));
    try (h) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "hello"), 1));
      assertEquals(TurnStatus.COMPLETED, outputs(h).get(0).status);
      ManualProcessingClock.named(clock).advance(100);
      h.processElement(new StreamRecord<>(Event.turn(cid, "t2", "u", "hello"), 2));
      TurnResult failed = outputs(h).get(1);
      assertEquals(TurnStatus.FAILED, failed.status);
      assertEquals("tool", failed.error.errorClass().wire());
      assertEquals(
          List.of("timer_fired", "tool_intent", "tool_failed", "turn_failed"), types(failed));
      h.processElement(new StreamRecord<>(Event.turn(cid, "t3", "u", "hello"), 3));
      assertEquals(TurnStatus.COMPLETED, outputs(h).get(2).status);
    }
  }

  @Test
  void aNullTextIsAPerKeyFailureNotAJobFailure() throws Exception {
    String cid = rnd("c");
    try (var h = harness(Workflows.billing())) {
      h.open();
      h.processElement(new StreamRecord<>(new Event(cid, "bad", "u", null, Map.of(), null), 1));
      TurnResult r = outputs(h).get(0);
      assertEquals(TurnStatus.FAILED, r.status);
      assertEquals("fatal", r.error.errorClass().wire());
      assertEquals("turn_failed", types(r).get(types(r).size() - 1));
      assertEquals(1, sideOutput(h).size());

      h.processElement(new StreamRecord<>(Event.turn(cid, "good", "u", "balance"), 2));
      assertEquals(TurnStatus.COMPLETED, outputs(h).get(1).status, "the key keeps working");
    }
  }

  @Test
  void anEventTimeResumeWithoutATimestampFailsTheTurnAndCannotBeResumed() throws Exception {
    Map<String, Object> wf =
        Workflows.withFlink(
            Workflows.approval(), Map.of("resume_after_ms", 1000, "timer_domain", "event_time"));
    String cid = rnd("c");
    String tid = rnd("t");
    try (var h = harness(wf)) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "refund please")));
      TurnResult r = outputs(h).get(0);
      assertEquals(TurnStatus.FAILED, r.status);
      assertEquals("fatal", r.error.errorClass().wire());
      assertTrue(r.error.message().contains("timestamps"), r.error.message());
      assertEquals(1, sideOutput(h).size());

      h.processElement(new StreamRecord<>(Event.resume(cid, tid, Map.of("approved", true))));
      assertEquals(
          TurnStatus.DUPLICATE, outputs(h).get(1).status, "a failed turn is no longer suspended");
    }
  }

  @Test
  void aTransientModelFailureGoesToTheSideOutputAndRunsAgainOnRedelivery() throws Exception {
    Map<String, Object> wf = new LinkedHashMap<>(Workflows.billing());
    @SuppressWarnings("unchecked")
    Map<String, Object> agent = new LinkedHashMap<>((Map<String, Object>) wf.get("agent"));
    agent.put(
        "paths", Map.of("general", Map.of("brain", "llm"), "billing", Map.of("brain", "llm")));
    wf.put("agent", agent);
    wf.put(
        "llm",
        Map.of("provider", "stub", "script", List.of(Map.of("text", "ok", "x-fail-attempts", 1))));
    String cid = rnd("c");
    String tid = rnd("t");
    try (var h = harness(wf)) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "hello"), 1));
      TurnResult failed = outputs(h).get(0);
      assertEquals(TurnStatus.FAILED, failed.status);
      assertEquals("transient", failed.error.errorClass().wire());
      assertEquals(1, sideOutput(h).size());

      h.processElement(new StreamRecord<>(Event.turn(cid, tid, "u", "hello"), 2));
      assertEquals(TurnStatus.COMPLETED, outputs(h).get(1).status);
      assertEquals(1, sideOutput(h).size());
    }
  }

  /** Port of the review's Loop.java: one turn on a real job used to make six external calls. */
  @Test
  void aFailingCompensationDoesNotRestartTheJobOrRepeatTheEffect() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/reserve",
        e -> {
          calls.incrementAndGet();
          e.getRequestBody().readAllBytes();
          byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
          e.sendResponseHeaders(200, body.length);
          e.getResponseBody().write(body);
          e.close();
        });
    server.start();
    Map<String, Object> wf =
        sagaWithFailingUndo(
            Map.of(
                "id",
                "reserve",
                "kind",
                "http",
                "effect",
                "read",
                "url",
                "http://127.0.0.1:" + server.getAddress().getPort() + "/reserve",
                "compensation",
                "release"));
    MiniCluster cluster = TestClusters.start(2);
    String cid = rnd("c");
    try (MiniClusterWorkflowDriver d =
        new MiniClusterWorkflowDriver(
            cluster, wf, FlinkRuntimeOptions.fromSpec(wf), savepoints.resolve(rnd("sp")))) {
      d.start();
      TurnResult failed = d.submit(Event.turn(cid, rnd("t"), "alice", "refund"));
      assertEquals(TurnStatus.FAILED, failed.status);
      assertNotNull(failed.error);
      assertEquals(1, calls.get(), "one turn, one external call: no restart loop");
      TurnResult other = d.submit(Event.turn(rnd("c"), rnd("t"), "bob", "refund"));
      assertEquals(TurnStatus.FAILED, other.status, "the job is still serving other keys");
      assertEquals(2, calls.get());
    } finally {
      cluster.close();
      server.stop(0);
    }
  }
}
