package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.agentic.flink.runtime.testkit.MiniClusterWorkflowDriver;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.WorkflowValidator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs {@link WorkflowTurnFunction} as a real Flink job on a MiniCluster and exercises the
 * properties Flink is responsible for: keyed-log idempotency across duplicate delivery, savepoint
 * restart and checkpoint recovery without re-invoking tools, timer-driven resume, state TTL,
 * per-key ordering, error handling, and a Kryo-free pipeline.
 */
class WorkflowTurnFunctionMiniClusterTest {

  static MiniCluster cluster;

  @TempDir
  static Path savepoints;

  @BeforeAll
  static void startCluster() throws Exception {
    cluster = TestClusters.start(4);
  }

  @AfterAll
  static void stopCluster() throws Exception {
    cluster.close();
  }

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  private static List<String> types(TurnResult r) {
    List<String> out = new ArrayList<>();
    for (LogEvent e : r.events) {
      out.add(e.type());
    }
    return out;
  }

  private static MiniClusterWorkflowDriver driver(Map<String, Object> wf) {
    return new MiniClusterWorkflowDriver(cluster, wf, FlinkRuntimeOptions.fromSpec(wf), savepoints.resolve(rnd("sp")));
  }

  @Test
  void executesWorkflowWithStructuredToolCalls() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    try (MiniClusterWorkflowDriver d = driver(Workflows.billing())) {
      d.start();
      TurnResult r = d.submit(Event.turn(cid, tid, "alice", "what is my balance?"));
      assertEquals(TurnStatus.COMPLETED, r.status);
      assertEquals("billing", r.path);
      assertEquals(1, r.calls.size());
      assertEquals("lookup_charge", r.calls.get(0).tool());
      assertEquals(Map.of("user", "alice"), r.calls.get(0).args());
      List<String> t = types(r);
      assertTrue(t.indexOf("turn_received") < t.indexOf("routed")
          && t.indexOf("routed") < t.indexOf("tool_called")
          && t.indexOf("tool_called") < t.indexOf("reply_drafted")
          && t.indexOf("reply_drafted") < t.indexOf("turn_completed"), t.toString());
      for (int i = 0; i < r.events.size(); i++) {
        assertEquals(i, r.events.get(i).sequence(), "dense zero-based sequence");
      }
      assertEquals(1L, ((Number) r.state.get("turn_count")).longValue());
    }
  }

  @Test
  void duplicateDeliveryIsDetectedAndDoesNotReinvokeTools() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    try (MiniClusterWorkflowDriver d = driver(Workflows.billing())) {
      d.start();
      TurnResult first = d.submit(Event.turn(cid, tid, "u", "what is my balance?"));
      assertEquals(TurnStatus.COMPLETED, first.status);
      TurnResult dup = d.submit(Event.turn(cid, tid, "u", "what is my balance?"));
      assertEquals(TurnStatus.DUPLICATE, dup.status);
      assertEquals(first.reply, dup.reply);
      assertTrue(dup.events.isEmpty(), "a duplicate appends nothing to the log");
      assertEquals(1L, ((Number) dup.state.get("turn_count")).longValue());

      TurnResult next = d.submit(Event.turn(cid, rnd("t"), "u", "hello"));
      assertEquals(TurnStatus.COMPLETED, next.status);
      assertEquals(2L, ((Number) next.state.get("turn_count")).longValue());
      assertEquals(first.events.size(), next.events.get(0).sequence(),
          "the second turn continues the same keyed log");
    }
  }

  @Test
  void savepointRestartRestoresLogAndKeepsIdempotency() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    try (MiniClusterWorkflowDriver d = driver(Workflows.billing())) {
      d.start();
      TurnResult first = d.submit(Event.turn(cid, tid, "u", "my balance"));
      assertEquals(TurnStatus.COMPLETED, first.status);
      assertEquals(1, first.calls.size());

      d.restart();

      TurnResult dup = d.submit(Event.turn(cid, tid, "u", "my balance"));
      assertEquals(TurnStatus.DUPLICATE, dup.status, "turn_id idempotency survives restart");
      assertTrue(dup.events.isEmpty(), "no brain or tool invocation for an already recorded turn");

      TurnResult second = d.submit(Event.turn(cid, rnd("t"), "u", "hi"));
      assertEquals(TurnStatus.COMPLETED, second.status);
      assertEquals(2L, ((Number) second.state.get("turn_count")).longValue(), "state is the fold of the restored log");
      assertEquals(first.events.size(), second.events.get(0).sequence());
    }
  }

  @Test
  void checkpointRecoveryAfterFailureKeepsRecordedTurns() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    try (MiniClusterWorkflowDriver d = driver(Workflows.billing())) {
      d.start();
      TurnResult first = d.submit(Event.turn(cid, tid, "u", "balance please"));
      assertEquals(TurnStatus.COMPLETED, first.status);
      d.checkpoint();

      d.failJobOnce();
      assertEquals(JobStatus.RUNNING, d.status());

      TurnResult dup = d.submit(Event.turn(cid, tid, "u", "balance please"));
      assertEquals(TurnStatus.DUPLICATE, dup.status, "the recovered log still holds the turn");
      assertTrue(dup.calls.isEmpty() || dup.events.isEmpty());
      TurnResult second = d.submit(Event.turn(cid, rnd("t"), "u", "thanks"));
      assertEquals(2L, ((Number) second.state.get("turn_count")).longValue());
    }
  }

  @Test
  void keyedOrderingPreservesTurnOrderWithinAConversation() throws Exception {
    String cid = rnd("c");
    int n = 5 + ThreadLocalRandom.current().nextInt(5);
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      events.add(Event.turn(cid, "t" + i + "-" + UUID.randomUUID(), "u", i % 2 == 0 ? "balance" : "hello"));
    }
    try (MiniClusterWorkflowDriver d = driver(Workflows.billing())) {
      d.start();
      List<TurnResult> results = d.submitAll(events);
      assertEquals(n, results.size());
      for (int i = 0; i < n; i++) {
        assertEquals(events.get(i).turnId(), results.get(i).turnId, "results arrive in submission order");
        assertEquals(i + 1L, ((Number) results.get(i).state.get("turn_count")).longValue());
      }
    }
  }

  @Test
  void suspendedTurnResumesFromRegisteredTimer() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    Map<String, Object> wf = Workflows.withFlink(Workflows.approval(), Map.of("resume_after_ms", 300));
    try (MiniClusterWorkflowDriver d = driver(wf)) {
      d.start();
      TurnResult suspended = d.submit(Event.turn(cid, tid, "u", "refund my last charge"));
      assertEquals(TurnStatus.SUSPENDED, suspended.status);
      List<String> t = types(suspended);
      assertTrue(t.contains("turn_suspended"), t.toString());
      assertEquals("timer_scheduled", t.get(t.size() - 1));
      Map<String, Object> scheduled = suspended.events.get(suspended.events.size() - 1).payload();
      assertEquals(tid, scheduled.get("turn_id"));
      assertEquals("processing_time", scheduled.get("domain"));

      TurnResult resumed = d.awaitNext();
      assertEquals(tid, resumed.turnId);
      assertEquals(TurnStatus.COMPLETED, resumed.status);
      List<String> rt = types(resumed);
      assertEquals("timer_fired", rt.get(0));
      assertTrue(rt.indexOf("turn_resumed") < rt.indexOf("turn_completed"), rt.toString());
      assertEquals(scheduled.get("timer_id"), resumed.events.get(0).payload().get("timer_id"));
      assertEquals(resumed.events.get(0).sequence(), suspended.events.get(suspended.events.size() - 1).sequence() + 1,
          "timer_fired continues the same keyed log");
    }
  }

  @Test
  void registeredTimerSurvivesSavepointRestart() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    Map<String, Object> wf = Workflows.withFlink(Workflows.approval(), Map.of("resume_after_ms", 1500));
    try (MiniClusterWorkflowDriver d = driver(wf)) {
      d.start();
      TurnResult suspended = d.submit(Event.turn(cid, tid, "u", "refund"));
      assertEquals(TurnStatus.SUSPENDED, suspended.status);
      d.restart();
      TurnResult resumed = d.awaitNext(r -> tid.equals(r.turnId) && r.status == TurnStatus.COMPLETED);
      assertEquals("timer_fired", resumed.events.get(0).type());
    }
  }

  @Test
  void explicitSignalBeforeTimerCompletesOnceAndTimerFiringIsOnlyRecorded() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    Map<String, Object> wf = Workflows.withFlink(Workflows.approval(), Map.of("resume_after_ms", 700));
    try (MiniClusterWorkflowDriver d = driver(wf)) {
      d.start();
      d.submit(Event.turn(cid, tid, "u", "refund"));
      TurnResult resumed = d.submit(Event.resume(cid, tid, Map.of("kind", "approval", "approved", true)));
      assertEquals(TurnStatus.COMPLETED, resumed.status, () -> d.allResults().toString());
      Thread.sleep(1200);
      TurnResult after = d.submit(Event.turn(cid, rnd("t"), "u", "another refund"));
      assertEquals(TurnStatus.SUSPENDED, after.status);
      assertEquals(2L, ((Number) after.state.get("turn_count")).longValue(), "the fired timer did not add a turn");
      assertEquals(1, d.allResults().stream().filter(r -> tid.equals(r.turnId) && r.status == TurnStatus.COMPLETED)
          .count(), "the turn completed exactly once");
    }
  }

  @Test
  void stateTtlExpiresTheConversationLog() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    Map<String, Object> wf = Workflows.withFlink(Workflows.billing(), Map.of("state_ttl_ms", 400));
    try (MiniClusterWorkflowDriver d = driver(wf)) {
      d.start();
      TurnResult first = d.submit(Event.turn(cid, tid, "u", "balance"));
      assertEquals(TurnStatus.COMPLETED, first.status);
      TurnResult dupWithinTtl = d.submit(Event.turn(cid, tid, "u", "balance"));
      assertEquals(TurnStatus.DUPLICATE, dupWithinTtl.status);

      Thread.sleep(1500);

      TurnResult afterTtl = d.submit(Event.turn(cid, tid, "u", "balance"));
      assertEquals(TurnStatus.COMPLETED, afterTtl.status, "an expired log no longer remembers the turn");
      assertEquals(1L, ((Number) afterTtl.state.get("turn_count")).longValue());
      assertEquals(0L, afterTtl.events.get(0).sequence(), "the sequence counter expired with the log");
    }
  }

  @Test
  void failingToolEndsTurnAsFailedAndJobKeepsRunning() throws Exception {
    String cid = rnd("c");
    try (MiniClusterWorkflowDriver d = driver(Workflows.failingTool())) {
      d.start();
      TurnResult failed = d.submit(Event.turn(cid, rnd("t"), "u", "look up this charge"));
      assertEquals(TurnStatus.FAILED, failed.status);
      assertEquals("tool", failed.error.errorClass().wire());
      assertEquals(1, failed.calls.size());
      assertNotEquals(null, failed.calls.get(0).error());
      assertTrue(types(failed).contains("tool_failed"));
      assertFalse(types(failed).contains("turn_completed"));

      TurnResult next = d.submit(Event.turn(cid, rnd("t"), "u", "hello there"));
      assertEquals(TurnStatus.COMPLETED, next.status);
      assertEquals(JobStatus.RUNNING, d.status());
      assertNull(next.error);
    }
  }

  @Test
  void pipelineRunsWithGenericTypesDisabled() throws Exception {
    String cid = rnd("c");
    Map<String, Object> wf = Workflows.billing();
    try (MiniClusterWorkflowDriver d = new MiniClusterWorkflowDriver(cluster, wf, FlinkRuntimeOptions.fromSpec(wf),
        savepoints.resolve(rnd("sp")), Duration.ofSeconds(60), true, true)) {
      d.start();
      TurnResult r = d.submit(Event.turn(cid, rnd("t"), "u", "balance"));
      assertEquals(TurnStatus.COMPLETED, r.status);
      d.restart();
      TurnResult r2 = d.submit(Event.turn(cid, rnd("t"), "u", "balance"));
      assertEquals(2L, ((Number) r2.state.get("turn_count")).longValue());
    }
  }

  @Test
  void invalidWorkflowFailsFastOnTheClient() {
    Map<String, Object> wf = new java.util.HashMap<>(Workflows.billing());
    wf.put("spec_version", "agentic/v" + (2 + ThreadLocalRandom.current().nextInt(9)));
    assertThrows(WorkflowValidator.WorkflowValidationException.class, () -> new WorkflowTurnFunction(wf));
  }
}
