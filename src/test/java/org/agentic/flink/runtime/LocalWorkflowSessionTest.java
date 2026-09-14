package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.JobStatus;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link LocalWorkflowSession} on its own per-job local cluster: turns flow through the in-JVM
 * queue, a restart stops with a savepoint and restores it, and the restored conversation is
 * rebuilt from the log without calling the tool again.
 */
class LocalWorkflowSessionTest {

  @TempDir
  static Path savepoints;

  static HttpServer tools;
  static final AtomicInteger LOOKUPS = new AtomicInteger();
  static double balance;

  @BeforeAll
  static void startToolServer() throws Exception {
    balance = ThreadLocalRandom.current().nextInt(1, 10_000) / 100.0;
    tools = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    tools.createContext("/lookup", exchange -> {
      LOOKUPS.incrementAndGet();
      byte[] body = ("{\"value\":" + balance + "}").getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    tools.start();
  }

  @AfterAll
  static void stopToolServer() {
    tools.stop(0);
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

  /** {@link Workflows#billing()} with {@code lookup_charge} served over HTTP so calls can be counted. */
  private static Map<String, Object> billingOverHttp() {
    Map<String, Object> wf = new LinkedHashMap<>(Workflows.billing());
    String url = "http://127.0.0.1:" + tools.getAddress().getPort() + "/lookup";
    wf.put("tools", List.of(Map.of("id", "lookup_charge", "kind", "http", "description", "Last charge", "url", url)));
    return wf;
  }

  private static LocalWorkflowSession session(Map<String, Object> wf) {
    int parallelism = ThreadLocalRandom.current().nextInt(1, 3);
    return new LocalWorkflowSession(wf, FlinkRuntimeOptions.fromSpec(wf), parallelism, null,
        savepoints.resolve(rnd("sp")), Duration.ofSeconds(60), rnd("job"));
  }

  @Test
  void restartRestoresTheLogAndDoesNotCallToolsAgain() throws Exception {
    String cid = rnd("c");
    String t1 = rnd("t1");
    String t2 = rnd("t2");
    int before = LOOKUPS.get();
    try (LocalWorkflowSession s = session(billingOverHttp())) {
      s.start();
      assertEquals(JobStatus.RUNNING, s.status());
      assertNull(s.lastSavepoint());

      TurnResult first = s.submit(Event.turn(cid, t1, "alice", "what is my balance?"));
      assertEquals(TurnStatus.COMPLETED, first.status);
      assertEquals("billing", first.path());
      assertEquals(List.of("lookup_charge"), first.toolCalls);
      assertEquals(before + 1, LOOKUPS.get());
      assertEquals(1, ((Number) first.state.get("turn_count")).intValue());

      s.restart();
      assertEquals(1, s.restarts());
      assertNotNull(s.lastSavepoint());
      assertEquals(JobStatus.RUNNING, s.status());

      TurnResult replayed = s.submit(Event.turn(cid, t1, "alice", "what is my balance?"));
      assertEquals(TurnStatus.DUPLICATE, replayed.status);
      assertTrue(replayed.events.isEmpty(), "a replayed turn appends nothing: " + replayed.events);
      assertEquals(before + 1, LOOKUPS.get(), "the tool must not run again during recovery");

      TurnResult second = s.submit(Event.turn(cid, t2, "alice", "I lost my password"));
      assertEquals(TurnStatus.COMPLETED, second.status);
      assertEquals("general", second.path());
      assertTrue(second.toolCalls.isEmpty());
      assertEquals(2, ((Number) second.state.get("turn_count")).intValue(),
          "the conversation continues from the restored log");
      assertEquals(before + 1, LOOKUPS.get());
    }
  }

  @Test
  void suspendedTurnSurvivesRestartAndResumes() throws Exception {
    String cid = rnd("c");
    String tid = rnd("t");
    try (LocalWorkflowSession s = session(Workflows.approval())) {
      s.start();
      TurnResult parked = s.submit(Event.turn(cid, tid, "bob", "refund my last charge"));
      assertEquals(TurnStatus.SUSPENDED, parked.status);
      assertTrue(types(parked).contains(EventType.TURN_SUSPENDED.wire()), types(parked).toString());

      s.restart();

      TurnResult done = s.submit(Event.resume(cid, tid, Map.of("kind", "approval", "approved", true)));
      assertEquals(TurnStatus.COMPLETED, done.status);
      List<String> after = types(done);
      assertTrue(after.contains(EventType.TURN_RESUMED.wire()), after.toString());
      assertTrue(after.contains(EventType.TURN_COMPLETED.wire()), after.toString());
    }
  }

  @Test
  void turnsAcrossConversationsAreAlignedToTheirSubmission() throws Exception {
    int n = ThreadLocalRandom.current().nextInt(3, 7);
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      events.add(Event.turn(rnd("c"), rnd("t"), "carol", i % 2 == 0 ? "what is my balance?" : "hello"));
    }
    try (LocalWorkflowSession s = session(Workflows.billing())) {
      s.start();
      List<TurnResult> results = s.submitAll(events);
      assertEquals(n, results.size());
      for (int i = 0; i < n; i++) {
        assertEquals(events.get(i).conversationId(), results.get(i).conversationId);
        assertEquals(events.get(i).turnId(), results.get(i).turnId);
        assertEquals(i % 2 == 0 ? "billing" : "general", results.get(i).path());
      }
    }
  }

  @Test
  void backToBackTurnsOfOneConversationKeepSubmissionOrderAtHigherParallelism() throws Exception {
    String cid = rnd("c");
    int n = ThreadLocalRandom.current().nextInt(4, 9);
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      events.add(Event.turn(cid, rnd("t" + i), "erin", "hello"));
    }
    Map<String, Object> wf = Workflows.billing();
    try (LocalWorkflowSession s = new LocalWorkflowSession(wf, FlinkRuntimeOptions.fromSpec(wf), 3, null,
        savepoints.resolve(rnd("sp")), Duration.ofSeconds(60), rnd("job"))) {
      s.start();
      List<TurnResult> results = s.submitAll(events);
      for (int i = 0; i < n; i++) {
        assertEquals(events.get(i).turnId(), results.get(i).turnId);
        assertEquals(i + 1, ((Number) results.get(i).state.get("turn_count")).intValue(),
            "turn " + i + " must see every earlier turn of its conversation");
      }
    }
  }

  @Test
  void lifecycleIsGuarded() throws Exception {
    LocalWorkflowSession s = session(Workflows.billing());
    assertThrows(IllegalStateException.class, () -> s.submit(Event.turn(rnd("c"), rnd("t"), "dave", "hi")));
    assertThrows(IllegalStateException.class, s::restart);
    s.start();
    assertThrows(IllegalStateException.class, s::start);
    s.close();
    assertThrows(IllegalStateException.class, s::start);
  }
}
