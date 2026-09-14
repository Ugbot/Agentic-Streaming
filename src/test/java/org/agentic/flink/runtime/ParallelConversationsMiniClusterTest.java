package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.runtime.testkit.MiniClusterWorkflowDriver;
import org.agentic.flink.runtime.testkit.TestClusters;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.ToolCall;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code parallelism} primitive on the Flink runtime: one job at a random parallelism above
 * one, N conversations whose keys span at least two subtasks, M turns each, delivered as a single
 * shuffled batch. An HTTP tool that holds each call until a call from another conversation is in
 * flight proves that distinct conversations execute concurrently; the per-conversation event
 * sequences, state and tool arguments prove that each conversation stays a serial, isolated writer.
 */
class ParallelConversationsMiniClusterTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String MEET = "meet";
  private static final String PROBE = "probe";

  static MiniCluster cluster;

  @TempDir
  static Path savepoints;

  private HttpServer server;
  private ExecutorService serverThreads;
  private final Object gate = new Object();
  private int inFlight;
  private int maxInFlight;
  private final Map<String, AtomicInteger> callsByUser = new ConcurrentHashMap<>();

  @BeforeAll
  static void startCluster() throws Exception {
    cluster = TestClusters.start(4);
  }

  @AfterAll
  static void stopCluster() throws Exception {
    cluster.close();
  }

  @BeforeEach
  void startToolServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    serverThreads = Executors.newCachedThreadPool();
    server.setExecutor(serverThreads);
    server.createContext("/" + MEET, ex -> {
      String user = user(ex);
      synchronized (gate) {
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        gate.notifyAll();
        long deadline = System.nanoTime() + 1_000_000_000L;
        try {
          while (inFlight < 2 && System.nanoTime() < deadline) {
            gate.wait(50);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          inFlight--;
        }
      }
      respond(ex, user, MEET);
    });
    server.createContext("/" + PROBE, ex -> respond(ex, user(ex), PROBE));
    server.start();
  }

  @AfterEach
  void stopToolServer() {
    server.stop(0);
    serverThreads.shutdownNow();
  }

  private String user(HttpExchange ex) throws IOException {
    Map<?, ?> body = JSON.readValue(ex.getRequestBody().readAllBytes(), Map.class);
    return String.valueOf(body.get("user"));
  }

  private void respond(HttpExchange ex, String user, String tool) throws IOException {
    int n = callsByUser.computeIfAbsent(user + "/" + tool, k -> new AtomicInteger()).incrementAndGet();
    byte[] out = JSON.writeValueAsBytes(Map.of("user", user, "tool", tool, "n", n));
    ex.getResponseHeaders().add("Content-Type", "application/json");
    ex.sendResponseHeaders(200, out.length);
    ex.getResponseBody().write(out);
    ex.close();
  }

  private Map<String, Object> workflow() {
    String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    Map<String, Object> main = new LinkedHashMap<>();
    main.put("brain", "rule");
    main.put("prompt", "You answer charge questions.");
    main.put("tool_triggers", Map.of("rendezvous", MEET, "charge", PROBE));
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "parallel-" + UUID.randomUUID());
    agent.put("router", Map.of("kind", "keyword", "default", "main", "rules", Map.of("main", List.of("charge"))));
    agent.put("paths", Map.of("main", main));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> wf = new HashMap<>();
    wf.put("spec_version", "agentic/v1");
    wf.put("backend", "local");
    wf.put("agent", agent);
    wf.put("tools", List.of(
        Map.of("id", MEET, "kind", "http", "description", "Waits for another conversation", "url", base + MEET),
        Map.of("id", PROBE, "kind", "http", "description", "Counts per user", "url", base + PROBE)));
    return wf;
  }

  private record Planned(String conversationId, String turnId, String text, String tool) {}

  /** Conversation ids whose key groups land on at least two of the {@code parallelism} subtasks. */
  private static List<String> conversationsSpanningSubtasks(int n, int parallelism) {
    int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
    while (true) {
      List<String> ids = new ArrayList<>();
      Set<Integer> subtasks = new HashSet<>();
      for (int i = 0; i < n; i++) {
        String cid = "c" + i + "-" + UUID.randomUUID().toString().substring(0, 8);
        ids.add(cid);
        subtasks.add(KeyGroupRangeAssignment.assignKeyToParallelOperator(cid, maxParallelism, parallelism));
      }
      if (subtasks.size() >= 2) {
        return ids;
      }
    }
  }

  /** M turns per conversation, then one random interleaving that keeps each conversation's own order. */
  private static List<Planned> shuffledPlan(List<String> conversations, int turnsEach) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    Map<String, List<Planned>> perConversation = new LinkedHashMap<>();
    for (String cid : conversations) {
      List<Planned> turns = new ArrayList<>();
      turns.add(new Planned(cid, cid + "-t0", "rendezvous from " + cid, MEET));
      for (int i = 1; i < turnsEach; i++) {
        boolean probes = rng.nextBoolean();
        turns.add(new Planned(cid, cid + "-t" + i, (probes ? "charge " : "hello ") + i + " from " + cid,
            probes ? PROBE : null));
      }
      perConversation.put(cid, turns);
    }
    List<Planned> interleaved = new ArrayList<>();
    List<String> remaining = new ArrayList<>(conversations);
    Map<String, Integer> cursor = new HashMap<>();
    while (!remaining.isEmpty()) {
      String cid = remaining.get(rng.nextInt(remaining.size()));
      int at = cursor.getOrDefault(cid, 0);
      interleaved.add(perConversation.get(cid).get(at));
      if (at + 1 == turnsEach) {
        remaining.remove(cid);
      } else {
        cursor.put(cid, at + 1);
      }
    }
    return interleaved;
  }

  @Test
  void conversationsRunOnDifferentSubtasksConcurrentlyAndStayIsolated() throws Exception {
    int parallelism = ThreadLocalRandom.current().nextInt(2, 5);
    int n = ThreadLocalRandom.current().nextInt(4, 9);
    int m = ThreadLocalRandom.current().nextInt(2, 6);
    List<String> conversations = conversationsSpanningSubtasks(n, parallelism);
    List<Planned> plan = shuffledPlan(conversations, m);
    Map<String, Object> wf = workflow();

    List<TurnResult> results;
    try (MiniClusterWorkflowDriver d = new MiniClusterWorkflowDriver(cluster, wf, FlinkRuntimeOptions.fromSpec(wf),
        savepoints.resolve("sp-" + UUID.randomUUID()), parallelism)) {
      d.start();
      List<Event> events = new ArrayList<>();
      for (Planned p : plan) {
        // The user id doubles as the conversation id so tool arguments reveal which conversation called.
        events.add(Event.turn(p.conversationId(), p.turnId(), p.conversationId(), p.text()));
      }
      results = d.submitAll(events);
    }
    assertEquals(plan.size(), results.size());
    synchronized (gate) {
      assertTrue(maxInFlight >= 2, "two conversations never held the tool at the same time; max in flight "
          + maxInFlight + " at parallelism " + parallelism);
    }

    Map<String, List<TurnResult>> byConversation = new LinkedHashMap<>();
    for (int i = 0; i < plan.size(); i++) {
      Planned p = plan.get(i);
      TurnResult r = results.get(i);
      assertSame(TurnStatus.COMPLETED, r.status, () -> p.turnId() + ": " + r.error);
      assertEquals(p.conversationId(), r.conversationId);
      assertEquals(p.turnId(), r.turnId);
      assertEquals(p.tool() == null ? 0 : 1, r.calls.size(), p.turnId());
      for (ToolCall call : r.calls) {
        assertEquals(p.tool(), call.tool());
        assertEquals(p.conversationId(), call.args().get("user"), "tool args belong to the calling conversation");
        assertEquals(p.conversationId(), ((Map<?, ?>) call.result()).get("user"), "tool result answers the caller");
      }
      for (LogEvent ev : r.events) {
        assertEquals(p.conversationId(), ev.conversationId());
        assertEquals(p.turnId(), ev.turnId());
      }
      byConversation.computeIfAbsent(p.conversationId(), k -> new ArrayList<>()).add(r);
    }

    for (String cid : conversations) {
      List<TurnResult> own = byConversation.get(cid);
      List<Planned> planned = plan.stream().filter(p -> p.conversationId().equals(cid)).toList();
      assertEquals(m, own.size());
      long sequence = 0;
      String open = null;
      for (int i = 0; i < own.size(); i++) {
        TurnResult r = own.get(i);
        assertEquals(planned.get(i).turnId(), r.turnId, "turns of " + cid + " complete in submission order");
        for (LogEvent ev : r.events) {
          assertEquals(sequence++, ev.sequence(), cid + " has one gapless sequence across its turns");
          if ("turn_received".equals(ev.type())) {
            assertNull(open, "a turn of " + cid + " started while " + open + " was open");
            open = ev.turnId();
          } else {
            assertEquals(open, ev.turnId(), "events of one turn never interleave within " + cid);
            if ("turn_completed".equals(ev.type())) {
              open = null;
            }
          }
        }
        assertEquals((long) (i + 1), ((Number) r.state.get("turn_count")).longValue(), "turn_count counts only " + cid);
        assertEquals(2L * (i + 1), ((Number) r.state.get("transcript_length")).longValue(),
            "transcript holds only " + cid + "'s messages");
      }
      assertNull(open, "every turn of " + cid + " reached a terminal event");
      long expectedProbes = planned.stream().filter(p -> PROBE.equals(p.tool())).count();
      assertEquals(1, callsByUser.getOrDefault(cid + "/" + MEET, new AtomicInteger()).get(), cid + " met once");
      assertEquals(expectedProbes, callsByUser.getOrDefault(cid + "/" + PROBE, new AtomicInteger()).get(),
          "probe calls attributed to " + cid);
    }
    int totalCalls = callsByUser.values().stream().mapToInt(AtomicInteger::get).sum();
    assertEquals(plan.stream().filter(p -> p.tool() != null).count(), totalCalls, "no call ran twice or went missing");
  }
}
