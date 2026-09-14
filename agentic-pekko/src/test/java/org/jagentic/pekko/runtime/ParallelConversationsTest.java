package org.jagentic.pekko.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.ToolCall;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;

/**
 * The {@code parallelism} primitive on the Pekko runtime: N conversation entities with M turns
 * each, delivered in one shuffled interleaving without waiting. A barrier tool that only trips
 * when every entity is inside its first turn together proves the entities run concurrently; the
 * per-entity event sequences, state and tool arguments prove that each entity's mailbox keeps its
 * own turns serial and that nothing leaks between conversations.
 */
class ParallelConversationsTest {

  private static final String MEET = "meet";
  private static final String PROBE = "probe";
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static String rnd() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  private static Map<String, Object> spec() {
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "a-" + rnd());
    agent.put("router", Map.of("kind", "keyword", "default", "main", "rules", Map.of("main", List.of("charge"))));
    agent.put("paths", Map.of("main", Map.of("brain", "rule", "prompt", "p",
        "tool_triggers", Map.of("rendezvous", MEET, "charge", PROBE))));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("spec_version", "agentic/v1");
    s.put("backend", "pekko");
    s.put("agent", agent);
    s.put("tools", List.of(Map.of("id", MEET, "kind", "constant", "value", "m"),
        Map.of("id", PROBE, "kind", "constant", "value", "p")));
    return s;
  }

  private record Planned(String conversationId, String turnId, String text, String tool) {}

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
  void entitiesRunConcurrentlyWhileEachMailboxStaysASerialIsolatedWriter() throws Exception {
    int n = ThreadLocalRandom.current().nextInt(3, 8);
    int m = ThreadLocalRandom.current().nextInt(3, 8);
    List<String> conversations = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      conversations.add("c" + i + "-" + rnd());
    }
    List<Planned> plan = shuffledPlan(conversations, m);

    // Trips only when all N entities are inside their first turn at the same time.
    CyclicBarrier barrier = new CyclicBarrier(n);
    Map<String, AtomicInteger> probesByUser = new ConcurrentHashMap<>();
    GraphBuilder.Built built = GraphBuilder.build(spec(), null);
    built.tools().register(MEET, "barrier", args -> {
      try {
        barrier.await(20, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new IllegalStateException("entities did not overlap: " + e, e);
      }
      return "met:" + args.get("user");
    });
    built.tools().register(PROBE, "per-user counter", args -> {
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(0, 3));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      String user = String.valueOf(args.get("user"));
      return "probe:" + user + ":" + probesByUser.computeIfAbsent(user, k -> new AtomicInteger()).incrementAndGet();
    });

    Map<Planned, TurnResult> results = new LinkedHashMap<>();
    try (PekkoSystem sys = new PekkoSystem(new AgentDeps(built.graph(), built.tools(), built.retriever()));
         PekkoRuntime runtime = new PekkoRuntime(sys.system(), TIMEOUT)) {
      Map<Planned, CompletableFuture<TurnResult>> futures = new LinkedHashMap<>();
      for (Planned p : plan) {
        // The user id doubles as the conversation id so tool args reveal which entity called.
        futures.put(p, runtime.submitAsync(Event.turn(p.conversationId(), p.turnId(), p.conversationId(), p.text())));
      }
      CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
      for (Map.Entry<Planned, CompletableFuture<TurnResult>> e : futures.entrySet()) {
        results.put(e.getKey(), e.getValue().get());
      }
    }
    assertFalse(barrier.isBroken(), "every entity reached the barrier together");

    for (Map.Entry<Planned, TurnResult> e : results.entrySet()) {
      Planned p = e.getKey();
      TurnResult r = e.getValue();
      assertSame(TurnStatus.COMPLETED, r.status, () -> p.turnId() + ": " + r.error);
      assertEquals(p.conversationId(), r.conversationId);
      assertEquals(p.turnId(), r.turnId);
      assertEquals(p.tool() == null ? 0 : 1, r.calls.size(), p.turnId());
      for (ToolCall call : r.calls) {
        assertEquals(p.tool(), call.tool());
        assertEquals(p.conversationId(), call.args().get("user"), "tool args belong to the calling entity");
        assertTrue(String.valueOf(call.result()).contains(p.conversationId()), call.toString());
      }
      for (LogEvent ev : r.events) {
        assertEquals(p.conversationId(), ev.conversationId());
        assertEquals(p.turnId(), ev.turnId());
      }
    }

    for (String cid : conversations) {
      List<Planned> own = plan.stream().filter(p -> p.conversationId().equals(cid)).toList();
      long sequence = 0;
      String open = null;
      for (int i = 0; i < own.size(); i++) {
        TurnResult r = results.get(own.get(i));
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
      long expectedProbes = own.stream().filter(p -> PROBE.equals(p.tool())).count();
      assertEquals(expectedProbes, probesByUser.getOrDefault(cid, new AtomicInteger()).get(),
          "probe calls attributed to " + cid);
    }
    int totalProbes = probesByUser.values().stream().mapToInt(AtomicInteger::get).sum();
    assertEquals(plan.stream().filter(p -> PROBE.equals(p.tool())).count(), totalProbes,
        "no probe ran twice or went missing");
  }
}
