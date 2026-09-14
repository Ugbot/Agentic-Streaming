package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
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
import org.jagentic.core.pipeline.GraphBuilder;
import org.junit.jupiter.api.Test;

/**
 * The {@code parallelism} primitive on {@link LocalRuntime}: N conversations with M turns each,
 * submitted in one shuffled interleaving. Different conversations must run at the same time (a
 * barrier tool that only trips when every conversation is inside a turn together), while each
 * conversation stays a single writer whose state, transcript and tool calls never leak into
 * another conversation.
 */
class ParallelConversationsTest {

  private static final String MEET = "meet";
  private static final String PROBE = "probe";

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
    s.put("backend", "local");
    s.put("agent", agent);
    s.put("tools", List.of(Map.of("id", MEET, "kind", "constant", "value", "m"),
        Map.of("id", PROBE, "kind", "constant", "value", "p")));
    return s;
  }

  private record Planned(String conversationId, String turnId, String text, boolean probes) {}

  /** M turns per conversation, then one random interleaving that keeps each conversation's own order. */
  private static List<Planned> shuffledPlan(List<String> conversations, int turnsEach) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    Map<String, List<Planned>> perConversation = new LinkedHashMap<>();
    for (String cid : conversations) {
      List<Planned> turns = new ArrayList<>();
      turns.add(new Planned(cid, cid + "-t0", "rendezvous from " + cid, false));
      for (int i = 1; i < turnsEach; i++) {
        boolean probes = rng.nextBoolean();
        turns.add(new Planned(cid, cid + "-t" + i, (probes ? "charge " : "hello ") + i + " from " + cid, probes));
      }
      perConversation.put(cid, turns);
    }
    List<Planned> interleaved = new ArrayList<>();
    List<String> remaining = new ArrayList<>(conversations);
    Map<String, Integer> cursor = new LinkedHashMap<>();
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
  void conversationsRunConcurrentlyWhileEachStaysASerialIsolatedWriter() throws Exception {
    int n = ThreadLocalRandom.current().nextInt(3, 8);
    int m = ThreadLocalRandom.current().nextInt(3, 9);
    List<String> conversations = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      conversations.add("c" + i + "-" + rnd());
    }
    List<Planned> plan = shuffledPlan(conversations, m);

    // Trips only when all N conversations are inside their first turn at the same time.
    CyclicBarrier barrier = new CyclicBarrier(n);
    Map<String, AtomicInteger> probesByUser = new ConcurrentHashMap<>();
    GraphBuilder.Built built = GraphBuilder.build(spec(), null);
    built.tools().register(MEET, "barrier", args -> {
      try {
        barrier.await(20, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new IllegalStateException("conversations did not overlap: " + e, e);
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
    LocalRuntime rt = new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), built.retriever(), new ConversationLog.InMemory());

    Map<Planned, CompletableFuture<TurnResult>> futures = new LinkedHashMap<>();
    for (Planned p : plan) {
      // The user id doubles as the conversation id so tool args reveal which conversation called.
      futures.put(p, rt.submitAsync(Event.turn(p.conversationId(), p.turnId(), p.conversationId(), p.text())));
    }
    CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
    assertFalse(barrier.isBroken(), "every conversation reached the barrier together");

    for (Map.Entry<Planned, CompletableFuture<TurnResult>> e : futures.entrySet()) {
      Planned p = e.getKey();
      TurnResult r = e.getValue().get();
      assertSame(TurnStatus.COMPLETED, r.status, () -> p.turnId() + ": " + r.error);
      assertEquals(p.conversationId(), r.conversationId);
      assertEquals(p.turnId(), r.turnId);
      assertEquals(p.probes() || p.text().startsWith("rendezvous") ? 1 : 0, r.calls.size(), p.turnId());
      for (ToolCall call : r.calls) {
        assertEquals(p.conversationId(), call.args().get("user"), "tool args belong to the calling conversation");
        assertTrue(String.valueOf(call.result()).endsWith(p.conversationId())
            || String.valueOf(call.result()).contains(":" + p.conversationId() + ":"), call.toString());
      }
      for (LogEvent ev : r.events) {
        assertEquals(p.conversationId(), ev.conversationId());
        assertEquals(p.turnId(), ev.turnId());
      }
    }

    for (String cid : conversations) {
      List<Planned> own = plan.stream().filter(p -> p.conversationId().equals(cid)).toList();
      List<LogEvent> events = rt.log().events(cid);
      for (int i = 0; i < events.size(); i++) {
        assertEquals(i, events.get(i).sequence(), cid + " has a gapless sequence");
        assertEquals(cid, events.get(i).conversationId());
      }
      List<String> receivedOrder = events.stream().filter(ev -> ev.is(EventType.TURN_RECEIVED))
          .map(LogEvent::turnId).toList();
      assertEquals(own.stream().map(Planned::turnId).toList(), receivedOrder, "arrival order kept within " + cid);
      String open = null;
      for (LogEvent ev : events) {
        if (ev.is(EventType.TURN_RECEIVED)) {
          assertNull(open, "a turn of " + cid + " started while " + open + " was open");
          open = ev.turnId();
        } else {
          assertEquals(open, ev.turnId(), "events of one turn never interleave within " + cid);
          if (ev.is(EventType.TURN_COMPLETED)) {
            open = null;
          }
        }
      }
      assertNull(open, "every turn of " + cid + " reached a terminal event");

      ConversationState folded = rt.replay(cid);
      assertEquals(m, folded.turnCount(), "turn_count counts only " + cid + "'s turns");
      assertEquals(2L * m, folded.transcriptLength(), "transcript holds only " + cid + "'s messages");
      List<ChatMessage> history = rt.store().history(cid);
      assertEquals(2 * m, history.size());
      for (ChatMessage msg : history) {
        assertTrue(msg.content().contains(cid), "transcript of " + cid + " leaked: " + msg.content());
      }
      TurnResult last = futures.get(own.get(own.size() - 1)).get();
      assertEquals((long) m, last.state.get("turn_count"));
      assertEquals(2L * m, last.state.get("transcript_length"));

      long expectedProbes = own.stream().filter(Planned::probes).count();
      assertEquals(expectedProbes, probesByUser.getOrDefault(cid, new AtomicInteger()).get(),
          "probe calls attributed to " + cid);
      long recordedProbes = own.stream().map(futures::get).map(CompletableFuture::join)
          .flatMap(r -> r.calls.stream()).filter(c -> PROBE.equals(c.tool())).count();
      assertEquals(expectedProbes, recordedProbes);
    }
    int totalProbes = probesByUser.values().stream().mapToInt(AtomicInteger::get).sum();
    assertEquals(plan.stream().filter(Planned::probes).count(), totalProbes, "no probe ran twice or went missing");
  }
}
