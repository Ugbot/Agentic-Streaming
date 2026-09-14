package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

/**
 * {@code context.compaction: window}: the fold retains the most recent {@code max_items} messages in
 * log order, {@code transcript_length} reports what is retained, the log and {@code turn_count} are
 * untouched, and {@code none} retains everything.
 */
class ContextWindowTest {

  private static String rnd() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  /** One completed turn: turn_received, memory_written (two messages), turn_completed. */
  private static void completeTurn(ConversationLog log, String cid, int i) {
    String tid = "t" + i;
    log.append(cid, tid, EventType.TURN_RECEIVED, Map.of("turn_id", tid, "text", "u" + i));
    List<Map<String, Object>> messages = new ArrayList<>();
    messages.add(Map.of("role", "user", "text", "u" + i));
    messages.add(Map.of("role", "assistant", "text", "a" + i));
    log.append(cid, tid, EventType.MEMORY_WRITTEN, Map.of("messages", messages));
    log.append(cid, tid, EventType.TURN_COMPLETED, Map.of("reply", "a" + i));
  }

  @Test
  void windowRetainsTheMostRecentMaxItemsInOrderAndCountsAllTurns() {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    int maxItems = r.nextInt(1, 9);
    int turns = r.nextInt(1, 12);
    int written = 2 * turns;
    ConversationLog log = new ConversationLog.InMemory();
    String cid = "c-" + rnd();
    for (int i = 0; i < turns; i++) {
      completeTurn(log, cid, i);
    }
    ConversationState full = log.state(cid);
    ConversationState windowed = log.state(cid, ContextWindow.window(maxItems));

    assertEquals(written, full.transcriptLength(), "no window keeps every message");
    assertEquals(Math.min(written, maxItems), windowed.transcriptLength());
    assertEquals(Math.min(written, maxItems), windowed.transcript().size());
    assertEquals(turns, windowed.turnCount(), "turn_count is unaffected by compaction");
    assertEquals(full.turnCount(), windowed.turnCount());
    assertEquals(full.transcript().subList(written - windowed.transcript().size(), written),
        windowed.transcript(), "the retained messages are the most recent, in log order");
    assertEquals(3L * turns, log.events(cid).size(), "the log itself is never compacted");
    assertEquals(full.turns(), windowed.turns(), "per-turn records are unaffected");
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("turn_count", (long) turns);
    expected.put("transcript_length", (long) Math.min(written, maxItems));
    assertEquals(expected, windowed.reduced());
  }

  @Test
  void conversationsAreWindowedIndependently() {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    int maxItems = r.nextInt(2, 7);
    int turnsA = r.nextInt(maxItems, maxItems + 6);
    int turnsB = r.nextInt(1, maxItems);
    ConversationLog log = new ConversationLog.InMemory();
    String a = "a-" + rnd();
    String b = "b-" + rnd();
    for (int i = 0; i < turnsA; i++) {
      completeTurn(log, a, i);
    }
    for (int i = 0; i < turnsB; i++) {
      completeTurn(log, b, i);
    }
    ContextWindow w = ContextWindow.window(maxItems);
    assertEquals(maxItems, log.state(a, w).transcriptLength());
    assertEquals(turnsA, log.state(a, w).turnCount());
    assertEquals(Math.min(2 * turnsB, maxItems), log.state(b, w).transcriptLength());
    assertEquals(turnsB, log.state(b, w).turnCount());
  }

  @Test
  void noneAndMoscowLeaveTheRetainedTranscriptAlone() {
    int turns = ThreadLocalRandom.current().nextInt(1, 10);
    ConversationLog log = new ConversationLog.InMemory();
    String cid = "c-" + rnd();
    for (int i = 0; i < turns; i++) {
      completeTurn(log, cid, i);
    }
    ConversationState none = log.state(cid, ContextWindow.fromMap(Map.of("compaction", "none", "max_items", 1)));
    ConversationState moscow = log.state(cid, ContextWindow.fromMap(Map.of("compaction", "moscow", "max_tokens", 8)));
    assertEquals(2L * turns, none.transcriptLength());
    assertEquals(2L * turns, moscow.transcriptLength());
    assertEquals(log.state(cid), none);
    assertEquals(log.state(cid), moscow);
    assertSame(ContextWindow.NONE, ContextWindow.fromMap(null));
    assertFalse(ContextWindow.fromMap(Map.of("max_items", 3)).bounded(), "compaction defaults to none");
  }

  @Test
  void retainKeepsTheTailAndFromMapValidatesTheWindow() {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    int maxItems = r.nextInt(1, 6);
    int n = r.nextInt(maxItems, maxItems + 10);
    List<Integer> items = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      items.add(i);
    }
    ContextWindow w = ContextWindow.fromMap(Map.of("compaction", "window", "max_items", maxItems));
    assertTrue(w.bounded());
    assertEquals(items.subList(n - maxItems, n), w.retain(items));
    assertEquals(maxItems, w.retainedLength(n));
    assertEquals(0, w.retainedLength(0));
    assertSame(items, ContextWindow.NONE.retain(items));
    assertThrows(IllegalArgumentException.class, () -> ContextWindow.fromMap(Map.of("compaction", "window")));
    assertThrows(IllegalArgumentException.class, () -> ContextWindow.window(0));
  }

  @Test
  void aRunningWorkflowReportsTheRetainedWindowInResultState() {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    int maxItems = r.nextInt(1, 7);
    int turns = r.nextInt(1, 8);
    Map<String, Object> path = new LinkedHashMap<>();
    path.put("brain", "rule");
    path.put("prompt", "You chat.");
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "chat");
    agent.put("router", Map.of("kind", "keyword", "default", "main"));
    agent.put("paths", Map.of("main", path));
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("spec_version", "agentic/v1");
    spec.put("backend", "local");
    spec.put("context", Map.of("max_items", maxItems, "compaction", "window"));
    spec.put("agent", agent);
    org.jagentic.core.pipeline.GraphBuilder.Built built =
        org.jagentic.core.pipeline.GraphBuilder.build(spec, null);
    assertEquals(ContextWindow.window(maxItems), built.graph().contextWindow());
    ConversationLog log = new ConversationLog.InMemory();
    LocalRuntime rt = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), built.retriever(), log);
    String cid = "c-" + rnd();
    TurnResult last = null;
    for (int i = 1; i <= turns; i++) {
      last = rt.submit(Event.turn(cid, "t" + i, "u", "hello " + rnd()));
      assertEquals(TurnStatus.COMPLETED, last.status);
      assertEquals((long) i, last.state.get("turn_count"));
      assertEquals((long) Math.min(2 * i, maxItems), last.state.get("transcript_length"));
    }
    assertEquals(2L * turns, log.state(cid).transcriptLength(), "the log keeps every message");
    assertEquals(last.state, rt.replay(cid).reduced(), "replay reads the retained window too");
  }
}
