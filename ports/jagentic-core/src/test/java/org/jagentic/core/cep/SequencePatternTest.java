package org.jagentic.core.cep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.core.pipeline.WorkflowValidator;

/**
 * The {@code agentic/v1} sequence pattern fold against a naive search-style matcher on random turn
 * sequences, plus the in-turn wiring: a match completing on a turn records the tool call on that
 * turn, between {@code routed} and the brain.
 */
class SequencePatternTest {

  private static final String[] WORDS = {"anomaly", "heartbeat", "disk", "cpu", "ok"};

  /**
   * A naive matcher: scan left to right, start an attempt at the first turn matching stage one and
   * walk the remaining stages by explicit search. A turn outside the window ends the attempt and is
   * where the scan resumes; a turn breaking a {@code next} stage ends the attempt and is consumed;
   * a completed match is consumed whole.
   */
  static List<Integer> naiveCompletions(SequencePattern p, List<SequencePattern.Turn> turns) {
    List<SequencePattern.Stage> stages = p.stages();
    List<Integer> out = new ArrayList<>();
    int n = turns.size();
    int i = 0;
    while (i < n) {
      if (!stages.get(0).matches(turns.get(i).text())) {
        i++;
        continue;
      }
      if (stages.size() == 1) {
        out.add(i);
        i++;
        continue;
      }
      long start = p.tsKey() == null ? 0 : p.timestamp(turns.get(i));
      int stage = 1;
      int completed = -1;
      int resumeAt = n;
      for (int j = i + 1; j < n; j++) {
        if (p.withinMs() != null && p.timestamp(turns.get(j)) - start > p.withinMs()) {
          resumeAt = j;
          break;
        }
        SequencePattern.Stage s = stages.get(stage);
        if (s.matches(turns.get(j).text())) {
          stage++;
          if (stage == stages.size()) {
            completed = j;
            break;
          }
        } else if (s.contiguity() == SequencePattern.Contiguity.NEXT) {
          resumeAt = j + 1;
          break;
        }
      }
      i = completed >= 0 ? completed + 1 : resumeAt;
      if (completed >= 0) {
        out.add(completed);
      }
    }
    return out;
  }

  static SequencePattern randomPattern(Random rnd, boolean withTs) {
    int count = 1 + rnd.nextInt(4);
    List<SequencePattern.Stage> stages = new ArrayList<>();
    for (int k = 0; k < count; k++) {
      String needle = rnd.nextInt(5) == 0 ? null : WORDS[rnd.nextInt(WORDS.length)];
      SequencePattern.Contiguity c = rnd.nextBoolean()
          ? SequencePattern.Contiguity.NEXT : SequencePattern.Contiguity.FOLLOWED_BY;
      stages.add(new SequencePattern.Stage("s" + k, needle, c));
    }
    Long within = withTs && rnd.nextBoolean() ? (long) (rnd.nextInt(6) * 60_000) : null;
    return new SequencePattern("p" + rnd.nextInt(1000), withTs ? "event_time_ms" : null, within, stages, "open_ticket");
  }

  static List<SequencePattern.Turn> randomTurns(Random rnd, int n) {
    List<SequencePattern.Turn> turns = new ArrayList<>();
    long ts = rnd.nextInt(1000);
    for (int i = 0; i < n; i++) {
      StringBuilder text = new StringBuilder();
      int words = 1 + rnd.nextInt(3);
      for (int w = 0; w < words; w++) {
        String word = WORDS[rnd.nextInt(WORDS.length)];
        text.append(w == 0 ? "" : " ").append(rnd.nextBoolean() ? word.toUpperCase(Locale.ROOT) : word);
      }
      // mostly increasing event time; sometimes a late turn arrives with an earlier stamp
      ts += rnd.nextInt(10) == 0 ? -rnd.nextInt(120_000) : rnd.nextInt(200_000);
      turns.add(new SequencePattern.Turn("t" + i, text.toString(), Map.of("event_time_ms", Long.toString(ts))));
    }
    return turns;
  }

  @Test
  void foldAgreesWithNaiveMatcherOnRandomSequences() {
    long seed = new Random().nextLong();
    Random rnd = new Random(seed);
    for (int round = 0; round < 400; round++) {
      SequencePattern p = randomPattern(rnd, rnd.nextInt(4) != 0);
      List<SequencePattern.Turn> turns = randomTurns(rnd, rnd.nextInt(14));
      List<Integer> expected = naiveCompletions(p, turns);
      for (int k = 0; k < turns.size(); k++) {
        boolean completes = p.completesOn(turns.subList(0, k + 1));
        assertEquals(expected.contains(k), completes,
            "seed " + seed + " round " + round + " turn " + k + " pattern " + p.stages() + " within "
                + p.withinMs() + " turns " + turns);
      }
      assertFalse(p.completesOn(List.of()), "an empty log never completes a match");
    }
  }

  @Test
  void matchesAreCaseInsensitiveAndConsumeTheirTurns() {
    SequencePattern p = new SequencePattern("two", null, null, List.of(
        new SequencePattern.Stage("a", "Anomaly", SequencePattern.Contiguity.NEXT),
        new SequencePattern.Stage("b", "anomaly", SequencePattern.Contiguity.FOLLOWED_BY)), "t");
    List<SequencePattern.Turn> turns = List.of(
        new SequencePattern.Turn("1", "ANOMALY cpu", Map.of()),
        new SequencePattern.Turn("2", "heartbeat", Map.of()),
        new SequencePattern.Turn("3", "anomaly io", Map.of()),
        new SequencePattern.Turn("4", "anomaly mem", Map.of()));
    assertFalse(p.completesOn(turns.subList(0, 1)));
    assertFalse(p.completesOn(turns.subList(0, 2)));
    assertTrue(p.completesOn(turns.subList(0, 3)), "1 and 3 complete the match across the heartbeat");
    assertFalse(p.completesOn(turns), "turn 4 starts a new partial; 1 and 3 were consumed");
  }

  @Test
  void windowIsMeasuredFromTheFirstMatchedTurn() {
    SequencePattern p = new SequencePattern("w", "event_time_ms", 100L, List.of(
        new SequencePattern.Stage("a", "x", SequencePattern.Contiguity.NEXT),
        new SequencePattern.Stage("b", "x", SequencePattern.Contiguity.FOLLOWED_BY)), "t");
    assertTrue(p.completesOn(List.of(turn("1", "x", 0), turn("2", "x", 100))));
    assertFalse(p.completesOn(List.of(turn("1", "x", 0), turn("2", "x", 101))),
        "the late turn drops the partial and starts a new one");
    assertTrue(p.completesOn(List.of(turn("1", "x", 0), turn("2", "x", 101), turn("3", "x", 150))));
    assertThrows(IllegalArgumentException.class,
        () -> p.completesOn(List.of(turn("1", "x", 0), new SequencePattern.Turn("2", "x", Map.of()))),
        "a turn without the ts key is an error, not a silent zero");
  }

  @Test
  void specCompilationKeepsToolActionsOnlyAndValidates() {
    Map<String, Object> tool = rule("host_incident", Map.of("kind", "tool", "tool", "open_ticket"));
    Map<String, Object> submit = rule("escalate", Map.of("kind", "submit", "text", "incident on {key}"));
    List<SequencePattern> compiled = SequencePattern.compile(List.of(tool, submit));
    assertEquals(1, compiled.size());
    assertEquals("host_incident", compiled.get(0).name());
    assertEquals("event_time_ms", compiled.get(0).tsKey());
    assertEquals(300_000L, compiled.get(0).withinMs());
    assertEquals(List.of(submit), SequencePattern.withoutToolActions(List.of(tool, submit)));
    assertEquals(List.of(tool), SequencePattern.toolActions(List.of(tool, submit)));
    assertTrue(SequencePattern.compile(null).isEmpty());

    Map<String, Object> noTs = new LinkedHashMap<>(tool);
    noTs.remove("ts");
    assertThrows(WorkflowValidator.WorkflowValidationException.class,
        () -> SequencePattern.compile(List.of(noTs)), "within without ts");
    Map<String, Object> noTool = new LinkedHashMap<>(tool);
    noTool.put("on_match", Map.of("kind", "tool"));
    assertThrows(WorkflowValidator.WorkflowValidationException.class,
        () -> SequencePattern.compile(List.of(noTool)), "kind tool without a tool id");
  }

  @Test
  void completingTurnRecordsTheToolCallBetweenRoutedAndTheBrain() {
    Random rnd = new Random();
    String host = "host-" + rnd.nextInt(1_000_000);
    Map<String, Object> workflow = new LinkedHashMap<>();
    workflow.put("spec_version", "agentic/v1");
    workflow.put("backend", "local");
    workflow.put("agent", Map.of("id", "monitor",
        "router", Map.of("kind", "keyword", "default", "monitor"),
        "paths", Map.of("monitor", Map.of("brain", "rule", "prompt", "You acknowledge signals."))));
    workflow.put("tools", List.of(Map.of("id", "open_ticket", "kind", "constant", "value", "TICKET-OPENED")));
    workflow.put("cep", List.of(rule("host_incident", Map.of("kind", "tool", "tool", "open_ticket"))));

    GraphBuilder.Built built = GraphBuilder.build(workflow, null);
    assertEquals(1, built.graph().cep().size());
    ConversationLog log = new ConversationLog.InMemory();
    LocalRuntime runtime = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), built.retriever(), log);

    long base = rnd.nextInt(1_000_000);
    List<TurnResult> results = new ArrayList<>();
    String[] texts = {"anomaly: cpu", "heartbeat ok", "anomaly: mem", "anomaly: io", "anomaly: cpu"};
    for (int i = 0; i < texts.length; i++) {
      results.add(runtime.submit(Event.turn(host, "t" + i, "u", texts[i],
          Map.of("event_time_ms", Long.toString(base + i * 60_000L)))));
    }
    for (int i = 0; i < results.size(); i++) {
      assertEquals(TurnStatus.COMPLETED, results.get(i).status);
      assertEquals(i == 3 ? 1 : 0, results.get(i).calls.size(), "only the third anomaly opens a ticket");
    }
    TurnResult match = results.get(3);
    assertEquals("open_ticket", match.calls.get(0).tool());
    assertEquals(0, match.calls.get(0).index());
    assertEquals(Map.of("pattern", "host_incident", "key", host), match.calls.get(0).args());
    List<String> types = match.events.stream().map(e -> e.eventType().orElseThrow().wire()).toList();
    assertEquals(List.of("turn_received", "routed", "tool_called", "brain_started"), types.subList(0, 4));
    assertEquals("turn_completed", types.get(types.size() - 1));

    LogEvent received = match.events.get(0);
    assertEquals(EventType.TURN_RECEIVED, received.eventType().orElseThrow());
    assertEquals(base + 3 * 60_000L, received.payload().get("event_time_ms"));
    assertEquals(Map.of("event_time_ms", Long.toString(base + 3 * 60_000L)), received.payload().get("metadata"));

    // Replay: the same log folded again yields the same turns, so the decision is reproducible.
    assertTrue(built.graph().cep().get(0).completesOn(TurnPatterns.turnsOf(log.events(host)).subList(0, 4)));
    assertFalse(built.graph().cep().get(0).completesOn(TurnPatterns.turnsOf(log.events(host))));
  }

  private static SequencePattern.Turn turn(String id, String text, long ts) {
    return new SequencePattern.Turn(id, text, Map.of("event_time_ms", Long.toString(ts)));
  }

  private static Map<String, Object> rule(String name, Map<String, Object> onMatch) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("key", "conversation_id");
    m.put("ts", "metadata.event_time_ms");
    m.put("within", 300_000);
    m.put("pattern", List.of(
        Map.of("stage", "first", "where", Map.of("text_contains", "anomaly")),
        Map.of("stage", "second", "where", Map.of("text_contains", "anomaly"), "contiguity", "followedBy"),
        Map.of("stage", "third", "where", Map.of("text_contains", "anomaly"), "contiguity", "followedBy")));
    m.put("on_match", onMatch);
    return m;
  }
}
