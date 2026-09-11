package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.jagentic.core.pipeline.GraphBuilder;
import org.junit.jupiter.api.Test;

/** Runtime semantics of the v1 spec on LocalRuntime/RoutedGraph, driven through the portable IR. */
class SpecSemanticsTest {

  private static String rnd() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  private static Map<String, Object> spec(Map<String, Object> policies, List<Map<String, Object>> tools,
                                          Map<String, Object> mainPath, Map<String, Object> extra) {
    Map<String, Object> agent = new LinkedHashMap<>();
    agent.put("id", "a-" + rnd());
    agent.put("router", Map.of("kind", "keyword", "default", "main", "rules", Map.of("main", List.of("charge"))));
    agent.put("paths", Map.of("main", mainPath));
    agent.put("verifier", Map.of("kind", "none"));
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("spec_version", "agentic/v1");
    s.put("backend", "local");
    s.put("agent", agent);
    if (policies != null) {
      s.put("policies", policies);
    }
    if (tools != null) {
      s.put("tools", tools);
    }
    s.putAll(extra);
    return s;
  }

  private static LocalRuntime runtime(GraphBuilder.Built built, ConversationLog log) {
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), built.retriever(), log);
  }

  private static LocalRuntime runtime(Map<String, Object> spec) {
    return runtime(GraphBuilder.build(spec, null), new ConversationLog.InMemory());
  }

  private static List<String> types(List<LogEvent> events) {
    return events.stream().map(LogEvent::type).toList();
  }

  @Test
  void duplicateTurnReturnsRecordedResultAndRunsNothing() {
    AtomicInteger toolRuns = new AtomicInteger();
    Map<String, Object> path = Map.of("brain", "rule", "prompt", "p", "tool_triggers", Map.of("charge", "t"));
    GraphBuilder.Built built = GraphBuilder.build(spec(null,
        List.of(Map.of("id", "t", "kind", "constant", "value", "v")), path, Map.of()), null);
    built.tools().register("t", "counting", args -> "v" + toolRuns.incrementAndGet());
    LocalRuntime rt = runtime(built, new ConversationLog.InMemory());

    String cid = "c-" + rnd();
    String tid = "t-" + rnd();
    TurnResult first = rt.submit(Event.turn(cid, tid, "u", "a charge question"));
    assertEquals(TurnStatus.COMPLETED, first.status);
    assertEquals(1, toolRuns.get());
    int logSize = rt.log().events(cid).size();

    TurnResult dup = rt.submit(Event.turn(cid, tid, "u", "a charge question"));
    assertEquals(TurnStatus.DUPLICATE, dup.status);
    assertEquals(first.reply(), dup.reply());
    assertEquals(first.path(), dup.path());
    assertEquals(first.calls, dup.calls);
    assertTrue(dup.events.isEmpty(), "a duplicate appends no events");
    assertEquals(logSize, rt.log().events(cid).size());
    assertEquals(1, toolRuns.get(), "a duplicate re-runs no tools");
    assertEquals(1L, dup.state.get("turn_count"));
    assertEquals("duplicate", dup.toMap().get("status"));
  }

  @Test
  void structuredToolArgumentsAreRecordedAsTypedMaps() {
    Map<String, Object> path = Map.of("brain", "rule", "prompt", "p", "tool_triggers", Map.of("charge", "t"));
    LocalRuntime rt = runtime(spec(null, List.of(Map.of("id", "t", "kind", "constant", "value", 42.5,
        "parameters", Map.of("type", "object", "properties", Map.of("user", Map.of("type", "string"))))),
        path, Map.of()));
    TurnResult r = rt.submit(Event.turn("c", "t1", "alice", "charge please"));
    assertEquals(1, r.calls.size());
    ToolCall call = r.calls.get(0);
    assertEquals(Map.of("user", "alice"), call.args());
    assertEquals(42.5, call.result());
    assertEquals(0, call.index());
    assertEquals(1, call.attempt());
    assertEquals(Map.of("user", "alice"),
        rt.log().events("c").stream().filter(e -> e.is(EventType.TOOL_CALLED)).findFirst().orElseThrow()
            .payload().get("args"));
  }

  @Test
  void retryPolicyNumbersAttemptsAndOnlyTheLastOneSucceeds() {
    int failures = ThreadLocalRandom.current().nextInt(1, 4);
    Map<String, Object> path = Map.of("brain", "rule", "prompt", "p", "tool_triggers", Map.of("charge", "flaky"));
    Map<String, Object> flaky = new HashMap<>(Map.of("id", "flaky", "kind", "failing", "value", "ok"));
    flaky.put("x-fail-attempts", failures);
    LocalRuntime rt = runtime(spec(
        Map.of("retry", Map.of("kind", "fixed", "max_attempts", failures + 1, "initial_delay_ms", 0)),
        List.of(flaky), path, Map.of()));
    TurnResult r = rt.submit(Event.turn("c", "t1", "u", "charge"));
    assertEquals(TurnStatus.COMPLETED, r.status);
    assertEquals(failures + 1, r.calls.size());
    for (int i = 0; i < failures; i++) {
      assertEquals(i + 1, r.calls.get(i).attempt());
      assertEquals(0, r.calls.get(i).index());
      assertFalse(r.calls.get(i).ok());
    }
    assertTrue(r.calls.get(failures).ok());
    assertEquals(1, types(r.events).stream().filter("turn_received"::equals).count(),
        "retries never append another turn_received");
    assertEquals(failures, types(r.events).stream().filter("tool_failed"::equals).count());
  }

  @Test
  void exhaustedRetriesFailTheTurnWithToolErrorClass() {
    Map<String, Object> path = Map.of("brain", "rule", "prompt", "p", "tool_triggers", Map.of("charge", "dead"));
    LocalRuntime rt = runtime(spec(Map.of("retry", Map.of("kind", "none", "max_attempts", 1)),
        List.of(Map.of("id", "dead", "kind", "failing")), path, Map.of()));
    TurnResult r = rt.submit(Event.turn("c", "t1", "u", "charge"));
    assertEquals(TurnStatus.FAILED, r.status);
    assertEquals(TurnError.ErrorClass.TOOL, r.error.errorClass());
    assertEquals("turn_failed", types(r.events).get(types(r.events).size() - 1));
    assertEquals(Map.of("class", "tool", "message", r.error.message()), r.error.toMap());
  }

  @Test
  void verificationExhaustionIsUnverifiedByDefaultAndFailedWhenConfigured() {
    Map<String, Object> verifier = Map.of("kind", "regex", "pattern", "^never-" + rnd() + "$");
    Map<String, Object> agentPath = Map.of("brain", "rule", "prompt", "p");
    for (String onExhausted : List.of("unverified", "fail")) {
      Map<String, Object> s = spec(Map.of("verification", Map.of("max_attempts", 2, "on_exhausted", onExhausted)),
          null, agentPath, Map.of());
      ((Map<String, Object>) s.get("agent")).put("verifier", verifier);
      TurnResult r = runtime(s).submit(Event.turn("c", "t1", "u", "hello"));
      assertEquals("fail".equals(onExhausted) ? TurnStatus.FAILED : TurnStatus.UNVERIFIED, r.status);
      assertEquals(TurnError.ErrorClass.VERIFICATION, r.error.errorClass());
      assertEquals(2, types(r.events).stream().filter("reply_drafted"::equals).count());
      assertEquals(2, types(r.events).stream().filter("verification_failed"::equals).count());
    }
  }

  @Test
  void sagaCompensatesCompletedStepsInReverseOrder() {
    List<Map<String, Object>> tools = List.of(
        Map.of("id", "a", "kind", "constant", "value", "a", "compensation", "undo_a"),
        Map.of("id", "undo_a", "kind", "constant", "value", "ua"),
        Map.of("id", "b", "kind", "constant", "value", "b"),
        Map.of("id", "undo_b", "kind", "constant", "value", "ub"),
        Map.of("id", "boom", "kind", "failing"));
    Map<String, Object> saga = Map.of("steps", List.of(
        Map.of("name", "s1", "tool", "a"),
        Map.of("name", "s2", "tool", "b", "compensate_with", "undo_b"),
        Map.of("name", "s3", "tool", "boom")));
    LocalRuntime rt = runtime(spec(Map.of("on_tool_error", "fail"), tools,
        Map.of("brain", "rule", "prompt", "p"), Map.of("saga", saga)));
    TurnResult r = rt.submit(Event.turn("c", "t1", "u", "charge"));
    assertEquals(TurnStatus.FAILED, r.status);
    assertEquals(List.of("a", "b", "boom", "undo_b", "undo_a"), r.calls.stream().map(ToolCall::tool).toList());
    assertEquals(List.of(0, 1, 2, 3, 4), r.calls.stream().map(ToolCall::index).toList());
    List<String> t = types(r.events);
    int started = t.indexOf("compensation_started");
    assertTrue(started > 0);
    assertEquals(List.of("compensation_step", "compensation_step", "compensation_completed", "turn_failed"),
        t.subList(started + 1, t.size()));
  }

  @Test
  void suspendedTurnResumesAfterRestartFromTheLogAlone() {
    Map<String, Object> path = new HashMap<>(Map.of("brain", "rule", "prompt", "p"));
    path.put("x-suspend-until", "approval");
    GraphBuilder.Built built = GraphBuilder.build(spec(null, null, path, Map.of()), null);
    ConversationLog log = new ConversationLog.InMemory();
    TurnResult s = runtime(built, log).submit(Event.turn("c", "t1", "u", "charge"));
    assertEquals(TurnStatus.SUSPENDED, s.status);
    assertNull(s.reply());
    assertTrue(types(s.events).contains("turn_suspended"));

    LocalRuntime restarted = runtime(built, log);
    TurnResult r = restarted.submit(Event.resume("c", "t1", Map.of("approval", true)));
    assertEquals(TurnStatus.COMPLETED, r.status);
    assertEquals("main", r.path());
    assertEquals(List.of("turn_resumed", "brain_started", "reply_drafted", "memory_written", "turn_completed"),
        types(r.events));
    assertEquals(1L, r.state.get("turn_count"));
    assertEquals(restarted.replay("c").reduced(), r.state);
  }

  @Test
  void turnsOfOneConversationAreSerializedWhileConversationsInterleave() throws Exception {
    LocalRuntime rt = runtime(spec(null, null, Map.of("brain", "rule", "prompt", "p"), Map.of()));
    int n = ThreadLocalRandom.current().nextInt(5, 12);
    List<CompletableFuture<TurnResult>> futures = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      futures.add(rt.submitAsync(Event.turn("c1", "t" + i, "u", "m" + i)));
      futures.add(rt.submitAsync(Event.turn("c2", "t" + i, "u", "m" + i)));
    }
    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    for (String cid : List.of("c1", "c2")) {
      List<LogEvent> events = rt.log().events(cid);
      for (int i = 0; i < events.size(); i++) {
        assertEquals(i, events.get(i).sequence());
      }
      List<String> receivedOrder = events.stream().filter(e -> e.is(EventType.TURN_RECEIVED))
          .map(LogEvent::turnId).toList();
      List<String> expected = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        expected.add("t" + i);
      }
      assertEquals(expected, receivedOrder, "arrival order is preserved within " + cid);
      String open = null;
      for (LogEvent e : events) {
        if (e.is(EventType.TURN_RECEIVED)) {
          assertNull(open, "a turn started while " + open + " was still open in " + cid);
          open = e.turnId();
        } else {
          assertEquals(open, e.turnId(), "events of one turn never interleave with another");
          if (e.is(EventType.TURN_COMPLETED)) {
            open = null;
          }
        }
      }
      assertNull(open);
      assertEquals(n, rt.replay(cid).turnCount());
    }
    assertSame(TurnStatus.COMPLETED, futures.get(0).get().status);
  }

  @Test
  void materializedStateIsTheFoldOfTheLog() {
    LocalRuntime rt = runtime(spec(null, null, Map.of("brain", "rule", "prompt", "p"), Map.of()));
    int n = ThreadLocalRandom.current().nextInt(2, 6);
    TurnResult last = null;
    for (int i = 0; i < n; i++) {
      last = rt.submit(Event.turn("c", "t" + i, "u", "hello " + i));
    }
    ConversationState folded = ConversationState.fold(rt.log().events("c"));
    assertEquals(folded.reduced(), last.state);
    assertEquals((long) n, folded.turnCount());
    assertEquals(2L * n, folded.transcriptLength());
    assertEquals(folded, rt.replay("c"));
  }
}
