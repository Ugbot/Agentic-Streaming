package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

/** The spec nouns: turn ids, structured tool calls, the closed event set, dense logs and the fold. */
class SpecPrimitivesTest {

  private static String rnd() {
    return UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  void legacyEventConstructorsAssignAFreshTurnIdAndSpecFactoryKeepsTheGivenOne() {
    Event a = new Event("c", "u", "hi");
    Event b = new Event("c", "u", "hi");
    assertNotNull(a.turnId());
    assertNotEquals(a.turnId(), b.turnId(), "an unspecified turn id must never collide");
    String id = rnd();
    assertEquals(id, Event.turn("c", id, "u", "hi").turnId());
    assertTrue(Event.resume("c", id, Map.of("approved", true)).isResume());
    assertFalse(Event.turn("c", id, "u", "hi").isResume());
  }

  @Test
  void toolCallIsTypedIndexedAndAttemptNumbered() {
    Map<String, Object> args = Map.of("user", rnd(), "amount", ThreadLocalRandom.current().nextInt(1, 999));
    ToolCall ok = ToolCall.succeeded("balance", 2, args, "42", 3);
    assertEquals(args, ok.args());
    assertTrue(ok.ok());
    Map<String, Object> wire = ok.toMap();
    assertEquals(2, wire.get("index"));
    assertEquals(3, wire.get("attempt"));
    assertNull(wire.get("error"));
    ToolCall failed = ToolCall.failed("balance", 0, args, "boom", 1);
    assertFalse(failed.ok());
    assertEquals("boom", failed.toMap().get("error"));
    assertThrows(IllegalArgumentException.class, () -> new ToolCall("t", 0, Map.of(), null, null, 0));
  }

  @Test
  void eventTypesAndStatusesAreTheClosedSpecSets() {
    List<String> wire = List.of("turn_received", "guardrail_rejected", "routed", "brain_started", "tool_called",
        "tool_failed", "retrieved", "reply_drafted", "verification_failed", "memory_written", "turn_completed",
        "turn_failed", "turn_suspended", "turn_resumed", "timer_scheduled", "timer_fired",
        "compensation_started", "compensation_step", "compensation_completed", "delegated");
    assertEquals(wire.size(), EventType.values().length);
    for (String w : wire) {
      assertEquals(w, EventType.parse(w).orElseThrow().wire());
    }
    assertTrue(EventType.parse("not_an_event_" + rnd()).isEmpty());
    assertEquals(List.of("completed", "rejected", "unverified", "failed", "suspended", "duplicate"),
        java.util.Arrays.stream(TurnStatus.values()).map(TurnStatus::wire).toList());
    assertEquals(List.of("validation", "guardrail", "verification", "tool", "transient", "fatal"),
        java.util.Arrays.stream(TurnError.ErrorClass.values()).map(TurnError.ErrorClass::wire).toList());
  }

  @Test
  void logSequencesAreDenseZeroBasedAndPerConversation() {
    ConversationLog log = new ConversationLog.InMemory();
    String c1 = "c-" + rnd();
    String c2 = "c-" + rnd();
    int n = ThreadLocalRandom.current().nextInt(3, 9);
    for (int i = 0; i < n; i++) {
      LogEvent e = log.append(c1, "t" + i, EventType.TURN_RECEIVED, Map.of("turn_id", "t" + i));
      assertEquals(i, e.sequence());
    }
    assertEquals(0, log.append(c2, "x", EventType.TURN_RECEIVED, Map.of("turn_id", "x")).sequence());
    assertEquals(n, log.events(c1).size());
    assertEquals(n, log.state(c1).nextSequence());
    assertEquals(n, log.state(c1).turnCount());
    assertEquals(1, log.state(c2).turnCount());
  }

  @Test
  void foldRejectsGapsAndIgnoresUnknownEventTypes() {
    String c = "c-" + rnd();
    List<LogEvent> gap = List.of(
        new LogEvent(c, 0, "t1", "turn_received", Map.of("turn_id", "t1")),
        new LogEvent(c, 2, "t1", "turn_completed", Map.of("reply", "x")));
    assertThrows(IllegalStateException.class, () -> ConversationState.fold(gap));

    List<LogEvent> withUnknown = new ArrayList<>();
    withUnknown.add(new LogEvent(c, 0, "t1", "turn_received", Map.of("turn_id", "t1", "text", "hi")));
    withUnknown.add(new LogEvent(c, 1, "t1", "future_" + rnd(), Map.of("anything", rnd())));
    withUnknown.add(new LogEvent(c, 2, "t1", "routed", Map.of("path", "main")));
    withUnknown.add(new LogEvent(c, 3, "t1", "memory_written", Map.of("messages", List.of(
        Map.of("role", "user", "text", "hi"), Map.of("role", "assistant", "text", "yo")))));
    withUnknown.add(new LogEvent(c, 4, "t1", "turn_completed", Map.of("reply", "yo")));
    ConversationState s = ConversationState.fold(withUnknown);
    assertEquals(1, s.turnCount());
    assertEquals(2, s.transcriptLength());
    assertEquals(TurnStatus.COMPLETED, s.turn("t1").status());
    assertEquals("main", s.turn("t1").path());
    assertEquals(Map.of("turn_count", 1L, "transcript_length", 2L), s.reduced(),
        "last_retrieved_ids only appears once a retrieved event has been folded");
  }

  @Test
  void foldReconstructsStructuredToolCallsFromAttemptEvents() {
    String c = "c-" + rnd();
    Map<String, Object> args = Map.of("user", rnd());
    List<LogEvent> log = List.of(
        new LogEvent(c, 0, "t1", "turn_received", Map.of("turn_id", "t1", "text", "x")),
        new LogEvent(c, 1, "t1", "tool_failed", Map.of("tool", "flaky", "index", 0, "attempt", 1, "args", args,
            "error", "boom")),
        new LogEvent(c, 2, "t1", "tool_called", Map.of("tool", "flaky", "index", 0, "attempt", 2, "args", args,
            "result", "ok")),
        new LogEvent(c, 3, "t1", "turn_completed", Map.of("reply", "done")));
    List<ToolCall> calls = ConversationState.fold(log).turn("t1").calls();
    assertEquals(2, calls.size());
    assertEquals(1, calls.get(0).attempt());
    assertFalse(calls.get(0).ok());
    assertEquals(2, calls.get(1).attempt());
    assertEquals(args, calls.get(1).args());
    assertEquals(0, calls.get(1).index());
  }

  @Test
  void primitivesAreSerializable() throws Exception {
    String c = "c-" + rnd();
    LogEvent e = new LogEvent(c, 0, "t", "turn_received", Map.of("turn_id", "t"));
    ToolCall call = ToolCall.succeeded("t", 0, Map.of("k", rnd()), List.of(1, 2), 1);
    Policies p = Policies.fromMap(Map.of("retry", Map.of("kind", "fixed", "max_attempts", 2, "initial_delay_ms", 5)));
    SagaPlan saga = new SagaPlan(List.of(new SagaPlan.Step("s", "tool", Map.of(), "undo")));
    ConversationState state = ConversationState.fold(List.of(e));
    for (Object o : List.of(e, call, p, saga, state, new TurnError(TurnError.ErrorClass.TOOL, "m"))) {
      assertEquals(o.getClass(), roundTrip(o).getClass());
    }
    assertEquals(call, roundTrip(call));
    assertEquals(state, roundTrip(state));
  }

  @Test
  void retryPolicyDelaysAreDeterministicWithoutJitter() {
    Policies.RetryPolicy exp = Policies.RetryPolicy.exponential(4, 100, 2.0, 350);
    java.util.random.RandomGenerator rng = ThreadLocalRandom.current();
    assertEquals(100, exp.delayBefore(2, rng));
    assertEquals(200, exp.delayBefore(3, rng));
    assertEquals(350, exp.delayBefore(4, rng), "capped at max_delay_ms");
    assertEquals(1, Policies.RetryPolicy.NONE.attempts());
    Policies parsed = Policies.fromMap(Map.of(
        "ordering", "per-conversation", "idempotency", "turn-id", "on_tool_error", "continue",
        "retry", Map.of("kind", "exponential", "max_attempts", 3, "initial_delay_ms", 1, "multiplier", 2.0,
            "max_delay_ms", 5, "jitter", false),
        "verification", Map.of("max_attempts", 2, "on_exhausted", "fail")));
    assertEquals(Policies.OnToolError.CONTINUE, parsed.onToolError());
    assertEquals(3, parsed.retry().attempts());
    assertEquals(Policies.VerificationPolicy.OnExhausted.FAIL, parsed.verification().onExhausted());
    assertEquals(2, parsed.verification().maxAttempts());
  }

  @Test
  void sagaResolveFillsToolLevelCompensationWithoutOverridingStepLevel() {
    SagaPlan plan = new SagaPlan(List.of(
        new SagaPlan.Step("a", "reserve", Map.of(), null),
        new SagaPlan.Step("b", "post", Map.of(), "explicit_undo")));
    SagaPlan resolved = plan.resolve(Map.of("reserve", "release", "post", "tool_undo"));
    assertEquals("release", resolved.steps().get(0).compensateWith());
    assertEquals("explicit_undo", resolved.steps().get(1).compensateWith());
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T o) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(o);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
