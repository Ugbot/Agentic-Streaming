package org.jagentic.core.conformance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * One dynamic test per fixture in {@code spec/conformance/v1/fixtures}. A fixture whose
 * {@code requires} names a capability this runtime lacks is aborted (reported as skipped by JUnit),
 * never passed.
 */
class ConformanceTest {

  @TestFactory
  Stream<DynamicTest> fixtures() {
    List<Path> files = ConformanceHarness.fixtureFiles();
    assertEquals(15, files.size(), "expected the 15 v1 fixtures at " + ConformanceHarness.fixturesDir());
    return files.stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> {
      ConformanceHarness.Outcome o = ConformanceHarness.run(p);
      Assumptions.assumeFalse(o.skipped(), () -> "skip " + o.id() + ": " + o.skipReason());
      assertTrue(o.problems().isEmpty(), () -> "FAIL " + o.id() + "\n  " + String.join("\n  ", o.problems()));
    }));
  }

  @Test
  void comparatorMatchesReferenceRules() {
    Map<String, Object> call = new java.util.HashMap<>(Map.of("tool", "balance", "index", 0, "attempt", 1,
        "args", Map.of("user", "anonymous")));
    call.put("error", null);
    Map<String, Object> actual = Map.of(
        "conversation_id", "c1", "status", "completed", "path", "billing", "reply", "[billing] ok",
        "state", Map.of("turn_count", 2L),
        "tool_calls", List.of(call),
        "events", List.of(Map.of("type", "turn_received"), Map.of("type", "routed"),
            Map.of("type", "tool_called"), Map.of("type", "turn_completed")),
        "error", Map.of("class", "tool"));

    assertTrue(ConformanceHarness.checkExpectation(Map.of(
        "conversation_id", "c1", "status", "completed", "path", "billing", "reply_matches", "^\\[billing\\]",
        "error_class", "tool", "state_includes", Map.of("turn_count", 2),
        "tool_calls", List.of(Map.of("tool", "balance", "index", 0, "args", Map.of("user", "anonymous"))),
        "events_include", List.of("turn_received", "tool_called"),
        "events_exclude", List.of("turn_failed")), actual).isEmpty());

    List<String> problems = ConformanceHarness.checkExpectation(Map.of(
        "status", "failed",
        "tool_calls", List.of(Map.of("tool", "balance", "failed", true)),
        "events_include", List.of("tool_called", "routed"),
        "events_exclude", List.of("routed"),
        "state_includes", Map.of("turn_count", 3)), actual);
    assertEquals(5, problems.size(), problems.toString());
    assertFalse(ConformanceHarness.checkExpectation(Map.of("reply", "other"), actual).isEmpty());
  }
}
