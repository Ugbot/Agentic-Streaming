package org.jagentic.pekko.conformance;

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
 * One dynamic test per fixture in {@code spec/conformance/v1/fixtures}, run on the Pekko actor
 * runtime. A fixture whose {@code requires} names a capability this runtime lacks is aborted
 * (reported as skipped by JUnit), never passed.
 */
class PekkoConformanceTest {

  @TestFactory
  Stream<DynamicTest> fixtures() {
    List<Path> files = PekkoConformanceHarness.fixtureFiles();
    assertFalse(files.isEmpty(), "no fixtures found at " + PekkoConformanceHarness.fixturesDir());
    return files.stream().map(p -> DynamicTest.dynamicTest(p.getFileName().toString(), () -> {
      PekkoConformanceHarness.Outcome o = PekkoConformanceHarness.run(p);
      if (o.skipped()) {
        assertJustifiedSkip(p, o);
        Assumptions.abort("skip " + o.id() + ": " + o.skipReason());
      }
      assertTrue(o.problems().isEmpty(), () -> "FAIL " + o.id() + "\n  " + String.join("\n  ", o.problems()));
    }));
  }

  /**
   * A skip is legitimate only when the fixture requires a capability this runtime does not declare
   * and the reason says so; any other skip is a silent gap and fails the suite.
   */
  @SuppressWarnings("unchecked")
  static void assertJustifiedSkip(Path fixture, PekkoConformanceHarness.Outcome o) {
    String reason = o.skipReason();
    assertTrue(reason != null && !reason.isBlank(), () -> o.id() + " skipped without a reason");
    List<String> requires = (List<String>) PekkoConformanceHarness.load(fixture).get("requires");
    List<String> undeclared = requires.stream().filter(c -> !PekkoConformanceHarness.CAPABILITIES.contains(c)).toList();
    assertFalse(undeclared.isEmpty(), () -> o.id() + " skipped (" + reason + ") although every required capability is declared");
    assertTrue(undeclared.stream().allMatch(reason::contains),
        () -> o.id() + " skip reason '" + reason + "' does not name the undeclared capabilities " + undeclared);
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

    assertTrue(PekkoConformanceHarness.checkExpectation(Map.of(
        "conversation_id", "c1", "status", "completed", "path", "billing", "reply_matches", "^\\[billing\\]",
        "error_class", "tool", "state_includes", Map.of("turn_count", 2),
        "tool_calls", List.of(Map.of("tool", "balance", "index", 0, "args", Map.of("user", "anonymous"))),
        "events_include", List.of("turn_received", "tool_called"),
        "events_exclude", List.of("turn_failed")), actual).isEmpty());

    List<String> problems = PekkoConformanceHarness.checkExpectation(Map.of(
        "status", "failed",
        "tool_calls", List.of(Map.of("tool", "balance", "failed", true)),
        "events_include", List.of("tool_called", "routed"),
        "events_exclude", List.of("routed"),
        "state_includes", Map.of("turn_count", 3)), actual);
    assertEquals(5, problems.size(), problems.toString());
    assertFalse(PekkoConformanceHarness.checkExpectation(Map.of("reply", "other"), actual).isEmpty());
  }
}
