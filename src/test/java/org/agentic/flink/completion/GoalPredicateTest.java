package org.agentic.flink.completion;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for the GoalPredicate system: factory methods, concrete implementations, composite logic,
 * confidence scoring, and diagnostics.
 *
 * <p>All test data is generated at runtime using {@link UUID#randomUUID()} and {@link
 * ThreadLocalRandom} to avoid hardcoded happy paths.
 *
 * @author Agentic Flink Team
 */
class GoalPredicateTest {

  // ==================== Helpers ====================

  private static AgentEvent makeEvent(AgentEventType type) {
    AgentEvent event =
        new AgentEvent(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            type);
    return event;
  }

  private static List<AgentEvent> generateEvents(AgentEventType type, int count) {
    List<AgentEvent> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(makeEvent(type));
    }
    return events;
  }

  // ==================== StateExistsPredicate ====================

  @Nested
  @DisplayName("stateExists")
  class StateExistsTests {

    @RepeatedTest(5)
    @DisplayName("should be satisfied when key is present with non-null value")
    void satisfiedWhenKeyPresent() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.stateExists(key);

      Map<String, Object> state = new HashMap<>();
      state.put(key, "value-" + UUID.randomUUID());

      assertTrue(predicate.isSatisfied(state, Collections.emptyList()));
      assertEquals(1.0, predicate.getConfidence(state, Collections.emptyList()));
    }

    @RepeatedTest(5)
    @DisplayName("should not be satisfied when key is absent")
    void notSatisfiedWhenKeyAbsent() {
      String key = UUID.randomUUID().toString();
      String otherKey = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.stateExists(key);

      Map<String, Object> state = new HashMap<>();
      state.put(otherKey, "irrelevant");

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
      assertEquals(0.0, predicate.getConfidence(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("should not be satisfied when key maps to null")
    void notSatisfiedWhenValueIsNull() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.stateExists(key);

      Map<String, Object> state = new HashMap<>();
      state.put(key, null);

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("description should contain the key name")
    void descriptionContainsKey() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.stateExists(key);

      assertTrue(predicate.getDescription().contains(key));
    }

    @Test
    @DisplayName("diagnostics should report key existence")
    void diagnosticsReportExistence() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.stateExists(key);

      Map<String, Object> state = new HashMap<>();
      state.put(key, ThreadLocalRandom.current().nextInt());

      Map<String, Boolean> diag = predicate.getDiagnostics(state, Collections.emptyList());
      assertFalse(diag.isEmpty());
      assertTrue(diag.containsKey("key_" + key + "_exists"));
      assertTrue(diag.get("key_" + key + "_exists"));
    }
  }

  // ==================== NumericThresholdPredicate ====================

  @Nested
  @DisplayName("greaterThan")
  class NumericThresholdTests {

    @RepeatedTest(5)
    @DisplayName("should be satisfied when value exceeds threshold")
    void satisfiedWhenAboveThreshold() {
      String key = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(1.0, 100.0);
      double value = threshold + ThreadLocalRandom.current().nextDouble(0.01, 50.0);
      GoalPredicate predicate = GoalPredicate.greaterThan(key, threshold);

      Map<String, Object> state = new HashMap<>();
      state.put(key, value);

      assertTrue(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @RepeatedTest(5)
    @DisplayName("should not be satisfied when value is below threshold")
    void notSatisfiedWhenBelowThreshold() {
      String key = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(10.0, 100.0);
      double value = threshold - ThreadLocalRandom.current().nextDouble(0.01, 9.0);
      GoalPredicate predicate = GoalPredicate.greaterThan(key, threshold);

      Map<String, Object> state = new HashMap<>();
      state.put(key, value);

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("should not be satisfied when value equals threshold exactly")
    void notSatisfiedWhenEqualToThreshold() {
      String key = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(1.0, 100.0);
      GoalPredicate predicate = GoalPredicate.greaterThan(key, threshold);

      Map<String, Object> state = new HashMap<>();
      state.put(key, threshold);

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("should not be satisfied when key is missing")
    void notSatisfiedWhenKeyMissing() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.greaterThan(key, 50.0);

      Map<String, Object> state = new HashMap<>();

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
      assertEquals(0.0, predicate.getConfidence(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("should not be satisfied when value is non-numeric")
    void notSatisfiedWhenNonNumeric() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.greaterThan(key, 50.0);

      Map<String, Object> state = new HashMap<>();
      state.put(key, "not-a-number-" + UUID.randomUUID());

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
      assertEquals(0.0, predicate.getConfidence(state, Collections.emptyList()));
    }

    @RepeatedTest(5)
    @DisplayName("confidence should be proportional to value/threshold")
    void confidenceIsProportional() {
      String key = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(10.0, 100.0);
      double fraction = ThreadLocalRandom.current().nextDouble(0.1, 0.99);
      double value = threshold * fraction;
      GoalPredicate predicate = GoalPredicate.greaterThan(key, threshold);

      Map<String, Object> state = new HashMap<>();
      state.put(key, value);

      double confidence = predicate.getConfidence(state, Collections.emptyList());
      assertEquals(fraction, confidence, 0.001);
      assertTrue(confidence >= 0.0 && confidence <= 1.0);
    }

    @Test
    @DisplayName("confidence should be capped at 1.0 for values well above threshold")
    void confidenceCappedAtOne() {
      String key = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(1.0, 50.0);
      double value = threshold * 5.0;
      GoalPredicate predicate = GoalPredicate.greaterThan(key, threshold);

      Map<String, Object> state = new HashMap<>();
      state.put(key, value);

      assertEquals(1.0, predicate.getConfidence(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("should work with Integer values")
    void worksWithIntegers() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.greaterThan(key, 5.0);

      Map<String, Object> state = new HashMap<>();
      state.put(key, 10);

      assertTrue(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("description should contain key and threshold")
    void descriptionContainsKeyAndThreshold() {
      String key = UUID.randomUUID().toString();
      double threshold = 42.5;
      GoalPredicate predicate = GoalPredicate.greaterThan(key, threshold);

      String desc = predicate.getDescription();
      assertTrue(desc.contains(key));
      assertTrue(desc.contains("42.5"));
    }
  }

  // ==================== EventCountPredicate ====================

  @Nested
  @DisplayName("eventCount")
  class EventCountTests {

    @RepeatedTest(5)
    @DisplayName("should be satisfied when enough events of the type exist")
    void satisfiedWhenCountReached() {
      AgentEventType type = AgentEventType.FLOW_COMPLETED;
      int targetCount = ThreadLocalRandom.current().nextInt(1, 10);
      int actualCount = targetCount + ThreadLocalRandom.current().nextInt(0, 5);
      GoalPredicate predicate = GoalPredicate.eventCount(type, targetCount);

      List<AgentEvent> events = generateEvents(type, actualCount);

      assertTrue(predicate.isSatisfied(Collections.emptyMap(), events));
    }

    @RepeatedTest(5)
    @DisplayName("should not be satisfied when fewer events than target")
    void notSatisfiedWhenBelowTarget() {
      AgentEventType type = AgentEventType.TOOL_CALL_COMPLETED;
      int targetCount = ThreadLocalRandom.current().nextInt(5, 15);
      int actualCount = ThreadLocalRandom.current().nextInt(0, targetCount);
      GoalPredicate predicate = GoalPredicate.eventCount(type, targetCount);

      List<AgentEvent> events = generateEvents(type, actualCount);

      assertFalse(predicate.isSatisfied(Collections.emptyMap(), events));
    }

    @Test
    @DisplayName("should only count matching event types")
    void countsOnlyMatchingType() {
      AgentEventType targetType = AgentEventType.VALIDATION_PASSED;
      AgentEventType otherType = AgentEventType.FLOW_STARTED;
      GoalPredicate predicate = GoalPredicate.eventCount(targetType, 2);

      List<AgentEvent> events = new ArrayList<>();
      events.addAll(generateEvents(otherType, 10));
      events.addAll(generateEvents(targetType, 1));

      assertFalse(predicate.isSatisfied(Collections.emptyMap(), events));

      events.addAll(generateEvents(targetType, 1));
      assertTrue(predicate.isSatisfied(Collections.emptyMap(), events));
    }

    @RepeatedTest(5)
    @DisplayName("confidence should be proportional to count/targetCount")
    void confidenceIsProportional() {
      AgentEventType type = AgentEventType.LOOP_ITERATION_COMPLETED;
      int targetCount = ThreadLocalRandom.current().nextInt(3, 20);
      int actualCount = ThreadLocalRandom.current().nextInt(1, targetCount);
      GoalPredicate predicate = GoalPredicate.eventCount(type, targetCount);

      List<AgentEvent> events = generateEvents(type, actualCount);

      double expected = (double) actualCount / targetCount;
      double confidence = predicate.getConfidence(Collections.emptyMap(), events);
      assertEquals(expected, confidence, 0.001);
      assertTrue(confidence >= 0.0 && confidence <= 1.0);
    }

    @Test
    @DisplayName("confidence should be 0.0 when no matching events")
    void confidenceZeroWhenNoEvents() {
      GoalPredicate predicate = GoalPredicate.eventCount(AgentEventType.SUPERVISOR_APPROVED, 3);

      assertEquals(0.0, predicate.getConfidence(Collections.emptyMap(), Collections.emptyList()));
    }

    @Test
    @DisplayName("description should contain target count and event type")
    void descriptionContent() {
      GoalPredicate predicate = GoalPredicate.eventCount(AgentEventType.FLOW_COMPLETED, 5);

      String desc = predicate.getDescription();
      assertTrue(desc.contains("5"));
      assertTrue(desc.contains("FLOW_COMPLETED"));
    }
  }

  // ==================== CompositeGoalPredicate: all() ====================

  @Nested
  @DisplayName("all (AND)")
  class AllTests {

    @RepeatedTest(5)
    @DisplayName("should be satisfied when all children are satisfied")
    void satisfiedWhenAllSatisfied() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.all(
          GoalPredicate.stateExists(key1),
          GoalPredicate.stateExists(key2));

      Map<String, Object> state = new HashMap<>();
      state.put(key1, ThreadLocalRandom.current().nextInt());
      state.put(key2, ThreadLocalRandom.current().nextInt());

      assertTrue(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @RepeatedTest(5)
    @DisplayName("should not be satisfied when any child is unsatisfied")
    void notSatisfiedWhenOneFails() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.all(
          GoalPredicate.stateExists(key1),
          GoalPredicate.stateExists(key2));

      Map<String, Object> state = new HashMap<>();
      state.put(key1, ThreadLocalRandom.current().nextInt());
      // key2 intentionally missing

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("confidence should be the minimum of children's confidences")
    void confidenceIsMinimum() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      double threshold1 = 100.0;
      double threshold2 = 100.0;

      GoalPredicate predicate = GoalPredicate.all(
          GoalPredicate.greaterThan(key1, threshold1),
          GoalPredicate.greaterThan(key2, threshold2));

      Map<String, Object> state = new HashMap<>();
      state.put(key1, 80.0); // 0.8 confidence
      state.put(key2, 50.0); // 0.5 confidence

      double confidence = predicate.getConfidence(state, Collections.emptyList());
      assertEquals(0.5, confidence, 0.001);
    }
  }

  // ==================== CompositeGoalPredicate: any() ====================

  @Nested
  @DisplayName("any (OR)")
  class AnyTests {

    @RepeatedTest(5)
    @DisplayName("should be satisfied when any child is satisfied")
    void satisfiedWhenAnySatisfied() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.any(
          GoalPredicate.stateExists(key1),
          GoalPredicate.stateExists(key2));

      Map<String, Object> state = new HashMap<>();
      state.put(key2, "present-" + UUID.randomUUID());

      assertTrue(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("should not be satisfied when all children unsatisfied")
    void notSatisfiedWhenNoneSatisfied() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.any(
          GoalPredicate.stateExists(key1),
          GoalPredicate.stateExists(key2));

      Map<String, Object> state = new HashMap<>();
      // Neither key present

      assertFalse(predicate.isSatisfied(state, Collections.emptyList()));
    }

    @Test
    @DisplayName("confidence should be the maximum of children's confidences")
    void confidenceIsMaximum() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      double threshold1 = 100.0;
      double threshold2 = 100.0;

      GoalPredicate predicate = GoalPredicate.any(
          GoalPredicate.greaterThan(key1, threshold1),
          GoalPredicate.greaterThan(key2, threshold2));

      Map<String, Object> state = new HashMap<>();
      state.put(key1, 80.0); // 0.8 confidence
      state.put(key2, 50.0); // 0.5 confidence

      double confidence = predicate.getConfidence(state, Collections.emptyList());
      assertEquals(0.8, confidence, 0.001);
    }
  }

  // ==================== CompositeGoalPredicate: not() ====================

  @Nested
  @DisplayName("not (NOT)")
  class NotTests {

    @RepeatedTest(5)
    @DisplayName("should invert child satisfaction")
    void invertsChild() {
      String key = UUID.randomUUID().toString();
      GoalPredicate inner = GoalPredicate.stateExists(key);
      GoalPredicate negated = GoalPredicate.not(inner);

      Map<String, Object> emptyState = new HashMap<>();
      Map<String, Object> fullState = new HashMap<>();
      fullState.put(key, "value-" + UUID.randomUUID());

      // Inner unsatisfied -> NOT satisfied
      assertTrue(negated.isSatisfied(emptyState, Collections.emptyList()));
      // Inner satisfied -> NOT unsatisfied
      assertFalse(negated.isSatisfied(fullState, Collections.emptyList()));
    }

    @Test
    @DisplayName("confidence should be 1.0 - child confidence")
    void confidenceInversion() {
      String key = UUID.randomUUID().toString();
      double threshold = 100.0;
      GoalPredicate inner = GoalPredicate.greaterThan(key, threshold);
      GoalPredicate negated = GoalPredicate.not(inner);

      Map<String, Object> state = new HashMap<>();
      state.put(key, 60.0); // inner confidence = 0.6

      double confidence = negated.getConfidence(state, Collections.emptyList());
      assertEquals(0.4, confidence, 0.001);
    }
  }

  // ==================== Complex Composition ====================

  @Nested
  @DisplayName("complex composition")
  class ComplexCompositionTests {

    @RepeatedTest(5)
    @DisplayName("all(stateExists, any(greaterThan, eventCount)) should work correctly")
    void complexComposite() {
      String existsKey = UUID.randomUUID().toString();
      String numericKey = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(0.1, 1.0);

      GoalPredicate goal = GoalPredicate.all(
          GoalPredicate.stateExists(existsKey),
          GoalPredicate.any(
              GoalPredicate.greaterThan(numericKey, threshold),
              GoalPredicate.eventCount(AgentEventType.FLOW_COMPLETED, 1)));

      // Scenario 1: existsKey present + numeric above threshold -> satisfied
      Map<String, Object> state1 = new HashMap<>();
      state1.put(existsKey, "present");
      state1.put(numericKey, threshold + 1.0);
      assertTrue(goal.isSatisfied(state1, Collections.emptyList()));

      // Scenario 2: existsKey present + eventCount met -> satisfied
      Map<String, Object> state2 = new HashMap<>();
      state2.put(existsKey, "present");
      List<AgentEvent> events = generateEvents(AgentEventType.FLOW_COMPLETED, 1);
      assertTrue(goal.isSatisfied(state2, events));

      // Scenario 3: existsKey missing -> not satisfied regardless of inner OR
      Map<String, Object> state3 = new HashMap<>();
      state3.put(numericKey, threshold + 100.0);
      assertFalse(goal.isSatisfied(state3, events));

      // Scenario 4: existsKey present but both OR children fail -> not satisfied
      Map<String, Object> state4 = new HashMap<>();
      state4.put(existsKey, "present");
      state4.put(numericKey, threshold * 0.5);
      assertFalse(goal.isSatisfied(state4, Collections.emptyList()));
    }

    @Test
    @DisplayName("deeply nested predicates maintain correct semantics")
    void deeplyNested() {
      String a = UUID.randomUUID().toString();
      String b = UUID.randomUUID().toString();
      String c = UUID.randomUUID().toString();

      // not(all(stateExists(a), not(stateExists(b))))
      // Satisfied when NOT (a exists AND b does NOT exist)
      // i.e., satisfied when a missing OR b exists
      GoalPredicate goal = GoalPredicate.not(
          GoalPredicate.all(
              GoalPredicate.stateExists(a),
              GoalPredicate.not(GoalPredicate.stateExists(b))));

      Map<String, Object> stateAOnly = new HashMap<>();
      stateAOnly.put(a, "val");
      // a exists AND b missing -> inner all is true -> NOT makes it false
      assertFalse(goal.isSatisfied(stateAOnly, Collections.emptyList()));

      Map<String, Object> stateBoth = new HashMap<>();
      stateBoth.put(a, "val");
      stateBoth.put(b, "val");
      // a exists AND b exists -> not(stateExists(b)) is false -> inner all is false -> NOT makes true
      assertTrue(goal.isSatisfied(stateBoth, Collections.emptyList()));

      Map<String, Object> empty = new HashMap<>();
      // a missing -> inner all is false -> NOT makes true
      assertTrue(goal.isSatisfied(empty, Collections.emptyList()));
    }
  }

  // ==================== Confidence Bounds ====================

  @Nested
  @DisplayName("confidence bounds")
  class ConfidenceBoundsTests {

    @RepeatedTest(10)
    @DisplayName("confidence should always be between 0.0 and 1.0 for stateExists")
    void stateExistsConfidenceBounded() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.stateExists(key);

      Map<String, Object> state = new HashMap<>();
      if (ThreadLocalRandom.current().nextBoolean()) {
        state.put(key, UUID.randomUUID().toString());
      }

      double confidence = predicate.getConfidence(state, Collections.emptyList());
      assertTrue(confidence >= 0.0, "Confidence must be >= 0.0, was: " + confidence);
      assertTrue(confidence <= 1.0, "Confidence must be <= 1.0, was: " + confidence);
    }

    @RepeatedTest(10)
    @DisplayName("confidence should always be between 0.0 and 1.0 for greaterThan")
    void greaterThanConfidenceBounded() {
      String key = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(1.0, 1000.0);
      GoalPredicate predicate = GoalPredicate.greaterThan(key, threshold);

      Map<String, Object> state = new HashMap<>();
      state.put(key, ThreadLocalRandom.current().nextDouble(-100.0, 5000.0));

      double confidence = predicate.getConfidence(state, Collections.emptyList());
      assertTrue(confidence >= 0.0, "Confidence must be >= 0.0, was: " + confidence);
      assertTrue(confidence <= 1.0, "Confidence must be <= 1.0, was: " + confidence);
    }

    @RepeatedTest(10)
    @DisplayName("confidence should always be between 0.0 and 1.0 for eventCount")
    void eventCountConfidenceBounded() {
      int targetCount = ThreadLocalRandom.current().nextInt(1, 20);
      int actualCount = ThreadLocalRandom.current().nextInt(0, 40);
      GoalPredicate predicate =
          GoalPredicate.eventCount(AgentEventType.TOOL_CALL_COMPLETED, targetCount);

      List<AgentEvent> events = generateEvents(AgentEventType.TOOL_CALL_COMPLETED, actualCount);

      double confidence = predicate.getConfidence(Collections.emptyMap(), events);
      assertTrue(confidence >= 0.0, "Confidence must be >= 0.0, was: " + confidence);
      assertTrue(confidence <= 1.0, "Confidence must be <= 1.0, was: " + confidence);
    }

    @RepeatedTest(10)
    @DisplayName("confidence should always be between 0.0 and 1.0 for composite predicates")
    void compositeConfidenceBounded() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      double threshold = ThreadLocalRandom.current().nextDouble(1.0, 100.0);

      GoalPredicate predicate = GoalPredicate.all(
          GoalPredicate.stateExists(key1),
          GoalPredicate.any(
              GoalPredicate.greaterThan(key2, threshold),
              GoalPredicate.not(GoalPredicate.stateExists(UUID.randomUUID().toString()))));

      Map<String, Object> state = new HashMap<>();
      if (ThreadLocalRandom.current().nextBoolean()) {
        state.put(key1, UUID.randomUUID().toString());
      }
      if (ThreadLocalRandom.current().nextBoolean()) {
        state.put(key2, ThreadLocalRandom.current().nextDouble(-50.0, 200.0));
      }

      double confidence = predicate.getConfidence(state, Collections.emptyList());
      assertTrue(confidence >= 0.0, "Confidence must be >= 0.0, was: " + confidence);
      assertTrue(confidence <= 1.0, "Confidence must be <= 1.0, was: " + confidence);
    }
  }

  // ==================== Diagnostics ====================

  @Nested
  @DisplayName("diagnostics")
  class DiagnosticsTests {

    @Test
    @DisplayName("stateExists diagnostics should contain key existence info")
    void stateExistsDiagnostics() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.stateExists(key);

      Map<String, Object> state = new HashMap<>();
      state.put(key, "something");

      Map<String, Boolean> diag = predicate.getDiagnostics(state, Collections.emptyList());
      assertNotNull(diag);
      assertFalse(diag.isEmpty());
    }

    @Test
    @DisplayName("composite diagnostics should include child info")
    void compositeDiagnostics() {
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.all(
          GoalPredicate.stateExists(key1),
          GoalPredicate.stateExists(key2));

      Map<String, Object> state = new HashMap<>();
      state.put(key1, "val1");

      Map<String, Boolean> diag = predicate.getDiagnostics(state, Collections.emptyList());
      assertNotNull(diag);
      // Should have child_0_satisfied and child_1_satisfied
      assertTrue(diag.containsKey("child_0_satisfied"));
      assertTrue(diag.containsKey("child_1_satisfied"));
      assertTrue(diag.get("child_0_satisfied"));
      assertFalse(diag.get("child_1_satisfied"));
    }

    @Test
    @DisplayName("eventCount diagnostics should report count status")
    void eventCountDiagnostics() {
      GoalPredicate predicate = GoalPredicate.eventCount(AgentEventType.FLOW_COMPLETED, 3);

      List<AgentEvent> events = generateEvents(AgentEventType.FLOW_COMPLETED, 3);

      Map<String, Boolean> diag = predicate.getDiagnostics(Collections.emptyMap(), events);
      assertNotNull(diag);
      assertTrue(diag.get("goal"));
    }
  }

  // ==================== CompletionTracker Integration ====================

  @Nested
  @DisplayName("CompletionTracker integration")
  class CompletionTrackerIntegrationTests {

    @Test
    @DisplayName("isGoalSatisfied should delegate to the predicate with tracker state")
    void isGoalSatisfiedDelegates() {
      String key = UUID.randomUUID().toString();
      String value = UUID.randomUUID().toString();

      TaskList taskList = TaskList.builder().addTask("dummy", true).build();
      CompletionTracker tracker = new CompletionTracker("flow-" + UUID.randomUUID(), taskList);

      // Feed an event that populates state
      AgentEvent event = makeEvent(AgentEventType.FLOW_STARTED);
      event.putData(key, value);
      tracker.processEvent(event);

      GoalPredicate goal = GoalPredicate.stateExists(key);
      assertTrue(tracker.isGoalSatisfied(goal));
    }

    @Test
    @DisplayName("getGoalConfidence should delegate to the predicate with tracker state")
    void getGoalConfidenceDelegates() {
      String key = UUID.randomUUID().toString();
      double threshold = 100.0;
      double value = 60.0;

      TaskList taskList = TaskList.builder().addTask("dummy", true).build();
      CompletionTracker tracker = new CompletionTracker("flow-" + UUID.randomUUID(), taskList);

      AgentEvent event = makeEvent(AgentEventType.FLOW_STARTED);
      event.putData(key, value);
      tracker.processEvent(event);

      GoalPredicate goal = GoalPredicate.greaterThan(key, threshold);
      assertEquals(0.6, tracker.getGoalConfidence(goal), 0.001);
    }

    @Test
    @DisplayName("isGoalSatisfied with eventCount should use tracker event history")
    void isGoalSatisfiedWithEventHistory() {
      TaskList taskList = TaskList.builder().addTask("dummy", true).build();
      CompletionTracker tracker = new CompletionTracker("flow-" + UUID.randomUUID(), taskList);

      GoalPredicate goal = GoalPredicate.eventCount(AgentEventType.TOOL_CALL_COMPLETED, 3);
      assertFalse(tracker.isGoalSatisfied(goal));

      for (int i = 0; i < 3; i++) {
        AgentEvent event = makeEvent(AgentEventType.TOOL_CALL_COMPLETED);
        event.putData("tool_name", "tool-" + UUID.randomUUID());
        tracker.processEvent(event);
      }

      assertTrue(tracker.isGoalSatisfied(goal));
    }
  }

  // ==================== Edge Cases ====================

  @Nested
  @DisplayName("edge cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("NOT mode should reject multiple children")
    void notRejectsMultipleChildren() {
      assertThrows(IllegalArgumentException.class, () ->
          new CompositeGoalPredicate(
              CompositeGoalPredicate.CompositeMode.NOT,
              List.of(GoalPredicate.stateExists("a"), GoalPredicate.stateExists("b"))));
    }

    @Test
    @DisplayName("composite with empty children should throw")
    void compositeRejectsEmptyChildren() {
      assertThrows(IllegalArgumentException.class, () ->
          new CompositeGoalPredicate(CompositeGoalPredicate.CompositeMode.AND, List.of()));
    }

    @Test
    @DisplayName("StateExistsPredicate should reject null key")
    void stateExistsRejectsNullKey() {
      assertThrows(IllegalArgumentException.class, () -> new StateExistsPredicate(null));
    }

    @Test
    @DisplayName("NumericThresholdPredicate should reject null key")
    void numericThresholdRejectsNullKey() {
      assertThrows(IllegalArgumentException.class, () ->
          new NumericThresholdPredicate(null, 1.0));
    }

    @Test
    @DisplayName("EventCountPredicate should reject null event type")
    void eventCountRejectsNullType() {
      assertThrows(IllegalArgumentException.class, () -> new EventCountPredicate(null, 1));
    }

    @Test
    @DisplayName("EventCountPredicate should reject non-positive target count")
    void eventCountRejectsZeroTarget() {
      assertThrows(IllegalArgumentException.class, () ->
          new EventCountPredicate(AgentEventType.FLOW_COMPLETED, 0));
    }

    @Test
    @DisplayName("greaterThan with negative values should behave correctly")
    void greaterThanWithNegativeValues() {
      String key = UUID.randomUUID().toString();
      GoalPredicate predicate = GoalPredicate.greaterThan(key, -10.0);

      Map<String, Object> state = new HashMap<>();
      state.put(key, -5.0);

      assertTrue(predicate.isSatisfied(state, Collections.emptyList()));
    }
  }
}
