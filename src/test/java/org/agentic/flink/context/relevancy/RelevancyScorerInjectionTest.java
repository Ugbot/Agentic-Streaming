package org.agentic.flink.context.relevancy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.Scorer;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RelevancyScorerInjectionTest {

  @Test
  @DisplayName("Injected Scorer overrides the built-in heuristic path")
  void injectedScorerWins() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    double fixed = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
    Scorer scorer =
        new Scorer() {
          @Override
          public double score(String input, InferenceSetup setup) {
            return fixed;
          }

          @Override
          public double scorePair(String input, String reference, InferenceSetup setup) {
            calls.incrementAndGet();
            return fixed;
          }
        };

    InferenceSetup setup =
        InferenceSetup.builder()
            .withModelName("scorer-v1")
            .withModelUri("djl://test/" + UUID.randomUUID())
            .build();

    RelevancyScorer rs = new RelevancyScorer(scorer, setup);
    ContextItem item =
        new ContextItem(
            "user prefers SI units", ContextPriority.MUST, MemoryType.SHORT_TERM);
    double observed = rs.scoreRelevancy(item, "convert measurements").get();
    assertEquals(fixed, observed, 1e-9);
    assertEquals(1, calls.get());
  }

  @Test
  @DisplayName("Injected Scorer score is clamped into [0, 1]")
  void scoreIsClamped() throws Exception {
    Scorer wild = (text, setup) -> 5.0;
    Scorer wildPair =
        new Scorer() {
          @Override
          public double score(String input, InferenceSetup setup) {
            return wild.score(input, setup);
          }

          @Override
          public double scorePair(String i, String r, InferenceSetup s) {
            return 5.0;
          }
        };
    RelevancyScorer rs =
        new RelevancyScorer(
            wildPair,
            InferenceSetup.builder().withModelName("m").withModelUri("u").build());
    double observed =
        rs.scoreRelevancy(
                new ContextItem("anything", ContextPriority.MUST, MemoryType.SHORT_TERM),
                "anything")
            .get();
    assertTrue(observed >= 0.0 && observed <= 1.0);
    assertEquals(1.0, observed, 1e-9);
  }

  @Test
  @DisplayName("Heuristic path still works when no Scorer is injected")
  void heuristicPathStillWorks() throws Exception {
    RelevancyScorer rs = new RelevancyScorer(new HashMap<>());
    ContextItem item =
        new ContextItem("hello world", ContextPriority.SHOULD, MemoryType.SHORT_TERM);
    Double score = rs.scoreRelevancy(item, "hello").get();
    assertNotNull(score);
    assertTrue(score >= 0.0 && score <= 1.0);
  }
}
