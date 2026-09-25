package org.agentic.flink.example.moderation;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.inference.ClassificationResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * toxic-bert is a multi-label classifier: every post gets a top label, so a benign post still
 * comes back as {@code toxic} with a score near zero. The showcase must gate on the score as
 * well as the label, otherwise every post lands on the blocked side output and the summarize
 * path never runs.
 */
class ContentModerationExampleTest {

  private static final Set<String> BLOCKED = Set.of("toxic", "severe_toxic", "obscene", "threat");

  @Test
  @DisplayName("A blocked label with a low score is not blocked")
  void lowScoreBlockedLabelPasses() {
    double score = ThreadLocalRandom.current().nextDouble(0.0, ContentModerationExample.BLOCK_THRESHOLD);
    ClassificationResult cls = new ClassificationResult("toxic", score, Map.of("toxic", score));
    assertFalse(ContentModerationExample.shouldBlock(cls, BLOCKED));
  }

  @Test
  @DisplayName("A blocked label at or above the threshold is blocked")
  void highScoreBlockedLabelIsBlocked() {
    double score =
        ThreadLocalRandom.current().nextDouble(ContentModerationExample.BLOCK_THRESHOLD, 1.0);
    ClassificationResult cls = new ClassificationResult("threat", score, Map.of("threat", score));
    assertTrue(ContentModerationExample.shouldBlock(cls, BLOCKED));
  }

  @Test
  @DisplayName("A label outside the blocked set is never blocked")
  void unlistedLabelPasses() {
    double score = ThreadLocalRandom.current().nextDouble(0.5, 1.0);
    ClassificationResult cls =
        new ClassificationResult("identity_hate", score, Map.of("identity_hate", score));
    assertFalse(ContentModerationExample.shouldBlock(cls, BLOCKED));
  }
}
