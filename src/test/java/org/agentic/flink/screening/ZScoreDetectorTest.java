package org.agentic.flink.screening;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Zero-infra, deterministic tests for the rolling-window z-score detector. */
class ZScoreDetectorTest {

  private static boolean fired(ScreeningResult r, Phase p) {
    return r.fired.stream().anyMatch(s -> s.phase() == p);
  }

  @Test
  void noFireUntilWarmupAndOnFlatBaseline() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new ZScoreDetector("spread-z", Phase.CLASSIFIER, 3.0, 5, 0.9))
            .withReviewThreshold(0.5)
            .withBlockThreshold(2.0)
            .build();
    // Below warmup → never fire.
    for (int i = 0; i < 3; i++) {
      assertFalse(
          fired(p.screen(ScreenItem.of("k", 10.0, "x", i * 1000)), Phase.CLASSIFIER),
          "must not fire before warmup");
    }
    // After warmup, flat baseline (std=0) → still no fire (z undefined).
    for (int i = 3; i < 8; i++) {
      assertFalse(
          fired(p.screen(ScreenItem.of("k", 10.0, "x", i * 1000)), Phase.CLASSIFIER),
          "flat baseline must not fire");
    }
  }

  @Test
  void firesOnClearSpikeAfterWarmup() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new ZScoreDetector("v-z", Phase.CLASSIFIER, 3.0, 5, 0.9))
            .withReviewThreshold(0.5)
            .withBlockThreshold(2.0)
            .build();
    // Build a non-flat baseline.
    double[] baseline = {10.0, 10.5, 9.8, 10.2, 10.1, 9.9, 10.3};
    for (int i = 0; i < baseline.length; i++) {
      ScreeningResult r = p.screen(ScreenItem.of("k", baseline[i], "x", i * 1000));
      assertFalse(fired(r, Phase.CLASSIFIER), "in-baseline value " + baseline[i] + " should not fire");
    }
    // Now a huge spike: should be many σ above mean ~10.
    ScreeningResult spike = p.screen(ScreenItem.of("k", 100.0, "x", 8_000));
    assertTrue(fired(spike, Phase.CLASSIFIER), "100 against ~10 baseline should fire z-score");
  }

  @Test
  void onAttrModeReadsNumericAttribute() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(ZScoreDetector.onAttr("depth", Phase.CLASSIFIER, 2.5, 5, 0.8))
            .withReviewThreshold(0.5)
            .withBlockThreshold(2.0)
            .build();
    double[] depths = {5000, 5100, 4800, 5050, 4950, 5200, 5150};
    for (int i = 0; i < depths.length; i++) {
      p.screen(new ScreenItem("k", 1.0, "x", i * 1000, Map.of("depth", Double.toString(depths[i]))));
    }
    ScreeningResult collapse =
        p.screen(new ScreenItem("k", 1.0, "x", 8_000, Map.of("depth", "100")));
    assertTrue(fired(collapse, Phase.CLASSIFIER), "depth collapse should fire on-attr z-score");
  }

  @Test
  void missingAttributeDoesNotFire() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(ZScoreDetector.onAttr("imbalance", Phase.CLASSIFIER, 2.0, 3, 0.8))
            .build();
    // No "imbalance" key in attrs.
    for (int i = 0; i < 6; i++) {
      ScreeningResult r =
          p.screen(new ScreenItem("k", 1.0, "x", i * 1000, Map.of("other", "1.0")));
      assertEquals(ScreeningResult.Tier.RULES, r.decidedBy);
      assertEquals("ALLOW", r.verdict);
    }
  }
}
