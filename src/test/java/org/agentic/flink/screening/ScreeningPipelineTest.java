package org.agentic.flink.screening;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.inference.LexiconInferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.Test;

/**
 * Zero-infra, deterministic tests for the layered screening cascade. No model server, no API key;
 * timestamps are explicit so window math is reproducible.
 */
class ScreeningPipelineTest {

  private static ChatConnection scripted(String reply) {
    return new ChatConnection() {
      @Override
      public ChatClient bind(RuntimeContext rc) {
        return new ChatClient() {
          @Override
          public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
            return new ChatResponse(reply, "scripted", List.of(), 0L, ChatResponse.FinishReason.STOP);
          }

          @Override
          public String providerName() {
            return "scripted";
          }
        };
      }
    };
  }

  private static boolean firedPhase(ScreeningResult r, Phase p) {
    return r.fired.stream().anyMatch(s -> s.phase() == p);
  }

  private static ScreenItem item(String key, double value, String label, long ts) {
    return ScreenItem.of(key, value, label, ts);
  }

  @Test
  void inBandItemAllowedAtRules() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new BandPassDetector(0, 1000, 0.6))
            .build();
    ScreeningResult r = p.screen(item("a1", 50, "coffee", 0));
    assertEquals(ScreeningResult.Tier.RULES, r.decidedBy);
    assertEquals("ALLOW", r.verdict);
    assertTrue(r.fired.isEmpty());
  }

  @Test
  void outOfBandFiresBandPass() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new BandPassDetector(0, 1000, 0.6))
            .build(); // no classifier, no chat
    ScreeningResult r = p.screen(item("a2", 5000, "coffee", 0));
    assertTrue(firedPhase(r, Phase.BAND_PASS));
    assertEquals("REVIEW", r.verdict); // risk 0.6 >= review 0.5, no LLM
    assertEquals(ScreeningResult.Tier.RULES, r.decidedBy);
  }

  @Test
  void threeIdenticalInARowFiresRepeat() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new RepeatDetector(3, 0.8))
            .build();
    // In-band-equivalent (no band-pass detector), spaced 10s apart so velocity is irrelevant.
    assertFalse(firedPhase(p.screen(item("pay", 50, "x", 0)), Phase.REPEAT));
    assertFalse(firedPhase(p.screen(item("pay", 50, "x", 10_000)), Phase.REPEAT));
    ScreeningResult third = p.screen(item("pay", 50, "x", 20_000));
    assertTrue(firedPhase(third, Phase.REPEAT), "3rd identical item should fire repeat");
  }

  @Test
  void velocityFiresWithinWindowOnly() {
    ScreeningPipeline fast =
        ScreeningPipeline.builder()
            .addDetector(new VelocityDetector(3, Duration.ofSeconds(5), 0.7))
            .build();
    fast.screen(item("acc", 1, "x", 0));
    fast.screen(item("acc", 1, "x", 1_000));
    assertTrue(firedPhase(fast.screen(item("acc", 1, "x", 2_000)), Phase.VELOCITY));

    ScreeningPipeline slow =
        ScreeningPipeline.builder()
            .addDetector(new VelocityDetector(3, Duration.ofSeconds(5), 0.7))
            .build();
    slow.screen(item("acc", 1, "x", 0));
    slow.screen(item("acc", 1, "x", 10_000));
    assertFalse(firedPhase(slow.screen(item("acc", 1, "x", 20_000)), Phase.VELOCITY));
  }

  @Test
  void weakRuleClearsAtMlBelowThreshold() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new BandPassDetector(0, 1000, 0.2)) // weak
            .withClassifier(new LexiconInferenceConnection(),
                InferenceSetup.builder().withModelName("lexicon").withModelUri("lexicon://x").build())
            .build();
    ScreeningResult r = p.screen(item("a", 5000, "coffee with friends", 0)); // benign text
    assertEquals(ScreeningResult.Tier.ML, r.decidedBy);
    assertEquals("ALLOW", r.verdict);
    assertTrue(r.combinedRisk < 0.5, "combined risk " + r.combinedRisk);
  }

  @Test
  void confirmedSuspiciousWithoutLlmRoutesToReview() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new BandPassDetector(0, 1000, 0.6))
            .withClassifier(new LexiconInferenceConnection(),
                InferenceSetup.builder().withModelName("lexicon").withModelUri("lexicon://x").build())
            .build(); // no chat
    ScreeningResult r =
        p.screen(item("a", 5000, "urgent wire transfer gift card verify your account", 0));
    assertEquals(ScreeningResult.Tier.ML, r.decidedBy);
    assertEquals("REVIEW", r.verdict);
    assertTrue(r.combinedRisk >= 0.5);
  }

  @Test
  void escalatesToLlmAndBlocks() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new BandPassDetector(0, 1000, 0.6))
            .withClassifier(new LexiconInferenceConnection(),
                InferenceSetup.builder().withModelName("lexicon").withModelUri("lexicon://x").build())
            .withChatConnection(scripted("BLOCK - layered fraud signals."), null)
            .build();
    ScreeningResult r = p.screen(item("a", 5000, "wire transfer gift card", 0));
    assertEquals(ScreeningResult.Tier.LLM, r.decidedBy);
    assertEquals("BLOCK", r.verdict);
  }

  @Test
  void autoBlocksOnOverwhelmingRisk() {
    ScreeningPipeline p =
        ScreeningPipeline.builder()
            .addDetector(new BandPassDetector(0, 1000, 1.5))
            .addDetector(new RepeatDetector(2, 1.0))
            .withBlockThreshold(2.0)
            .withChatConnection(scripted("ALLOW - should not be consulted"), null)
            .build();
    p.screen(item("k", 9999, "x", 0));
    ScreeningResult r = p.screen(item("k", 9999, "x", 100)); // band-pass 1.5 + repeat 1.0 = 2.5
    assertEquals("BLOCK", r.verdict);
    assertTrue(r.decidedBy != ScreeningResult.Tier.LLM, "auto-block should not consult the LLM");
  }
}
