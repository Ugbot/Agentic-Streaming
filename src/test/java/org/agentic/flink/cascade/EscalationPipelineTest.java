package org.agentic.flink.cascade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.Test;

/**
 * Zero-infra tests for the filter -> ML -> LLM cascade. Uses the built-in lexicon classifier and a
 * scripted chat connection, so no model server or API key is needed.
 */
class EscalationPipelineTest {

  /** A chat connection that always returns a fixed verdict line. */
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

  @Test
  void cleanInputStopsAtFilter() {
    EscalationPipeline p = EscalationPipeline.builder().build();
    EscalationPipeline.Decision d = p.evaluate("Lovely weather today, lunch at noon?");
    assertEquals(EscalationPipeline.Tier.FILTER, d.decidedBy);
    assertEquals("CLEAN", d.verdict);
    assertFalse(d.suspicious);
  }

  @Test
  void weakMatchClearsAtMlTier() {
    // "refund" is a low-weight term: filter matches, but ML score stays under 0.5.
    EscalationPipeline p = EscalationPipeline.builder().build();
    EscalationPipeline.Decision d = p.evaluate("Hi, I would like a refund please.");
    assertEquals(EscalationPipeline.Tier.ML, d.decidedBy);
    assertEquals("CLEAN", d.verdict);
    assertFalse(d.suspicious);
    assertTrue(d.mlScore < 0.5, "expected sub-threshold ML score, got " + d.mlScore);
  }

  @Test
  void confirmedSuspiciousWithoutLlmRoutesToReview() {
    EscalationPipeline p = EscalationPipeline.builder().build(); // no chat configured
    EscalationPipeline.Decision d =
        p.evaluate("URGENT: verify your account and send a wire transfer plus a gift card now");
    assertEquals(EscalationPipeline.Tier.ML, d.decidedBy);
    assertEquals("REVIEW", d.verdict);
    assertTrue(d.suspicious);
    assertTrue(d.mlScore >= 0.5);
  }

  @Test
  void confirmedSuspiciousEscalatesToLlmAndBlocks() {
    EscalationPipeline p =
        EscalationPipeline.builder()
            .withChatConnection(scripted("BLOCK - classic phishing: account verification + wire transfer."), null)
            .build();
    EscalationPipeline.Decision d =
        p.evaluate("URGENT: verify your account and send a wire transfer plus a gift card now");
    assertEquals(EscalationPipeline.Tier.LLM, d.decidedBy);
    assertEquals("BLOCK", d.verdict);
    assertTrue(d.suspicious);
    assertTrue(d.llmRationale.startsWith("BLOCK"));
  }

  @Test
  void llmCanAllowAFalsePositive() {
    EscalationPipeline p =
        EscalationPipeline.builder()
            .withChatConnection(scripted("ALLOW - legitimate refund request, no fraud indicators."), null)
            .build();
    EscalationPipeline.Decision d =
        p.evaluate("Please process the refund and wire transfer to my verified account.");
    assertEquals(EscalationPipeline.Tier.LLM, d.decidedBy);
    assertEquals("ALLOW", d.verdict);
    assertFalse(d.suspicious);
  }
}
