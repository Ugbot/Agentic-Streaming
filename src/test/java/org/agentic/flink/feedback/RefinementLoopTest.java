package org.agentic.flink.feedback;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.LexiconInferenceConnection;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.Test;

/**
 * Zero-infra tests for the generate → check → feed back → retry loop. A scripted generator returns
 * a different output per attempt (keyed off how many user turns are in the conversation), so we can
 * model "improves once it sees the critique" deterministically with no LLM.
 */
class RefinementLoopTest {

  /** Returns outputs[attemptIndex], where attemptIndex = (#user messages) - 1, clamped. */
  private static ChatConnection scriptedSequence(String... outputs) {
    return new ChatConnection() {
      @Override
      public ChatClient bind(RuntimeContext rc) {
        return new ChatClient() {
          @Override
          public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
            long userTurns = messages.stream().filter(m -> m.getRole() == ChatRole.USER).count();
            int idx = (int) Math.min(Math.max(userTurns - 1, 0), outputs.length - 1);
            return new ChatResponse(outputs[idx], "scripted", List.of(), 0L, ChatResponse.FinishReason.STOP);
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
  void improvesAndAcceptsWithinBudget() {
    RefinementLoop loop =
        RefinementLoop.builder()
            .withChatConnection(
                scriptedSequence("a rough draft about streaming",
                    "final answer mentioning flink and streaming"),
                null)
            .withCheck(KeywordQualityCheck.requiring("flink", "streaming"))
            .withMaxAttempts(3)
            .build();
    RefinementResult r = loop.refine("Explain Apache Flink");
    assertTrue(r.accepted);
    assertEquals(2, r.attemptsUsed);
    assertEquals(2, r.trace.size());
    assertTrue(r.trace.get(1).score > r.trace.get(0).score, "score should improve after feedback");
    assertTrue(r.finalText.toLowerCase().contains("flink"));
  }

  @Test
  void acceptsImmediatelyWhenFirstPasses() {
    RefinementLoop loop =
        RefinementLoop.builder()
            .withChatConnection(scriptedSequence("flink is a streaming engine"), null)
            .withCheck(KeywordQualityCheck.requiring("flink", "streaming"))
            .withMaxAttempts(3)
            .build();
    RefinementResult r = loop.refine("Explain Flink");
    assertTrue(r.accepted);
    assertEquals(1, r.attemptsUsed);
  }

  @Test
  void exhaustsBudgetReturnsBestSoFar() {
    // attempt1 score 0, attempt2 score 0.5 (one of two terms), attempt3 score 0 → best is attempt2.
    RefinementLoop loop =
        RefinementLoop.builder()
            .withChatConnection(
                scriptedSequence("nothing useful", "mentions flink only", "still nothing"), null)
            .withCheck(KeywordQualityCheck.requiring("flink", "streaming"))
            .withMaxAttempts(3)
            .build();
    RefinementResult r = loop.refine("Explain Flink");
    assertFalse(r.accepted);
    assertEquals(3, r.attemptsUsed);
    assertEquals("mentions flink only", r.finalText, "best-so-far is the highest-scoring attempt");
    assertEquals(0.5, r.finalScore(), 1e-9);
  }

  @Test
  void classifierCheckZeroInfraAcceptsBenignOutput() {
    RefinementLoop loop =
        RefinementLoop.builder()
            .withChatConnection(scriptedSequence("a friendly note about lunch plans"), null)
            .withCheck(
                new ClassifierQualityCheck(
                    new LexiconInferenceConnection(),
                    InferenceSetup.builder().withModelName("lexicon").withModelUri("lexicon://x").build(),
                    0.3, // ceiling
                    true)) // pass if suspicion below 0.3
            .withMaxAttempts(2)
            .build();
    RefinementResult r = loop.refine("Write a benign note");
    assertTrue(r.accepted, "benign text should pass the suspicion-ceiling check");
    assertEquals(1, r.attemptsUsed);
  }
}
