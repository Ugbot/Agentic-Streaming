package org.agentic.flink.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The legacy {@link ValidationExecutor} reads verdicts through the shared parser. */
@SuppressWarnings("deprecation")
class ValidationExecutorTest {

  private static ValidationExecutor executor(String judgeResponse) {
    LLMClient llm =
        LLMClient.builder()
            .withModel("m")
            .build(
                new AgentExecutorTest.ScriptedConnection(
                    List.of(AgentExecutorTest.text(judgeResponse))));
    return new ValidationExecutor(llm);
  }

  @Test
  @DisplayName("INVALID and NOT VALID wordings fail with score 0")
  void invalidFails() {
    for (String judge :
        List.of(
            "INVALID\nScore: 0.2\nReason: " + UUID.randomUUID(),
            "The output is not valid: " + UUID.randomUUID(),
            "not-VALID " + UUID.randomUUID(),
            "invalid")) {
      ValidationExecutor.ValidationResult r =
          executor(judge).validate("out-" + UUID.randomUUID(), null);
      assertFalse(r.isValid(), judge);
      assertEquals(0.0, r.getScore(), 1e-9, judge);
      assertEquals(judge.trim(), r.getMessage());
    }
  }

  @Test
  @DisplayName("VALID passes and an explicit Score: line is honoured")
  void validPassesWithScore() {
    double score = ThreadLocalRandom.current().nextInt(1, 100) / 100.0;
    ValidationExecutor.ValidationResult r =
        executor("VALID\nScore: " + score + "\nReason: " + UUID.randomUUID())
            .validate("out-" + UUID.randomUUID(), "custom prompt " + UUID.randomUUID());
    assertTrue(r.isValid());
    assertEquals(score, r.getScore(), 1e-9);

    ValidationExecutor.ValidationResult bare = executor("valid, looks fine").validate("x", null);
    assertTrue(bare.isValid());
    assertEquals(1.0, bare.getScore(), 1e-9);
  }

  @Test
  @DisplayName("a response with no verdict word fails instead of passing")
  void noVerdictFails() {
    ValidationExecutor.ValidationResult r =
        executor("looks fine " + UUID.randomUUID()).validate("x", null);
    assertFalse(r.isValid());
    assertEquals(0.0, r.getScore(), 1e-9);
  }
}
