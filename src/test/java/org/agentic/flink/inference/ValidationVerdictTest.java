package org.agentic.flink.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.agentic.flink.llm.ChatResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Regression for the validator collapsing {@code INVALID} into {@code VALID} via substring match. */
class ValidationVerdictTest {

  private static String reason() {
    return "reason-" + UUID.randomUUID();
  }

  private static String randomCase(String word) {
    StringBuilder sb = new StringBuilder();
    for (char c : word.toCharArray()) {
      sb.append(ThreadLocalRandom.current().nextBoolean() ? Character.toUpperCase(c) : Character.toLowerCase(c));
    }
    return sb.toString();
  }

  static Stream<Arguments> invalidResponses() {
    return Stream.of(
        Arguments.of("INVALID: " + reason()),
        Arguments.of(randomCase("invalid") + " - " + reason()),
        Arguments.of("The response is INVALID because " + reason()),
        Arguments.of("Verdict: INVALID\nConfidence: 0." + ThreadLocalRandom.current().nextInt(10, 99)),
        Arguments.of("invalid"));
  }

  static Stream<Arguments> validResponses() {
    return Stream.of(
        Arguments.of("VALID: " + reason()),
        Arguments.of(randomCase("valid") + " - " + reason()),
        Arguments.of("The response is VALID; " + reason()),
        Arguments.of("Verdict: VALID (score 0.9" + ThreadLocalRandom.current().nextInt(10) + ")"),
        Arguments.of("valid"));
  }

  @ParameterizedTest
  @MethodSource("invalidResponses")
  @DisplayName("INVALID is never read as VALID")
  void invalidIsInvalid(String response) {
    ValidationVerdict v = ValidationVerdict.parse(response);
    assertEquals(ValidationVerdict.Outcome.INVALID, v.getOutcome(), response);
    assertFalse(v.isValid(), response);
  }

  @ParameterizedTest
  @MethodSource("validResponses")
  @DisplayName("VALID is read as VALID")
  void validIsValid(String response) {
    ValidationVerdict v = ValidationVerdict.parse(response);
    assertEquals(ValidationVerdict.Outcome.VALID, v.getOutcome(), response);
    assertTrue(v.isValid(), response);
  }

  @Test
  @DisplayName("a score following the verdict is extracted")
  void scoreExtracted() {
    double score = ThreadLocalRandom.current().nextInt(0, 101) / 100.0;
    ValidationVerdict v = ValidationVerdict.parse("VALID\nscore: " + score);
    assertEquals(score, v.getScore(), 1e-9);
    ValidationVerdict inv = ValidationVerdict.parse("INVALID (score=" + score + ")");
    assertEquals(score, inv.getScore(), 1e-9);
    assertEquals(ValidationVerdict.Outcome.INVALID, inv.getOutcome());
  }

  @Test
  @DisplayName("no verdict word, blank or null responses are UNDETERMINED, not VALID")
  void undetermined() {
    for (String r : List.of("", "   ", reason(), "validation pending", "invalidated data")) {
      ValidationVerdict v = ValidationVerdict.parse(r);
      assertEquals(ValidationVerdict.Outcome.UNDETERMINED, v.getOutcome(), r);
      assertFalse(v.isValid(), r);
    }
    assertEquals(ValidationVerdict.Outcome.UNDETERMINED, ValidationVerdict.parse(null).getOutcome());
  }

  @Test
  @DisplayName("ValidationGuardrail blocks INVALID and UNDETERMINED, allows VALID")
  void guardrail() {
    ValidationGuardrail guardrail = new ValidationGuardrail("judge-" + UUID.randomUUID());
    String model = "model-" + UUID.randomUUID();
    String agent = "agent-" + UUID.randomUUID();

    GuardrailDecision blocked =
        guardrail.afterChat(agent, new ChatResponse("INVALID: " + reason(), model, null, null, null));
    assertTrue(blocked.isBlock());
    assertEquals(model, blocked.getModelName());
    assertTrue(blocked.getReason().contains("INVALID"), blocked.getReason());

    GuardrailDecision allowed =
        guardrail.afterChat(agent, new ChatResponse("VALID: " + reason(), model, null, null, null));
    assertFalse(allowed.isBlock());

    assertTrue(guardrail.afterChat(agent, new ChatResponse(reason(), model, null, null, null)).isBlock());
    assertTrue(guardrail.afterChat(agent, null).isBlock());
  }
}
