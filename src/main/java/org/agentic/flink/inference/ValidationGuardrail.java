package org.agentic.flink.inference;

import org.agentic.flink.llm.ChatResponse;

/**
 * Post-chat {@link Guardrail} for validator/judge agents: blocks the response unless the model
 * answered {@code VALID}. {@code INVALID} and responses with no verdict word are both blocked.
 */
public final class ValidationGuardrail implements Guardrail {
  private static final long serialVersionUID = 1L;

  private final String name;

  public ValidationGuardrail() {
    this("ValidationGuardrail");
  }

  public ValidationGuardrail(String name) {
    this.name = name == null ? "ValidationGuardrail" : name;
  }

  @Override
  public GuardrailDecision afterChat(String agentId, ChatResponse response) {
    String text = response == null ? null : response.getText();
    ValidationVerdict verdict = ValidationVerdict.parse(text);
    if (verdict.isValid()) {
      return GuardrailDecision.allow();
    }
    String model = response == null ? null : response.getModelName();
    return GuardrailDecision.block(
        "validation verdict " + verdict.getOutcome() + " (score=" + verdict.getScore() + ")", model);
  }

  @Override
  public String name() {
    return name;
  }
}
