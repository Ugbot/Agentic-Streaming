package org.agentic.flink.execution;

import java.io.Serializable;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.inference.ValidationVerdict;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes validation of agent outputs.
 *
 * <p>Validates agent responses against:
 *
 * <ul>
 *   <li>Output format/schema
 *   <li>Business rules
 *   <li>Quality thresholds
 *   <li>Custom validation prompts
 * </ul>
 *
 * <p><b>Placeholder Implementation:</b> This is a stub for Phase 3. Full implementation will
 * include:
 *
 * <ul>
 *   <li>LLM-based validation (using a validator prompt)
 *   <li>Rule-based validation (JSON schema, regex)
 *   <li>Quality scoring
 *   <li>Validation retries
 * </ul>
 *
 * @author Agentic Flink Team
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction}.
 */
@Deprecated
public class ValidationExecutor implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ValidationExecutor.class);

  private final LLMClient llmClient;

  public ValidationExecutor(LLMClient llmClient) {
    this.llmClient = llmClient;
  }

  /**
   * Validates agent output using LLM.
   *
   * @param output The output to validate
   * @param validationPrompt Optional custom validation prompt
   * @return Validation result
   */
  public ValidationResult validate(String output, String validationPrompt) {
    LOG.debug("Validating output with LLM");

    try {
      // Build validation prompt
      String prompt = buildValidationPrompt(output, validationPrompt);

      // Call LLM for validation
      String llmResponse = llmClient.generate(prompt);

      // Parse LLM response to extract validation decision
      ValidationResult result = parseValidationResponse(llmResponse);

      LOG.info("Validation result: valid={}, score={}", result.isValid(), result.getScore());
      return result;

    } catch (Exception e) {
      LOG.error("Validation failed with error: {}", e.getMessage(), e);

      // On error, return failed validation
      ValidationResult result = new ValidationResult();
      result.setValid(false);
      result.setScore(0.0);
      result.setMessage("Validation error: " + e.getMessage());
      return result;
    }
  }

  private String buildValidationPrompt(String output, String customPrompt) {
    if (customPrompt != null && !customPrompt.isEmpty()) {
      return customPrompt
          + "\n\nOutput to validate:\n"
          + output
          + "\n\nRespond with: VALID or INVALID, followed by a score (0.0-1.0) and reason.";
    }

    return "You are a validator. Review the following output and determine if it is valid.\n\n"
        + "Output:\n"
        + output
        + "\n\n"
        + "Respond in this format:\n"
        + "VALID or INVALID\n"
        + "Score: 0.0-1.0\n"
        + "Reason: <your reason>";
  }

  ValidationResult parseValidationResponse(String llmResponse) {
    ValidationResult result = new ValidationResult();
    ValidationVerdict verdict = ValidationVerdict.parse(llmResponse);
    result.setValid(verdict.isValid());

    double score = verdict.getScore();
    if (llmResponse != null && (llmResponse.contains("Score:") || llmResponse.contains("score:"))) {
      try {
        String[] parts = llmResponse.split("[Ss]core:");
        if (parts.length > 1) {
          String scoreStr = parts[1].trim().split("\\s+")[0];
          score = Double.parseDouble(scoreStr);
        }
      } catch (Exception e) {
        LOG.warn("Could not parse score from validation response, using verdict score");
      }
    }

    result.setScore(result.isValid() ? score : 0.0);
    result.setMessage(llmResponse == null ? "" : llmResponse.trim());

    return result;
  }

  /** Validates an agent event. */
  public ValidationResult validate(AgentEvent event, String validationPrompt) {
    Object output = event.getData("result");
    if (output == null) {
      output = event.getData("output");
    }

    String outputStr = output != null ? output.toString() : "";
    return validate(outputStr, validationPrompt);
  }

  // ==================== Validation Result ====================

  public static class ValidationResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean valid;
    private double score;
    private String message;

    public boolean isValid() {
      return valid;
    }

    public void setValid(boolean valid) {
      this.valid = valid;
    }

    public double getScore() {
      return score;
    }

    public void setScore(double score) {
      this.score = score;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }
}
