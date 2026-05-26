package org.agentic.flink.execution;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes validation of agent outputs.
 *
 * <p>Validates agent responses against:
 * <ul>
 *   <li>Output format/schema</li>
 *   <li>Business rules</li>
 *   <li>Quality thresholds</li>
 *   <li>Custom validation prompts</li>
 * </ul>
 *
 * <p><b>Placeholder Implementation:</b>
 * This is a stub for Phase 3. Full implementation will include:
 * <ul>
 *   <li>LLM-based validation (using a validator prompt)</li>
 *   <li>Rule-based validation (JSON schema, regex)</li>
 *   <li>Quality scoring</li>
 *   <li>Validation retries</li>
 * </ul>
 *
 * @author Agentic Flink Team
 */
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
      return customPrompt + "\n\nOutput to validate:\n" + output +
          "\n\nRespond with: VALID or INVALID, followed by a score (0.0-1.0) and reason.";
    }

    return "You are a validator. Review the following output and determine if it is valid.\n\n" +
        "Output:\n" + output + "\n\n" +
        "Respond in this format:\n" +
        "VALID or INVALID\n" +
        "Score: 0.0-1.0\n" +
        "Reason: <your reason>";
  }

  private ValidationResult parseValidationResponse(String llmResponse) {
    ValidationResult result = new ValidationResult();

    // Simple parsing - look for VALID/INVALID and score
    String upper = llmResponse.toUpperCase();
    result.setValid(upper.contains("VALID") && !upper.contains("INVALID"));

    // Try to extract score
    double score = 0.9; // Default score if valid
    if (llmResponse.contains("Score:") || llmResponse.contains("score:")) {
      try {
        String[] parts = llmResponse.split("[Ss]core:");
        if (parts.length > 1) {
          String scoreStr = parts[1].trim().split("\\s+")[0];
          score = Double.parseDouble(scoreStr);
        }
      } catch (Exception e) {
        LOG.warn("Could not parse score from validation response, using default");
      }
    }

    result.setScore(result.isValid() ? score : 0.0);
    result.setMessage(llmResponse.trim());

    return result;
  }

  /**
   * Validates an agent event.
   */
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

    public boolean isValid() { return valid; }
    public void setValid(boolean valid) { this.valid = valid; }

    public double getScore() { return score; }
    public void setScore(double score) { this.score = score; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
  }
}
