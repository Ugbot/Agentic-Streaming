package org.agentic.flink.execution;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes correction of failed validations.
 *
 * <p>When validation fails, the corrector:
 * <ul>
 *   <li>Analyzes the validation failure</li>
 *   <li>Generates a correction prompt</li>
 *   <li>Calls LLM to fix the output</li>
 *   <li>Returns corrected output for re-validation</li>
 * </ul>
 *
 * <p><b>Correction Loop:</b>
 * <pre>
 * 1. Agent produces output
 * 2. Validation fails (score < threshold)
 * 3. Corrector analyzes failure: "Output format incorrect"
 * 4. Corrector calls LLM: "Fix this output: [original] Error: [validation message]"
 * 5. LLM returns corrected output
 * 6. Loop back to validation
 * </pre>
 *
 * <p><b>Placeholder Implementation:</b>
 * This is a stub for Phase 3. Full implementation will include:
 * <ul>
 *   <li>LLM-based correction with feedback</li>
 *   <li>Correction attempt tracking</li>
 *   <li>Quality improvement scoring</li>
 *   <li>Max correction attempts</li>
 * </ul>
 *
 * @author Agentic Flink Team
 */
public class CorrectionExecutor implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CorrectionExecutor.class);

  private final LLMClient llmClient;

  public CorrectionExecutor(LLMClient llmClient) {
    this.llmClient = llmClient;
  }

  /**
   * Corrects failed output using LLM.
   *
   * @param originalOutput The failed output
   * @param validationMessage Why it failed
   * @param correctionPrompt Optional custom correction prompt
   * @return Corrected output
   */
  public CorrectionResult correct(
      String originalOutput,
      String validationMessage,
      String correctionPrompt) {

    LOG.info("Correcting output. Validation message: {}", validationMessage);

    try {
      // Build correction prompt
      String prompt = buildCorrectionPrompt(originalOutput, validationMessage, correctionPrompt);

      // Call LLM for correction
      String correctedOutput = llmClient.generate(prompt);

      CorrectionResult result = new CorrectionResult();
      result.setCorrectedOutput(correctedOutput);
      result.setSuccess(true);
      result.setMessage("LLM correction applied successfully");

      LOG.info("Correction completed: {} characters", correctedOutput.length());
      return result;

    } catch (Exception e) {
      LOG.error("Correction failed with error: {}", e.getMessage(), e);

      CorrectionResult result = new CorrectionResult();
      result.setCorrectedOutput(originalOutput);
      result.setSuccess(false);
      result.setMessage("Correction error: " + e.getMessage());
      return result;
    }
  }

  private String buildCorrectionPrompt(
      String originalOutput, String validationMessage, String customPrompt) {

    if (customPrompt != null && !customPrompt.isEmpty()) {
      return customPrompt + "\n\n" +
          "Original output:\n" + originalOutput + "\n\n" +
          "Validation failure reason:\n" + validationMessage + "\n\n" +
          "Please provide the corrected output.";
    }

    return "You are a correction assistant. The following output failed validation.\n\n" +
        "Original output:\n" + originalOutput + "\n\n" +
        "Why it failed:\n" + validationMessage + "\n\n" +
        "Please provide a corrected version that addresses the validation failures. " +
        "Output only the corrected content, no explanations.";
  }

  /**
   * Corrects a failed agent event.
   */
  public CorrectionResult correct(
      AgentEvent failedEvent,
      String validationMessage,
      String correctionPrompt) {

    Object output = failedEvent.getData("result");
    if (output == null) {
      output = failedEvent.getData("output");
    }

    String outputStr = output != null ? output.toString() : "";
    return correct(outputStr, validationMessage, correctionPrompt);
  }

  // ==================== Correction Result ====================

  public static class CorrectionResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String correctedOutput;
    private boolean success;
    private String message;

    public String getCorrectedOutput() { return correctedOutput; }
    public void setCorrectedOutput(String correctedOutput) { this.correctedOutput = correctedOutput; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
  }
}
