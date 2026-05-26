package org.agentic.flink.serde;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult implements Serializable {

  private String flowId;
  private String userId;
  private String agentId;

  private boolean isValid;
  private Double validationScore;

  private List<String> errors;
  private List<String> warnings;
  private String correctionSuggestion;

  private Long timestamp;

  public ValidationResult(String flowId, String userId, String agentId) {
    this.flowId = flowId;
    this.userId = userId;
    this.agentId = agentId;
    this.errors = new ArrayList<>();
    this.warnings = new ArrayList<>();
    this.timestamp = System.currentTimeMillis();
  }

  public void addError(String error) {
    if (this.errors == null) {
      this.errors = new ArrayList<>();
    }
    this.errors.add(error);
    this.isValid = false;
  }

  public void addWarning(String warning) {
    if (this.warnings == null) {
      this.warnings = new ArrayList<>();
    }
    this.warnings.add(warning);
  }

  public boolean hasErrors() {
    return this.errors != null && !this.errors.isEmpty();
  }
}
