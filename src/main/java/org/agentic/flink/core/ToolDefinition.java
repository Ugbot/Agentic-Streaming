package org.agentic.flink.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ToolDefinition implements Serializable {

  private String toolId;
  private String name;
  private String description;

  // JSON Schema for input parameters
  private Map<String, Object> inputSchema;

  // JSON Schema for output
  private Map<String, Object> outputSchema;

  // Tool metadata
  private String version;
  private boolean requiresApproval;
  private boolean isAsync;
  private Long timeoutMs;

  // Execution hints
  private String executorClass;
  private Map<String, String> executorConfig;

  public ToolDefinition(String toolId, String name, String description) {
    this.toolId = toolId;
    this.name = name;
    this.description = description;
    this.inputSchema = new HashMap<>();
    this.outputSchema = new HashMap<>();
    this.executorConfig = new HashMap<>();
    this.isAsync = true;
    this.timeoutMs = 30000L; // 30 seconds default
    this.requiresApproval = false;
  }

  public void addInputParameter(String name, String type, String description, boolean required) {
    if (this.inputSchema == null) {
      this.inputSchema = new HashMap<>();
    }

    Map<String, Object> properties = getOrCreateProperties();
    Map<String, Object> parameter = new HashMap<>();
    parameter.put("type", type);
    parameter.put("description", description);
    properties.put(name, parameter);

    if (required) {
      addRequiredField(name);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getOrCreateProperties() {
    return (Map<String, Object>)
        this.inputSchema.computeIfAbsent("properties", k -> new HashMap<String, Object>());
  }

  @SuppressWarnings("unchecked")
  private void addRequiredField(String name) {
    java.util.List<String> required =
        (java.util.List<String>)
            this.inputSchema.computeIfAbsent("required", k -> new java.util.ArrayList<String>());
    if (!required.contains(name)) {
      required.add(name);
    }
  }
}
