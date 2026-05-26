package org.agentic.flink.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgentConfig implements Serializable {

  private String agentId;
  private String name;
  private String description;

  // Available tools for this agent
  private List<String> allowedToolIds;

  // LLM configuration
  private String llmModel;
  private String systemPrompt;
  private Map<String, String> llmProperties;

  // Execution limits
  private Integer maxIterations;
  private Long executionTimeoutMs;

  // Validation config
  private boolean enableValidation;
  private String validationPrompt;

  // Supervisor config
  private boolean requireSupervisor;
  private String supervisorAgentId;

  // Routing config
  private boolean enableAutoCorrection;
  private Integer maxCorrectionAttempts;

  // State management
  private Long inactivityTimeoutMs;
  private boolean enableAutoOffload;

  public AgentConfig(String agentId, String name) {
    this.agentId = agentId;
    this.name = name;
    this.allowedToolIds = new ArrayList<>();
    this.llmProperties = new HashMap<>();
    this.maxIterations = 10;
    this.executionTimeoutMs = 300000L; // 5 minutes
    this.enableValidation = true;
    this.requireSupervisor = false;
    this.enableAutoCorrection = true;
    this.maxCorrectionAttempts = 3;
    this.inactivityTimeoutMs = 1800000L; // 30 minutes
    this.enableAutoOffload = true;
  }

  public void addAllowedTool(String toolId) {
    if (this.allowedToolIds == null) {
      this.allowedToolIds = new ArrayList<>();
    }
    if (!this.allowedToolIds.contains(toolId)) {
      this.allowedToolIds.add(toolId);
    }
  }

  public void removeAllowedTool(String toolId) {
    if (this.allowedToolIds != null) {
      this.allowedToolIds.remove(toolId);
    }
  }

  public boolean isToolAllowed(String toolId) {
    return this.allowedToolIds != null && this.allowedToolIds.contains(toolId);
  }

  public void addLlmProperty(String key, String value) {
    if (this.llmProperties == null) {
      this.llmProperties = new HashMap<>();
    }
    this.llmProperties.put(key, value);
  }
}
