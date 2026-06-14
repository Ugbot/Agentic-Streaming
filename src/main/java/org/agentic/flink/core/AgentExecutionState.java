package org.agentic.flink.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.agentic.flink.typeinfo.JsonTypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

@Data
@NoArgsConstructor
@TypeInfo(AgentExecutionState.Factory.class)
public class AgentExecutionState implements Serializable {

  /** JSON (FlinkJson) serialization in keyed state instead of Kryo (contextData is Map&lt;String,Object&gt;). */
  public static final class Factory extends JsonTypeInfoFactory<AgentExecutionState> {
    public Factory() {
      super(AgentExecutionState.class, true);
    }
  }

  // Identifiers
  private String flowId;
  private String userId;
  private String agentId;

  // Flow state
  private AgentFlowState flowState;
  private String currentStage;
  private Integer currentIteration;
  private Integer maxIterations;

  // Execution history
  private List<ToolCallHistory> toolCallHistory;
  private List<String> validationResults;
  private List<String> correctionAttempts;

  // Context data
  private Map<String, Object> contextData;

  // Timestamps
  private Long createdAt;
  private Long lastActiveAt;
  private Long completedAt;

  // Restoration metadata
  private boolean restoredFromArchive;
  private Long restoredAt;
  private String resumeFromPattern;

  // Configuration
  private AgentConfig config;

  public AgentExecutionState(String flowId, String userId, String agentId) {
    this.flowId = flowId;
    this.userId = userId;
    this.agentId = agentId;
    this.flowState = AgentFlowState.ACTIVE;
    this.currentIteration = 0;
    this.maxIterations = 10;
    this.toolCallHistory = new ArrayList<>();
    this.validationResults = new ArrayList<>();
    this.correctionAttempts = new ArrayList<>();
    this.contextData = new HashMap<>();
    this.createdAt = System.currentTimeMillis();
    this.lastActiveAt = System.currentTimeMillis();
    this.restoredFromArchive = false;
  }

  public void updateLastActive() {
    this.lastActiveAt = System.currentTimeMillis();
  }

  public void incrementIteration() {
    this.currentIteration++;
  }

  public boolean hasReachedMaxIterations() {
    return this.currentIteration >= this.maxIterations;
  }

  public void addToolCall(ToolCallHistory toolCall) {
    if (this.toolCallHistory == null) {
      this.toolCallHistory = new ArrayList<>();
    }
    this.toolCallHistory.add(toolCall);
  }

  public void addValidationResult(String result) {
    if (this.validationResults == null) {
      this.validationResults = new ArrayList<>();
    }
    this.validationResults.add(result);
  }

  public void addCorrectionAttempt(String attempt) {
    if (this.correctionAttempts == null) {
      this.correctionAttempts = new ArrayList<>();
    }
    this.correctionAttempts.add(attempt);
  }

  public void putContextData(String key, Object value) {
    if (this.contextData == null) {
      this.contextData = new HashMap<>();
    }
    this.contextData.put(key, value);
  }

  public Object getContextData(String key) {
    return this.contextData != null ? this.contextData.get(key) : null;
  }

  public AgentEvent toEvent() {
    AgentEvent event = new AgentEvent();
    event.setFlowId(this.flowId);
    event.setUserId(this.userId);
    event.setAgentId(this.agentId);
    event.setCurrentStage(this.currentStage);
    event.setIterationNumber(this.currentIteration);
    event.setTimestamp(System.currentTimeMillis());
    event.setData(new HashMap<>(this.contextData));
    return event;
  }
}
