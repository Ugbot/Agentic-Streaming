package org.agentic.flink.compensation;

import org.agentic.flink.core.AgentEvent;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a single compensation action to undo an operation.
 *
 * <p>Each compensation action is typically the inverse of a successful operation.
 *
 * <p><b>Examples:</b>
 * <ul>
 *   <li>database-insert → database-delete</li>
 *   <li>api-create-order → api-cancel-order</li>
 *   <li>file-upload → file-delete</li>
 * </ul>
 *
 * @author Agentic Flink Team
 */
public class CompensationAction implements Serializable {

  private static final long serialVersionUID = 1L;

  private String actionName;
  private String toolName;
  private Map<String, Object> parameters;
  private AgentEvent originalEvent;

  public CompensationAction() {
    this.parameters = new HashMap<>();
  }

  public CompensationAction(String actionName, String toolName, Map<String, Object> parameters) {
    this.actionName = actionName;
    this.toolName = toolName;
    this.parameters = parameters != null ? parameters : new HashMap<>();
  }

  public String getActionName() {
    return actionName;
  }

  public void setActionName(String actionName) {
    this.actionName = actionName;
  }

  public String getToolName() {
    return toolName;
  }

  public void setToolName(String toolName) {
    this.toolName = toolName;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  public AgentEvent getOriginalEvent() {
    return originalEvent;
  }

  public void setOriginalEvent(AgentEvent originalEvent) {
    this.originalEvent = originalEvent;
  }

  @Override
  public String toString() {
    return String.format("CompensationAction[name=%s, tool=%s, params=%s]",
        actionName, toolName, parameters);
  }
}
