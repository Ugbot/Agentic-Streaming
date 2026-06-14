package org.agentic.flink.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.agentic.flink.typeinfo.JsonTypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TypeInfo(AgentEvent.Factory.class)
public class AgentEvent implements Serializable {

  /**
   * Serializes via JSON ({@link org.agentic.flink.typeinfo.FlinkJson}) instead of Kryo wherever this
   * event flows (stream elements + keyed state). Mutable, so Flink object reuse deep-copies it.
   */
  public static final class Factory extends JsonTypeInfoFactory<AgentEvent> {
    public Factory() {
      super(AgentEvent.class, true);
    }
  }

  private String flowId;
  private String userId;
  private String agentId;
  private AgentEventType eventType;
  private Long timestamp;

  // Payload data
  private Map<String, Object> data;

  // Execution context
  private String currentStage;
  private Integer iterationNumber;

  // Routing hints
  private String sourcePattern;
  private String targetPattern;

  // Error information
  private String errorMessage;
  private String errorCode;

  // ==================== Saga Metadata (Phase 1.4) ====================

  /**
   * Parent flow ID for hierarchical/nested agent flows.
   *
   * <p>Enables sub-flows where a parent agent spawns child agent workflows. The child flows can
   * reference the parent via this field.
   *
   * <p>Example: Research coordinator (parent) spawns web-search, analysis, synthesis (children)
   */
  private String parentFlowId;

  /**
   * Correlation ID for saga pattern coordination.
   *
   * <p>Links related events across multiple agents/flows. Similar to the saga kit's orderId that
   * correlates all events for a single transaction.
   *
   * <p>Example: All events for order-12345 have correlationId="order-12345"
   */
  private String correlationId;

  /**
   * Flexible metadata map for routing logic and CEP conditions.
   *
   * <p>Used by supervisor routers and CEP patterns to make decisions. Separate from 'data' to
   * distinguish routing metadata from business payload.
   *
   * <p>Example metadata:
   * <ul>
   *   <li>"priority" → "high" (route to fast-track supervisor)</li>
   *   <li>"requires_approval" → true (route to approval tier)</li>
   *   <li>"iteration_count" → 3 (track retry attempts)</li>
   *   <li>"state" → "validating" (current agent state)</li>
   * </ul>
   */
  private Map<String, Object> metadata;

  /**
   * Compensation data for saga rollback scenarios.
   *
   * <p>When an agent execution fails and requires compensation (rollback), this field stores the
   * data needed to reverse the operation.
   *
   * <p>Example: If a tool call modified external state, compensationData might contain:
   * <pre>{@code
   * {
   *   "tool_name": "database_insert",
   *   "rollback_command": "DELETE FROM users WHERE id=123",
   *   "original_state": {...}
   * }
   * }</pre>
   */
  private Map<String, Object> compensationData;

  /**
   * Optional reference to which task this event completes.
   *
   * <p>Used by CompletionTracker to automatically mark tasks as complete when specific events
   * occur.
   *
   * <p>Example: TOOL_CALL_COMPLETED event with completionTaskId="web-search" → marks "web-search"
   * task complete
   */
  private String completionTaskId;

  public AgentEvent(String flowId, String userId, String agentId, AgentEventType eventType) {
    this.flowId = flowId;
    this.userId = userId;
    this.agentId = agentId;
    this.eventType = eventType;
    this.timestamp = System.currentTimeMillis();
    this.data = new HashMap<>();
    this.iterationNumber = 0;
  }

  public void putData(String key, Object value) {
    if (this.data == null) {
      this.data = new HashMap<>();
    }
    this.data.put(key, value);
  }

  public Object getData(String key) {
    return this.data != null ? this.data.get(key) : null;
  }

  public <T> T getData(String key, Class<T> type) {
    Object value = getData(key);
    if (value != null && type.isInstance(value)) {
      return type.cast(value);
    }
    return null;
  }

  public boolean hasError() {
    return errorMessage != null || errorCode != null;
  }

  // ==================== Saga Metadata Helper Methods ====================

  /**
   * Puts a value into the metadata map.
   *
   * @param key Metadata key
   * @param value Metadata value
   */
  public void putMetadata(String key, Object value) {
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.put(key, value);
  }

  /**
   * Gets a value from the metadata map.
   *
   * @param key Metadata key
   * @return metadata value, or null if not found
   */
  public Object getMetadata(String key) {
    return this.metadata != null ? this.metadata.get(key) : null;
  }

  /**
   * Gets a typed value from the metadata map.
   *
   * @param key Metadata key
   * @param type Expected value type
   * @param <T> Type parameter
   * @return typed metadata value, or null if not found or wrong type
   */
  public <T> T getMetadata(String key, Class<T> type) {
    Object value = getMetadata(key);
    if (value != null && type.isInstance(value)) {
      return type.cast(value);
    }
    return null;
  }

  /**
   * Checks if metadata contains a specific key.
   *
   * @param key Metadata key
   * @return true if key exists in metadata
   */
  public boolean hasMetadata(String key) {
    return this.metadata != null && this.metadata.containsKey(key);
  }

  /**
   * Puts compensation data for rollback scenarios.
   *
   * @param key Compensation data key
   * @param value Compensation data value
   */
  public void putCompensationData(String key, Object value) {
    if (this.compensationData == null) {
      this.compensationData = new HashMap<>();
    }
    this.compensationData.put(key, value);
  }

  /**
   * Gets compensation data.
   *
   * @param key Compensation data key
   * @return compensation value, or null if not found
   */
  public Object getCompensationData(String key) {
    return this.compensationData != null ? this.compensationData.get(key) : null;
  }

  /**
   * Checks if this event has compensation data (requires rollback).
   *
   * @return true if compensation data exists
   */
  public boolean requiresCompensation() {
    return this.compensationData != null && !this.compensationData.isEmpty();
  }

  /**
   * Checks if this event is part of a sub-flow (has parent).
   *
   * @return true if parentFlowId is set
   */
  public boolean isSubFlow() {
    return this.parentFlowId != null;
  }

  /**
   * Creates a child event that inherits context from this parent.
   *
   * <p>The child event will have:
   * <ul>
   *   <li>Same correlationId as parent</li>
   *   <li>parentFlowId set to this event's flowId</li>
   *   <li>New unique flowId</li>
   *   <li>Inherits userId and agentId</li>
   * </ul>
   *
   * @param childFlowId The flow ID for the child
   * @param eventType The event type for the child
   * @return new child event
   */
  public AgentEvent createChildEvent(String childFlowId, AgentEventType eventType) {
    AgentEvent child = new AgentEvent(childFlowId, this.userId, this.agentId, eventType);
    child.setParentFlowId(this.flowId);
    child.setCorrelationId(this.correlationId != null ? this.correlationId : this.flowId);
    return child;
  }

  /**
   * Creates a compensation event for rollback.
   *
   * <p>The compensation event:
   * <ul>
   *   <li>Has same flowId and correlationId</li>
   *   <li>Copies compensationData to data field</li>
   *   <li>Sets eventType to indicate compensation</li>
   * </ul>
   *
   * @return new compensation event
   */
  public AgentEvent createCompensationEvent() {
    AgentEvent compensation = new AgentEvent(
        this.flowId,
        this.userId,
        this.agentId,
        AgentEventType.FLOW_FAILED  // Will be enhanced with COMPENSATION_REQUESTED later
    );
    compensation.setCorrelationId(this.correlationId);
    compensation.putMetadata("is_compensation", true);
    compensation.putMetadata("original_event_type", this.eventType.toString());

    // Copy compensation data to main data field
    if (this.compensationData != null) {
      this.compensationData.forEach(compensation::putData);
    }

    return compensation;
  }

  /**
   * Increments the iteration number and returns the updated value.
   *
   * <p>Useful for loop tracking in retry scenarios.
   *
   * @return the new iteration number
   */
  public int incrementIteration() {
    if (this.iterationNumber == null) {
      this.iterationNumber = 1;
    } else {
      this.iterationNumber++;
    }
    return this.iterationNumber;
  }

  /**
   * Creates a copy of this event with a new event type.
   *
   * <p>All other fields are preserved. Useful for state transitions.
   *
   * @param newEventType The new event type
   * @return new event with updated type
   */
  public AgentEvent withEventType(AgentEventType newEventType) {
    AgentEvent copy = new AgentEvent(this.flowId, this.userId, this.agentId, newEventType);
    copy.setTimestamp(System.currentTimeMillis());
    copy.setData(new HashMap<>(this.data != null ? this.data : new HashMap<>()));
    copy.setCurrentStage(this.currentStage);
    copy.setIterationNumber(this.iterationNumber);
    copy.setSourcePattern(this.sourcePattern);
    copy.setTargetPattern(this.targetPattern);
    copy.setParentFlowId(this.parentFlowId);
    copy.setCorrelationId(this.correlationId);
    copy.setMetadata(this.metadata != null ? new HashMap<>(this.metadata) : null);
    copy.setCompensationData(this.compensationData != null ? new HashMap<>(this.compensationData) : null);
    copy.setCompletionTaskId(this.completionTaskId);
    return copy;
  }
}
