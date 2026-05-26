package org.agentic.flink.plugins.flintagents.adapter;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;

/**
 * Adapter for converting between our AgentEvent model and Apache Flink Agents Event model.
 *
 * <p>This class provides bidirectional translation between:
 *
 * <ul>
 *   <li>Our custom {@link AgentEvent} (org.agentic.flink.core)
 *   <li>Flink Agents {@code Event} (org.apache.flink.agents.api.event)
 * </ul>
 *
 * <p><b>Architecture:</b>
 *
 * <ul>
 *   <li>Flink Agents uses Event base class with attributes Map
 *   <li>Our framework uses AgentEvent with typed fields
 *   <li>This adapter bridges the two models losslessly
 * </ul>
 *
 * <p><b>Example Usage:</b>
 *
 * <pre>{@code
 * // Convert our event to Flink Agents event
 * AgentEvent ourEvent = new AgentEvent("flow-001", "user-001", "agent-001",
 *                                       AgentEventType.TOOL_CALL_REQUESTED);
 * Event flinkEvent = FlinkAgentsEventAdapter.toFlinkAgentEvent(ourEvent);
 *
 * // Convert Flink Agents event back to our event
 * AgentEvent converted = FlinkAgentsEventAdapter.fromFlinkAgentEvent(flinkEvent);
 * }</pre>
 *
 * @author Agentic Flink Team
 * @see AgentEvent
 * @see Event
 */
public class FlinkAgentsEventAdapter {

  private static final String ATTR_FLOW_ID = "flowId";
  private static final String ATTR_USER_ID = "userId";
  private static final String ATTR_AGENT_ID = "agentId";
  private static final String ATTR_EVENT_TYPE = "eventType";
  private static final String ATTR_CURRENT_STAGE = "currentStage";
  private static final String ATTR_ITERATION_NUMBER = "iterationNumber";
  private static final String ATTR_ERROR_MESSAGE = "errorMessage";
  private static final String ATTR_ERROR_CODE = "errorCode";
  private static final String ATTR_DATA_PREFIX = "data.";

  /**
   * Converts our AgentEvent to Apache Flink Agents Event.
   *
   * <p><b>Mapping Strategy:</b>
   *
   * <ul>
   *   <li>flowId → Event.attributes["flowId"]
   *   <li>userId → Event.attributes["userId"]
   *   <li>agentId → Event.attributes["agentId"]
   *   <li>eventType → Event.attributes["eventType"]
   *   <li>data → Event.attributes with "data." prefix
   *   <li>timestamp → Event.sourceTimestamp
   * </ul>
   *
   * @param ourEvent Our AgentEvent to convert
   * @return Flink Agents Event (InputEvent for input events, OutputEvent for output events)
   */
  public static Event toFlinkAgentEvent(AgentEvent ourEvent) {
    // Create appropriate event type based on our event type
    // We store our event data as the input/output object
    Event flinkEvent;
    if (isInputEventType(ourEvent.getEventType())) {
      flinkEvent = new InputEvent(ourEvent.getData());
    } else {
      flinkEvent = new OutputEvent(ourEvent.getData());
    }

    // Set core attributes
    flinkEvent.setAttr(ATTR_FLOW_ID, ourEvent.getFlowId());
    flinkEvent.setAttr(ATTR_USER_ID, ourEvent.getUserId());
    flinkEvent.setAttr(ATTR_AGENT_ID, ourEvent.getAgentId());
    flinkEvent.setAttr(ATTR_EVENT_TYPE, ourEvent.getEventType().name());

    // Set optional fields
    if (ourEvent.getCurrentStage() != null) {
      flinkEvent.setAttr(ATTR_CURRENT_STAGE, ourEvent.getCurrentStage());
    }

    if (ourEvent.getIterationNumber() != null) {
      flinkEvent.setAttr(ATTR_ITERATION_NUMBER, ourEvent.getIterationNumber());
    }

    // Set error information if present
    if (ourEvent.getErrorMessage() != null) {
      flinkEvent.setAttr(ATTR_ERROR_MESSAGE, ourEvent.getErrorMessage());
    }
    if (ourEvent.getErrorCode() != null) {
      flinkEvent.setAttr(ATTR_ERROR_CODE, ourEvent.getErrorCode());
    }

    // Transfer data payload
    if (ourEvent.getData() != null && !ourEvent.getData().isEmpty()) {
      for (Map.Entry<String, Object> entry : ourEvent.getData().entrySet()) {
        flinkEvent.setAttr(ATTR_DATA_PREFIX + entry.getKey(), entry.getValue());
      }
    }

    // Set timestamp
    if (ourEvent.getTimestamp() != null) {
      flinkEvent.setSourceTimestamp(ourEvent.getTimestamp());
    }

    return flinkEvent;
  }

  /**
   * Converts Apache Flink Agents Event to our AgentEvent.
   *
   * <p><b>Mapping Strategy:</b> Reverse of {@link #toFlinkAgentEvent(AgentEvent)}
   *
   * @param flinkEvent Flink Agents Event to convert
   * @return Our AgentEvent
   */
  public static AgentEvent fromFlinkAgentEvent(Event flinkEvent) {
    // Extract core fields
    String flowId = (String) flinkEvent.getAttr(ATTR_FLOW_ID);
    String userId = (String) flinkEvent.getAttr(ATTR_USER_ID);
    String agentId = (String) flinkEvent.getAttr(ATTR_AGENT_ID);
    String eventTypeName = (String) flinkEvent.getAttr(ATTR_EVENT_TYPE);
    AgentEventType eventType =
        eventTypeName != null ? AgentEventType.valueOf(eventTypeName) : AgentEventType.FLOW_STARTED;

    // Create our event
    AgentEvent ourEvent = new AgentEvent(flowId, userId, agentId, eventType);

    // Set optional fields
    String currentStage = (String) flinkEvent.getAttr(ATTR_CURRENT_STAGE);
    if (currentStage != null) {
      ourEvent.setCurrentStage(currentStage);
    }

    Integer iterationNumber = (Integer) flinkEvent.getAttr(ATTR_ITERATION_NUMBER);
    if (iterationNumber != null) {
      ourEvent.setIterationNumber(iterationNumber);
    }

    // Restore error information
    String errorMessage = (String) flinkEvent.getAttr(ATTR_ERROR_MESSAGE);
    if (errorMessage != null) {
      ourEvent.setErrorMessage(errorMessage);
    }

    String errorCode = (String) flinkEvent.getAttr(ATTR_ERROR_CODE);
    if (errorCode != null) {
      ourEvent.setErrorCode(errorCode);
    }

    // Extract data payload
    Map<String, Object> data = new HashMap<>();
    for (Map.Entry<String, Object> entry : flinkEvent.getAttributes().entrySet()) {
      if (entry.getKey().startsWith(ATTR_DATA_PREFIX)) {
        String dataKey = entry.getKey().substring(ATTR_DATA_PREFIX.length());
        data.put(dataKey, entry.getValue());
      }
    }
    if (!data.isEmpty()) {
      ourEvent.setData(data);
    }

    // Restore timestamp
    if (flinkEvent.hasSourceTimestamp()) {
      ourEvent.setTimestamp(flinkEvent.getSourceTimestamp());
    }

    return ourEvent;
  }

  /**
   * Determines if our event type represents an input event in Flink Agents.
   *
   * @param ourType Our event type
   * @return true if this is an input event, false if output event
   */
  private static boolean isInputEventType(AgentEventType ourType) {
    switch (ourType) {
      case FLOW_STARTED:
      case TOOL_CALL_REQUESTED:
      case CORRECTION_REQUESTED:
      case SUPERVISOR_REVIEW_REQUESTED:
        return true;
      case FLOW_COMPLETED:
      case TOOL_CALL_COMPLETED:
      case TOOL_CALL_FAILED:
      case VALIDATION_PASSED:
      case VALIDATION_FAILED:
      case CORRECTION_COMPLETED:
      case SUPERVISOR_APPROVED:
      case SUPERVISOR_REJECTED:
        return false;
      default:
        return true; // Default to input
    }
  }

  /**
   * Validates that an event conversion is lossless.
   *
   * @param original Original AgentEvent
   * @param converted Converted and back-converted AgentEvent
   * @return true if conversion preserved all data
   */
  public static boolean validateConversion(AgentEvent original, AgentEvent converted) {
    return original.getFlowId().equals(converted.getFlowId())
        && original.getUserId().equals(converted.getUserId())
        && original.getAgentId().equals(converted.getAgentId())
        && original.getEventType().equals(converted.getEventType())
        && (original.getTimestamp() == null
            || original.getTimestamp().equals(converted.getTimestamp()));
  }

  /**
   * Creates a summary string for debugging event conversions.
   *
   * @param event Our AgentEvent
   * @return Debug summary
   */
  public static String debugSummary(AgentEvent event) {
    return String.format(
        "AgentEvent{flowId=%s, userId=%s, agentId=%s, type=%s, stage=%s, iteration=%d}",
        event.getFlowId(),
        event.getUserId(),
        event.getAgentId(),
        event.getEventType(),
        event.getCurrentStage(),
        event.getIterationNumber());
  }
}
