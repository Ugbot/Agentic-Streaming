package org.agentic.flink.execution;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.dsl.Agent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Execution context that tracks state during agent execution.
 *
 * <p>The ExecutionContext maintains:
 * <ul>
 *   <li>Input event and agent configuration</li>
 *   <li>History of events generated during execution</li>
 *   <li>Iteration count and timing metrics</li>
 *   <li>Execution metadata</li>
 * </ul>
 *
 * @author Agentic Flink Team
 */
public class ExecutionContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private final AgentEvent inputEvent;
  private final Agent agent;
  private final List<AgentEvent> events;
  private final long startTime;
  private int currentIteration;

  public ExecutionContext(AgentEvent inputEvent, Agent agent) {
    this.inputEvent = inputEvent;
    this.agent = agent;
    this.events = new ArrayList<>();
    this.startTime = System.currentTimeMillis();
    this.currentIteration = 0;
  }

  public String getFlowId() {
    return inputEvent.getFlowId();
  }

  public AgentEvent getInputEvent() {
    return inputEvent;
  }

  public Agent getAgent() {
    return agent;
  }

  public List<AgentEvent> getEvents() {
    return events;
  }

  public void addEvent(AgentEvent event) {
    events.add(event);
  }

  public int getCurrentIteration() {
    return currentIteration;
  }

  public void incrementIteration() {
    currentIteration++;
  }

  public long getElapsedMs() {
    return System.currentTimeMillis() - startTime;
  }

  public long getStartTime() {
    return startTime;
  }
}
