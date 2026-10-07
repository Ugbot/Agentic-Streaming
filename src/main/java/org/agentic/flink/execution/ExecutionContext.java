package org.agentic.flink.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.agentic.flink.annotation.Public;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.dsl.Agent;

/**
 * Execution context that tracks state during agent execution.
 *
 * <p>The ExecutionContext maintains:
 *
 * <ul>
 *   <li>Input event and agent configuration
 *   <li>History of events generated during execution
 *   <li>Iteration count and timing metrics
 *   <li>Execution metadata
 * </ul>
 *
 * @author Agentic Flink Team
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction}.
 */
@Deprecated(since = "1.0.0")
@Public
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
