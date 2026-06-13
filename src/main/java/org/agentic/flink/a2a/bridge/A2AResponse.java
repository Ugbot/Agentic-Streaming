package org.agentic.flink.a2a.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.agentic.flink.a2a.A2AArtifact;
import org.agentic.flink.a2a.A2ATaskState;

/**
 * Envelope carrying an A2A result (or streaming delta) from the Flink job back to the gateway.
 *
 * <p>The agent operator emits one or more of these per {@link A2ARequest}: for a streaming request
 * it emits intermediate {@code working} updates and artifact deltas, ending with {@link #isFinal()}
 * true; for a non-streaming request it emits a single final response. The gateway translates them
 * into A2A SSE events / push payloads and persists task state. Correlated by {@link #getTaskId()}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2AResponse implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String taskId;
  private final String contextId;
  private final A2ATaskState state;
  private final String statusMessage;
  private final List<A2AArtifact> artifacts;
  private final boolean isFinal;
  private final String errorMessage;

  public A2AResponse(
      String taskId,
      String contextId,
      A2ATaskState state,
      String statusMessage,
      List<A2AArtifact> artifacts,
      boolean isFinal,
      String errorMessage) {
    this.taskId = Objects.requireNonNull(taskId, "taskId");
    this.contextId = contextId;
    this.state = state == null ? A2ATaskState.UNKNOWN : state;
    this.statusMessage = statusMessage;
    this.artifacts =
        artifacts == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(artifacts));
    this.isFinal = isFinal;
    this.errorMessage = errorMessage;
  }

  /** A terminal success response carrying the produced artifacts. */
  public static A2AResponse completed(String taskId, String contextId, List<A2AArtifact> artifacts) {
    return new A2AResponse(
        taskId, contextId, A2ATaskState.COMPLETED, null, artifacts, true, null);
  }

  /** A terminal failure response. */
  public static A2AResponse failed(String taskId, String contextId, String error) {
    return new A2AResponse(taskId, contextId, A2ATaskState.FAILED, error, null, true, error);
  }

  /** A non-final progress update (e.g. {@code working}) for a streaming request. */
  public static A2AResponse working(String taskId, String contextId, String statusMessage) {
    return new A2AResponse(
        taskId, contextId, A2ATaskState.WORKING, statusMessage, null, false, null);
  }

  public String getTaskId() {
    return taskId;
  }

  public String getContextId() {
    return contextId;
  }

  public A2ATaskState getState() {
    return state;
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public List<A2AArtifact> getArtifacts() {
    return artifacts;
  }

  public boolean isFinal() {
    return isFinal;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof A2AResponse)) {
      return false;
    }
    A2AResponse that = (A2AResponse) o;
    return isFinal == that.isFinal
        && Objects.equals(taskId, that.taskId)
        && Objects.equals(contextId, that.contextId)
        && state == that.state
        && Objects.equals(statusMessage, that.statusMessage)
        && Objects.equals(artifacts, that.artifacts)
        && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskId, contextId, state, statusMessage, artifacts, isFinal, errorMessage);
  }

  @Override
  public String toString() {
    return "A2AResponse{taskId=" + taskId + ", state=" + state + ", artifacts=" + artifacts.size()
        + ", final=" + isFinal + '}';
  }
}
