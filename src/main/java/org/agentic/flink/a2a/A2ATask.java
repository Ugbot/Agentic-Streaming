package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An A2A task — the unit of work a remote agent performs in response to a {@link A2AMessage}.
 *
 * <p>A task is identified by {@code id} and grouped into a conversation by {@code contextId}. It
 * carries the current {@link A2ATaskState}, the message {@code history}, and any {@link A2AArtifact}
 * outputs. This type is both the outbound client return value and the persisted record in {@code
 * A2ATaskStore}, so it is immutable + {@link Serializable} and exposes copy-style {@code with*}
 * mutators for lifecycle progression.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2ATask implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final String contextId;
  private final A2ATaskState state;
  private final String statusMessage;
  private final List<A2AMessage> history;
  private final List<A2AArtifact> artifacts;
  private final Map<String, Object> metadata;
  private final long createdAtEpochMs;
  private final long updatedAtEpochMs;

  public A2ATask(
      String id,
      String contextId,
      A2ATaskState state,
      String statusMessage,
      List<A2AMessage> history,
      List<A2AArtifact> artifacts,
      Map<String, Object> metadata,
      long createdAtEpochMs,
      long updatedAtEpochMs) {
    this.id = Objects.requireNonNull(id, "id");
    this.contextId = contextId;
    this.state = state == null ? A2ATaskState.UNKNOWN : state;
    this.statusMessage = statusMessage;
    this.history =
        history == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(history));
    this.artifacts =
        artifacts == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(artifacts));
    this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
    this.createdAtEpochMs = createdAtEpochMs;
    this.updatedAtEpochMs = updatedAtEpochMs;
  }

  /** A freshly-submitted task with the supplied id/context and the originating message in history. */
  public static A2ATask submitted(
      String id, String contextId, A2AMessage first, long nowEpochMs) {
    return new A2ATask(
        id,
        contextId,
        A2ATaskState.SUBMITTED,
        null,
        first == null ? List.of() : List.of(first),
        List.of(),
        null,
        nowEpochMs,
        nowEpochMs);
  }

  public String getId() {
    return id;
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

  public List<A2AMessage> getHistory() {
    return history;
  }

  public List<A2AArtifact> getArtifacts() {
    return artifacts;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public long getCreatedAtEpochMs() {
    return createdAtEpochMs;
  }

  public long getUpdatedAtEpochMs() {
    return updatedAtEpochMs;
  }

  /** Copy with a new state + status message, stamping {@code updatedAtEpochMs}. */
  public A2ATask withState(A2ATaskState newState, String newStatusMessage, long nowEpochMs) {
    return new A2ATask(
        id, contextId, newState, newStatusMessage, history, artifacts, metadata, createdAtEpochMs, nowEpochMs);
  }

  /** Copy with an artifact appended (or replaced by artifactId), stamping {@code updatedAtEpochMs}. */
  public A2ATask withArtifact(A2AArtifact artifact, long nowEpochMs) {
    List<A2AArtifact> next = new ArrayList<>(artifacts);
    next.removeIf(
        a -> a.getArtifactId() != null && a.getArtifactId().equals(artifact.getArtifactId()));
    next.add(artifact);
    return new A2ATask(
        id, contextId, state, statusMessage, history, next, metadata, createdAtEpochMs, nowEpochMs);
  }

  /** Copy with a message appended to history, stamping {@code updatedAtEpochMs}. */
  public A2ATask withMessage(A2AMessage message, long nowEpochMs) {
    List<A2AMessage> next = new ArrayList<>(history);
    next.add(message);
    return new A2ATask(
        id, contextId, state, statusMessage, next, artifacts, metadata, createdAtEpochMs, nowEpochMs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof A2ATask)) {
      return false;
    }
    A2ATask that = (A2ATask) o;
    return createdAtEpochMs == that.createdAtEpochMs
        && updatedAtEpochMs == that.updatedAtEpochMs
        && Objects.equals(id, that.id)
        && Objects.equals(contextId, that.contextId)
        && state == that.state
        && Objects.equals(statusMessage, that.statusMessage)
        && Objects.equals(history, that.history)
        && Objects.equals(artifacts, that.artifacts)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, contextId, state, statusMessage, history, artifacts, metadata, createdAtEpochMs, updatedAtEpochMs);
  }

  @Override
  public String toString() {
    return "A2ATask{id=" + id + ", contextId=" + contextId + ", state=" + state + ", artifacts="
        + artifacts.size() + '}';
  }
}
