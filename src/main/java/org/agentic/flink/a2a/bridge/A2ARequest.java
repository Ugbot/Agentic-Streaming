package org.agentic.flink.a2a.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.agentic.flink.a2a.A2AMessage;

/**
 * Envelope carrying an inbound A2A request from the Quarkus gateway into the Flink job, across the
 * pluggable bridge {@link org.agentic.flink.channel.Channel}.
 *
 * <p>The gateway creates the A2A task, then publishes one of these to the bridge request channel;
 * the agent operator consumes it (its input stream is {@code union}-ed with the bridge channel),
 * runs the agent keyed by {@link #getContextId()}, and emits an {@link A2AResponse} back. Correlated
 * end-to-end by {@link #getTaskId()}. A plain Jackson POJO so it round-trips over JSON (Redis,
 * Kafka) and Java serialization (ZeroMQ, in-JVM) alike.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2ARequest implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String taskId;
  private final String contextId;
  private final String targetAgentId;
  private final A2AMessage message;
  private final boolean streaming;
  private final String replyChannel;
  private final Map<String, Object> claims;

  public A2ARequest(
      String taskId,
      String contextId,
      String targetAgentId,
      A2AMessage message,
      boolean streaming,
      String replyChannel,
      Map<String, Object> claims) {
    this.taskId = Objects.requireNonNull(taskId, "taskId");
    this.contextId = contextId;
    this.targetAgentId = targetAgentId;
    this.message = Objects.requireNonNull(message, "message");
    this.streaming = streaming;
    this.replyChannel = replyChannel;
    this.claims = claims == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(claims));
  }

  public String getTaskId() {
    return taskId;
  }

  public String getContextId() {
    return contextId;
  }

  public String getTargetAgentId() {
    return targetAgentId;
  }

  public A2AMessage getMessage() {
    return message;
  }

  public boolean isStreaming() {
    return streaming;
  }

  /** Optional hint identifying which response channel/endpoint the reply should be routed to. */
  public String getReplyChannel() {
    return replyChannel;
  }

  /** Authenticated caller claims (subject, scopes, …) extracted by the gateway, if any. */
  public Map<String, Object> getClaims() {
    return claims;
  }

  /** The Flink key for this request: the context id, falling back to the task id. */
  public String key() {
    return contextId != null ? contextId : taskId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof A2ARequest)) {
      return false;
    }
    A2ARequest that = (A2ARequest) o;
    return streaming == that.streaming
        && Objects.equals(taskId, that.taskId)
        && Objects.equals(contextId, that.contextId)
        && Objects.equals(targetAgentId, that.targetAgentId)
        && Objects.equals(message, that.message)
        && Objects.equals(replyChannel, that.replyChannel)
        && Objects.equals(claims, that.claims);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskId, contextId, targetAgentId, message, streaming, replyChannel, claims);
  }

  @Override
  public String toString() {
    return "A2ARequest{taskId=" + taskId + ", contextId=" + contextId + ", target=" + targetAgentId
        + ", streaming=" + streaming + '}';
  }
}
