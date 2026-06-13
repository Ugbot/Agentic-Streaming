package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An A2A message exchanged between a client agent and a remote agent.
 *
 * <p>A message carries one or more {@link A2APart}s and a {@link Role}. Continuity is tracked by
 * {@code contextId} (a logical conversation) and {@code taskId} (a single unit of work) — both map
 * cleanly onto Flink keyed state. Immutable and {@link Serializable}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2AMessage implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum Role {
    USER,
    AGENT
  }

  private final Role role;
  private final String messageId;
  private final List<A2APart> parts;
  private final String contextId;
  private final String taskId;
  private final Map<String, Object> metadata;

  public A2AMessage(
      Role role,
      String messageId,
      List<A2APart> parts,
      String contextId,
      String taskId,
      Map<String, Object> metadata) {
    this.role = Objects.requireNonNull(role, "role");
    this.messageId = messageId;
    this.parts =
        parts == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(parts));
    this.contextId = contextId;
    this.taskId = taskId;
    this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
  }

  /** Build a user message containing a single text part. */
  public static A2AMessage userText(String messageId, String text) {
    return new A2AMessage(Role.USER, messageId, List.of(A2APart.text(text)), null, null, null);
  }

  /** Build a user message from arbitrary parts. */
  public static A2AMessage user(String messageId, List<A2APart> parts) {
    return new A2AMessage(Role.USER, messageId, parts, null, null, null);
  }

  public Role getRole() {
    return role;
  }

  public String getMessageId() {
    return messageId;
  }

  public List<A2APart> getParts() {
    return parts;
  }

  public String getContextId() {
    return contextId;
  }

  public String getTaskId() {
    return taskId;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  /** Concatenate the text of every {@link A2APart.Kind#TEXT} part (newline-joined). */
  public String textContent() {
    StringBuilder sb = new StringBuilder();
    for (A2APart part : parts) {
      if (part.getKind() == A2APart.Kind.TEXT && part.getText() != null) {
        if (sb.length() > 0) {
          sb.append('\n');
        }
        sb.append(part.getText());
      }
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof A2AMessage)) {
      return false;
    }
    A2AMessage that = (A2AMessage) o;
    return role == that.role
        && Objects.equals(messageId, that.messageId)
        && Objects.equals(parts, that.parts)
        && Objects.equals(contextId, that.contextId)
        && Objects.equals(taskId, that.taskId)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(role, messageId, parts, contextId, taskId, metadata);
  }

  @Override
  public String toString() {
    return "A2AMessage{role=" + role + ", messageId=" + messageId + ", parts=" + parts.size()
        + ", taskId=" + taskId + ", contextId=" + contextId + '}';
  }
}
