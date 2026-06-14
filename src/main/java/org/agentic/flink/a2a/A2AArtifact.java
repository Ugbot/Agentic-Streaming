package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An A2A artifact — a named, agent-generated output composed of {@link A2APart}s.
 *
 * <p>Artifacts are what a remote agent returns as the result of a {@link A2ATask}; the outbound
 * tool ({@code A2AToolExecutor}) flattens them into the tool result the local agent loop consumes.
 * Immutable and {@link Serializable}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2AArtifact implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String artifactId;
  private final String name;
  private final String description;
  private final List<A2APart> parts;
  private final Map<String, Object> metadata;

  public A2AArtifact(
      String artifactId,
      String name,
      String description,
      List<A2APart> parts,
      Map<String, Object> metadata) {
    this.artifactId = artifactId;
    this.name = name;
    this.description = description;
    this.parts =
        parts == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(parts));
    this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
  }

  public static A2AArtifact text(String artifactId, String name, String text) {
    return new A2AArtifact(artifactId, name, null, List.of(A2APart.text(text)), null);
  }

  public String getArtifactId() {
    return artifactId;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public List<A2APart> getParts() {
    return parts;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  /** Newline-joined text of every text part in this artifact. */
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
    if (!(o instanceof A2AArtifact)) {
      return false;
    }
    A2AArtifact that = (A2AArtifact) o;
    return Objects.equals(artifactId, that.artifactId)
        && Objects.equals(name, that.name)
        && Objects.equals(description, that.description)
        && Objects.equals(parts, that.parts)
        && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(artifactId, name, description, parts, metadata);
  }

  @Override
  public String toString() {
    return "A2AArtifact{artifactId=" + artifactId + ", name=" + name + ", parts=" + parts.size() + '}';
  }
}
