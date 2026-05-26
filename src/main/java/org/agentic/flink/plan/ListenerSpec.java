package org.agentic.flink.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * Listener flavour for the agent operator: a Java {@code AgentEventListener} resolved via FQN,
 * or a Python listener whose callable rides in the plan as cloudpickle bytes.
 */
public final class ListenerSpec implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String KIND_JAVA = "java";
  public static final String KIND_PYTHON = "python";

  private final String kind;
  private final String fqn;
  private final String cloudpickleB64;

  @JsonCreator
  public ListenerSpec(
      @JsonProperty("kind") String kind,
      @JsonProperty("fqn") String fqn,
      @JsonProperty("cloudpickle_b64") String cloudpickleB64) {
    if (kind == null || (!KIND_JAVA.equals(kind) && !KIND_PYTHON.equals(kind))) {
      throw new IllegalArgumentException(
          "ListenerSpec.kind must be 'java' or 'python'; got: " + kind);
    }
    if (KIND_JAVA.equals(kind) && (fqn == null || fqn.isEmpty())) {
      throw new IllegalArgumentException("java-kind ListenerSpec requires fqn");
    }
    if (KIND_PYTHON.equals(kind) && (cloudpickleB64 == null || cloudpickleB64.isEmpty())) {
      throw new IllegalArgumentException("python-kind ListenerSpec requires cloudpickle_b64");
    }
    this.kind = kind;
    this.fqn = fqn;
    this.cloudpickleB64 = cloudpickleB64;
  }

  @JsonProperty("kind")
  public String getKind() {
    return kind;
  }

  @JsonProperty("fqn")
  public String getFqn() {
    return fqn;
  }

  @JsonProperty("cloudpickle_b64")
  public String getCloudpickleB64() {
    return cloudpickleB64;
  }

  @JsonIgnore
  public boolean isJava() {
    return KIND_JAVA.equals(kind);
  }

  @JsonIgnore
  public boolean isPython() {
    return KIND_PYTHON.equals(kind);
  }
}
