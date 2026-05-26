package org.agentic.flink.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * One tool registered on an agent. Two flavours, distinguished by {@link #getKind()}:
 *
 * <ul>
 *   <li>{@code "java"} — references a {@link org.agentic.flink.tools.ToolExecutor}
 *       implementation by fully-qualified class name; instantiated via
 *       {@link PlanReader}.
 *   <li>{@code "python"} — carries cloudpickle-encoded Python bytes; instantiated as a
 *       {@code PythonToolExecutor} that runs the callable through PEMJA.
 * </ul>
 */
public final class ToolSpec implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String KIND_JAVA = "java";
  public static final String KIND_PYTHON = "python";

  private final String kind;
  private final String name;
  private final String description;

  // java-kind:
  private final String fqn;
  private final Map<String, String> config;

  // python-kind:
  private final String cloudpickleB64;
  private final List<String> paramNames;

  @JsonCreator
  public ToolSpec(
      @JsonProperty("kind") String kind,
      @JsonProperty("name") String name,
      @JsonProperty("description") String description,
      @JsonProperty("fqn") String fqn,
      @JsonProperty("config") Map<String, String> config,
      @JsonProperty("cloudpickle_b64") String cloudpickleB64,
      @JsonProperty("param_names") List<String> paramNames) {
    if (kind == null || (!KIND_JAVA.equals(kind) && !KIND_PYTHON.equals(kind))) {
      throw new IllegalArgumentException("ToolSpec.kind must be 'java' or 'python'; got: " + kind);
    }
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("ToolSpec.name must be non-empty");
    }
    this.kind = kind;
    this.name = name;
    this.description = description == null ? name : description;
    this.fqn = fqn;
    this.config = config == null ? Collections.emptyMap() : Map.copyOf(config);
    this.cloudpickleB64 = cloudpickleB64;
    this.paramNames = paramNames == null ? Collections.emptyList() : List.copyOf(paramNames);

    if (KIND_JAVA.equals(kind) && (fqn == null || fqn.isEmpty())) {
      throw new IllegalArgumentException("java-kind ToolSpec requires fqn");
    }
    if (KIND_PYTHON.equals(kind) && (cloudpickleB64 == null || cloudpickleB64.isEmpty())) {
      throw new IllegalArgumentException("python-kind ToolSpec requires cloudpickle_b64");
    }
  }

  @JsonProperty("kind")
  public String getKind() {
    return kind;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  @JsonProperty("fqn")
  public String getFqn() {
    return fqn;
  }

  @JsonProperty("config")
  public Map<String, String> getConfig() {
    return config;
  }

  @JsonProperty("cloudpickle_b64")
  public String getCloudpickleB64() {
    return cloudpickleB64;
  }

  @JsonProperty("param_names")
  public List<String> getParamNames() {
    return paramNames;
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
