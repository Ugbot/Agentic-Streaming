package org.agentic.flink.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * One Java SPI implementation referenced by fully-qualified class name plus an init config.
 *
 * <p>Concrete classes that come through this path implement one of the framework's SPIs
 * ({@link org.agentic.flink.llm.ChatConnection}, {@code EmbeddingConnection},
 * {@code InferenceConnection}, {@code VectorMemorySpec}, {@code Channel}, etc.) and either
 * provide a no-arg constructor (then {@code initialize(config)} for stores) or a single-arg
 * constructor that takes the config map.
 *
 * <p>This is the JSON shape Python sends across the gateway as part of an {@link AgentPlan}.
 */
public final class ResourceSpec implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String fqn;
  private final Map<String, String> config;

  @JsonCreator
  public ResourceSpec(
      @JsonProperty("fqn") String fqn,
      @JsonProperty("config") Map<String, String> config) {
    if (fqn == null || fqn.isEmpty()) {
      throw new IllegalArgumentException("ResourceSpec.fqn must be non-empty");
    }
    this.fqn = fqn;
    this.config = config == null ? Collections.emptyMap() : Map.copyOf(config);
  }

  @JsonProperty("fqn")
  public String getFqn() {
    return fqn;
  }

  @JsonProperty("config")
  public Map<String, String> getConfig() {
    return config;
  }

  @Override
  public String toString() {
    return "ResourceSpec[" + fqn + ", " + config.size() + " config keys]";
  }
}
