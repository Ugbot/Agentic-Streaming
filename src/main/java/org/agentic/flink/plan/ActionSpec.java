package org.agentic.flink.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * An event-keyed Python action: when an event of one of {@link #getEvents()} types arrives at
 * the agent operator, the cloudpickled callable in {@link #getCloudpickleB64()} is invoked via
 * PEMJA with {@code (event, ctx)} arguments.
 *
 * <p>Java-side actions are expressed indirectly through other plan fields (chat, listeners,
 * tools); this descriptor is specifically for Python callbacks that ride in the plan as
 * cloudpickle bytes.
 */
public final class ActionSpec implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final List<String> events;
  private final String cloudpickleB64;

  @JsonCreator
  public ActionSpec(
      @JsonProperty("name") String name,
      @JsonProperty("events") List<String> events,
      @JsonProperty("cloudpickle_b64") String cloudpickleB64) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("ActionSpec.name must be non-empty");
    }
    if (cloudpickleB64 == null || cloudpickleB64.isEmpty()) {
      throw new IllegalArgumentException("ActionSpec.cloudpickle_b64 must be non-empty");
    }
    this.name = name;
    this.events = events == null ? Collections.emptyList() : List.copyOf(events);
    this.cloudpickleB64 = cloudpickleB64;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("events")
  public List<String> getEvents() {
    return events;
  }

  @JsonProperty("cloudpickle_b64")
  public String getCloudpickleB64() {
    return cloudpickleB64;
  }
}
