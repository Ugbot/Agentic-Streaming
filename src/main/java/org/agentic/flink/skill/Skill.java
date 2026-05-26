package org.agentic.flink.skill;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A named bundle of agent capability — tools + system-prompt fragment + required facts —
 * borrowed in spirit from Apache Flink Agents' {@code @Skills} annotation, scaled down to fit
 * our builder DSL.
 *
 * <p>Skills are additive: {@code AgentBuilder.withSkill(...)} fans them out to {@link
 * #getTools()} (added to allowed tools) and {@link #getSystemPromptFragment()} (concatenated onto
 * the system prompt). {@link #getRequiredFacts()} are surfaced to the hydration layer as a hint
 * about which long-term facts to load eagerly.
 */
public final class Skill implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final String description;
  private final List<String> tools;
  private final String systemPromptFragment;
  private final List<String> requiredFacts;

  private Skill(Builder b) {
    this.name = Objects.requireNonNull(b.name, "name");
    this.description = b.description == null ? "" : b.description;
    this.tools = b.tools == null ? Collections.emptyList() : List.copyOf(b.tools);
    this.systemPromptFragment =
        b.systemPromptFragment == null ? "" : b.systemPromptFragment;
    this.requiredFacts =
        b.requiredFacts == null ? Collections.emptyList() : List.copyOf(b.requiredFacts);
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getTools() {
    return tools;
  }

  public String getSystemPromptFragment() {
    return systemPromptFragment;
  }

  public List<String> getRequiredFacts() {
    return requiredFacts;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private String description;
    private List<String> tools;
    private String systemPromptFragment;
    private List<String> requiredFacts;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder withTools(List<String> tools) {
      this.tools = tools;
      return this;
    }

    public Builder withTools(String... tools) {
      this.tools = List.of(tools);
      return this;
    }

    public Builder withSystemPromptFragment(String fragment) {
      this.systemPromptFragment = fragment;
      return this;
    }

    public Builder withRequiredFacts(String... facts) {
      this.requiredFacts = List.of(facts);
      return this;
    }

    public Skill build() {
      return new Skill(this);
    }
  }
}
