package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A skill advertised by an A2A agent in its {@link A2AAgentCard}.
 *
 * <p>Mirrors the protocol {@code AgentSkill} object: an {@code id} the caller targets, plus
 * human/LLM-facing {@code name}, {@code description}, {@code tags}, and {@code examples}. The
 * outbound {@code A2ASkillMapper} turns each of these into a framework {@link
 * org.agentic.flink.skill.Skill} so the local agent's LLM knows when to delegate to the peer.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class A2AAgentSkill implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final String name;
  private final String description;
  private final List<String> tags;
  private final List<String> examples;
  private final List<String> inputModes;
  private final List<String> outputModes;

  public A2AAgentSkill(
      String id,
      String name,
      String description,
      List<String> tags,
      List<String> examples,
      List<String> inputModes,
      List<String> outputModes) {
    this.id = Objects.requireNonNull(id, "id");
    this.name = name == null ? id : name;
    this.description = description == null ? "" : description;
    this.tags = copy(tags);
    this.examples = copy(examples);
    this.inputModes = copy(inputModes);
    this.outputModes = copy(outputModes);
  }

  private static List<String> copy(List<String> in) {
    return in == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(in));
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getTags() {
    return tags;
  }

  public List<String> getExamples() {
    return examples;
  }

  public List<String> getInputModes() {
    return inputModes;
  }

  public List<String> getOutputModes() {
    return outputModes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof A2AAgentSkill)) {
      return false;
    }
    A2AAgentSkill that = (A2AAgentSkill) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(description, that.description)
        && Objects.equals(tags, that.tags)
        && Objects.equals(examples, that.examples)
        && Objects.equals(inputModes, that.inputModes)
        && Objects.equals(outputModes, that.outputModes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, description, tags, examples, inputModes, outputModes);
  }

  @Override
  public String toString() {
    return "A2AAgentSkill{id=" + id + ", name=" + name + '}';
  }
}
