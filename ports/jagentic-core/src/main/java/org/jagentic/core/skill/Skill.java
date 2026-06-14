package org.jagentic.core.skill;

import java.util.List;

/** A named bundle of (tools + a system-prompt fragment + required facts) — the portable
 * analogue of the Flink {@code Skill}. The builder expands a path's skills into its tool
 * set + prompt. */
public record Skill(String name, List<String> tools, String promptFragment, List<String> requiredFacts) {
  public Skill {
    tools = tools == null ? List.of() : List.copyOf(tools);
    requiredFacts = requiredFacts == null ? List.of() : List.copyOf(requiredFacts);
    promptFragment = promptFragment == null ? "" : promptFragment;
  }
}
