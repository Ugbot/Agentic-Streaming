package org.agentic.flink.a2a;

import java.util.List;
import org.agentic.flink.skill.Skill;

/**
 * Maps a remote A2A agent into a framework {@link Skill}, so the local agent's LLM is told the peer
 * exists, what it can do, and which synthetic tool ({@code a2a:<name>}) to call to delegate to it.
 *
 * <p>Two entry points reflecting when information is available:
 *
 * <ul>
 *   <li>{@link #fromSpec(RemoteAgentSpec)} — at DSL build time (no network): a minimal skill from
 *       the static {@link RemoteAgentSpec}.
 *   <li>{@link #fromCard(RemoteAgentSpec, A2AAgentCard)} — at runtime after Agent Card discovery: a
 *       richer skill describing the peer's advertised skills/examples.
 * </ul>
 */
public final class A2ASkillMapper {

  private A2ASkillMapper() {}

  /** Build a minimal skill from a spec, used by {@code AgentBuilder.withRemoteAgent}. */
  public static Skill fromSpec(RemoteAgentSpec spec) {
    String desc =
        spec.description() != null && !spec.description().isEmpty()
            ? spec.description()
            : "Remote A2A agent '" + spec.name() + "'";
    StringBuilder fragment = new StringBuilder();
    fragment
        .append("You can delegate to the remote A2A agent '")
        .append(spec.name())
        .append("' by calling the tool '")
        .append(spec.toolId())
        .append("'. ")
        .append(desc)
        .append('.');
    if (spec.skillId() != null) {
      fragment.append(" Targeted skill: ").append(spec.skillId()).append('.');
    }
    return Skill.builder()
        .withName("a2a-" + spec.name())
        .withDescription(desc)
        .withTools(spec.toolId())
        .withSystemPromptFragment(fragment.toString())
        .build();
  }

  /** Build a richer skill from a discovered Agent Card. Falls back to {@link #fromSpec} if empty. */
  public static Skill fromCard(RemoteAgentSpec spec, A2AAgentCard card) {
    if (card == null || card.getSkills().isEmpty()) {
      return fromSpec(spec);
    }
    List<A2AAgentSkill> skills = card.getSkills();
    StringBuilder fragment = new StringBuilder();
    fragment
        .append("You can delegate to the remote A2A agent '")
        .append(card.getName())
        .append("' (")
        .append(card.getDescription())
        .append(") by calling the tool '")
        .append(spec.toolId())
        .append("'. It offers these skills: ");
    boolean first = true;
    for (A2AAgentSkill skill : skills) {
      if (spec.skillId() != null && !spec.skillId().equals(skill.getId())) {
        continue;
      }
      if (!first) {
        fragment.append("; ");
      }
      first = false;
      fragment.append(skill.getName()).append(" — ").append(skill.getDescription());
      if (!skill.getExamples().isEmpty()) {
        fragment.append(" (e.g. ").append(skill.getExamples().get(0)).append(")");
      }
    }
    fragment.append('.');
    return Skill.builder()
        .withName("a2a-" + spec.name())
        .withDescription(card.getDescription())
        .withTools(spec.toolId())
        .withSystemPromptFragment(fragment.toString())
        .build();
  }
}
