package org.agentic.flink.skill;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * In-process registry of {@link Skill}s addressable by name.
 *
 * <p>Parallel to {@code ToolRegistry}, but for the higher-level capability concept. Used by
 * agents that need to look skills up at runtime — e.g. to surface a list to the model or to
 * decide which sub-state-machine to enter.
 */
public final class SkillRegistry implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Map<String, Skill> skills;

  private SkillRegistry(Map<String, Skill> skills) {
    this.skills = Collections.unmodifiableMap(new LinkedHashMap<>(skills));
  }

  public Optional<Skill> get(String name) {
    return Optional.ofNullable(skills.get(name));
  }

  public Collection<Skill> all() {
    return skills.values();
  }

  public int size() {
    return skills.size();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final Map<String, Skill> skills = new LinkedHashMap<>();

    public Builder register(Skill skill) {
      skills.put(skill.getName(), skill);
      return this;
    }

    public Builder registerAll(Collection<Skill> skills) {
      for (Skill s : skills) {
        this.skills.put(s.getName(), s);
      }
      return this;
    }

    public SkillRegistry build() {
      return new SkillRegistry(skills);
    }
  }
}
