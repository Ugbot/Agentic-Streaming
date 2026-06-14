package org.jagentic.core.skill;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Named skills, looked up by the builder when a path lists {@code skills}. */
public final class SkillRegistry {
  private final Map<String, Skill> skills = new LinkedHashMap<>();

  public SkillRegistry register(Skill s) {
    skills.put(s.name(), s);
    return this;
  }

  public Skill get(String name) {
    Skill s = skills.get(name);
    if (s == null) {
      throw new IllegalArgumentException("unknown skill " + name);
    }
    return s;
  }

  /** Result of expanding skill names: extra tools, joined prompt fragment, required facts. */
  public record Expanded(List<String> tools, String promptFragment, List<String> facts) {}

  public Expanded expand(List<String> names) {
    List<String> tools = new ArrayList<>();
    List<String> facts = new ArrayList<>();
    StringBuilder frag = new StringBuilder();
    for (String n : names == null ? List.<String>of() : names) {
      Skill s = get(n);
      for (String t : s.tools()) {
        if (!tools.contains(t)) tools.add(t);
      }
      if (!s.promptFragment().isBlank()) {
        if (frag.length() > 0) frag.append("\n");
        frag.append(s.promptFragment());
      }
      facts.addAll(s.requiredFacts());
    }
    return new Expanded(tools, frag.toString(), facts);
  }

  @SuppressWarnings("unchecked")
  public static SkillRegistry fromSpecs(List<Map<String, Object>> specs) {
    SkillRegistry reg = new SkillRegistry();
    for (Map<String, Object> s : specs == null ? List.<Map<String, Object>>of() : specs) {
      reg.register(new Skill(
          (String) s.get("name"),
          (List<String>) s.getOrDefault("tools", List.of()),
          (String) s.getOrDefault("prompt", ""),
          (List<String>) s.getOrDefault("facts", List.of())));
    }
    return reg;
  }
}
