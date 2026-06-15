package org.jagentic.pekko.http;

import java.util.List;
import java.util.Map;

/** Minimal A2A-style Agent Card served at {@code /.well-known/agent-card.json} so external A2A
 * callers (and our own A2AClient) can discover this agent. */
public record AgentCard(String name, String description, String url, String version,
                        List<Skill> skills) {

  public record Skill(String id, String name, String description) {}

  public static AgentCard defaultCard(String url) {
    return new AgentCard(
        "agentic-pekko",
        "Agentic Pekko — event-sourced conversation agents on Apache Pekko (router→path→verifier).",
        url,
        "0.1.0",
        List.of(new Skill("chat", "Conversational agent",
            "Processes a turn for a conversation; durable, single-writer per conversation.")));
  }

  /** Shape the card as the JSON map the endpoint returns. */
  public Map<String, Object> toJson() {
    return Map.of(
        "name", name,
        "description", description,
        "url", url,
        "version", version,
        "capabilities", Map.of("streaming", false),
        "skills", skills.stream()
            .map(s -> Map.of("id", s.id(), "name", s.name(), "description", s.description()))
            .toList());
  }
}
