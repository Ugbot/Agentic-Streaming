package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.skill.Skill;
import org.agentic.flink.tool.ToolRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Verifies the AgentBuilder DSL wiring and the A2AToolRegistry job-assembly helper. */
class A2ADslWiringTest {

  private static final A2AClientFactory FAKE = spec -> new FakeA2AClient(spec, 0, false);

  @Test
  @DisplayName("withRemoteAgent registers the peer as an allowed tool + skill on the Agent")
  void withRemoteAgentWires() {
    RemoteAgentSpec planner =
        RemoteAgentSpec.card("planner", "https://planner/.well-known/agent-card.json");
    RemoteAgentSpec researcher =
        RemoteAgentSpec.endpoint("researcher", "https://r/a2a", A2ATransport.GRPC);

    Agent agent =
        Agent.builder()
            .withId("orchestrator-" + UUID.randomUUID())
            .withSystemPrompt("Coordinate peers.")
            .withRemoteAgent(planner, researcher)
            .withA2AClientFactory(FAKE)
            .withStateMachine(A2ATestSupport.minimalStateMachine())
            .build();

    assertEquals(2, agent.getRemoteAgents().size());
    assertTrue(agent.hasRemoteAgents());
    assertTrue(agent.getAllowedTools().contains("a2a:planner"));
    assertTrue(agent.getAllowedTools().contains("a2a:researcher"));
    // Each peer contributed a skill whose prompt fragment mentions its tool id.
    boolean plannerSkill =
        agent.getSkillRegistry().all().stream()
            .anyMatch(s -> s.getSystemPromptFragment().contains("a2a:planner"));
    assertTrue(plannerSkill, "expected a skill describing the planner peer");
  }

  @Test
  @DisplayName("A2AToolRegistry.registerInto adds one a2a:<peer> executor per remote agent")
  void registerIntoToolRegistry() {
    Agent agent =
        Agent.builder()
            .withId("orch")
            .withSystemPrompt("x")
            .withRemoteAgent(
                RemoteAgentSpec.endpoint("alpha", "https://a/a2a", A2ATransport.JSONRPC),
                RemoteAgentSpec.endpoint("beta", "https://b/a2a", A2ATransport.JSONRPC))
            .withA2AClientFactory(FAKE)
            .withStateMachine(A2ATestSupport.minimalStateMachine())
            .build();

    ToolRegistry.ToolRegistryBuilder b = ToolRegistry.builder();
    A2AToolRegistry.registerInto(b, agent);
    ToolRegistry registry = b.build();

    assertTrue(registry.hasTool("a2a:alpha"));
    assertTrue(registry.hasTool("a2a:beta"));
    assertTrue(registry.getExecutor("a2a:alpha").isPresent());
    assertEquals("a2a:alpha", registry.getExecutor("a2a:alpha").get().getToolId());
  }

  @Test
  @DisplayName("A2ASkillMapper.fromCard produces a richer fragment than fromSpec")
  void skillMapperFromCard() {
    RemoteAgentSpec spec = RemoteAgentSpec.card("geo", "https://geo/card");
    A2AAgentCard card =
        A2AAgentCard.builder()
            .name("geo")
            .description("geospatial routing")
            .addSkill(
                new A2AAgentSkill(
                    "route", "Router", "optimizes routes", null, java.util.List.of("MTV->SFO"), null, null))
            .build();
    Skill fromCard = A2ASkillMapper.fromCard(spec, card);
    Skill fromSpec = A2ASkillMapper.fromSpec(spec);

    assertTrue(fromCard.getTools().contains("a2a:geo"));
    assertTrue(fromCard.getSystemPromptFragment().contains("Router"));
    assertTrue(fromCard.getSystemPromptFragment().contains("MTV->SFO"));
    assertFalse(fromSpec.getSystemPromptFragment().contains("Router"));
  }
}
