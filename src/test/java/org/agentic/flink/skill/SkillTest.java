package org.agentic.flink.skill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SkillTest {

  @Test
  @DisplayName("AgentBuilder.withSkill() fans tools out to allowedTools and concatenates prompt")
  void skillFanOutThroughAgentBuilder() {
    Skill s =
        Skill.builder()
            .withName("research")
            .withDescription("Pull recent papers from arxiv and summarize")
            .withTools("web-search", "doc-fetch", "summarize")
            .withSystemPromptFragment("Prefer primary sources. Cite arxiv IDs.")
            .withRequiredFacts("user_research_area")
            .build();

    String basePrompt = "you are a research assistant " + UUID.randomUUID();
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt(basePrompt)
            .withSkill(s)
            .withStateMachine(minimalStateMachine())
            .build();

    assertTrue(agent.canUseTool("web-search"));
    assertTrue(agent.canUseTool("doc-fetch"));
    assertTrue(agent.canUseTool("summarize"));
    assertTrue(agent.getSystemPrompt().contains(basePrompt));
    assertTrue(agent.getSystemPrompt().contains("Prefer primary sources"));
    assertTrue(agent.getSystemPrompt().contains("# Skill: research"));
    assertTrue(agent.hasSkills());
    assertEquals(1, agent.getSkillRegistry().size());
    assertSame(s, agent.getSkillRegistry().get("research").orElseThrow());
  }

  /**
   * State machine satisfying the framework's validator: every non-terminal {@link AgentState}
   * (everything except COMPLETED / FAILED / COMPENSATED) has at least one outgoing transition,
   * and every state is reachable from the initial state.
   */
  private static AgentStateMachine minimalStateMachine() {
    AgentStateMachine.Builder b =
        AgentStateMachine.builder()
            .withId("sm-" + UUID.randomUUID())
            .withInitialState(AgentState.INITIALIZED);

    b.addTransition(transition(AgentState.INITIALIZED, AgentState.EXECUTING, AgentEventType.FLOW_STARTED));
    b.addTransition(transition(AgentState.EXECUTING, AgentState.VALIDATING, AgentEventType.VALIDATION_REQUESTED));
    b.addTransition(transition(AgentState.VALIDATING, AgentState.CORRECTING, AgentEventType.VALIDATION_FAILED));
    b.addTransition(transition(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED));
    b.addTransition(transition(AgentState.CORRECTING, AgentState.EXECUTING, AgentEventType.CORRECTION_COMPLETED));
    b.addTransition(transition(AgentState.EXECUTING, AgentState.SUPERVISOR_REVIEW, AgentEventType.SUPERVISOR_REVIEW_REQUESTED));
    b.addTransition(transition(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED));
    b.addTransition(transition(AgentState.PAUSED, AgentState.EXECUTING, AgentEventType.FLOW_RESUMED));
    b.addTransition(transition(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(transition(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED));
    return b.build();
  }

  private static AgentTransition transition(AgentState from, AgentState to, AgentEventType on) {
    return AgentTransition.builder().from(from).to(to).on(on).build();
  }

  @Test
  @DisplayName("SkillRegistry preserves insertion order and supports lookup-or-empty")
  void registryLookups() {
    Skill a = Skill.builder().withName("a").build();
    Skill b = Skill.builder().withName("b").build();
    SkillRegistry r = SkillRegistry.builder().register(a).register(b).build();

    assertEquals(2, r.size());
    assertNotNull(r.get("a").orElseThrow());
    assertNotNull(r.get("b").orElseThrow());
    assertEquals(java.util.Optional.empty(), r.get("missing"));
    assertEquals(java.util.List.of("a", "b"),
        r.all().stream().map(Skill::getName).toList());
  }
}
