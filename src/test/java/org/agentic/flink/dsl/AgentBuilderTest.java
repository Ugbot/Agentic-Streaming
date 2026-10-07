package org.agentic.flink.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.a2a.A2AClientFactory;
import org.agentic.flink.a2a.A2ATransport;
import org.agentic.flink.a2a.FakeA2AClient;
import org.agentic.flink.a2a.RemoteAgentSpec;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.execution.AgentExecutorTest;
import org.agentic.flink.inference.EchoInferenceConnection;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.job.AgentTurnDispatcher;
import org.agentic.flink.job.TurnDispatchDedupFunction;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.OutputSchema;
import org.agentic.flink.memory.conversation.InMemoryConversationStore;
import org.agentic.flink.skill.Skill;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.storage.memory.InMemoryLongTermStore;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.mcp.McpServerSpec;
import org.junit.jupiter.api.Test;

/** Every public {@code withX} of the legacy builder is observable on the built {@link Agent}. */
@SuppressWarnings("deprecation")
class AgentBuilderTest {

  static final class NamedGuardrail implements Guardrail {
    private static final long serialVersionUID = 1L;
    final String id;

    NamedGuardrail(String id) {
      this.id = id;
    }

    @Override
    public String name() {
      return id;
    }
  }

  static final class NoopListener implements AgentEventListener {
    private static final long serialVersionUID = 1L;
  }

  private static AgentBuilder minimal() {
    return Agent.builder()
        .withId("a-" + UUID.randomUUID())
        .withSystemPrompt("p-" + UUID.randomUUID());
  }

  private static AgentEvent event(AgentEventType type, String key, int value) {
    AgentEvent e = new AgentEvent("f", "u", "a", type);
    e.putData(key, value);
    return e;
  }

  @Test
  void identityMethodsAreObservable() {
    String id = "id-" + UUID.randomUUID();
    String name = "name-" + UUID.randomUUID();
    String desc = "desc-" + UUID.randomUUID();
    Agent agent =
        Agent.builder()
            .withId(id)
            .withName(name)
            .withDescription(desc)
            .withType(AgentType.VALIDATOR)
            .withSystemPrompt("s")
            .build();
    assertEquals(id, agent.getAgentId());
    assertEquals(name, agent.getAgentName());
    assertEquals(desc, agent.getDescription());
    assertEquals(AgentType.VALIDATOR, agent.getAgentType());
  }

  @Test
  void typeDefaultsFlowIntoBudgetsAndChatSetup() {
    Agent researcher = minimal().withType(AgentType.RESEARCHER).build();
    assertEquals(15, researcher.getMaxIterations());
    assertEquals(Duration.ofMinutes(5), researcher.getTimeout());
    assertEquals(0.4, researcher.getTemperature(), 1e-9);
    assertEquals("Researcher Agent", researcher.getAgentName());
    assertEquals(AgentType.RESEARCHER.getDescription(), researcher.getDescription());

    Agent validator = minimal().withType(AgentType.VALIDATOR).build();
    assertEquals(3, validator.getMaxValidationAttempts());
    assertEquals(0.1, validator.getTemperature(), 1e-9);
  }

  @Test
  void systemPromptSkillsAndToolsAreObservable() {
    String prompt = "prompt-" + UUID.randomUUID();
    String fragment = "frag-" + UUID.randomUUID();
    String skillTool = "tool-" + UUID.randomUUID();
    Skill skill =
        Skill.builder()
            .withName("sk")
            .withTools(skillTool)
            .withSystemPromptFragment(fragment)
            .build();
    Agent agent = minimal().withSystemPrompt(prompt).withSkill(skill).withTools("t1", "t2").build();

    assertTrue(agent.getSystemPrompt().startsWith(prompt));
    assertTrue(agent.getSystemPrompt().contains(fragment));
    assertTrue(agent.canUseTool("t1") && agent.canUseTool("t2") && agent.canUseTool(skillTool));
    assertFalse(agent.canUseTool("other"));
    assertTrue(agent.hasSkills());
    assertEquals(Optional.of(skill), agent.getSkillRegistry().get("sk"));
    assertThrows(UnsupportedOperationException.class, () -> agent.getAllowedTools().add("x"));
  }

  @Test
  void chatConnectionSetupAndOutputSchemaAreObservable() {
    ChatConnection conn =
        new AgentExecutorTest.ScriptedConnection(List.of(AgentExecutorTest.text("x")));
    ChatSetup setup =
        ChatSetup.builder()
            .withModel("m-" + UUID.randomUUID())
            .withTemperature(0.11)
            .withMaxResponseTokens(123)
            .build();
    OutputSchema<Map> schema = OutputSchema.of(Map.class);
    Agent agent =
        minimal().withChatConnection(conn).withChatSetup(setup).withOutputSchema(schema).build();

    assertSame(conn, agent.getChatConnection());
    assertEquals(setup.getModelName(), agent.getLlmModel());
    assertEquals(0.11, agent.getTemperature(), 1e-9);
    assertEquals(123, agent.getMaxResponseTokens());
    assertTrue(agent.getChatSetup().hasOutputSchema());
    assertSame(schema, agent.getChatSetup().getOutputSchema());

    Agent implicit = minimal().withOutputSchema(schema).build();
    assertSame(schema, implicit.getChatSetup().getOutputSchema());
    assertEquals(0.7, implicit.getTemperature(), 1e-9);
  }

  @Test
  void requiredToolsAndToolDefaultsAreObservableAndCopied() {
    String required = "req-" + UUID.randomUUID();
    Map<String, Object> defaults = new HashMap<>();
    int region = ThreadLocalRandom.current().nextInt();
    defaults.put("region", region);
    Agent agent =
        minimal().withRequiredTools(required).withToolDefaults(required, defaults).build();
    defaults.put("region", region + 1);

    assertEquals(java.util.Set.of(required), agent.getRequiredTools());
    assertTrue(agent.canUseTool(required), "required tools are also allowed");
    assertEquals(region, agent.getToolDefaults().get(required).get("region"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> agent.getToolDefaults().get(required).put("k", 1));
    assertThrows(IllegalArgumentException.class, () -> minimal().withToolDefaults(null, defaults));
    assertThrows(IllegalArgumentException.class, () -> minimal().withToolDefaults("t", null));
  }

  @Test
  void executionBudgetsAreObservable() {
    int iterations = ThreadLocalRandom.current().nextInt(1, 50);
    Duration timeout = Duration.ofMillis(ThreadLocalRandom.current().nextLong(1_000, 100_000));
    Duration toolTimeout = Duration.ofMillis(ThreadLocalRandom.current().nextLong(1, 1_000));
    Agent agent =
        minimal()
            .withMaxIterations(iterations)
            .withTimeout(timeout)
            .withToolTimeout(toolTimeout)
            .build();
    assertEquals(iterations, agent.getMaxIterations());
    assertEquals(timeout, agent.getTimeout());
    assertEquals(toolTimeout, agent.getToolTimeout());
  }

  @Test
  void retryBudgetsShapeTheDefaultStateMachine() {
    int validation = ThreadLocalRandom.current().nextInt(2, 6);
    int correction = ThreadLocalRandom.current().nextInt(2, 6);
    Agent agent =
        minimal()
            .withMaxValidationAttempts(validation)
            .withMaxCorrectionAttempts(correction)
            .build();
    assertEquals(validation, agent.getMaxValidationAttempts());
    assertEquals(correction, agent.getMaxCorrectionAttempts());
    AgentStateMachine sm = agent.getStateMachine();
    assertEquals(agent.getAgentId() + "-state-machine", sm.getStateMachineId());

    assertEquals(
        Optional.of(AgentState.CORRECTING),
        sm.getNextState(
            AgentState.VALIDATING,
            event(AgentEventType.VALIDATION_FAILED, "validation_attempts", validation - 1)));
    assertEquals(
        Optional.of(AgentState.FAILED),
        sm.getNextState(
            AgentState.VALIDATING,
            event(AgentEventType.VALIDATION_FAILED, "validation_attempts", validation)));
    assertEquals(
        Optional.of(AgentState.CORRECTING),
        sm.getNextState(
            AgentState.SUPERVISOR_REVIEW,
            event(AgentEventType.SUPERVISOR_REJECTED, "correction_attempts", correction - 1)));
    assertEquals(
        Optional.of(AgentState.FAILED),
        sm.getNextState(
            AgentState.SUPERVISOR_REVIEW,
            event(AgentEventType.SUPERVISOR_REJECTED, "correction_attempts", correction)));
  }

  @Test
  void explicitStateMachineIsUsedVerbatim() {
    AgentStateMachine sm = AgentExecutorTest.stateMachine();
    assertSame(
        sm, minimal().withStateMachine(sm).withMaxValidationAttempts(9).build().getStateMachine());
  }

  @Test
  void compensationOptionsAreObservable() {
    AgentEvent failure = new AgentEvent("f", "u", "a", AgentEventType.FLOW_FAILED);
    Agent off = minimal().build();
    assertFalse(off.isCompensationEnabled());
    assertEquals(
        Optional.of(AgentState.FAILED),
        off.getStateMachine().getNextState(AgentState.EXECUTING, failure));

    Agent on = minimal().withCompensationEnabled(true).build();
    assertTrue(on.isCompensationEnabled());
    assertEquals(
        Optional.of(AgentState.COMPENSATING),
        on.getStateMachine().getNextState(AgentState.EXECUTING, failure));

    Agent saga =
        minimal()
            .withCompensatingTool("charge", "refund")
            .withCompensatingTool("ship", "recall")
            .build();
    assertTrue(saga.isCompensationEnabled(), "withCompensatingTool implies compensation");
    assertEquals(List.of("charge", "ship"), List.copyOf(saga.getCompensatingTools().keySet()));
    assertEquals("refund", saga.getCompensatingTools().get("charge"));
    assertEquals("recall", saga.getCompensatingTools().get("ship"));
    assertThrows(IllegalArgumentException.class, () -> minimal().withCompensatingTool("x", null));
  }

  @Test
  void shortTermTtlIsObservableThroughTheDispatcher() {
    Duration ttl = Duration.ofMinutes(ThreadLocalRandom.current().nextInt(1, 600));
    Agent agent = minimal().withShortTermTtl(ttl).build();
    assertEquals(ttl, agent.getShortTermTtl());
    assertEquals(ttl, new TurnDispatchDedupFunction(agent).getDedupTtl());
    assertEquals(ttl, new AgentTurnDispatcher(agent, ToolRegistry.empty()).dedup().getDedupTtl());

    Agent unset = minimal().withShortTermTtl(null).build();
    assertEquals(Duration.ZERO, unset.getShortTermTtl());
    assertEquals(
        TurnDispatchDedupFunction.DEFAULT_DEDUP_TTL,
        new TurnDispatchDedupFunction(unset).getDedupTtl());
    assertEquals(
        TurnDispatchDedupFunction.DEFAULT_DEDUP_TTL,
        new AgentTurnDispatcher(unset, ToolRegistry.empty()).dedup().getDedupTtl());
  }

  @Test
  void storesAreObservableAndConversationStoreDefaultsToDiscovery() {
    InMemoryLongTermStore lts = new InMemoryLongTermStore();
    InMemoryConversationStore conv = new InMemoryConversationStore();
    Agent agent = minimal().withLongTermStore(lts).withConversationStore(conv).build();
    assertSame(lts, agent.getLongTermStore());
    assertTrue(agent.hasLongTermStore());
    assertSame(conv, agent.getConversationStore());

    Agent defaults = minimal().build();
    assertFalse(defaults.hasLongTermStore());
    assertNotNull(defaults.getConversationStore(), "a conversation store is always discovered");
  }

  @Test
  void integrationsAreObservable() {
    NoopListener l1 = new NoopListener();
    NoopListener l2 = new NoopListener();
    McpServerSpec mcp = McpServerSpec.http("mcp-" + UUID.randomUUID(), "http://localhost:1/mcp");
    RemoteAgentSpec peer =
        RemoteAgentSpec.endpoint(
            "peer-" + UUID.randomUUID(), "https://p/a2a", A2ATransport.JSONRPC);
    A2AClientFactory factory = spec -> new FakeA2AClient(spec, 0, false);
    InferenceToolAdapter inference =
        new InferenceToolAdapter(
            "inf-" + UUID.randomUUID(),
            null,
            new EchoInferenceConnection(),
            InferenceSetup.builder().withModelName("m").withModelUri("u").build(),
            InferenceToolAdapter.TaskKind.CLASSIFIER);
    NamedGuardrail guard = new NamedGuardrail("g-" + UUID.randomUUID());

    Agent agent =
        minimal()
            .withListener(l1, l2)
            .withMcpServer(mcp)
            .withRemoteAgent(peer)
            .withA2AClientFactory(factory)
            .withInferenceTool(inference)
            .withGuardrail(guard)
            .build();

    assertEquals(List.of(l1, l2), agent.getListeners());
    assertEquals(List.of(mcp), agent.getMcpServers());
    assertTrue(agent.hasMcpServers());
    assertEquals(List.of(peer), agent.getRemoteAgents());
    assertTrue(agent.hasRemoteAgents());
    assertTrue(agent.canUseTool(peer.toolId()), "remote agent is exposed as an allowed tool");
    assertTrue(
        agent.getSkillRegistry().get("a2a-" + peer.name()).isPresent(),
        "remote agent is described as a skill");
    assertSame(factory, agent.getA2AClientFactory());
    assertEquals(List.of(inference), agent.getInferenceTools());
    assertTrue(agent.canUseTool(inference.getToolId()));
    assertEquals(List.of(guard), agent.getGuardrails());
    assertTrue(agent.hasGuardrails());
    assertThrows(IllegalArgumentException.class, () -> minimal().withInferenceTool(null));
  }

  @Test
  void nullA2AFactoryKeepsTheDiscoveringDefault() {
    assertNotNull(minimal().withA2AClientFactory(null).build().getA2AClientFactory());
  }

  @Test
  void requiredFieldsAreValidated() {
    IllegalStateException noId =
        assertThrows(
            IllegalStateException.class, () -> Agent.builder().withSystemPrompt("s").build());
    assertTrue(noId.getMessage().contains("Agent ID"));
    IllegalStateException emptyId =
        assertThrows(
            IllegalStateException.class,
            () -> Agent.builder().withId("").withSystemPrompt("s").build());
    assertTrue(emptyId.getMessage().contains("Agent ID"));
    IllegalStateException noPrompt =
        assertThrows(IllegalStateException.class, () -> Agent.builder().withId("a").build());
    assertTrue(noPrompt.getMessage().contains("System prompt"));
    assertThrows(
        IllegalStateException.class,
        () -> Agent.builder().withId("a").withSystemPrompt("").build());
  }

  @Test
  void toBuilderRoundTripsRetainedConfiguration() {
    NamedGuardrail guard = new NamedGuardrail("g");
    InMemoryLongTermStore lts = new InMemoryLongTermStore();
    Duration ttl = Duration.ofSeconds(ThreadLocalRandom.current().nextInt(1, 1000));
    Agent original =
        minimal()
            .withName("n")
            .withType(AgentType.CORRECTOR)
            .withTools("t")
            .withRequiredTools("r")
            .withToolDefaults("t", Map.of("k", "v"))
            .withMaxIterations(7)
            .withTimeout(Duration.ofSeconds(77))
            .withToolTimeout(Duration.ofSeconds(3))
            .withCompensatingTool("t", "undo-t")
            .withShortTermTtl(ttl)
            .withLongTermStore(lts)
            .withListener(new NoopListener())
            .withGuardrail(guard)
            .build();

    Agent copy = original.toBuilder().build();
    assertNotSame(original, copy);
    assertEquals(original.getAgentId(), copy.getAgentId());
    assertEquals(original.getAgentName(), copy.getAgentName());
    assertEquals(original.getAgentType(), copy.getAgentType());
    assertEquals(original.getSystemPrompt(), copy.getSystemPrompt());
    assertEquals(original.getAllowedTools(), copy.getAllowedTools());
    assertEquals(original.getRequiredTools(), copy.getRequiredTools());
    assertEquals(original.getToolDefaults(), copy.getToolDefaults());
    assertEquals(7, copy.getMaxIterations());
    assertEquals(Duration.ofSeconds(77), copy.getTimeout());
    assertEquals(Duration.ofSeconds(3), copy.getToolTimeout());
    assertEquals(original.getMaxCorrectionAttempts(), copy.getMaxCorrectionAttempts());
    assertTrue(copy.isCompensationEnabled());
    assertEquals(original.getCompensatingTools(), copy.getCompensatingTools());
    assertEquals(ttl, copy.getShortTermTtl());
    assertSame(lts, copy.getLongTermStore());
    assertSame(original.getConversationStore(), copy.getConversationStore());
    assertSame(original.getChatSetup(), copy.getChatSetup());
    assertEquals(original.getListeners(), copy.getListeners());
    assertEquals(List.of(guard), copy.getGuardrails());

    Agent modified = original.toBuilder().withMaxIterations(1).build();
    assertEquals(1, modified.getMaxIterations());
    assertEquals(7, original.getMaxIterations());
  }
}
