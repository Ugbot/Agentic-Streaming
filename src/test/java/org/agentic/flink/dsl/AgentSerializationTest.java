package org.agentic.flink.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.a2a.A2ATransport;
import org.agentic.flink.a2a.RemoteAgentSpec;
import org.agentic.flink.dsl.Agent.AgentType;
import org.agentic.flink.execution.AgentExecutorTest;
import org.agentic.flink.inference.EchoInferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.InferenceToolAdapter;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.memory.conversation.InMemoryConversationStore;
import org.agentic.flink.skill.Skill;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.storage.memory.InMemoryLongTermStore;
import org.agentic.flink.tools.mcp.McpServerSpec;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

/** A fully configured {@link Agent} survives Flink's {@link InstantiationUtil} round trip. */
@SuppressWarnings("deprecation")
class AgentSerializationTest {

  @Test
  void builtAgentRoundTripsThroughInstantiationUtil() throws Exception {
    String id = "a-" + UUID.randomUUID();
    String prompt = "p-" + UUID.randomUUID();
    String fragment = "frag-" + UUID.randomUUID();
    Duration ttl = Duration.ofMinutes(ThreadLocalRandom.current().nextInt(1, 100));
    int iterations = ThreadLocalRandom.current().nextInt(1, 30);
    InMemoryLongTermStore lts = new InMemoryLongTermStore();
    lts.initialize(Map.of());
    RemoteAgentSpec peer =
        RemoteAgentSpec.endpoint(
            "peer-" + UUID.randomUUID(), "https://p/a2a", A2ATransport.JSONRPC);
    InferenceToolAdapter inference =
        new InferenceToolAdapter(
            "inf-" + UUID.randomUUID(),
            "desc",
            new EchoInferenceConnection(),
            InferenceSetup.builder().withModelName("m").withModelUri("u").build(),
            InferenceToolAdapter.TaskKind.CLASSIFIER);

    Agent original =
        Agent.builder()
            .withId(id)
            .withName("name")
            .withDescription("desc")
            .withType(AgentType.COORDINATOR)
            .withSystemPrompt(prompt)
            .withChatConnection(
                new AgentExecutorTest.ScriptedConnection(List.of(AgentExecutorTest.text("ok"))))
            .withChatSetup(
                ChatSetup.builder()
                    .withModel("m-" + UUID.randomUUID())
                    .withTemperature(0.2)
                    .build())
            .withTools("t1")
            .withRequiredTools("t2")
            .withToolDefaults("t1", Map.of("k", "v"))
            .withMaxIterations(iterations)
            .withTimeout(Duration.ofSeconds(42))
            .withToolTimeout(Duration.ofSeconds(4))
            .withMaxValidationAttempts(4)
            .withMaxCorrectionAttempts(5)
            .withCompensatingTool("t1", "undo-t1")
            .withShortTermTtl(ttl)
            .withLongTermStore(lts)
            .withConversationStore(new InMemoryConversationStore())
            .withListener(new AgentBuilderTest.NoopListener())
            .withSkill(
                Skill.builder()
                    .withName("sk")
                    .withTools("t3")
                    .withSystemPromptFragment(fragment)
                    .build())
            .withMcpServer(McpServerSpec.http("mcp", "http://localhost:1/mcp"))
            .withRemoteAgent(peer)
            .withInferenceTool(inference)
            .withGuardrail(new AgentBuilderTest.NamedGuardrail("g"))
            .build();

    byte[] bytes = InstantiationUtil.serializeObject(original);
    Agent copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

    assertEquals(id, copy.getAgentId());
    assertEquals("name", copy.getAgentName());
    assertEquals("desc", copy.getDescription());
    assertEquals(AgentType.COORDINATOR, copy.getAgentType());
    assertEquals(original.getSystemPrompt(), copy.getSystemPrompt());
    assertTrue(copy.getSystemPrompt().contains(fragment));
    assertEquals(original.getLlmModel(), copy.getLlmModel());
    assertEquals(0.2, copy.getTemperature(), 1e-9);
    assertInstanceOf(AgentExecutorTest.ScriptedConnection.class, copy.getChatConnection());
    assertEquals(original.getAllowedTools(), copy.getAllowedTools());
    assertEquals(original.getRequiredTools(), copy.getRequiredTools());
    assertEquals(original.getToolDefaults(), copy.getToolDefaults());
    assertEquals(iterations, copy.getMaxIterations());
    assertEquals(Duration.ofSeconds(42), copy.getTimeout());
    assertEquals(Duration.ofSeconds(4), copy.getToolTimeout());
    assertEquals(4, copy.getMaxValidationAttempts());
    assertEquals(5, copy.getMaxCorrectionAttempts());
    assertEquals(
        original.getStateMachine().getStateMachineId(), copy.getStateMachine().getStateMachineId());
    assertEquals(
        original.getStateMachine().getTransitions().size(),
        copy.getStateMachine().getTransitions().size());
    assertTrue(copy.isCompensationEnabled());
    assertEquals(original.getCompensatingTools(), copy.getCompensatingTools());
    assertEquals(ttl, copy.getShortTermTtl());
    assertInstanceOf(InMemoryLongTermStore.class, copy.getLongTermStore());
    assertTrue(((InMemoryLongTermStore) copy.getLongTermStore()).isConfigured());
    assertInstanceOf(InMemoryConversationStore.class, copy.getConversationStore());
    assertEquals(1, copy.getListeners().size());
    assertTrue(copy.getSkillRegistry().get("sk").isPresent());
    assertEquals(1, copy.getMcpServers().size());
    assertEquals("mcp", copy.getMcpServers().get(0).getName());
    assertEquals(1, copy.getRemoteAgents().size());
    assertEquals(peer.toolId(), copy.getRemoteAgents().get(0).toolId());
    assertNotNull(copy.getA2AClientFactory());
    assertEquals(1, copy.getInferenceTools().size());
    assertEquals(inference.getToolId(), copy.getInferenceTools().get(0).getToolId());
    assertEquals(1, copy.getGuardrails().size());
    assertEquals("g", copy.getGuardrails().get(0).name());
    assertFalse(copy.getStateMachine().getTransitionsFrom(AgentState.COMPENSATING).isEmpty());
  }

  private static void assertFalse(boolean condition) {
    org.junit.jupiter.api.Assertions.assertFalse(condition);
  }
}
