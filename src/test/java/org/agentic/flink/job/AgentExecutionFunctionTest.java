package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import org.agentic.flink.tool.ToolRegistry;
import java.io.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.junit.jupiter.api.*;

/**
 * Unit tests for {@link AgentExecutionFunction}.
 *
 * <p>Because {@code AgentExecutionFunction} is a CEP {@link PatternProcessFunction}, full
 * integration testing requires a Flink MiniCluster with CEP infrastructure. These tests focus on
 * verifiable unit-level properties:
 * <ul>
 *   <li>Construction with Agent and ToolRegistry</li>
 *   <li>Serialization (required for Flink distribution across task managers)</li>
 *   <li>Type hierarchy: implements both {@link PatternProcessFunction} and
 *       {@link TimedOutPartialMatchHandler}</li>
 * </ul>
 *
 * <p>All test data uses randomized identifiers via {@link UUID#randomUUID()}.
 *
 * @author Agentic Flink Team
 * @see AgentExecutionFunction
 */
class AgentExecutionFunctionTest {

  // ==================== Helpers ====================

  /**
   * Builds a valid state machine with all required transitions for validation to pass.
   *
   * <p>All transitions use only condition-free definitions (no {@code .when()} lambdas) so
   * the resulting state machine is fully serializable, which is required for Flink distribution.
   * The standard factory methods like {@code AgentTransition.validationFailedWithRetry()} use
   * non-serializable {@code Predicate} lambdas, so we avoid them in favor of simple transitions.
   */
  private static AgentStateMachine buildValidStateMachine(String id, int timeoutSec,
      boolean compensation) {

    AgentStateMachine.Builder builder = AgentStateMachine.builder()
        .withId(id)
        .withGlobalTimeout(timeoutSec);

    if (compensation) {
      // When compensation is enabled, FAILED is not terminal (it transitions to COMPENSATING)
      builder.withTerminalStates(AgentState.COMPLETED, AgentState.COMPENSATED);
    }

    // INITIALIZED transitions
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.INITIALIZED)
        .to(AgentState.VALIDATING)
        .on(AgentEventType.VALIDATION_REQUESTED)
        .withDescription("Start validation from initialized")
        .build());
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.INITIALIZED)
        .to(AgentState.EXECUTING)
        .on(AgentEventType.FLOW_STARTED)
        .withDescription("Start execution from initialized")
        .build());

    // VALIDATING transitions (no lambdas)
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.VALIDATING)
        .to(AgentState.EXECUTING)
        .on(AgentEventType.VALIDATION_PASSED)
        .withDescription("Validation passed")
        .build());
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.VALIDATING)
        .to(AgentState.CORRECTING)
        .on(AgentEventType.VALIDATION_FAILED)
        .withDescription("Validation failed, attempting correction")
        .withPriority(10)
        .build());

    // CORRECTING transitions
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.CORRECTING)
        .to(AgentState.EXECUTING)
        .on(AgentEventType.CORRECTION_COMPLETED)
        .withDescription("Correction completed")
        .build());

    // EXECUTING transitions
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.EXECUTING)
        .to(AgentState.SUPERVISOR_REVIEW)
        .on(AgentEventType.SUPERVISOR_REVIEW_REQUESTED)
        .withDescription("Execution complete, supervisor review")
        .withPriority(10)
        .build());
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.EXECUTING)
        .to(AgentState.COMPLETED)
        .on(AgentEventType.FLOW_COMPLETED)
        .withDescription("Execution complete, no review")
        .withPriority(5)
        .build());

    // SUPERVISOR_REVIEW transitions
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.SUPERVISOR_REVIEW)
        .to(AgentState.COMPLETED)
        .on(AgentEventType.SUPERVISOR_APPROVED)
        .withDescription("Supervisor approved")
        .build());
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.SUPERVISOR_REVIEW)
        .to(AgentState.CORRECTING)
        .on(AgentEventType.SUPERVISOR_REJECTED)
        .withDescription("Supervisor rejected, correcting")
        .build());

    // PAUSED transitions
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.PAUSED)
        .to(AgentState.EXECUTING)
        .on(AgentEventType.FLOW_RESUMED)
        .withDescription("Resume from paused")
        .build());

    // OFFLOADING transitions
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.OFFLOADING)
        .to(AgentState.EXECUTING)
        .on(AgentEventType.STATE_OFFLOADED)
        .withDescription("Return from offloading")
        .build());

    if (compensation) {
      // FAILED -> COMPENSATING
      builder.addTransition(AgentTransition.builder()
          .from(AgentState.FAILED)
          .to(AgentState.COMPENSATING)
          .on(AgentEventType.COMPENSATION_REQUESTED)
          .withDescription("Start compensation after failure")
          .build());
    }

    // COMPENSATING -> COMPENSATED (always present, even if not compensation-enabled,
    // because COMPENSATING is non-terminal in the AgentState enum)
    builder.addTransition(AgentTransition.builder()
        .from(AgentState.COMPENSATING)
        .to(AgentState.COMPENSATED)
        .on(AgentEventType.COMPENSATION_COMPLETED)
        .withDescription("Compensation completed")
        .build());

    return builder.build();
  }

  /**
   * Builds a minimal Agent with randomized id and a valid, serializable state machine.
   */
  private static Agent randomAgent() {
    String id = "agent-" + UUID.randomUUID().toString().substring(0, 8);
    int timeout = ThreadLocalRandom.current().nextInt(5, 120);

    return Agent.builder()
        .withId(id)
        .withName("Test Agent " + ThreadLocalRandom.current().nextInt(1000))
        .withSystemPrompt("You are a test agent. Respond concisely.")
        .withLlmModel("qwen2.5:3b")
        .withTemperature(0.1 + ThreadLocalRandom.current().nextDouble(0.8))
        .withMaxIterations(ThreadLocalRandom.current().nextInt(1, 20))
        .withTimeout(Duration.ofSeconds(timeout))
        .withStateMachine(buildValidStateMachine(id + "-sm", timeout, false))
        .build();
  }

  /**
   * Builds a complex Agent with tools, validation, supervision, and compensation.
   */
  private static Agent complexAgent() {
    String id = "complex-" + UUID.randomUUID().toString().substring(0, 8);

    return Agent.builder()
        .withId(id)
        .withName("Complex Agent")
        .withSystemPrompt("You are an advanced agent with validation and supervision.")
        .withTools("web-search", "calculator", "database-query")
        .withMaxIterations(10)
        .withTimeout(Duration.ofMinutes(5))
        .withValidationEnabled(true)
        .withCorrectionEnabled(true)
        .withSupervisor("supervisor-" + UUID.randomUUID().toString().substring(0, 6))
        .withCompensationEnabled(true)
        .withStateMachine(buildValidStateMachine(id + "-sm", 300, true))
        .build();
  }

  // ==================== Construction Tests ====================

  @Nested
  @DisplayName("construction")
  class ConstructionTests {

    @RepeatedTest(5)
    @DisplayName("should construct with minimal agent and empty registry")
    void constructWithMinimalAgent() {
      Agent agent = randomAgent();
      ToolRegistry registry = ToolRegistry.empty();

      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);
      assertNotNull(function);
    }

    @Test
    @DisplayName("should construct with complex agent and populated registry")
    void constructWithComplexAgent() {
      Agent agent = complexAgent();
      ToolRegistry registry = ToolRegistry.builder()
          .registerTool("web-search", "Search the web")
          .registerTool("calculator", "Perform calculations")
          .registerTool("database-query", "Query a database")
          .build();

      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);
      assertNotNull(function);
    }
  }

  // ==================== Type Hierarchy Tests ====================

  @Nested
  @DisplayName("type hierarchy")
  class TypeHierarchyTests {

    @Test
    @DisplayName("should implement PatternProcessFunction")
    void implementsPatternProcessFunction() {
      Agent agent = randomAgent();
      ToolRegistry registry = ToolRegistry.empty();
      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);

      assertInstanceOf(PatternProcessFunction.class, function);
    }

    @Test
    @DisplayName("should implement TimedOutPartialMatchHandler")
    void implementsTimedOutPartialMatchHandler() {
      Agent agent = randomAgent();
      ToolRegistry registry = ToolRegistry.empty();
      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);

      assertInstanceOf(TimedOutPartialMatchHandler.class, function);
    }

    @Test
    @DisplayName("should be a Serializable type")
    void isSerializableType() {
      Agent agent = randomAgent();
      ToolRegistry registry = ToolRegistry.empty();
      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);

      assertInstanceOf(Serializable.class, function);
    }
  }

  // ==================== Serialization Tests ====================

  @Nested
  @DisplayName("serialization")
  class SerializationTests {

    @RepeatedTest(3)
    @DisplayName("should serialize and deserialize with minimal agent (Flink requirement)")
    void serializeWithMinimalAgent() throws Exception {
      Agent agent = randomAgent();
      ToolRegistry registry = ToolRegistry.empty();
      AgentExecutionFunction original = new AgentExecutionFunction(agent, registry);

      byte[] bytes = serialize(original);
      assertTrue(bytes.length > 0, "Serialized bytes should not be empty");

      AgentExecutionFunction deserialized = deserialize(bytes);
      assertNotNull(deserialized);
      assertInstanceOf(PatternProcessFunction.class, deserialized);
      assertInstanceOf(TimedOutPartialMatchHandler.class, deserialized);
    }

    @Test
    @DisplayName("should serialize and deserialize with complex agent and tools")
    void serializeWithComplexAgent() throws Exception {
      Agent agent = complexAgent();
      ToolRegistry registry = ToolRegistry.builder()
          .registerTool("web-search", "Search the web")
          .registerTool("calculator", "Perform calculations")
          .build();

      AgentExecutionFunction original = new AgentExecutionFunction(agent, registry);

      byte[] bytes = serialize(original);
      assertTrue(bytes.length > 0);

      AgentExecutionFunction deserialized = deserialize(bytes);
      assertNotNull(deserialized);
    }

    @Test
    @DisplayName("should produce consistent serialization size for same config")
    void consistentSerializationSize() throws Exception {
      AgentStateMachine sm = buildValidStateMachine("deterministic-sm", 30, false);

      Agent agent = Agent.builder()
          .withId("deterministic-agent")
          .withName("Deterministic")
          .withSystemPrompt("Fixed prompt for size consistency test")
          .withMaxIterations(5)
          .withTimeout(Duration.ofSeconds(30))
          .withStateMachine(sm)
          .build();
      ToolRegistry registry = ToolRegistry.empty();

      AgentExecutionFunction func1 = new AgentExecutionFunction(agent, registry);
      AgentExecutionFunction func2 = new AgentExecutionFunction(agent, registry);

      byte[] bytes1 = serialize(func1);
      byte[] bytes2 = serialize(func2);

      assertEquals(bytes1.length, bytes2.length,
          "Same configuration should produce same serialization size");
    }

    private byte[] serialize(AgentExecutionFunction function) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(function);
      }
      return bos.toByteArray();
    }

    private AgentExecutionFunction deserialize(byte[] bytes)
        throws IOException, ClassNotFoundException {
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        return (AgentExecutionFunction) ois.readObject();
      }
    }
  }

  // ==================== Agent Configuration Preservation Tests ====================

  @Nested
  @DisplayName("agent configuration preservation")
  class ConfigPreservationTests {

    @Test
    @DisplayName("should preserve agent configuration through serialization round-trip")
    void preservesAgentConfig() throws Exception {
      String agentId = "preserve-test-" + UUID.randomUUID().toString().substring(0, 8);
      String prompt = "Test prompt " + UUID.randomUUID();
      int maxIter = ThreadLocalRandom.current().nextInt(1, 50);
      AgentStateMachine sm = buildValidStateMachine(agentId + "-sm", 180, false);

      Agent agent = Agent.builder()
          .withId(agentId)
          .withName("Preservation Test Agent")
          .withSystemPrompt(prompt)
          .withMaxIterations(maxIter)
          .withTimeout(Duration.ofMinutes(3))
          .withTools("tool-a", "tool-b")
          .withValidationEnabled(true)
          .withStateMachine(sm)
          .build();

      ToolRegistry registry = ToolRegistry.builder()
          .registerTool("tool-a", "Tool A description")
          .registerTool("tool-b", "Tool B description")
          .build();

      AgentExecutionFunction original = new AgentExecutionFunction(agent, registry);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(original);
      }
      AgentExecutionFunction deserialized;
      try (ObjectInputStream ois =
               new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
        deserialized = (AgentExecutionFunction) ois.readObject();
      }

      assertNotNull(deserialized);
      assertInstanceOf(PatternProcessFunction.class, deserialized);
      assertInstanceOf(TimedOutPartialMatchHandler.class, deserialized);
    }

    @Test
    @DisplayName("should handle agent with compensation enabled")
    void handlesCompensationAgent() throws Exception {
      String id = "compensation-" + UUID.randomUUID().toString().substring(0, 8);
      AgentStateMachine sm = buildValidStateMachine(id + "-sm", 30, true);

      Agent agent = Agent.builder()
          .withId(id)
          .withSystemPrompt("Agent with saga compensation")
          .withCompensationEnabled(true)
          .withStateMachine(sm)
          .build();

      ToolRegistry registry = ToolRegistry.empty();
      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        assertDoesNotThrow(() -> oos.writeObject(function));
      }
    }
  }

  // ==================== Edge Cases ====================

  @Nested
  @DisplayName("edge cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("should handle agent with empty tool set")
    void agentWithNoTools() {
      String id = "no-tools-" + UUID.randomUUID().toString().substring(0, 8);
      AgentStateMachine sm = buildValidStateMachine(id + "-sm", 30, false);

      Agent agent = Agent.builder()
          .withId(id)
          .withSystemPrompt("Agent without any tools")
          .withStateMachine(sm)
          .build();

      ToolRegistry registry = ToolRegistry.empty();
      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);
      assertNotNull(function);
    }

    @Test
    @DisplayName("should handle agent with very long system prompt")
    void agentWithLongPrompt() throws Exception {
      StringBuilder longPrompt = new StringBuilder();
      for (int i = 0; i < 1000; i++) {
        longPrompt.append("Paragraph ").append(i).append(": ")
            .append(UUID.randomUUID()).append(". ");
      }

      String id = "long-prompt-" + UUID.randomUUID().toString().substring(0, 8);
      AgentStateMachine sm = buildValidStateMachine(id + "-sm", 30, false);

      Agent agent = Agent.builder()
          .withId(id)
          .withSystemPrompt(longPrompt.toString())
          .withStateMachine(sm)
          .build();

      ToolRegistry registry = ToolRegistry.empty();
      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(function);
      }
      assertTrue(bos.toByteArray().length > 0);
    }

    @Test
    @DisplayName("should handle agent with many tools registered")
    void agentWithManyTools() {
      String[] toolNames = new String[50];
      ToolRegistry.ToolRegistryBuilder registryBuilder = ToolRegistry.builder();

      for (int i = 0; i < 50; i++) {
        toolNames[i] = "tool-" + i + "-" + UUID.randomUUID().toString().substring(0, 4);
        registryBuilder.registerTool(toolNames[i], "Description for tool " + i);
      }

      String id = "many-tools-" + UUID.randomUUID().toString().substring(0, 8);
      AgentStateMachine sm = buildValidStateMachine(id + "-sm", 30, false);

      Agent agent = Agent.builder()
          .withId(id)
          .withSystemPrompt("Agent with many tools")
          .withTools(toolNames)
          .withStateMachine(sm)
          .build();

      ToolRegistry registry = registryBuilder.build();
      AgentExecutionFunction function = new AgentExecutionFunction(agent, registry);
      assertNotNull(function);
    }
  }
}
