package org.agentic.flink.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutorTest;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.GuardrailDecision;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.memory.conversation.InMemoryConversationStore;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.storage.memory.InMemoryLongTermStore;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/**
 * The builder options consumed by {@link AgentExecutionFunction} are observable through the
 * operator: required tools, tool defaults and timeout, guardrails, listeners, the conversation and
 * long-term stores, and the compensation request attached to a failed turn.
 */
@SuppressWarnings("deprecation")
class AgentExecutionFunctionWiringTest {

  /** Records the parameters of every call; shared across serialization by id. */
  static final class RecordingTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    static final Map<String, List<Map<String, Object>>> CALLS = new ConcurrentHashMap<>();
    final String id;

    RecordingTool(String id) {
      this.id = id;
      CALLS.put(id, new CopyOnWriteArrayList<>());
    }

    List<Map<String, Object>> calls() {
      return CALLS.get(id);
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      calls().add(parameters);
      return CompletableFuture.completedFuture("ok-" + id + "-" + calls().size());
    }

    @Override
    public String getToolId() {
      return id;
    }

    @Override
    public String getDescription() {
      return "recording";
    }
  }

  static final class BlockingGuardrail implements Guardrail {
    private static final long serialVersionUID = 1L;
    final String reason;

    BlockingGuardrail(String reason) {
      this.reason = reason;
    }

    @Override
    public GuardrailDecision beforeChat(String agentId, List<ChatMessage> messages) {
      return GuardrailDecision.block(reason, "test-model");
    }
  }

  static final class RecordingListener implements AgentEventListener {
    private static final long serialVersionUID = 1L;
    static final Map<String, List<String>> EVENTS = new ConcurrentHashMap<>();
    final String id;

    RecordingListener(String id) {
      this.id = id;
      EVENTS.put(id, new CopyOnWriteArrayList<>());
    }

    List<String> events() {
      return EVENTS.get(id);
    }

    @Override
    public void onAgentStart(String agentId) {
      events().add("start:" + agentId);
    }

    @Override
    public void onGuardrailBlock(String agentId, String modelName, String label) {
      events().add("block:" + label);
    }

    @Override
    public void onLongTermSync(String agentId, String flowId, int factsWritten) {
      events().add("sync:" + flowId + ":" + factsWritten);
    }
  }

  private static AgentEvent start(String userMessage) {
    AgentEvent e =
        new AgentEvent(
            "flow-" + UUID.randomUUID(),
            "user-" + UUID.randomUUID(),
            "a",
            AgentEventType.FLOW_STARTED);
    e.putData("user_message", userMessage);
    e.putData("turn_id", "t-" + UUID.randomUUID());
    return e;
  }

  private static OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> harness(
      AgentExecutionFunction fn) throws Exception {
    AsyncWaitOperatorFactory<AgentEvent, AgentEvent> factory =
        new AsyncWaitOperatorFactory<>(
            fn, fn.getTimeout().toMillis(), 4, AsyncDataStream.OutputMode.UNORDERED);
    OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h =
        new OneInputStreamOperatorTestHarness<>(
            factory,
            TypeInformation.of(AgentEvent.class)
                .createSerializer(new ExecutionConfig().getSerializerConfig()));
    h.open();
    return h;
  }

  private static List<AgentEvent> outputs(
      OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h) {
    List<AgentEvent> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((AgentEvent) r.getValue());
      }
    }
    return out;
  }

  private static LLMClient llm(List<ChatResponse> script) {
    return LLMClient.builder()
        .withModel("m")
        .build(new AgentExecutorTest.ScriptedConnection(script));
  }

  private static ToolRegistry registry(ToolExecutor... tools) {
    ToolRegistry.ToolRegistryBuilder reg = ToolRegistry.builder();
    for (ToolExecutor t : tools) {
      reg.registerTool(t.getToolId(), t);
    }
    return reg.build();
  }

  private static Agent.AgentType anyType() {
    return Agent.AgentType.EXECUTOR;
  }

  @Test
  void missingRequiredToolFailsOpen() {
    String missing = "missing-" + UUID.randomUUID();
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("s")
            .withType(anyType())
            .withRequiredTools(missing)
            .withStateMachine(AgentExecutorTest.stateMachine())
            .build();
    AgentExecutionFunction fn =
        new AgentExecutionFunction(agent, ToolRegistry.empty(), llm(List.of()));
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> harness(fn));
    assertTrue(e.getMessage().contains(missing), e.getMessage());
  }

  @Test
  void toolDefaultsAreMergedUnderModelArgumentsAndToolTimeoutApplies() throws Exception {
    RecordingTool tool = new RecordingTool("rec-" + UUID.randomUUID());
    AgentExecutorTest.CountingTool slow =
        new AgentExecutorTest.CountingTool("slow-" + UUID.randomUUID());
    slow.block = new java.util.concurrent.CountDownLatch(1);
    String region = "region-" + UUID.randomUUID();
    int amount = ThreadLocalRandom.current().nextInt(1, 1000);
    long toolTimeoutMs = ThreadLocalRandom.current().nextLong(50, 200);
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("s")
            .withTools(tool.id, slow.id)
            .withToolDefaults(tool.id, Map.of("region", region, "amount", -1))
            .withToolTimeout(Duration.ofMillis(toolTimeoutMs))
            .withTimeout(Duration.ofSeconds(10))
            .withStateMachine(AgentExecutorTest.stateMachine())
            .build();
    AgentExecutionFunction fn =
        new AgentExecutionFunction(
            agent,
            registry(tool, slow),
            llm(
                List.of(
                    AgentExecutorTest.toolCall(tool.id, Map.of("amount", amount)),
                    AgentExecutorTest.text("done"))));
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h = harness(fn)) {
      h.processElement(new StreamRecord<>(start("hi"), 1L));
      h.endInput();
      assertEquals(AgentEventType.FLOW_COMPLETED, outputs(h).get(0).getEventType());
      assertEquals(1, tool.calls().size());
      assertEquals(region, tool.calls().get(0).get("region"), "default merged in");
      assertEquals(amount, tool.calls().get(0).get("amount"), "model argument wins over default");

      ToolExecutor configured = fn.getEffectiveToolRegistry().getExecutor(slow.id).orElseThrow();
      assertInstanceOf(AgentExecutionFunction.ConfiguredToolExecutor.class, configured);
      ExecutionException timedOut =
          assertThrows(
              ExecutionException.class,
              () -> configured.execute(Map.of()).get(5, TimeUnit.SECONDS));
      assertInstanceOf(TimeoutException.class, timedOut.getCause());
      slow.block.countDown();
    }
  }

  @Test
  void guardrailsAndListenersAreWiredIntoTheLlmClient() throws Exception {
    String reason = "blocked-" + UUID.randomUUID();
    RecordingListener listener = new RecordingListener("l-" + UUID.randomUUID());
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("s")
            .withGuardrail(new BlockingGuardrail(reason))
            .withListener(listener)
            .withStateMachine(AgentExecutorTest.stateMachine())
            .build();
    AgentExecutionFunction fn =
        new AgentExecutionFunction(
            agent, ToolRegistry.empty(), llm(List.of(AgentExecutorTest.text("never reached"))));
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h = harness(fn)) {
      h.processElement(new StreamRecord<>(start("hi"), 1L));
      h.endInput();
      AgentEvent out = outputs(h).get(0);
      assertEquals(AgentEventType.FLOW_COMPLETED, out.getEventType());
      assertEquals(reason, out.getData("output"), "guardrail block replaces the model answer");
      assertTrue(
          listener.events().contains("start:" + agent.getAgentId()), listener.events().toString());
      assertTrue(listener.events().contains("block:" + reason), listener.events().toString());
    }
  }

  @Test
  void conversationAndLongTermStoresReceiveTheTurn() throws Exception {
    InMemoryConversationStore conversations = new InMemoryConversationStore();
    InMemoryLongTermStore longTerm = new InMemoryLongTermStore();
    longTerm.initialize(Map.of());
    RecordingListener listener = new RecordingListener("l-" + UUID.randomUUID());
    String question = "q-" + UUID.randomUUID();
    String answer = "a-" + UUID.randomUUID();
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("s")
            .withConversationStore(conversations)
            .withLongTermStore(longTerm)
            .withListener(listener)
            .withStateMachine(AgentExecutorTest.stateMachine())
            .build();
    AgentExecutionFunction fn =
        new AgentExecutionFunction(
            agent, ToolRegistry.empty(), llm(List.of(AgentExecutorTest.text(answer))));
    AgentEvent input = start(question);
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h = harness(fn)) {
      h.processElement(new StreamRecord<>(input, 1L));
      h.endInput();
      assertEquals(AgentEventType.FLOW_COMPLETED, outputs(h).get(0).getEventType());
    }

    List<ChatMessage> history = conversations.history(input.getFlowId());
    assertEquals(2, history.size(), history.toString());
    assertEquals(question, history.get(0).getContent());
    assertEquals(answer, history.get(1).getContent());
    assertEquals(java.util.Optional.of(input.getUserId()), conversations.userOf(input.getFlowId()));

    Map<String, ContextItem> facts = longTerm.loadFacts(input.getFlowId());
    assertEquals(1, facts.size());
    Map.Entry<String, ContextItem> fact = facts.entrySet().iterator().next();
    assertTrue(fact.getKey().startsWith(AgentExecutionFunction.TURN_FACT_PREFIX));
    assertTrue(fact.getValue().getContent().contains(answer));
    assertEquals(
        AgentEventType.FLOW_COMPLETED.name(), fact.getValue().getMetadata().get("event_type"));
    assertTrue(
        listener.events().contains("sync:" + input.getFlowId() + ":1"),
        listener.events().toString());
  }

  @Test
  void failedTurnCarriesCompensationActionsInCallOrder() throws Exception {
    RecordingTool charge = new RecordingTool("charge-" + UUID.randomUUID());
    RecordingTool ship = new RecordingTool("ship-" + UUID.randomUUID());
    RecordingTool audit = new RecordingTool("audit-" + UUID.randomUUID());
    List<ChatResponse> script = new ArrayList<>();
    script.add(AgentExecutorTest.toolCall(charge.id, Map.of("amount", 5)));
    script.add(AgentExecutorTest.toolCall(audit.id, Map.of()));
    script.add(AgentExecutorTest.toolCall(ship.id, Map.of("sku", "x")));
    script.add(null);
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("s")
            .withTools(charge.id, ship.id, audit.id)
            .withMaxIterations(4)
            .withCompensatingTool(charge.id, "refund")
            .withCompensatingTool(ship.id, "recall")
            .withStateMachine(AgentExecutorTest.stateMachine())
            .build();
    AgentExecutionFunction fn =
        new AgentExecutionFunction(agent, registry(charge, ship, audit), llm(script));
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h = harness(fn)) {
      h.processElement(new StreamRecord<>(start("buy"), 1L));
      h.endInput();
      AgentEvent failed = outputs(h).get(0);
      assertEquals(AgentEventType.FLOW_FAILED, failed.getEventType());
      assertEquals(AgentState.COMPENSATING.name(), failed.getMetadata("state"));
      assertNull(failed.getCompensationData());
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> actions =
          (List<Map<String, Object>>)
              failed.getMetadata(AgentExecutionFunction.COMPENSATION_ACTIONS_METADATA);
      assertNotNull(actions);
      assertEquals(2, actions.size(), "only tools with a compensating tool are listed: " + actions);
      assertEquals("refund", actions.get(0).get("tool_name"));
      assertEquals("recall", actions.get(1).get("tool_name"));
      @SuppressWarnings("unchecked")
      Map<String, Object> refundParams = (Map<String, Object>) actions.get(0).get("parameters");
      assertEquals(charge.id, refundParams.get("original_tool"));
      assertEquals("ok-" + charge.id + "-1", refundParams.get("original_result"));
      assertEquals(failed.getFlowId(), refundParams.get("flow_id"));
    }
  }

  @Test
  void failedTurnWithoutCompensationStaysAPlainFailure() throws Exception {
    RecordingTool charge = new RecordingTool("charge-" + UUID.randomUUID());
    List<ChatResponse> script = new ArrayList<>();
    script.add(AgentExecutorTest.toolCall(charge.id, Map.of()));
    script.add(null);
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("s")
            .withTools(charge.id)
            .withMaxIterations(2)
            .withStateMachine(AgentExecutorTest.stateMachine())
            .build();
    AgentExecutionFunction fn = new AgentExecutionFunction(agent, registry(charge), llm(script));
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h = harness(fn)) {
      h.processElement(new StreamRecord<>(start("buy"), 1L));
      h.endInput();
      AgentEvent failed = outputs(h).get(0);
      assertEquals(AgentEventType.FLOW_FAILED, failed.getEventType());
      assertEquals(AgentState.FAILED.name(), failed.getMetadata("state"));
      assertNull(failed.getMetadata(AgentExecutionFunction.COMPENSATION_ACTIONS_METADATA));
    }
  }
}
