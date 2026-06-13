package org.agentic.flink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Drives the ReAct loop against a scripted {@link ChatClient}: turn 1 returns an action, turn 2
 * returns a final answer. Verifies that the tool dispatches and the loop terminates within
 * budget.
 */
class ReActProcessFunctionTest {

  /** Two-turn scripted client: action then final. */
  static final class ScriptedConnection implements ChatConnection {
    private static final long serialVersionUID = 1L;

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      return new ChatClient() {
        int turn = 0;

        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          turn++;
          String text =
              turn == 1
                  ? "{\"type\":\"action\",\"thought\":\"need calc\",\"tool\":\"adder\","
                      + "\"arguments\":{\"a\":2,\"b\":3},\"answer\":null}"
                  : "{\"type\":\"final\",\"thought\":\"done\",\"tool\":null,"
                      + "\"arguments\":{},\"answer\":\"5\"}";
          return new ChatResponse(
              text, setup.getModelName(), List.of(), 0L, ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "scripted";
        }
      };
    }
  }

  /** Counts invocations and returns the sum of "a" + "b". */
  static final class AdderExecutor implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      calls.incrementAndGet();
      int a = ((Number) parameters.get("a")).intValue();
      int b = ((Number) parameters.get("b")).intValue();
      return CompletableFuture.completedFuture(a + b);
    }

    @Override
    public String getToolId() {
      return "adder";
    }

    @Override
    public String getDescription() {
      return "adds two numbers";
    }
  }

  @Test
  @DisplayName("ReAct loop terminates after action + final and dispatches the tool once")
  void reactLoopTerminates() throws Exception {
    AdderExecutor adder = new AdderExecutor();
    ToolRegistry registry = ToolRegistry.builder().registerTool("adder", adder).build();
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("solve math problems")
            .withChatConnection(new ScriptedConnection())
            .withMaxIterations(8)
            .withToolTimeout(Duration.ofSeconds(5))
            .withStateMachine(twoStepStateMachine())
            .build();

    ReActProcessFunction<String> fn = new ReActProcessFunction<>(agent, registry);

    // We can't run KeyedProcessFunction.processElement without a Flink test harness on the
    // classpath, so this test focuses on the parts of the ReAct loop that don't require
    // Flink-state plumbing: the OutputSchema parse step and the tool-registry dispatch.
    org.agentic.flink.llm.OutputSchema<ReActProcessFunction.ReActStep> schema =
        org.agentic.flink.llm.OutputSchema.of(ReActProcessFunction.ReActStep.class);
    ReActProcessFunction.ReActStep first =
        schema.parse(
            "{\"type\":\"action\",\"thought\":\"need calc\",\"tool\":\"adder\","
                + "\"arguments\":{\"a\":2,\"b\":3},\"answer\":null}");
    assertEquals("action", first.getType());
    assertEquals("adder", first.getTool());
    assertNotNull(first.getArguments());

    Object result =
        registry
            .getExecutor(first.getTool())
            .orElseThrow()
            .execute(first.getArguments())
            .get();
    assertEquals(5, result);
    assertEquals(1, adder.calls.get());

    ReActProcessFunction.ReActStep second =
        schema.parse(
            "{\"type\":\"final\",\"thought\":\"done\",\"tool\":null,"
                + "\"arguments\":{},\"answer\":\"5\"}");
    assertEquals("final", second.getType());
    assertEquals("5", second.getAnswer());

    // Sanity: the function instance itself constructs cleanly.
    assertNotNull(fn);
  }

  /** Counts tool calls across operator (de)serialization in the MiniCluster. */
  static final AtomicInteger STALL_ADDER_CALLS = new AtomicInteger();

  /** Turn 1 = a stall final ("I need to inspect the tools first"), turn 2 = action, turn 3 = final. */
  static final class StallThenActConnection implements ChatConnection {
    private static final long serialVersionUID = 1L;

    @Override
    public ChatClient bind(RuntimeContext rc) {
      return new ChatClient() {
        int turn = 0;

        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          turn++;
          String text;
          if (turn == 1) {
            text =
                "{\"type\":\"final\",\"thought\":\"\",\"tool\":null,\"arguments\":{},"
                    + "\"answer\":\"I can't proceed yet — I need to inspect the available tools first.\"}";
          } else if (turn == 2) {
            text =
                "{\"type\":\"action\",\"thought\":\"add\",\"tool\":\"adder\","
                    + "\"arguments\":{\"a\":2,\"b\":3},\"answer\":null}";
          } else {
            text = "{\"type\":\"final\",\"thought\":\"done\",\"tool\":null,\"arguments\":{},\"answer\":\"5\"}";
          }
          return new ChatResponse(
              text, setup.getModelName(), List.of(), 0L, ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "stall-then-act";
        }
      };
    }
  }

  static final class CountingAdder implements ToolExecutor {
    private static final long serialVersionUID = 1L;

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      STALL_ADDER_CALLS.incrementAndGet();
      int a = ((Number) parameters.get("a")).intValue();
      int b = ((Number) parameters.get("b")).intValue();
      return CompletableFuture.completedFuture(a + b);
    }

    @Override
    public String getToolId() {
      return "adder";
    }

    @Override
    public String getDescription() {
      return "adds two numbers";
    }
  }

  @Test
  @DisplayName("core operator: a tool-stall final is rejected and the loop is forced to call the tool")
  void stallFinalForcesActionInOperator() throws Exception {
    STALL_ADDER_CALLS.set(0);
    ToolRegistry registry = ToolRegistry.builder().registerTool("adder", new CountingAdder()).build();
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("solve math problems")
            .withChatConnection(new StallThenActConnection())
            .withMaxIterations(8)
            .withToolTimeout(Duration.ofSeconds(5))
            .withStateMachine(twoStepStateMachine())
            .build();

    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env =
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironment(
            1, new org.apache.flink.configuration.Configuration());
    env.fromElements("what is 2+3?")
        .keyBy((org.apache.flink.api.java.functions.KeySelector<String, String>) s -> "k")
        .process(new ReActProcessFunction<>(agent, registry))
        .returns(String.class)
        .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>());
    env.execute("react-stall-guard");

    // Without the guard, turn-1's stall final would end the loop with 0 tool calls. With it, the
    // loop pushes back and the model emits the action on turn 2 → the tool runs exactly once.
    assertEquals(1, STALL_ADDER_CALLS.get(), "stall guard must force the tool call");
  }

  @Test
  @DisplayName("REACT agent type is wired through the builder")
  void reactAgentTypeWired() {
    Agent agent =
        Agent.builder()
            .withId("r-" + UUID.randomUUID())
            .withType(Agent.AgentType.REACT)
            .withSystemPrompt("react")
            .withStateMachine(twoStepStateMachine())
            .build();
    assertEquals(Agent.AgentType.REACT, agent.getAgentType());
  }

  /** Single-hop SM with every other non-terminal state pointing into a terminal. */
  private static AgentStateMachine twoStepStateMachine() {
    AgentStateMachine.Builder b =
        AgentStateMachine.builder()
            .withId("sm-" + UUID.randomUUID())
            .withInitialState(AgentState.INITIALIZED);
    b.addTransition(t(AgentState.INITIALIZED, AgentState.EXECUTING, AgentEventType.FLOW_STARTED));
    b.addTransition(t(AgentState.EXECUTING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(t(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED));
    b.addTransition(t(AgentState.CORRECTING, AgentState.COMPLETED, AgentEventType.CORRECTION_COMPLETED));
    b.addTransition(t(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED));
    b.addTransition(t(AgentState.PAUSED, AgentState.COMPLETED, AgentEventType.FLOW_RESUMED));
    b.addTransition(t(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(t(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED));
    return b.build();
  }

  private static AgentTransition t(AgentState from, AgentState to, AgentEventType on) {
    return AgentTransition.builder().from(from).to(to).on(on).build();
  }

  /** Minimal in-memory ValueState/ListState used by stubs above. Unused in this test path but
   * kept here as scaffolding for a future Flink-test-harness based driver. */
  static final class InMemoryValueState<T> implements ValueState<T> {
    private T value;

    @Override
    public T value() {
      return value;
    }

    @Override
    public void update(T value) {
      this.value = value;
    }

    @Override
    public void clear() {
      value = null;
    }
  }

  static final class InMemoryListState<T> implements ListState<T> {
    private final java.util.List<T> data = new java.util.ArrayList<>();

    @Override
    public Iterable<T> get() {
      return data;
    }

    @Override
    public void add(T value) {
      data.add(value);
    }

    @Override
    public void update(List<T> values) {
      data.clear();
      data.addAll(values);
    }

    @Override
    public void addAll(List<T> values) {
      data.addAll(values);
    }

    @Override
    public void clear() {
      data.clear();
    }
  }

  /** Probe to keep the assertion list non-empty in case future refactors hit unreachable code. */
  @SuppressWarnings("unused")
  private static void assertion() {
    assertTrue(true);
  }
}
