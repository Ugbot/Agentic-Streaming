package org.agentic.flink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.listener.AgentEventListener;
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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Drives the ReAct loop against a scripted {@link ChatClient}: turn 1 returns an action, turn 2
 * returns a final answer. Verifies that the tool dispatches and the loop terminates within budget.
 */
class ReActProcessFunctionTest {

  /** Two-turn scripted client: an {@code adder} action with the given operands, then a final. */
  static final class ScriptedConnection implements ChatConnection {
    private static final long serialVersionUID = 1L;

    final AtomicInteger chatCalls = new AtomicInteger();
    private final int a;
    private final int b;

    ScriptedConnection(int a, int b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      return new ChatClient() {
        int turn = 0;

        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          turn++;
          chatCalls.incrementAndGet();
          String text =
              turn == 1
                  ? "{\"type\":\"action\",\"thought\":\"need calc\",\"tool\":\"adder\","
                      + "\"arguments\":{\"a\":"
                      + a
                      + ",\"b\":"
                      + b
                      + "},\"answer\":null}"
                  : "{\"type\":\"final\",\"thought\":\"done\",\"tool\":null,"
                      + "\"arguments\":{},\"answer\":\""
                      + (a + b)
                      + "\"}";
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

  /** Records every tool lifecycle event the operator emits. */
  static final class RecordingListener implements AgentEventListener {
    private static final long serialVersionUID = 1L;
    final List<String> events = new ArrayList<>();

    @Override
    public void onAgentStart(String agentId) {
      events.add("start");
    }

    @Override
    public void onToolCallStart(String agentId, String toolName, String toolCallId) {
      events.add("tool-start:" + toolName + ":" + toolCallId);
    }

    @Override
    public void onToolCallEnd(
        String agentId, String toolName, String toolCallId, boolean success, long durationMs) {
      events.add("tool-end:" + toolName + ":" + toolCallId + ":" + success);
    }
  }

  /** Counts invocations, remembers the last arguments and returns the sum of "a" + "b". */
  static final class AdderExecutor implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    final AtomicInteger calls = new AtomicInteger();
    volatile Map<String, Object> lastParameters;

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      calls.incrementAndGet();
      lastParameters = parameters;
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

  private static List<String> outputs(
      KeyedOneInputStreamOperatorTestHarness<String, String, String> harness) {
    List<String> out = new ArrayList<>();
    for (Object o : harness.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((String) r.getValue());
      }
    }
    return out;
  }

  @Test
  @DisplayName("ReAct loop terminates after action + final and dispatches the tool once")
  void reactLoopTerminates() throws Exception {
    int a = ThreadLocalRandom.current().nextInt(1, 1_000);
    int b = ThreadLocalRandom.current().nextInt(1, 1_000);
    AdderExecutor adder = new AdderExecutor();
    ScriptedConnection connection = new ScriptedConnection(a, b);
    RecordingListener listener = new RecordingListener();
    ToolRegistry registry = ToolRegistry.builder().registerTool("adder", adder).build();
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("solve math problems")
            .withChatConnection(connection)
            .withListener(listener)
            .withMaxIterations(8)
            .withToolTimeout(Duration.ofSeconds(5))
            .withStateMachine(twoStepStateMachine())
            .build();

    String question = "what is " + a + "+" + b + "?";
    String key = "k-" + UUID.randomUUID();
    try (KeyedOneInputStreamOperatorTestHarness<String, String, String> harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new ReActProcessFunction<String>(agent, registry)),
            s -> key,
            Types.STRING)) {
      harness.open();
      harness.processElement(new StreamRecord<>(question));

      assertEquals(List.of(question), outputs(harness), "the event passes through once");
      assertEquals(2, connection.chatCalls.get(), "action turn, then final turn");
      assertEquals(1, adder.calls.get(), "the tool is dispatched exactly once");
      assertEquals(a, ((Number) adder.lastParameters.get("a")).intValue());
      assertEquals(b, ((Number) adder.lastParameters.get("b")).intValue());
      assertEquals(
          List.of("start", "tool-start:adder:react-1", "tool-end:adder:react-1:true"),
          listener.events);

      // The key is finished: a second element on it passes through without another chat round.
      harness.processElement(new StreamRecord<>(question));
      assertEquals(List.of(question, question), outputs(harness));
      assertEquals(2, connection.chatCalls.get(), "finished keys do not re-enter the loop");
      assertEquals(1, adder.calls.get());
    }
  }

  /** Counts tool calls across operator (de)serialization in the MiniCluster. */
  static final AtomicInteger STALL_ADDER_CALLS = new AtomicInteger();

  /**
   * Turn 1 = a stall final ("I need to inspect the tools first"), turn 2 = action, turn 3 = final.
   */
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
            text =
                "{\"type\":\"final\",\"thought\":\"done\",\"tool\":null,\"arguments\":{},\"answer\":\"5\"}";
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
  @DisplayName(
      "core operator: a tool-stall final is rejected and the loop is forced to call the tool")
  void stallFinalForcesActionInOperator() throws Exception {
    STALL_ADDER_CALLS.set(0);
    ToolRegistry registry =
        ToolRegistry.builder().registerTool("adder", new CountingAdder()).build();
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
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .createLocalEnvironment(1, new org.apache.flink.configuration.Configuration());
    env.fromElements("what is 2+3?")
        .keyBy((org.apache.flink.api.java.functions.KeySelector<String, String>) s -> "k")
        .process(new ReActProcessFunction<>(agent, registry))
        .returns(String.class)
        .addSink(new org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink<>());
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
    b.addTransition(
        t(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED));
    b.addTransition(
        t(AgentState.CORRECTING, AgentState.COMPLETED, AgentEventType.CORRECTION_COMPLETED));
    b.addTransition(
        t(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED));
    b.addTransition(t(AgentState.PAUSED, AgentState.COMPLETED, AgentEventType.FLOW_RESUMED));
    b.addTransition(t(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(
        t(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED));
    return b.build();
  }

  private static AgentTransition t(AgentState from, AgentState to, AgentEventType on) {
    return AgentTransition.builder().from(from).to(to).on(on).build();
  }
}
