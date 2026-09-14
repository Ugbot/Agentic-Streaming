package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.dsl.SupervisorChain;
import org.agentic.flink.execution.AgentExecutorTest;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

/** F1/F2 at the CEP layer: the match handler only dispatches (with keyed dedup) and the supervisor tier cancels on timeout. */
@SuppressWarnings("deprecation")
class LegacyCepFunctionsTest {

  /** Runs a {@link PatternProcessFunction} inside a keyed operator so it gets real keyed state. */
  static final class Adapter extends KeyedProcessFunction<String, AgentEvent, AgentEvent> {
    private static final long serialVersionUID = 1L;
    final AgentExecutionFunction delegate;
    final List<AgentEvent> sideOutputs = new ArrayList<>();

    Adapter(AgentExecutionFunction delegate) {
      this.delegate = delegate;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      delegate.setRuntimeContext(getRuntimeContext());
      delegate.open(openContext);
    }

    @Override
    public void processElement(AgentEvent value, Context ctx, Collector<AgentEvent> out) throws Exception {
      PatternProcessFunction.Context cepCtx = new PatternProcessFunction.Context() {
        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
          ctx.output(outputTag, value);
        }

        @Override
        public long timestamp() {
          return ctx.timestamp() == null ? 0L : ctx.timestamp();
        }

        @Override
        public long currentProcessingTime() {
          return ctx.timerService().currentProcessingTime();
        }
      };
      delegate.processMatch(Map.of("initial", List.of(value)), cepCtx, out);
    }
  }

  private static AgentEvent start(String flow, String turnId) {
    AgentEvent e = new AgentEvent(flow, "u", "a", AgentEventType.FLOW_STARTED);
    e.putData("user_message", "hi " + UUID.randomUUID());
    e.putData("turn_id", turnId);
    return e;
  }

  private static List<AgentEvent> outputs(KeyedOneInputStreamOperatorTestHarness<String, AgentEvent, AgentEvent> h) {
    List<AgentEvent> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((AgentEvent) r.getValue());
      }
    }
    return out;
  }

  @Test
  void matchHandlerDispatchesOncePerTurnAndDropsRedeliveries() throws Exception {
    Agent agent = Agent.builder().withId("a-" + UUID.randomUUID()).withSystemPrompt("s")
        .withStateMachine(AgentExecutorTest.stateMachine()).build();
    AgentExecutionFunction fn = new AgentExecutionFunction(agent, ToolRegistry.empty(), Duration.ofMinutes(5));
    assertEquals(Duration.ofMinutes(5), fn.getDedupTtl());
    assertThrows(IllegalArgumentException.class,
        () -> new AgentExecutionFunction(agent, ToolRegistry.empty(), Duration.ZERO));

    try (KeyedOneInputStreamOperatorTestHarness<String, AgentEvent, AgentEvent> h =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new Adapter(fn)), AgentEvent::getFlowId, Types.STRING)) {
      h.open();
      String flowA = "flow-" + UUID.randomUUID();
      String flowB = "flow-" + UUID.randomUUID();
      String turn = "turn-" + UUID.randomUUID();
      int redeliveries = ThreadLocalRandom.current().nextInt(2, 6);
      for (int i = 0; i < redeliveries; i++) {
        h.processElement(new StreamRecord<>(start(flowA, turn), i));
      }
      h.processElement(new StreamRecord<>(start(flowB, turn), 99L));

      List<AgentEvent> out = outputs(h);
      assertEquals(2, out.size(), "one dispatch per (key, turn_id)");
      for (AgentEvent e : out) {
        assertEquals(AgentEventType.FLOW_STARTED, e.getEventType());
        assertEquals(agent.getAgentId(), e.getAgentId());
        assertEquals(turn, e.getData(AgentExecutionFunction.REQUEST_TURN_ID));
        assertEquals(AgentState.EXECUTING.name(), e.getMetadata("state"));
      }
    }
  }

  @Test
  void dispatchStateExpiresAfterTtl() throws Exception {
    Agent agent = Agent.builder().withId("a-" + UUID.randomUUID()).withSystemPrompt("s")
        .withStateMachine(AgentExecutorTest.stateMachine()).build();
    long ttl = ThreadLocalRandom.current().nextLong(1_000, 10_000);
    AgentExecutionFunction fn = new AgentExecutionFunction(agent, ToolRegistry.empty(), Duration.ofMillis(ttl));
    try (KeyedOneInputStreamOperatorTestHarness<String, AgentEvent, AgentEvent> h =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new Adapter(fn)), AgentEvent::getFlowId, Types.STRING)) {
      h.open();
      String flow = "flow-" + UUID.randomUUID();
      String turn = "turn-" + UUID.randomUUID();
      h.setStateTtlProcessingTime(1L);
      h.processElement(new StreamRecord<>(start(flow, turn), 0L));
      h.setStateTtlProcessingTime(ttl / 2);
      h.processElement(new StreamRecord<>(start(flow, turn), 1L));
      assertEquals(1, outputs(h).size(), "still deduplicated inside the ttl");
      h.setStateTtlProcessingTime(ttl + 2);
      h.processElement(new StreamRecord<>(start(flow, turn), 2L));
      assertEquals(2, outputs(h).size(), "dispatch state forgotten after the ttl");
    }
  }

  private static final class CollectingContext implements PatternProcessFunction.Context {
    final List<AgentEvent> timeouts = new ArrayList<>();
    final List<AgentEvent> failures = new ArrayList<>();

    @Override
    @SuppressWarnings("unchecked")
    public <X> void output(OutputTag<X> outputTag, X value) {
      if (outputTag.equals(AgentJobGenerator.TIMEOUT_TAG)) {
        timeouts.add((AgentEvent) value);
      } else if (outputTag.equals(AgentJobGenerator.VALIDATION_FAILURES_TAG)) {
        failures.add((AgentEvent) value);
      }
    }

    @Override
    public long timestamp() {
      return 0L;
    }

    @Override
    public long currentProcessingTime() {
      return System.currentTimeMillis();
    }
  }

  @Test
  void supervisorTierTimeoutCancelsExecutionAndEmitsTimeoutSideOutput() throws Exception {
    long timeoutMs = ThreadLocalRandom.current().nextLong(100, 400);
    AgentExecutorTest.CountingTool slow = new AgentExecutorTest.CountingTool("slow");
    slow.block = new CountDownLatch(1);
    Agent agent = Agent.builder().withId("sup-" + UUID.randomUUID()).withSystemPrompt("s")
        .withTimeout(Duration.ofMillis(timeoutMs)).withStateMachine(AgentExecutorTest.stateMachine()).build();
    SupervisorChain chain = SupervisorChain.builder().withId("c").addSimpleTier("t0", agent).build();
    LLMClient llm = LLMClient.builder().withModel("m").build(new AgentExecutorTest.ScriptedConnection(
        List.of(AgentExecutorTest.toolCall("slow", Map.of()), AgentExecutorTest.text("never"))));
    SupervisorTierFunction fn = new SupervisorTierFunction(
        chain.getTiers().get(0), chain, ToolRegistry.builder().registerTool("slow", slow).build(), null, llm);
    assertEquals(Duration.ofMillis(timeoutMs), fn.getTimeout());

    CollectingContext ctx = new CollectingContext();
    List<AgentEvent> out = new ArrayList<>();
    long started = System.nanoTime();
    fn.processMatch(Map.of("initial", List.of(start("flow-" + UUID.randomUUID(), "t"))), ctx,
        new Collector<>() {
          @Override
          public void collect(AgentEvent record) {
            out.add(record);
          }

          @Override
          public void close() {}
        });
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertTrue(out.isEmpty());
    assertEquals(1, ctx.timeouts.size());
    AgentEvent timeout = ctx.timeouts.get(0);
    assertEquals(AgentEventType.FLOW_FAILED, timeout.getEventType());
    assertEquals(AgentResultRouter.FAILURE_KIND_TIMEOUT, timeout.getData("failure_kind"));
    assertTrue(String.valueOf(timeout.getData("error")).contains("cancelled"));
    assertTrue(elapsedMs < timeoutMs + 5_000, "returned promptly: " + elapsedMs);
    assertEquals(1, slow.executions.get());
    slow.block.countDown();
    fn.close();
  }

  @Test
  void supervisorTierRejectsNonPositiveTimeout() {
    Agent zero = Agent.builder().withId("sup").withSystemPrompt("s").withTimeout(Duration.ZERO)
        .withStateMachine(AgentExecutorTest.stateMachine()).build();
    SupervisorChain chain = SupervisorChain.builder().withId("c").addSimpleTier("t0", zero).build();
    assertThrows(IllegalArgumentException.class, () -> new SupervisorTierFunction(
        chain.getTiers().get(0), chain, ToolRegistry.empty()));

    Duration agentTimeout = Duration.ofSeconds(ThreadLocalRandom.current().nextInt(1, 120));
    Agent timed = Agent.builder().withId("sup2").withSystemPrompt("s").withTimeout(agentTimeout)
        .withStateMachine(AgentExecutorTest.stateMachine()).build();
    SupervisorChain chain2 = SupervisorChain.builder().withId("c2").addSimpleTier("t0", timed).build();
    assertEquals(agentTimeout, new SupervisorTierFunction(
        chain2.getTiers().get(0), chain2, ToolRegistry.empty(), Duration.ofHours(1)).getTimeout(),
        "agent timeout wins over the tier default");
  }
}
