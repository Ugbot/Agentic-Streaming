package org.agentic.flink.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutorTest;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.job.AgentJobGenerator;
import org.agentic.flink.job.AgentResultRouter;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/**
 * Saga rollback: a failed turn's compensation request is executed by {@link CompensationFunction}
 * against the registered compensating tools in reverse call order, and the result is routed by
 * {@link AgentResultRouter} the way {@link AgentJobGenerator} wires it.
 */
@SuppressWarnings("deprecation")
class CompensationFunctionTest {

  /** Appends its id to a shared per-test log so the execution order is observable. */
  static final class OrderedTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    static final Map<String, List<String>> LOGS = new ConcurrentHashMap<>();
    final String id;
    final String log;
    final boolean fail;

    OrderedTool(String log, String id, boolean fail) {
      this.log = log;
      this.id = id;
      this.fail = fail;
      LOGS.computeIfAbsent(log, k -> new CopyOnWriteArrayList<>());
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      LOGS.get(log).add(id + ":" + parameters.get("original_tool"));
      if (fail) {
        return CompletableFuture.failedFuture(new IllegalStateException("cannot undo " + id));
      }
      return CompletableFuture.completedFuture("undone-" + id);
    }

    @Override
    public String getToolId() {
      return id;
    }

    @Override
    public String getDescription() {
      return "ordered";
    }
  }

  private static <T extends org.apache.flink.api.common.functions.RichFunction>
      OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> asyncHarness(
          org.apache.flink.streaming.api.functions.async.AsyncFunction<AgentEvent, AgentEvent> fn,
          Duration timeout)
          throws Exception {
    AsyncWaitOperatorFactory<AgentEvent, AgentEvent> factory =
        new AsyncWaitOperatorFactory<>(
            fn, timeout.toMillis(), 4, AsyncDataStream.OutputMode.UNORDERED);
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

  private static AgentEvent start() {
    AgentEvent e =
        new AgentEvent("flow-" + UUID.randomUUID(), "u", "a", AgentEventType.FLOW_STARTED);
    e.putData("user_message", "buy " + UUID.randomUUID());
    e.putData("turn_id", "t-" + UUID.randomUUID());
    return e;
  }

  private static AgentEvent compensationRequest(List<Map<String, Object>> actions) {
    AgentEvent failed = start().withEventType(AgentEventType.FLOW_FAILED);
    failed.putMetadata("state", AgentState.COMPENSATING.name());
    failed.putMetadata(AgentExecutionFunction.COMPENSATION_ACTIONS_METADATA, actions);
    return failed;
  }

  private static Map<String, Object> action(String compensatingTool, String originalTool) {
    return Map.of(
        "action_name",
        "undo-" + originalTool,
        "tool_name",
        compensatingTool,
        "parameters",
        Map.of("original_tool", originalTool));
  }

  @Test
  void compensationsRunInReverseCallOrder() throws Exception {
    String log = "log-" + UUID.randomUUID();
    int steps = ThreadLocalRandom.current().nextInt(2, 6);
    ToolRegistry.ToolRegistryBuilder reg = ToolRegistry.builder();
    List<Map<String, Object>> actions = new ArrayList<>();
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < steps; i++) {
      String undo = "undo-" + i;
      reg.registerTool(undo, new OrderedTool(log, undo, false));
      actions.add(action(undo, "step-" + i));
      expected.add(0, undo + ":step-" + i);
    }
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h =
        asyncHarness(new CompensationFunction(reg.build()), Duration.ofSeconds(5))) {
      h.processElement(new StreamRecord<>(compensationRequest(actions), 1L));
      h.endInput();
      List<AgentEvent> out = outputs(h);
      assertEquals(1, out.size());
      assertEquals(AgentEventType.FLOW_COMPENSATED, out.get(0).getEventType());
      assertEquals(steps, out.get(0).getData("success_count"));
      assertEquals(0, out.get(0).getData("failure_count"));
    }
    assertEquals(expected, OrderedTool.LOGS.get(log), "LIFO rollback");
  }

  @Test
  void failingCompensationIsReportedButDoesNotStopTheOthers() throws Exception {
    String log = "log-" + UUID.randomUUID();
    ToolRegistry registry =
        ToolRegistry.builder()
            .registerTool("undo-a", new OrderedTool(log, "undo-a", false))
            .registerTool("undo-b", new OrderedTool(log, "undo-b", true))
            .build();
    List<Map<String, Object>> actions = List.of(action("undo-a", "a"), action("undo-b", "b"));
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h =
        asyncHarness(new CompensationFunction(registry), Duration.ofSeconds(5))) {
      h.processElement(new StreamRecord<>(compensationRequest(actions), 1L));
      h.endInput();
      AgentEvent out = outputs(h).get(0);
      assertEquals(AgentEventType.COMPENSATION_FAILED, out.getEventType());
      assertEquals(1, out.getData("success_count"));
      assertEquals(1, out.getData("failure_count"));
      assertNotNull(out.getErrorMessage());
      assertTrue(out.getErrorMessage().contains("undo-b"), out.getErrorMessage());
    }
    assertEquals(List.of("undo-b:b", "undo-a:a"), OrderedTool.LOGS.get(log));
  }

  @Test
  void eventsWithoutCompensationPassThroughUnchanged() throws Exception {
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h =
        asyncHarness(new CompensationFunction(ToolRegistry.empty()), Duration.ofSeconds(5))) {
      AgentEvent completed = start().withEventType(AgentEventType.FLOW_COMPLETED);
      AgentEvent plainFailure = start().withEventType(AgentEventType.FLOW_FAILED);
      h.processElement(new StreamRecord<>(completed, 1L));
      h.processElement(new StreamRecord<>(plainFailure, 2L));
      h.endInput();
      List<AgentEventType> types = outputs(h).stream().map(AgentEvent::getEventType).toList();
      assertEquals(2, types.size());
      assertTrue(types.contains(AgentEventType.FLOW_COMPLETED));
      assertTrue(types.contains(AgentEventType.FLOW_FAILED));
    }
  }

  @Test
  void executionFailureRollsBackThroughTheWiredOperators() throws Exception {
    String log = "log-" + UUID.randomUUID();
    OrderedTool charge = new OrderedTool(log, "charge", false);
    OrderedTool ship = new OrderedTool(log, "ship", false);
    OrderedTool refund = new OrderedTool(log, "refund", false);
    OrderedTool recall = new OrderedTool(log, "recall", false);
    ToolRegistry registry =
        ToolRegistry.builder()
            .registerTool(charge.id, charge)
            .registerTool(ship.id, ship)
            .registerTool(refund.id, refund)
            .registerTool(recall.id, recall)
            .build();
    List<ChatResponse> script = new ArrayList<>();
    script.add(AgentExecutorTest.toolCall(charge.id, Map.of()));
    script.add(AgentExecutorTest.toolCall(ship.id, Map.of()));
    script.add(null);
    Agent agent =
        Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("s")
            .withTools(charge.id, ship.id)
            .withMaxIterations(3)
            .withTimeout(Duration.ofSeconds(10))
            .withCompensatingTool(charge.id, refund.id)
            .withCompensatingTool(ship.id, recall.id)
            .withStateMachine(AgentExecutorTest.stateMachine())
            .build();
    LLMClient llm =
        LLMClient.builder().withModel("m").build(new AgentExecutorTest.ScriptedConnection(script));

    AgentEvent failed;
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> execute =
        asyncHarness(new AgentExecutionFunction(agent, registry, llm), agent.getTimeout())) {
      execute.processElement(new StreamRecord<>(start(), 1L));
      execute.endInput();
      failed = outputs(execute).get(0);
    }
    assertEquals(AgentEventType.FLOW_FAILED, failed.getEventType());
    assertEquals(List.of("charge:null", "ship:null"), OrderedTool.LOGS.get(log));

    AgentEvent compensated;
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> compensate =
        asyncHarness(new CompensationFunction(registry), agent.getTimeout())) {
      compensate.processElement(new StreamRecord<>(failed, 1L));
      compensate.endInput();
      compensated = outputs(compensate).get(0);
    }
    assertEquals(AgentEventType.FLOW_COMPENSATED, compensated.getEventType());
    assertEquals(2, compensated.getData("success_count"));
    assertEquals(
        List.of("charge:null", "ship:null", "recall:" + ship.id, "refund:" + charge.id),
        OrderedTool.LOGS.get(log),
        "compensations run after the forward steps, in reverse order");

    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> route =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new AgentResultRouter()))) {
      route.open();
      route.processElement(new StreamRecord<>(compensated, 1L));
      assertEquals(1, outputs(route).size(), "compensated flows continue on the main output");
      ConcurrentLinkedQueue<StreamRecord<AgentEvent>> comps =
          route.getSideOutput(AgentJobGenerator.COMPENSATION_TAG);
      assertTrue(comps == null || comps.isEmpty());
    }
  }
}
