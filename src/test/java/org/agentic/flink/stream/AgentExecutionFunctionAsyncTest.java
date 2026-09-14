package org.agentic.flink.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.execution.AgentExecutorTest;
import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.job.AgentJobGenerator;
import org.agentic.flink.job.AgentResultRouter;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.tool.ToolRegistry;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/** F1: the legacy single-agent execution runs through Flink async I/O with a cancelling timeout. */
@SuppressWarnings("deprecation")
class AgentExecutionFunctionAsyncTest {

  private static AgentEvent start() {
    AgentEvent e = new AgentEvent("flow-" + UUID.randomUUID(), "u", "a", AgentEventType.FLOW_STARTED);
    e.putData("user_message", "hi " + UUID.randomUUID());
    e.putData("turn_id", "t-" + UUID.randomUUID());
    return e;
  }

  private static OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> harness(
      AgentExecutionFunction fn) throws Exception {
    AsyncWaitOperatorFactory<AgentEvent, AgentEvent> factory = new AsyncWaitOperatorFactory<>(
        fn, fn.getTimeout().toMillis(), 4, AsyncDataStream.OutputMode.UNORDERED);
    OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h =
        new OneInputStreamOperatorTestHarness<>(
            factory,
            TypeInformation.of(AgentEvent.class)
                .createSerializer(new ExecutionConfig().getSerializerConfig()));
    h.open();
    return h;
  }

  private static List<AgentEvent> outputs(OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h) {
    List<AgentEvent> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((AgentEvent) r.getValue());
      }
    }
    return out;
  }

  private static AgentExecutionFunction function(
      Agent agent, AgentExecutorTest.CountingTool tool, List<org.agentic.flink.llm.ChatResponse> script) {
    ToolRegistry.ToolRegistryBuilder reg = ToolRegistry.builder();
    if (tool != null) {
      reg.registerTool(tool.id, tool);
    }
    LLMClient llm = LLMClient.builder().withModel("m")
        .build(new AgentExecutorTest.ScriptedConnection(script));
    return new AgentExecutionFunction(agent, reg.build(), llm);
  }

  @Test
  void rejectsNonPositiveTimeout() {
    Agent agent = Agent.builder().withId("a").withSystemPrompt("s").withTimeout(Duration.ZERO)
        .withStateMachine(AgentExecutorTest.stateMachine()).build();
    assertThrows(IllegalArgumentException.class,
        () -> new AgentExecutionFunction(agent, ToolRegistry.empty()));
  }

  @Test
  void completedTurnIsEmittedAsFlowCompleted() throws Exception {
    String answer = "ans-" + UUID.randomUUID();
    Agent agent = Agent.builder().withId("a-" + UUID.randomUUID()).withSystemPrompt("s")
        .withTimeout(Duration.ofSeconds(5)).withStateMachine(AgentExecutorTest.stateMachine()).build();
    AgentExecutionFunction fn = function(agent, null, List.of(AgentExecutorTest.text(answer)));
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h = harness(fn)) {
      h.processElement(new StreamRecord<>(start(), 1L));
      h.endInput();
      List<AgentEvent> out = outputs(h);
      assertEquals(1, out.size());
      assertEquals(AgentEventType.FLOW_COMPLETED, out.get(0).getEventType());
      assertEquals(answer, out.get(0).getData("output"));
    }
  }

  @Test
  void timeoutCancelsTheUnderlyingExecutionAndEmitsTimeoutFailure() throws Exception {
    long timeoutMs = ThreadLocalRandom.current().nextLong(200, 800);
    AgentExecutorTest.CountingTool slow = new AgentExecutorTest.CountingTool("slow");
    slow.block = new CountDownLatch(1);
    Agent agent = Agent.builder().withId("a-" + UUID.randomUUID()).withSystemPrompt("s")
        .withTimeout(Duration.ofMillis(timeoutMs)).withStateMachine(AgentExecutorTest.stateMachine()).build();
    AgentExecutionFunction fn = function(agent, slow,
        List.of(AgentExecutorTest.toolCall("slow", Map.of()), AgentExecutorTest.text("never")));
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h = harness(fn)) {
      h.setProcessingTime(0L);
      h.processElement(new StreamRecord<>(start(), 1L));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (slow.executions.get() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(5);
      }
      assertEquals(1, slow.executions.get(), "tool was started before the timeout");

      h.setProcessingTime(timeoutMs + 1);
      h.endInput();

      List<AgentEvent> out = outputs(h);
      assertEquals(1, out.size());
      AgentEvent failed = out.get(0);
      assertEquals(AgentEventType.FLOW_FAILED, failed.getEventType());
      assertEquals(AgentResultRouter.FAILURE_KIND_TIMEOUT, failed.getData(AgentResultRouter.FAILURE_KIND));
      assertEquals(AgentState.FAILED.name(), failed.getMetadata("state"));
      assertNotNull(failed.getErrorMessage());
      assertTrue(failed.getErrorMessage().contains(String.valueOf(timeoutMs)));

      slow.block.countDown();
      Thread.sleep(50);
      assertEquals(1, outputs(h).size(), "no late result after cancellation");
    }
  }

  @Test
  void routerSendsTimeoutsToTimeoutTagAndOtherFailuresToValidationTag() throws Exception {
    try (OneInputStreamOperatorTestHarness<AgentEvent, AgentEvent> h =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new AgentResultRouter()))) {
      h.open();
      AgentEvent ok = start().withEventType(AgentEventType.FLOW_COMPLETED);
      AgentEvent timeout = start().withEventType(AgentEventType.FLOW_FAILED);
      timeout.putData(AgentResultRouter.FAILURE_KIND, AgentResultRouter.FAILURE_KIND_TIMEOUT);
      AgentEvent failed = start().withEventType(AgentEventType.FLOW_FAILED);
      failed.putData(AgentResultRouter.FAILURE_KIND, AgentResultRouter.FAILURE_KIND_EXECUTION);
      AgentEvent compensating = start().withEventType(AgentEventType.FLOW_FAILED);
      compensating.putMetadata("state", AgentState.COMPENSATING.name());
      AgentEvent compensationRequested = start().withEventType(AgentEventType.COMPENSATION_REQUESTED);

      for (AgentEvent e : List.of(ok, timeout, failed, compensating, compensationRequested)) {
        h.processElement(new StreamRecord<>(e, 1L));
      }

      assertEquals(1, outputs(h).size());
      assertEquals(AgentEventType.FLOW_COMPLETED, outputs(h).get(0).getEventType());
      ConcurrentLinkedQueue<StreamRecord<AgentEvent>> timeouts = h.getSideOutput(AgentJobGenerator.TIMEOUT_TAG);
      ConcurrentLinkedQueue<StreamRecord<AgentEvent>> failures = h.getSideOutput(AgentJobGenerator.VALIDATION_FAILURES_TAG);
      ConcurrentLinkedQueue<StreamRecord<AgentEvent>> comps = h.getSideOutput(AgentJobGenerator.COMPENSATION_TAG);
      assertEquals(1, timeouts.size());
      assertEquals(1, failures.size());
      assertEquals(2, comps.size());
    }
  }
}
