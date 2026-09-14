package org.agentic.flink.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.AbstractToolExecutor;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** The agent's tool allowlist must be enforced at invocation time, not only advertised. */
class ToolAllowlistEnforcementTest {

  private final Random rnd = new Random();

  private static final class RecordingTool extends AbstractToolExecutor {
    private static final long serialVersionUID = 1L;
    final List<Map<String, Object>> calls = new CopyOnWriteArrayList<>();

    RecordingTool(String id) {
      super(id, "records invocations of " + id);
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      calls.add(parameters);
      return CompletableFuture.completedFuture("ran:" + getToolId());
    }
  }

  private static AgentStateMachine stateMachine() {
    AgentStateMachine.Builder b =
        AgentStateMachine.builder()
            .withId("sm-" + UUID.randomUUID())
            .withInitialState(AgentState.INITIALIZED);
    b.addTransition(t(AgentState.INITIALIZED, AgentState.EXECUTING, AgentEventType.FLOW_STARTED));
    b.addTransition(t(AgentState.EXECUTING, AgentState.VALIDATING, AgentEventType.VALIDATION_REQUESTED));
    b.addTransition(t(AgentState.VALIDATING, AgentState.CORRECTING, AgentEventType.VALIDATION_FAILED));
    b.addTransition(t(AgentState.VALIDATING, AgentState.COMPLETED, AgentEventType.VALIDATION_PASSED));
    b.addTransition(t(AgentState.CORRECTING, AgentState.EXECUTING, AgentEventType.CORRECTION_COMPLETED));
    b.addTransition(
        t(AgentState.EXECUTING, AgentState.SUPERVISOR_REVIEW, AgentEventType.SUPERVISOR_REVIEW_REQUESTED));
    b.addTransition(
        t(AgentState.SUPERVISOR_REVIEW, AgentState.COMPLETED, AgentEventType.SUPERVISOR_APPROVED));
    b.addTransition(t(AgentState.PAUSED, AgentState.EXECUTING, AgentEventType.FLOW_RESUMED));
    b.addTransition(t(AgentState.OFFLOADING, AgentState.COMPLETED, AgentEventType.FLOW_COMPLETED));
    b.addTransition(
        t(AgentState.COMPENSATING, AgentState.COMPENSATED, AgentEventType.COMPENSATION_COMPLETED));
    return b.build();
  }

  private static AgentTransition t(AgentState from, AgentState to, AgentEventType on) {
    return AgentTransition.builder().from(from).to(to).on(on).build();
  }

  private static ExecutionContext contextFor(Agent agent) {
    AgentEvent ev =
        new AgentEvent(
            "flow-" + UUID.randomUUID(),
            "user-" + UUID.randomUUID(),
            agent.getAgentId(),
            AgentEventType.FLOW_STARTED);
    return new ExecutionContext(ev, agent);
  }

  @RepeatedTest(10)
  void toolsOutsideTheAgentAllowlistAreRejectedBeforeExecution() {
    int n = 3 + rnd.nextInt(5);
    List<String> names = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      names.add("tool-" + UUID.randomUUID().toString().substring(0, 8));
    }
    Collections.shuffle(names, rnd);
    int allowedCount = 1 + rnd.nextInt(n - 1);
    List<String> allowed = names.subList(0, allowedCount);
    List<String> denied = names.subList(allowedCount, n);

    ToolRegistry.ToolRegistryBuilder rb = ToolRegistry.builder();
    Map<String, RecordingTool> tools = new java.util.HashMap<>();
    for (String name : names) {
      RecordingTool t = new RecordingTool(name);
      tools.put(name, t);
      rb.registerTool(name, t);
    }
    ToolRegistry registry = rb.build();

    Agent agent =
        Agent.builder()
            .withId("agent-" + UUID.randomUUID())
            .withSystemPrompt("test")
            .withTools(allowed.toArray(new String[0]))
            .withStateMachine(stateMachine())
            .build();
    ToolExecutionEngine engine = new ToolExecutionEngine(registry, null);
    ExecutionContext ctx = contextFor(agent);

    for (String name : allowed) {
      ToolCallResult r =
          engine.executeTool(new ToolCall("c-" + UUID.randomUUID(), name, Map.of("k", 1)), ctx).join();
      assertTrue(r.isSuccess(), name + " is allowed: " + r.getError());
      assertEquals(1, tools.get(name).calls.size());
    }
    for (String name : denied) {
      ToolCallResult r =
          engine.executeTool(new ToolCall("c-" + UUID.randomUUID(), name, Map.of("k", 2)), ctx).join();
      assertFalse(r.isSuccess(), name + " must be denied");
      assertEquals("Tool not permitted for this agent", r.getError());
      assertTrue(tools.get(name).calls.isEmpty(), "denied tool must never run");
    }
  }

  @Test
  void agentWithNoAllowlistCannotRunAnyRegisteredTool() {
    String name = "only-" + UUID.randomUUID().toString().substring(0, 8);
    RecordingTool t = new RecordingTool(name);
    ToolRegistry registry = ToolRegistry.builder().registerTool(name, t).build();
    Agent agent = Agent.builder()
            .withId("a-" + UUID.randomUUID())
            .withSystemPrompt("p")
            .withStateMachine(stateMachine())
            .build();
    ToolCallResult r =
        new ToolExecutionEngine(registry, null)
            .executeTool(new ToolCall("c", name, Map.of()), contextFor(agent))
            .join();
    assertFalse(r.isSuccess());
    assertTrue(t.calls.isEmpty());
  }
}
