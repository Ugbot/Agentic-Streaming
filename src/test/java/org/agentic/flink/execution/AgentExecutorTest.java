package org.agentic.flink.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.dsl.Agent;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.statemachine.AgentState;
import org.agentic.flink.statemachine.AgentStateMachine;
import org.agentic.flink.statemachine.AgentTransition;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.ChatToolCall;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.Test;

/**
 * Legacy {@link AgentExecutor}: turn deduplication, per-(turn, call) tool result reuse, retry
 * backoff and error context, cancellation.
 */
@SuppressWarnings("deprecation")
public class AgentExecutorTest {

  /** Scripted chat: each call pops the next response; a {@code null} entry throws. */
  public static final class ScriptedConnection implements ChatConnection {
    private static final long serialVersionUID = 1L;
    final List<ChatResponse> script;
    final AtomicInteger calls = new AtomicInteger();

    public ScriptedConnection(List<ChatResponse> script) {
      this.script = script;
    }

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          int i = calls.getAndIncrement();
          ChatResponse r = script.get(Math.min(i, script.size() - 1));
          if (r == null) {
            throw new IllegalStateException("scripted llm failure #" + i);
          }
          return r;
        }

        @Override
        public String providerName() {
          return "scripted";
        }

        @Override
        public void close() {}
      };
    }
  }

  public static ChatResponse text(String t) {
    return new ChatResponse(t, "m", List.of(), 1L, ChatResponse.FinishReason.STOP);
  }

  public static ChatResponse toolCall(String tool, Map<String, Object> args) {
    return new ChatResponse("", "m", List.of(new ChatToolCall("id-" + tool, tool, args)), 1L,
        ChatResponse.FinishReason.TOOL_CALLS);
  }

  public static final class CountingTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    public final String id;
    public final AtomicInteger executions = new AtomicInteger();
    final List<Map<String, Object>> seen = new CopyOnWriteArrayList<>();
    public volatile CountDownLatch block;

    public CountingTool(String id) {
      this.id = id;
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      executions.incrementAndGet();
      seen.add(parameters);
      CountDownLatch b = block;
      if (b == null) {
        return CompletableFuture.completedFuture("ok-" + executions.get());
      }
      return CompletableFuture.supplyAsync(() -> {
        try {
          b.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CancellationException();
        }
        return "late";
      });
    }

    @Override
    public String getToolId() {
      return id;
    }

    @Override
    public String getDescription() {
      return "counting";
    }
  }

  public static AgentStateMachine stateMachine() {
    AgentStateMachine.Builder b = AgentStateMachine.builder()
        .withId("sm-" + UUID.randomUUID()).withInitialState(AgentState.INITIALIZED);
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

  static Agent agent(int maxIterations) {
    return Agent.builder().withId("a-" + UUID.randomUUID()).withSystemPrompt("sys")
        .withMaxIterations(maxIterations).withStateMachine(stateMachine()).build();
  }

  private static AgentEvent turn(String turnId) {
    AgentEvent e = new AgentEvent("flow-" + UUID.randomUUID(), "u", "a", AgentEventType.FLOW_STARTED);
    e.putData("user_message", "hello " + UUID.randomUUID());
    e.putData("turn_id", turnId);
    return e;
  }

  private static AgentExecutor executor(
      List<ChatResponse> script, CountingTool tool, TurnResultStore store, int maxIterations) {
    Agent agent = agent(maxIterations);
    LLMClient llm = LLMClient.builder().withModel("m").build(new ScriptedConnection(script));
    ToolRegistry.ToolRegistryBuilder reg = ToolRegistry.builder();
    if (tool != null) {
      reg.registerTool(tool.id, tool);
    }
    AgentExecutor ex = AgentExecutor.builder().withAgent(agent).withLlmClient(llm)
        .withToolRegistry(reg.build()).withTurnResultStore(store)
        .withRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(2)).build();
    ex.setSleeper(ms -> 0L);
    return ex;
  }

  @Test
  void turnIdOfPrefersDataThenMetadataThenCorrelationThenFlow() {
    AgentEvent e = new AgentEvent("f", "u", "a", AgentEventType.FLOW_STARTED);
    e.setIterationNumber(3);
    assertEquals("f/3", AgentExecutor.turnIdOf(e));
    e.setCorrelationId("corr");
    assertEquals("corr", AgentExecutor.turnIdOf(e));
    e.putMetadata("turn_id", "meta");
    assertEquals("meta", AgentExecutor.turnIdOf(e));
    e.putData("turn_id", "data");
    assertEquals("data", AgentExecutor.turnIdOf(e));
  }

  @Test
  void redeliveredTurnReturnsRecordedResultWithoutCallingLlmAgain() throws Exception {
    String answer = "answer-" + UUID.randomUUID();
    ScriptedConnection conn = new ScriptedConnection(List.of(text(answer)));
    Agent agent = agent(3);
    InMemoryTurnResultStore store = new InMemoryTurnResultStore();
    try (AgentExecutor ex = AgentExecutor.builder().withAgent(agent)
        .withLlmClient(LLMClient.builder().withModel("m").build(conn))
        .withToolRegistry(ToolRegistry.empty()).withTurnResultStore(store).build()) {
      String turnId = "t-" + UUID.randomUUID();
      ExecutionResult first = ex.execute(turn(turnId)).get(10, TimeUnit.SECONDS);
      assertTrue(first.isSuccess());
      assertEquals(answer, first.getOutput());
      assertEquals(1, conn.calls.get());

      ExecutionResult again = ex.execute(turn(turnId)).get(10, TimeUnit.SECONDS);
      assertSame(first, again);
      assertEquals(1, conn.calls.get(), "duplicate turn must not call the LLM");
      assertEquals(1, store.size());
    }
  }

  @Test
  void recordedToolResultIsReusedInsteadOfReexecutingTheTool() throws Exception {
    CountingTool tool = new CountingTool("charge");
    Map<String, Object> args = Map.of("amount", ThreadLocalRandom.current().nextInt(1, 1000));
    InMemoryTurnResultStore store = new InMemoryTurnResultStore();
    String turnId = "t-" + UUID.randomUUID();

    List<ChatResponse> failing = new ArrayList<>();
    failing.add(toolCall("charge", args));
    failing.add(null);
    failing.add(null);
    try (AgentExecutor ex = executor(failing, tool, store, 2)) {
      ExecutionResult r = ex.execute(turn(turnId)).get(10, TimeUnit.SECONDS);
      assertFalse(r.isSuccess());
      assertEquals(1, tool.executions.get());
      assertTrue(store.getToolResult(turnId, 0).isPresent(), "tool result recorded by index");
      assertTrue(store.getTurnResult(turnId).isPresent());
      assertNotNull(r.getErrorMessage());
      assertTrue(r.getErrorMessage().contains("scripted llm failure"), r.getErrorMessage());
    }

    InMemoryTurnResultStore fresh = new InMemoryTurnResultStore();
    fresh.putToolResult(turnId, 0, store.getToolResult(turnId, 0).get());
    try (AgentExecutor ex = executor(List.of(toolCall("charge", args), text("done")), tool, fresh, 3)) {
      ExecutionResult r = ex.execute(turn(turnId)).get(10, TimeUnit.SECONDS);
      assertTrue(r.isSuccess());
      assertEquals(1, tool.executions.get(), "side-effecting tool must not run twice for the same (turn, call)");
      assertEquals(1, r.getToolCalls().size());
    }
  }

  @Test
  void maxIterationsCarriesLastErrorAndBacksOffBetweenRetries() throws Exception {
    List<ChatResponse> script = new ArrayList<>();
    script.add(null);
    int iterations = ThreadLocalRandom.current().nextInt(2, 5);
    List<Long> sleeps = new CopyOnWriteArrayList<>();
    Agent agent = agent(iterations);
    try (AgentExecutor ex = AgentExecutor.builder().withAgent(agent)
        .withLlmClient(LLMClient.builder().withModel("m").build(new ScriptedConnection(script)))
        .withToolRegistry(ToolRegistry.empty()).withTurnResultStore(new InMemoryTurnResultStore())
        .withRetryBackoff(Duration.ofMillis(100), Duration.ofMillis(350)).build()) {
      ex.setSleeper(ms -> {
        sleeps.add(ms);
        return 0L;
      });
      ExecutionResult r = ex.execute(turn("t-" + UUID.randomUUID())).get(10, TimeUnit.SECONDS);
      assertFalse(r.isSuccess());
      assertTrue(r.getErrorMessage().contains("scripted llm failure"), r.getErrorMessage());
      assertEquals(iterations - 1, sleeps.size(), "one backoff per retried iteration");
      for (long s : sleeps) {
        assertTrue(s >= 0 && s <= 351, "backoff within cap: " + s);
      }
      long errorEvents = r.getEvents().stream()
          .filter(e -> e.getEventType() == AgentEventType.ERROR_OCCURRED).count();
      assertEquals(iterations, errorEvents);
    }
  }

  @Test
  void backoffIsExponentialWithFullJitterAndCapped() {
    long base = ThreadLocalRandom.current().nextLong(10, 100);
    long cap = base * 8;
    assertEquals(0L, AgentExecutor.backoffMillis(0, base, cap, 0.0));
    assertEquals(base + 1, AgentExecutor.backoffMillis(0, base, cap, 1.0));
    assertEquals(base * 4 + 1, AgentExecutor.backoffMillis(2, base, cap, 1.0));
    assertEquals(cap + 1, AgentExecutor.backoffMillis(20, base, cap, 1.0));
    assertEquals(cap + 1, AgentExecutor.backoffMillis(62, base, cap, 1.0), "shift overflow guarded");
    double u = ThreadLocalRandom.current().nextDouble();
    long v = AgentExecutor.backoffMillis(3, base, cap, u);
    assertTrue(v >= 0 && v <= Math.min(cap, base * 8) + 1);
  }

  @Test
  void cancellingTheFutureStopsTheLoopAndRecordsNothing() throws Exception {
    CountingTool tool = new CountingTool("slow");
    tool.block = new CountDownLatch(1);
    InMemoryTurnResultStore store = new InMemoryTurnResultStore();
    String turnId = "t-" + UUID.randomUUID();
    try (AgentExecutor ex = executor(List.of(toolCall("slow", Map.of()), text("never")), tool, store, 5)) {
      CompletableFuture<ExecutionResult> f = ex.execute(turn(turnId));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (tool.executions.get() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(5);
      }
      assertEquals(1, tool.executions.get());
      assertTrue(f.cancel(true));
      assertThrows(CancellationException.class, () -> f.get(1, TimeUnit.SECONDS));
      tool.block.countDown();
      Thread.sleep(50);
      assertTrue(store.getTurnResult(turnId).isEmpty(), "cancelled turn leaves no recorded result");
      assertTrue(store.getToolResult(turnId, 0).isEmpty(), "cancelled tool call is not recorded");
      assertEquals(0, store.size());
    }
  }

  @Test
  void storeIsBoundedAndExpires() throws Exception {
    int max = ThreadLocalRandom.current().nextInt(3, 10);
    InMemoryTurnResultStore store = new InMemoryTurnResultStore(max, Duration.ofMillis(30));
    for (int i = 0; i < max + 2; i++) {
      store.putTurnResult("t" + i, ExecutionResult.success("f", "a", "o" + i, List.of(), List.of()));
    }
    assertTrue(store.size() <= max);
    assertTrue(store.getTurnResult("t0").isEmpty(), "eldest evicted");
    assertTrue(store.getTurnResult("t" + (max + 1)).isPresent());
    Thread.sleep(60);
    assertTrue(store.getTurnResult("t" + (max + 1)).isEmpty(), "expired after ttl");
    assertThrows(IllegalArgumentException.class, () -> new InMemoryTurnResultStore(0, Duration.ofSeconds(1)));
    assertThrows(IllegalArgumentException.class, () -> new InMemoryTurnResultStore(5, Duration.ZERO));
  }
}
