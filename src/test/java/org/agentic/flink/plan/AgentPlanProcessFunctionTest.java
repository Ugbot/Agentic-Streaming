package org.agentic.flink.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.tools.ToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class AgentPlanProcessFunctionTest {

  /** Empty plan: every element should pass through unchanged. */
  @Test
  void passThroughWhenNoActions() throws Exception {
    AgentPlan plan =
        new AgentPlan(
            "agent-" + UUID.randomUUID(), null, null, null, null, null, null, null);
    AgentPlanProcessFunction<String> fn = new AgentPlanProcessFunction<>(plan);
    fn.open(new Configuration());

    CapturingCollector<Object> out = new CapturingCollector<>();
    String key = "k-" + UUID.randomUUID();
    String elem = "elem-" + UUID.randomUUID();
    fn.processElement(elem, new StubContext<>(key), out);

    assertEquals(List.of(elem), out.values);
  }

  /** Java tool wired by FQN: registry hands back an executor that actually runs. */
  @Test
  void javaToolRegisteredFromPlan() throws Exception {
    ToolSpec ts =
        new ToolSpec(
            ToolSpec.KIND_JAVA, "echo", "echo-desc", EchoTool.class.getName(), Map.of(), null,
            null);
    AgentPlan plan =
        new AgentPlan("a", null, null, null, List.of(ts), null, null, null);
    AgentPlanProcessFunction<String> fn = new AgentPlanProcessFunction<>(plan);
    fn.open(new Configuration());

    assertNotNull(fn.getToolRegistry());
    ToolExecutor exec =
        fn.getToolRegistry()
            .getExecutor("echo")
            .orElseThrow(() -> new AssertionError("echo not registered"));
    Object out = exec.execute(Map.of("v", "hello-" + UUID.randomUUID())).get();
    assertTrue(((String) out).startsWith("hello-"));
  }

  /** Java tool whose class is not a ToolExecutor must fail loudly. */
  @Test
  void javaToolFqnMustImplementToolExecutor() {
    ToolSpec ts =
        new ToolSpec(
            ToolSpec.KIND_JAVA, "not-tool", "desc", NotATool.class.getName(), Map.of(), null, null);
    AgentPlan plan = new AgentPlan("a", null, null, null, List.of(ts), null, null, null);
    AgentPlanProcessFunction<String> fn = new AgentPlanProcessFunction<>(plan);
    assertThrows(IllegalStateException.class, () -> fn.open(new Configuration()));
  }

  @Test
  void inferEventTypeFromTypeKey() {
    assertEquals(
        "ticket",
        AgentPlanProcessFunction.inferEventType(Map.of("type", "ticket", "body", "x")));
    assertEquals("String", AgentPlanProcessFunction.inferEventType("plain"));
    assertEquals("null", AgentPlanProcessFunction.inferEventType(null));
  }

  // -------- test fixtures --------

  public static final class EchoTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;

    public EchoTool() {}

    public void initialize(Map<String, String> cfg) {
      // no-op
    }

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      return CompletableFuture.completedFuture(String.valueOf(parameters.get("v")));
    }

    @Override
    public String getToolId() {
      return "echo";
    }

    @Override
    public String getDescription() {
      return "echo back parameter v";
    }
  }

  public static final class NotATool {
    public NotATool() {}
  }

  private static final class CapturingCollector<T> implements Collector<T> {
    final List<T> values = new ArrayList<>();

    @Override
    public void collect(T record) {
      values.add(record);
    }

    @Override
    public void close() {}
  }

  /** Minimal {@link KeyedProcessFunction.Context} stub for unit tests. */
  private static final class StubContext<K>
      extends KeyedProcessFunction<K, Object, Object>.Context {

    private final K key;
    private final TimerService timerService = new StubTimerService();

    StubContext(K key) {
      // Outer instance synthesized by Java's nested-class rules — see explanation below.
      new AgentPlanProcessFunction<K>(
              new AgentPlan("stub", null, null, null, null, null, null, null))
          .super();
      this.key = key;
    }

    @Override
    public Long timestamp() {
      return 0L;
    }

    @Override
    public TimerService timerService() {
      return timerService;
    }

    @Override
    public <X> void output(
        org.apache.flink.util.OutputTag<X> outputTag, X value) {
      // no-op
    }

    @Override
    public K getCurrentKey() {
      return key;
    }
  }

  private static final class StubTimerService implements TimerService {
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public long currentProcessingTime() {
      return calls.incrementAndGet();
    }

    @Override
    public long currentWatermark() {
      return 0;
    }

    @Override
    public void registerProcessingTimeTimer(long time) {}

    @Override
    public void registerEventTimeTimer(long time) {}

    @Override
    public void deleteProcessingTimeTimer(long time) {}

    @Override
    public void deleteEventTimeTimer(long time) {}
  }
}
