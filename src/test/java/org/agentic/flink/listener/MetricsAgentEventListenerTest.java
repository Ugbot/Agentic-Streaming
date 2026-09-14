package org.agentic.flink.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

/** F8: transient counters survive serialization and metrics register through the Flink metric group. */
class MetricsAgentEventListenerTest {

  /** Records registered counters and gauges by "group/name". */
  static final class RecordingGroup extends UnregisteredMetricsGroup {
    final Map<String, Counter> counters = new LinkedHashMap<>();
    final Map<String, Gauge<?>> gauges = new LinkedHashMap<>();
    final String prefix;

    RecordingGroup(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public MetricGroup addGroup(String name) {
      return new UnregisteredMetricsGroup() {
        @Override
        public Counter counter(String n) {
          Counter c = new SimpleCounter();
          counters.put(prefix + name + "/" + n, c);
          return c;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String n, G gauge) {
          gauges.put(prefix + name + "/" + n, gauge);
          return gauge;
        }
      };
    }
  }

  private static MetricsAgentEventListener roundTrip(MetricsAgentEventListener in) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(in);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return (MetricsAgentEventListener) ois.readObject();
    }
  }

  @Test
  void countersAreUsableAfterDeserializationAndMetricsRegisterOnTheGroup() throws Exception {
    MetricsAgentEventListener listener = new MetricsAgentEventListener();
    listener.onChatRequest("a", "m", 1);
    MetricsAgentEventListener restored = roundTrip(listener);
    assertFalse(restored.isRegistered());

    int calls = ThreadLocalRandom.current().nextInt(1, 20);
    long tokens = ThreadLocalRandom.current().nextLong(1, 5000);
    for (int i = 0; i < calls; i++) {
      restored.onChatRequest("a", "m", 1);
      restored.onChatResponse("a", "m", 10, tokens);
      restored.onToolCallEnd("a", "t", "c" + i, i % 2 == 0, 5);
    }
    assertEquals(calls, restored.getChatRequests(), "transient adders are reinitialized, not null");
    assertEquals(calls, restored.getToolCalls());
    assertEquals(calls / 2, restored.getToolFailures());

    RecordingGroup group = new RecordingGroup("");
    restored.open(group);
    assertTrue(restored.isRegistered());
    String g = MetricsAgentEventListener.METRIC_GROUP + "/";
    assertNotNull(group.counters.get(g + "chat_requests"));
    assertNotNull(group.counters.get(g + "tool_failures"));
    assertNotNull(group.gauges.get(g + "tokens_used_total"));

    int more = ThreadLocalRandom.current().nextInt(1, 10);
    for (int i = 0; i < more; i++) {
      restored.onChatResponse("a", "m", 1, tokens);
      restored.onGuardrailBlock("a", "m", "x");
    }
    assertEquals(more, group.counters.get(g + "chat_responses").getCount());
    assertEquals(more * tokens, group.counters.get(g + "tokens_used").getCount());
    assertEquals(more, group.counters.get(g + "guardrail_blocks").getCount());
    assertEquals((calls + more) * tokens, group.gauges.get(g + "tokens_used_total").getValue());
  }

  /** Operator that owns the listener the way the examples do. */
  static final class CountingFunction extends ProcessFunction<String, String> {
    private static final long serialVersionUID = 1L;
    final MetricsAgentEventListener metrics = new MetricsAgentEventListener();

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
      metrics.open(getRuntimeContext());
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
      metrics.onChatRequest("a", "m", 1);
      out.collect(value);
    }
  }

  @Test
  void openRegistersThroughRuntimeContextMetricGroupInsideAnOperator() throws Exception {
    CountingFunction fn = new CountingFunction();
    try (OneInputStreamOperatorTestHarness<String, String> harness =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn))) {
      harness.open();
      CountingFunction opened = (CountingFunction) ((ProcessOperator<String, String>) harness.getOperator()).getUserFunction();
      assertTrue(opened.metrics.isRegistered(), "open() registered on getRuntimeContext().getMetricGroup()");
      int n = ThreadLocalRandom.current().nextInt(1, 10);
      for (int i = 0; i < n; i++) {
        harness.processElement("e" + i, i);
      }
      assertEquals(n, opened.metrics.getChatRequests());
      assertEquals(n, harness.extractOutputValues().size());
    }
  }
}
