package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Runs an {@link A2AToolExecutor} inside a real Flink job on a local minicluster: a source of call
 * payloads → a map operator that invokes the executor (serialized into the job graph, client/pool
 * rebuilt on the task side) → a collecting sink. Proves the outbound A2A tool chains as a workflow
 * step end-to-end.
 */
final class A2AToolFlinkJobTest {

  private static final ConcurrentLinkedQueue<String> RESULTS = new ConcurrentLinkedQueue<>();

  @Test
  @DisplayName("A2A tool executes as a Flink operator and emits peer artifacts downstream")
  void runsInFlinkJob() throws Exception {
    RESULTS.clear();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(2, new Configuration());

    List<String> prompts = List.of("alpha", "bravo", "charlie", "delta");
    DataStream<String> src = env.addSource(new ListSource(prompts));

    RemoteAgentSpec spec =
        RemoteAgentSpec.builder()
            .withName("echo-peer")
            .withEndpointUrl("https://peer/a2a")
            .withPollInterval(java.time.Duration.ofMillis(1))
            .build();

    src.map(new CallA2A(spec)).addSink(new Collect());
    env.execute("a2a-tool-flink-job-test");

    assertEquals(prompts.size(), RESULTS.size());
    for (String prompt : prompts) {
      assertTrue(
          RESULTS.stream().anyMatch(r -> r.contains("echo: " + prompt)),
          "missing echoed artifact for " + prompt + " in " + RESULTS);
    }
  }

  /** Map operator holding the (serializable) tool; factory rebuilds a fake client on task side. */
  static final class CallA2A extends RichMapFunction<String, String> {
    private static final long serialVersionUID = 1L;
    private final A2AToolExecutor tool;

    CallA2A(RemoteAgentSpec spec) {
      // Lambda factory captures nothing -> serializable (A2AClientFactory extends Serializable).
      this.tool = new A2AToolExecutor(spec, s -> new FakeA2AClient(s, 1, false));
    }

    @Override
    public String map(String prompt) throws Exception {
      @SuppressWarnings("unchecked")
      Map<String, Object> result =
          (Map<String, Object>) tool.execute(Map.of("input", prompt)).get();
      return (String) result.get("text");
    }
  }

  static final class ListSource implements SourceFunction<String> {
    private static final long serialVersionUID = 1L;
    private final List<String> values;
    private volatile boolean running = true;

    ListSource(List<String> values) {
      this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    @Override
    public void run(SourceContext<String> ctx) {
      for (String v : values) {
        if (!running) {
          return;
        }
        ctx.collect(v);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  static final class Collect implements SinkFunction<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(String value, Context context) {
      RESULTS.add(value);
    }
  }
}
