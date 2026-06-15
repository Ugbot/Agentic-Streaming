package org.agentic.flink.pipeline;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;

/** YAML→Flink-job runner: the shared pipeline.yaml assembled + run as a real (local MiniCluster)
 * Flink job. Proves the portable graph runs in a keyed operator and the cep: section becomes native
 * Flink CEP whose match escalates through the graph. */
class FlinkPipelineRunnerTest {

  private static final Path PIPELINES = Path.of("examples", "pipelines");

  private static List<String> run(Map<String, Object> spec, List<Event> seeds) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Event> source = env.fromData(seeds, TypeInformation.of(Event.class));
    DataStream<String> out = FlinkPipelineRunner.assemble(env, spec, source);
    List<String> lines = new ArrayList<>();
    try (CloseableIterator<String> it = out.executeAndCollect()) {
      it.forEachRemaining(lines::add);
    }
    return lines;
  }

  @Test
  void bankingYamlRunsAsAFlinkJob() throws Exception {
    Path yaml = PIPELINES.resolve("banking.yaml");
    Assumptions.assumeTrue(Files.exists(yaml), "banking.yaml not found");
    Map<String, Object> spec = FlinkPipelineRunner.loadYaml(yaml);

    List<String> out = run(spec, List.of(new Event("c1", "u", "what is my balance?", Map.of())));
    assertTrue(out.stream().anyMatch(l -> l.contains("path=payments") && l.contains("1234.56")),
        "expected a payments turn carrying the balance, got: " + out);
  }

  @Test
  void incidentYamlCepEscalatesAsNativeFlinkCep() throws Exception {
    Path yaml = PIPELINES.resolve("incident.yaml");
    Assumptions.assumeTrue(Files.exists(yaml), "incident.yaml not found");
    Map<String, Object> spec = FlinkPipelineRunner.loadYaml(yaml);

    // Three anomalies on one host within the 5-minute window → native CEP fires an escalation event
    // that routes through the graph to the escalate path.
    List<Event> anomalies = List.of(
        new Event("host-7", "monitor", "anomaly: cpu high", Map.of("ts", "0")),
        new Event("host-7", "monitor", "anomaly: cpu high", Map.of("ts", "60000")),
        new Event("host-7", "monitor", "anomaly: cpu high", Map.of("ts", "120000")));

    List<String> out = run(spec, anomalies);
    assertTrue(out.stream().anyMatch(l -> l.contains("path=escalate")),
        "expected a CEP-driven escalation turn, got: " + out);
  }
}
