package org.agentic.flink.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.agentic.flink.runtime.WorkflowTurnFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** YAML→Flink-job runner: the shared pipeline.yaml assembled + run as a real (local MiniCluster)
 * Flink job. Proves the portable graph runs in a keyed operator and the cep: section becomes native
 * Flink CEP whose match escalates through the graph. */
class FlinkPipelineRunnerTest {

  private static final Path PIPELINES = Path.of("examples", "pipelines");

  private static List<String> run(Map<String, Object> spec, List<Event> seeds) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Event> source = env.fromData(seeds, WorkflowTurnFunction.EVENT_TYPE);
    DataStream<String> out = FlinkPipelineRunner.assemble(env, spec, source);
    List<String> lines = new ArrayList<>();
    try (CloseableIterator<String> it = out.executeAndCollect()) {
      it.forEachRemaining(lines::add);
    }
    return lines;
  }

  private static List<TurnResult> runResults(Map<String, Object> spec, List<Event> seeds) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Event> source = env.fromData(seeds, WorkflowTurnFunction.EVENT_TYPE);
    List<TurnResult> results = new ArrayList<>();
    try (CloseableIterator<TurnResult> it = FlinkPipelineRunner.assembleResults(env, spec, source).executeAndCollect()) {
      it.forEachRemaining(results::add);
    }
    return results;
  }

  @Test
  void bankingYamlEmitsNormalizedResults() throws Exception {
    Path yaml = PIPELINES.resolve("banking.yaml");
    Assumptions.assumeTrue(Files.exists(yaml), "banking.yaml not found");
    Map<String, Object> spec = FlinkPipelineRunner.loadYaml(yaml);
    String cid = "c-" + UUID.randomUUID();
    String tid = "t-" + UUID.randomUUID();

    List<TurnResult> out = runResults(spec, List.of(
        Event.turn(cid, tid, "u", "what is my balance?"),
        Event.turn(cid, tid, "u", "what is my balance?")));
    assertEquals(2, out.size(), out.toString());
    assertEquals(TurnStatus.COMPLETED, out.get(0).status);
    assertEquals("payments", out.get(0).path);
    assertEquals(TurnStatus.DUPLICATE, out.get(1).status, "redelivered turn_id is a duplicate, not a second turn");
    assertEquals(1L, ((Number) out.get(1).state.get("turn_count")).longValue());
    Map<String, Object> doc = out.get(0).toMap();
    for (String field : List.of("conversation_id", "turn_id", "status", "path", "reply", "state", "tool_calls",
        "events", "error")) {
      assertTrue(doc.containsKey(field), field);
    }
  }

  @Test
  void incidentCepDerivedTurnIsDeterministicAndIdempotent() throws Exception {
    Path yaml = PIPELINES.resolve("incident.yaml");
    Assumptions.assumeTrue(Files.exists(yaml), "incident.yaml not found");
    Map<String, Object> spec = FlinkPipelineRunner.loadYaml(yaml);
    String host = "host-" + UUID.randomUUID();
    List<Event> anomalies = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      anomalies.add(new Event(host, "a" + i + "-" + UUID.randomUUID(), "monitor", "anomaly: cpu high",
          Map.of("ts", String.valueOf(i * 60_000L)), null));
    }
    List<TurnResult> first = runResults(spec, anomalies);
    List<TurnResult> second = runResults(spec, anomalies);
    TurnResult e1 = first.stream().filter(r -> "escalate".equals(r.path)).findFirst().orElseThrow();
    TurnResult e2 = second.stream().filter(r -> "escalate".equals(r.path)).findFirst().orElseThrow();
    assertEquals(e1.turnId, e2.turnId, "the same match yields the same derived turn_id");
    assertEquals(e1.reply, e2.reply);
    assertTrue(e1.events.stream().anyMatch(ev -> "turn_received".equals(ev.type())));
  }

  @Test
  @SuppressWarnings("deprecation")
  void deprecatedGraphFunctionDelegatesToTheCoreOperator() throws Exception {
    Path yaml = PIPELINES.resolve("banking.yaml");
    Assumptions.assumeTrue(Files.exists(yaml), "banking.yaml not found");
    Map<String, Object> spec = FlinkPipelineRunner.loadYaml(yaml);
    String cid = "c-" + UUID.randomUUID();
    String tid = "t-" + UUID.randomUUID();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<String> out = env.fromData(List.of(
            Event.turn(cid, tid, "u", "what is my balance?"),
            Event.turn(cid, tid, "u", "what is my balance?")), WorkflowTurnFunction.EVENT_TYPE)
        .keyBy(Event::conversationId)
        .process(new FlinkGraphFunction(spec));
    List<String> lines = new ArrayList<>();
    try (CloseableIterator<String> it = out.executeAndCollect()) {
      it.forEachRemaining(lines::add);
    }
    assertEquals(2, lines.size(), lines.toString());
    assertTrue(lines.get(0).startsWith(cid + " | path=payments | ok=true"), lines.get(0));
    assertTrue(lines.get(1).startsWith(cid + " | path=payments | ok=false"), "duplicate is not ok: " + lines.get(1));
    assertEquals(lines.get(0).substring(lines.get(0).lastIndexOf('|')), lines.get(1).substring(lines.get(1).lastIndexOf('|')),
        "the duplicate repeats the recorded reply");
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
