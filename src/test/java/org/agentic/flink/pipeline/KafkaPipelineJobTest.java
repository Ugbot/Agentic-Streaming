package org.agentic.flink.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.runtime.FlinkRuntimeOptions;
import org.agentic.flink.runtime.testkit.Workflows;
import org.agentic.flink.testkit.RepoRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jagentic.core.Event;
import org.junit.jupiter.api.Test;

/**
 * Everything of the Kafka job but the broker: the record format, the dead letters, and the job
 * graph run on a local cluster. {@code tools/kafka-flink-e2e.sh} proves the same job on a real
 * Flink cluster and a real broker.
 */
class KafkaPipelineJobTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static String turn(String cid, String tid, String text) {
    return "{\"conversation_id\":\""
        + cid
        + "\",\"turn_id\":\""
        + tid
        + "\",\"user_id\":\"alice\",\"text\":\""
        + text
        + "\"}";
  }

  @Test
  void aTurnRecordDecodes() throws Exception {
    Event e =
        KafkaPipelineJob.decode(
            "{\"conversation_id\":\"c\",\"turn_id\":\"t\",\"user_id\":\"u\",\"text\":\"hi\","
                + "\"metadata\":{\"event_time_ms\":\"5\"}}");
    assertEquals("c", e.conversationId());
    assertEquals("t", e.turnId());
    assertEquals("u", e.userId());
    assertEquals("hi", e.text());
    assertEquals(Map.of("event_time_ms", "5"), e.metadata());
    assertFalse(e.isResume());
  }

  @Test
  void aSignalRecordResumesAndAMissingUserIsAnonymous() throws Exception {
    Event e =
        KafkaPipelineJob.decode(
            "{\"conversation_id\":\"c\",\"turn_id\":\"t\",\"signal\":{\"approved\":true}}");
    assertTrue(e.isResume());
    assertEquals(Map.of("approved", true), e.signal());
    assertEquals("anonymous", e.userId());
    assertEquals("", e.text());
  }

  @Test
  void aRecordThatCannotBeATurnIsRefusedWithAReason() {
    for (String bad :
        List.of(
            "not json",
            "",
            "[1,2]",
            "{\"turn_id\":\"t\",\"text\":\"hi\"}",
            "{\"conversation_id\":\"\",\"turn_id\":\"t\",\"text\":\"hi\"}",
            "{\"conversation_id\":\"c\",\"text\":\"hi\"}",
            "{\"conversation_id\":\"c\",\"turn_id\":\"t\"}",
            "{\"conversation_id\":\"c\",\"turn_id\":\"t\",\"text\":7}")) {
      assertThrows(
          KafkaPipelineJob.MalformedRecord.class, () -> KafkaPipelineJob.decode(bad), bad);
    }
  }

  @Test
  void aTombstoneDoesNotReachTheDecoderAsNull() throws Exception {
    assertEquals("", new KafkaPipelineJob.RecordValue().deserialize(null));
    assertEquals(
        "hi", new KafkaPipelineJob.RecordValue().deserialize("hi".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void resultsAreKeyedByConversation() {
    ProducerRecord<byte[], byte[]> r =
        new KafkaPipelineJob.KeyedRecord("results").serialize(Tuple2.of("c1", "{}"), null, null);
    assertEquals("results", r.topic());
    assertEquals("c1", new String(r.key(), StandardCharsets.UTF_8));
    assertNull(
        new KafkaPipelineJob.KeyedRecord("results").serialize(Tuple2.of("", "{}"), null, null).key());
  }

  @Test
  void optionsComeFromTheCommandLineAndTheEnvironment() {
    KafkaPipelineJob.Options o =
        KafkaPipelineJob.Options.parse(
            new String[] {"--input-topic", "turns"}, Map.of("KAFKA_BOOTSTRAP", "kafka:29092"), "bank");
    assertEquals("kafka:29092", o.bootstrap());
    assertEquals("turns", o.inputTopic());
    assertEquals("turns.results", o.outputTopic());
    assertEquals("turns.results.failures", o.failuresTopic());
    assertEquals("agentic-bank", o.groupId());

    KafkaPipelineJob.Options explicit =
        KafkaPipelineJob.Options.parse(
            new String[] {
              "--kafka-bootstrap", "b:1", "--input-topic", "i", "--output-topic", "o",
              "--failures-topic", "f", "--group-id", "g"
            },
            Map.of("KAFKA_BOOTSTRAP", "ignored:2"),
            "bank");
    assertEquals(
        new KafkaPipelineJob.Options("b:1", "i", "o", "f", "g"), explicit);
    assertNull(KafkaPipelineJob.Options.parse(new String[] {"x.yaml"}, Map.of(), "bank"));
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaPipelineJob.Options.parse(new String[] {"--input-topic", "i"}, Map.of(), "bank"));
  }

  private record Run(List<JsonNode> results, List<JsonNode> failures) {}

  private static Run run(Map<String, Object> spec, List<String> records) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<String> raw = env.fromData(records, Types.STRING);
    KafkaPipelineJob.Wired wired =
        KafkaPipelineJob.wire(env, spec, raw, FlinkRuntimeOptions.fromSpec(spec));
    List<Tuple2<String, Tuple2<String, String>>> tagged = new ArrayList<>();
    DataStream<Tuple2<String, Tuple2<String, String>>> both =
        wired
            .results()
            .map(r -> Tuple2.of("result", r))
            .returns(
                Types.TUPLE(Types.STRING, Types.TUPLE(Types.STRING, Types.STRING)))
            .union(
                wired
                    .failures()
                    .map(r -> Tuple2.of("failure", r))
                    .returns(
                        Types.TUPLE(Types.STRING, Types.TUPLE(Types.STRING, Types.STRING))));
    try (CloseableIterator<Tuple2<String, Tuple2<String, String>>> it = both.executeAndCollect()) {
      it.forEachRemaining(tagged::add);
    }
    List<JsonNode> results = new ArrayList<>();
    List<JsonNode> failures = new ArrayList<>();
    for (Tuple2<String, Tuple2<String, String>> t : tagged) {
      ("result".equals(t.f0) ? results : failures).add(JSON.readTree(t.f1.f1));
    }
    return new Run(results, failures);
  }

  @Test
  void aBankingYamlRunsAndAPoisonRecordDoesNotStopIt() throws Exception {
    Map<String, Object> spec =
        FlinkPipelineRunner.loadYaml(RepoRoot.examplePipeline("banking.yaml"));
    String cid = "c-" + UUID.randomUUID();
    Run run =
        run(
            spec,
            List.of(
                turn(cid, "t1", "what is my balance?"),
                "not json at all",
                "{\"turn_id\":\"no-conversation\",\"text\":\"hi\"}",
                turn(cid, "t1", "what is my balance?"),
                turn(cid, "t2", "what is my balance?")));

    assertEquals(3, run.results().size(), run.results().toString());
    assertEquals("completed", run.results().get(0).path("status").asText());
    assertEquals("payments", run.results().get(0).path("path").asText());
    assertTrue(run.results().get(0).path("reply").asText().contains("1234.56"));
    assertEquals("duplicate", run.results().get(1).path("status").asText());
    assertEquals("completed", run.results().get(2).path("status").asText());

    assertEquals(2, run.failures().size(), run.failures().toString());
    for (JsonNode f : run.failures()) {
      assertEquals("malformed_record", f.path("kind").asText());
      assertFalse(f.path("reason").asText().isBlank());
    }
    assertEquals("not json at all", run.failures().get(0).path("record").asText());
  }

  @Test
  void aFatalTurnReachesTheFailuresStreamToo() throws Exception {
    Map<String, Object> wf = new java.util.LinkedHashMap<>(Workflows.billing());
    wf.put(
        "tools",
        List.of(
            Map.of("id", "lookup_charge", "kind", "constant", "value", 1),
            Map.of("id", "reserve", "kind", "constant", "value", 1, "compensation", "release"),
            Map.of("id", "post", "kind", "failing"),
            Map.of("id", "release", "kind", "failing")));
    wf.put(
        "saga",
        Map.of(
            "steps",
            List.of(
                Map.of("name", "reserve", "tool", "reserve"),
                Map.of("name", "post", "tool", "post"))));
    Run run = run(wf, List.of(turn("c", "t1", "refund")));
    assertEquals("failed", run.results().get(0).path("status").asText());
    assertEquals(1, run.failures().size());
    assertEquals("turn_failure", run.failures().get(0).path("kind").asText());
    assertEquals("t1", run.failures().get(0).path("result").path("turn_id").asText());
  }
}
