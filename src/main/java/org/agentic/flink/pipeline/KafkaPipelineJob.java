package org.agentic.flink.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.agentic.flink.annotation.Public;
import org.agentic.flink.runtime.ChatClientFactories;
import org.agentic.flink.runtime.FlinkRuntimeOptions;
import org.agentic.flink.runtime.TurnResultCodec;
import org.agentic.flink.runtime.WorkflowTurnFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;

/**
 * A {@code pipeline.yaml} agent as a Flink job between Kafka topics: turns in, normalized results
 * out, and everything that needs attention on a failures topic.
 *
 * <p>A turn is a JSON record, keyed by {@code conversation_id} so that a conversation stays on one
 * partition, which is what keeps its turns in order up to the {@code keyBy}:
 *
 * <pre>{"conversation_id": "c1", "turn_id": "t1", "user_id": "alice", "text": "...",
 *  "metadata": {"k": "v"}}
 * {"conversation_id": "c1", "turn_id": "t1", "signal": {"approved": true}}</pre>
 *
 * <p>{@code turn_id} is required rather than minted: it is what makes a redelivered record a {@code
 * duplicate}. A record that cannot be a turn goes to the failures topic and the job carries on; so
 * does a turn that ends {@code fatal} or {@code transient}, as well as appearing on the results
 * topic.
 *
 * <p>Results are written at-least-once under the conversation key. A consumer can see the same
 * result again after a restore, and tools re-run as the README's known failures describe.
 */
@Public
public final class KafkaPipelineJob {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeInformation<Tuple2<String, String>> KEYED =
      Types.TUPLE(Types.STRING, Types.STRING);
  private static final OutputTag<Tuple2<String, String>> MALFORMED =
      new OutputTag<>("malformed-records", KEYED);

  private KafkaPipelineJob() {}

  /** Where the job reads and writes. */
  public record Options(
      String bootstrap,
      String inputTopic,
      String outputTopic,
      String failuresTopic,
      String groupId) {

    /**
     * Reads the Kafka flags, or returns null when {@code --input-topic} is absent, which means this
     * is not a Kafka run. {@code --kafka-bootstrap} defaults to {@code env.KAFKA_BOOTSTRAP}.
     */
    public static Options parse(String[] args, Map<String, String> env, String agentId) {
      Map<String, String> flags = new HashMap<>();
      for (int i = 0; i + 1 < args.length; i++) {
        if (args[i].startsWith("--")) {
          flags.put(args[i], args[i + 1]);
        }
      }
      String input = flags.get("--input-topic");
      if (input == null) {
        return null;
      }
      String bootstrap = flags.getOrDefault("--kafka-bootstrap", env.get("KAFKA_BOOTSTRAP"));
      if (bootstrap == null || bootstrap.isBlank()) {
        throw new IllegalArgumentException(
            "--input-topic needs --kafka-bootstrap or KAFKA_BOOTSTRAP in the environment");
      }
      String output = flags.getOrDefault("--output-topic", input + ".results");
      return new Options(
          bootstrap,
          input,
          output,
          flags.getOrDefault("--failures-topic", output + ".failures"),
          flags.getOrDefault("--group-id", "agentic-" + agentId));
    }
  }

  /** A record that cannot be a turn; the message is the reason recorded on the failures topic. */
  public static final class MalformedRecord extends Exception {
    private static final long serialVersionUID = 1L;

    MalformedRecord(String message) {
      super(message);
    }
  }

  /** Results and failures as {@code (key, json)}, ready for a Kafka sink. */
  public record Wired(
      DataStream<Tuple2<String, String>> results, DataStream<Tuple2<String, String>> failures) {}

  /** Builds the whole job onto {@code env}; the caller runs {@code env.execute}. */
  public static void assemble(
      StreamExecutionEnvironment env, Map<String, Object> spec, Options options) {
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(options.bootstrap())
            .setTopics(options.inputTopic())
            .setGroupId(options.groupId())
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new RecordValue())
            .build();
    DataStream<String> raw =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "turns")
            .uid("agentic-kafka-source");
    Wired wired = wire(env, spec, raw, FlinkRuntimeOptions.fromSpec(spec));
    wired
        .results()
        .sinkTo(sink(options.bootstrap(), options.outputTopic()))
        .name("results")
        .uid("agentic-kafka-results");
    wired
        .failures()
        .sinkTo(sink(options.bootstrap(), options.failuresTopic()))
        .name("failures")
        .uid("agentic-kafka-failures");
  }

  /** The job between raw record values and encoded results; everything but the connectors. */
  static Wired wire(
      StreamExecutionEnvironment env,
      Map<String, Object> spec,
      DataStream<String> raw,
      FlinkRuntimeOptions runtimeOptions) {
    SingleOutputStreamOperator<Event> events =
        raw.process(new Decode())
            .returns(WorkflowTurnFunction.EVENT_TYPE)
            .name("decode")
            .uid("agentic-decode");
    SingleOutputStreamOperator<TurnResult> turns =
        FlinkPipelineRunner.assembleResults(
                env, spec, events, runtimeOptions, ChatClientFactories.fromWorkflow())
            .name("agent")
            .uid("agentic-workflow-turn");
    DataStream<Tuple2<String, String>> results =
        turns
            .map(KafkaPipelineJob::encodeResult)
            .returns(KEYED)
            .name("encode")
            .uid("agentic-encode");
    DataStream<Tuple2<String, String>> failures =
        turns
            .getSideOutput(WorkflowTurnFunction.TURN_FAILURES)
            .map(KafkaPipelineJob::encodeFailure)
            .returns(KEYED)
            .name("encode-failure")
            .uid("agentic-encode-failure")
            .union(events.getSideOutput(MALFORMED));
    return new Wired(results, failures);
  }

  private static KafkaSink<Tuple2<String, String>> sink(String bootstrap, String topic) {
    return KafkaSink.<Tuple2<String, String>>builder()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(new KeyedRecord(topic))
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  /** Turns a record value into an {@link Event}; see the class comment for the format. */
  static Event decode(String raw) throws MalformedRecord {
    JsonNode node;
    try {
      node = JSON.readTree(raw);
    } catch (JsonProcessingException e) {
      throw new MalformedRecord("not JSON: " + e.getOriginalMessage());
    }
    if (node == null || !node.isObject()) {
      throw new MalformedRecord("a turn must be a JSON object");
    }
    String conversationId = required(node, "conversation_id");
    String turnId = required(node, "turn_id");
    Map<String, Object> signal = null;
    if (node.hasNonNull("signal")) {
      if (!node.get("signal").isObject()) {
        throw new MalformedRecord("signal must be an object");
      }
      signal = new LinkedHashMap<>();
      for (Map.Entry<String, JsonNode> e : node.get("signal").properties()) {
        signal.put(e.getKey(), JSON.convertValue(e.getValue(), Object.class));
      }
    }
    String text = signal == null ? required(node, "text") : node.path("text").asText("");
    Map<String, String> metadata = new LinkedHashMap<>();
    for (Map.Entry<String, JsonNode> e : node.path("metadata").properties()) {
      metadata.put(e.getKey(), e.getValue().asText());
    }
    String user = node.path("user_id").asText("anonymous");
    return new Event(
        conversationId,
        turnId,
        user,
        text,
        metadata,
        signal,
        org.jagentic.core.Principal.fromMap(
            JSON.convertValue(node.get("principal"), Object.class)));
  }

  private static String required(JsonNode node, String field) throws MalformedRecord {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual() || value.asText().isBlank()) {
      throw new MalformedRecord(field + " must be a non-empty string");
    }
    return value.asText();
  }

  private static Tuple2<String, String> encodeResult(TurnResult r) {
    try {
      return Tuple2.of(
          r.conversationId, new String(TurnResultCodec.encode(r), StandardCharsets.UTF_8));
    } catch (IOException e) {
      return Tuple2.of(r.conversationId, unencodable(r, e));
    }
  }

  private static Tuple2<String, String> encodeFailure(TurnResult r) {
    Map<String, Object> failure = new LinkedHashMap<>();
    failure.put("kind", "turn_failure");
    failure.put("result", r.toMap());
    try {
      return Tuple2.of(r.conversationId, JSON.writeValueAsString(failure));
    } catch (IOException e) {
      return Tuple2.of(r.conversationId, unencodable(r, e));
    }
  }

  /** A tool result the codec cannot write must not fail the job; the ids still get out. */
  private static String unencodable(TurnResult r, IOException e) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("conversation_id", r.conversationId);
    m.put("turn_id", r.turnId);
    m.put("status", r.status.wire());
    m.put("error", Map.of("class", "fatal", "message", "result not encodable: " + e.getMessage()));
    try {
      return JSON.writeValueAsString(m);
    } catch (JsonProcessingException unreachable) {
      throw new IllegalStateException(unreachable);
    }
  }

  private static final class Decode extends ProcessFunction<String, Event> {
    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(String raw, Context ctx, Collector<Event> out) throws Exception {
      try {
        out.collect(decode(raw));
      } catch (MalformedRecord e) {
        Map<String, Object> failure = new LinkedHashMap<>();
        failure.put("kind", "malformed_record");
        failure.put("reason", e.getMessage());
        failure.put("record", raw);
        ctx.output(MALFORMED, Tuple2.of("", JSON.writeValueAsString(failure)));
      }
    }
  }

  /** A record value as text. A tombstone has none, and must reach the decoder rather than NPE. */
  static final class RecordValue extends SimpleStringSchema {
    private static final long serialVersionUID = 1L;

    @Override
    public String deserialize(byte[] message) {
      return message == null ? "" : new String(message, StandardCharsets.UTF_8);
    }
  }

  /** {@code (key, json)} to a record on one topic; an empty key is no key. */
  static final class KeyedRecord implements KafkaRecordSerializationSchema<Tuple2<String, String>> {
    private static final long serialVersionUID = 1L;
    private final String topic;

    KeyedRecord(String topic) {
      this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
        Tuple2<String, String> element, KafkaSinkContext context, Long timestamp) {
      byte[] key = element.f0.isEmpty() ? null : element.f0.getBytes(StandardCharsets.UTF_8);
      return new ProducerRecord<>(topic, key, element.f1.getBytes(StandardCharsets.UTF_8));
    }
  }
}
