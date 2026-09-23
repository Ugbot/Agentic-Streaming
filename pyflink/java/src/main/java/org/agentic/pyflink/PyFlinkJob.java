package org.agentic.pyflink;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.agentic.flink.pipeline.FlinkPipelineRunner;
import org.agentic.flink.runtime.FlinkRuntimeOptions;
import org.agentic.flink.runtime.ManualProcessingClock;
import org.agentic.flink.runtime.TurnResultCodec;
import org.agentic.flink.runtime.WorkflowTurnFunction;
import org.agentic.flink.typeinfo.FlinkJson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.WorkflowValidator;

/**
 * Glue between a PyFlink-authored job graph and the Flink adapter of the canonical core.
 *
 * <p>PyFlink builds the graph over Py4J, so everything it hands the adapter must already be a Java
 * object: this class turns a {@code DataStream<String>} of JSON turn documents into
 * {@link Event}s, runs {@link FlinkPipelineRunner#assembleResults} on the <em>same</em> portable
 * workflow document ({@code spec/v1/workflow.schema.json}) every other runtime loads, and hands
 * back a {@code DataStream<String>} of normalized result documents
 * ({@code spec/v1/result.schema.json}, encoded by {@link TurnResultCodec}).
 *
 * <p>The turn wire form is the fixture turn shape of {@code spec/conformance/v1}:
 * {@code {"conversation_id", "turn_id", "user_id", "text", "metadata", "signal"}}; a turn carrying
 * {@code signal} resumes a suspended turn of the same {@code turn_id}.
 */
public final class PyFlinkJob {
  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

  private PyFlinkJob() {}

  /** Parses a workflow document given as JSON and validates it against the v1 IR rules. */
  public static Map<String, Object> parseWorkflow(String workflowJson) throws IOException {
    Map<String, Object> spec = FlinkJson.mapper().readValue(workflowJson, MAP);
    return WorkflowValidator.validate(spec);
  }

  /**
   * Wires {@code turns} (JSON turn documents) through the keyed workflow operator and returns the
   * normalized results as JSON lines. Runtime knobs under the document's {@code runtime.flink}
   * block are honoured exactly as in the JVM runner.
   *
   * <p>The JSON stream is keyed by {@code conversation_id} before decoding so that, when the
   * decoder runs at a higher parallelism than the source, turns of one conversation still travel
   * through one subtask and reach the (identically keyed) workflow operator in delivery order.
   */
  public static DataStream<String> assemble(StreamExecutionEnvironment env, String workflowJson,
                                            DataStream<String> turns) throws IOException {
    return assemble(env, workflowJson, turns, null);
  }

  /**
   * As {@link #assemble(StreamExecutionEnvironment, String, DataStream)}, with workflow
   * {@code timers} reading the {@link ManualProcessingClock} registered under {@code clockId}
   * instead of the operator's processing time when {@code clockId} is not {@code null}. The Python
   * side advances that clock through {@link ManualProcessingClock#named(String)} on the same JVM
   * (PyFlink's local mode runs the cluster inside the gateway process), and keeps the id across
   * a stop-with-savepoint restart so the reading and the restored pending timers stay aligned.
   */
  public static DataStream<String> assemble(StreamExecutionEnvironment env, String workflowJson,
                                            DataStream<String> turns, String clockId) throws IOException {
    Map<String, Object> spec = parseWorkflow(workflowJson);
    DataStream<Event> events = turns.keyBy(new ConversationKey())
        .map(new JsonToEvent()).name("json->event");
    FlinkRuntimeOptions options = FlinkRuntimeOptions.fromSpec(spec);
    if (clockId != null) {
      options = options.withManualClock(clockId);
    }
    DataStream<TurnResult> results = FlinkPipelineRunner.assembleResults(env, spec, events, options);
    return results.map(new ResultToJson()).name("result->json")
        .returns(TypeInformation.of(String.class));
  }

  /** Decodes one JSON turn document into an {@link Event}. */
  @SuppressWarnings("unchecked")
  public static Event toEvent(String json) throws IOException {
    Map<String, Object> m = FlinkJson.mapper().readValue(json, MAP);
    String cid = requireString(m, "conversation_id");
    String turnId = requireString(m, "turn_id");
    Map<String, Object> signal = m.get("signal") instanceof Map<?, ?> s
        ? new LinkedHashMap<>((Map<String, Object>) s) : null;
    if (signal != null) {
      return Event.resume(cid, turnId, signal);
    }
    Map<String, String> metadata = new LinkedHashMap<>();
    if (m.get("metadata") instanceof Map<?, ?> md) {
      for (Map.Entry<?, ?> e : md.entrySet()) {
        metadata.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
      }
    }
    Object user = m.get("user_id");
    Object text = m.get("text");
    return new Event(cid, turnId, user == null ? "anonymous" : String.valueOf(user),
        text == null ? "" : String.valueOf(text), metadata, null);
  }

  private static String requireString(Map<String, Object> m, String key) {
    Object v = m.get(key);
    if (v == null || String.valueOf(v).isBlank()) {
      throw new IllegalArgumentException("turn document is missing required field '" + key + "'");
    }
    return String.valueOf(v);
  }

  /** Extracts {@code conversation_id} from a JSON turn document. */
  public static final class ConversationKey implements KeySelector<String, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(String json) throws Exception {
      return requireString(FlinkJson.mapper().readValue(json, MAP), "conversation_id");
    }
  }

  /** {@code String -> Event}; the output type is the framework's JSON type info for events. */
  public static final class JsonToEvent implements MapFunction<String, Event>, ResultTypeQueryable<Event> {
    private static final long serialVersionUID = 1L;

    @Override
    public Event map(String json) throws Exception {
      return toEvent(json);
    }

    @Override
    public TypeInformation<Event> getProducedType() {
      return WorkflowTurnFunction.EVENT_TYPE;
    }
  }

  /** {@code TurnResult -> String}: one normalized result document per line. */
  public static final class ResultToJson implements MapFunction<TurnResult, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String map(TurnResult result) throws Exception {
      return new String(TurnResultCodec.encode(result), StandardCharsets.UTF_8);
    }
  }
}
