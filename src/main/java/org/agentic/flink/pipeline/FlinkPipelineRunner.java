package org.agentic.flink.pipeline;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.agentic.flink.cep.CepSpecTranslator;
import org.jagentic.core.Event;

/**
 * The YAML→Flink-job runner: read a portable {@code pipeline.yaml} (the same schema the cores load)
 * and assemble + run a real Flink job — a source → (native CEP, if a {@code cep:} section is present)
 * → keyBy(conversation) → the portable agent graph in a keyed operator → sink.
 *
 * <p>This is the Flink-native counterpart of {@code PipelineLoader} (cores) and {@code PipelineMain}
 * (Pekko): the <i>same</i> spec, now a watermarked Flink streaming job. A {@code cep:} rule becomes a
 * native {@code CEP.pattern(...)} (via {@link CepSpecTranslator}); a {@code kind: submit} match emits
 * a derived event that is unioned back into the agent input (so it routes through the graph, e.g. to
 * an {@code escalate} path) — the Flink-idiomatic form of the portable submit action.</p>
 */
public final class FlinkPipelineRunner {

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
  /** Metadata flag marking a CEP-derived event (kept identical to the cores' CepWiring.DERIVED). */
  public static final String DERIVED = "__cep_derived__";

  private FlinkPipelineRunner() {}

  @SuppressWarnings("unchecked")
  public static Map<String, Object> loadYaml(Path path) throws Exception {
    return YAML.readValue(Files.readString(path), Map.class);
  }

  /**
   * Assemble the job graph onto {@code env}: wire native CEP (if any) over {@code source}, union the
   * derived events into the agent input, and run the portable graph keyed by conversation. Returns
   * the per-turn summary stream (caller adds a sink / collects / executes).
   */
  @SuppressWarnings("unchecked")
  public static DataStream<String> assemble(StreamExecutionEnvironment env, Map<String, Object> spec,
                                            DataStream<Event> source) {
    List<Map<String, Object>> cepRules = (List<Map<String, Object>>) spec.get("cep");
    DataStream<Event> agentInput = source;

    if (cepRules != null && !cepRules.isEmpty()) {
      // Event-time from the first rule's ts metadata field, so `within` windows are real.
      String tsField = metadataField(String.valueOf(cepRules.get(0).getOrDefault("ts", "")));
      DataStream<Event> timed = tsField == null ? source
          : source.assignTimestampsAndWatermarks(WatermarkStrategy
              .<Event>forMonotonousTimestamps()
              .withTimestampAssigner((e, t) -> parseLong(meta(e, tsField))));

      for (Map<String, Object> rule : cepRules) {
        KeySelector<Event, String> key = keySelector(rule);
        Pattern<Event, Event> pattern = CepSpecTranslator.toPattern(rule, Event::text, Event::metadata);
        DataStream<Event> derived = CEP.pattern(timed.keyBy(key), pattern)
            .process(new SubmitOnMatch(rule))
            .returns(TypeInformation.of(Event.class));
        agentInput = agentInput.union(derived);
      }
    }

    return agentInput.keyBy(Event::conversationId).process(new FlinkGraphFunction(spec));
  }

  /** A CEP match → a derived agent event (the native form of the portable {@code on_match: submit}). */
  private static final class SubmitOnMatch extends PatternProcessFunction<Event, Event> {
    private final String firstStage;
    private final String keySpec;
    private final String text;

    @SuppressWarnings("unchecked")
    SubmitOnMatch(Map<String, Object> rule) {
      List<Map<String, Object>> stages = (List<Map<String, Object>>) rule.get("pattern");
      this.firstStage = String.valueOf(stages.get(0).getOrDefault("stage", "s"));
      this.keySpec = String.valueOf(rule.getOrDefault("key", "conversation_id"));
      Map<String, Object> onMatch = (Map<String, Object>) rule.get("on_match");
      this.text = onMatch == null ? "cep match" : String.valueOf(onMatch.getOrDefault("text", "cep match"));
    }

    @Override
    public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<Event> out) {
      Event first = match.get(firstStage).get(0);
      String key = extractKey(keySpec, first);
      String body = text.replace("{key}", key == null ? "" : key);
      out.collect(new Event(key, "cep", body, Map.of(DERIVED, "true")));
    }
  }

  // ---- key / timestamp helpers (shared shape with the cores' CepSpec) ----

  private static KeySelector<Event, String> keySelector(Map<String, Object> rule) {
    String keySpec = String.valueOf(rule.getOrDefault("key", "conversation_id"));
    String metaField = metadataField(keySpec);
    if (metaField != null) {
      return e -> {
        String v = meta(e, metaField);
        return v == null ? "" : v;
      };
    }
    return Event::conversationId;
  }

  private static String extractKey(String keySpec, Event e) {
    String metaField = metadataField(keySpec);
    return metaField != null ? meta(e, metaField) : e.conversationId();
  }

  private static String metadataField(String spec) {
    return spec != null && spec.startsWith("metadata.") ? spec.substring("metadata.".length()) : null;
  }

  private static String meta(Event e, String field) {
    return e.metadata() == null ? null : e.metadata().get(field);
  }

  private static long parseLong(String s) {
    try {
      return s == null ? 0L : Long.parseLong(s);
    } catch (NumberFormatException ex) {
      return 0L;
    }
  }

  // ---- CLI ----

  public static void main(String[] args) throws Exception {
    String yaml = "examples/pipelines/banking.yaml";
    String text = null;
    String cid = "c1";
    for (int i = 0; i < args.length; i++) {
      if ("--text".equals(args[i]) && i + 1 < args.length) {
        text = args[++i];
      } else if ("--conversation".equals(args[i]) && i + 1 < args.length) {
        cid = args[++i];
      } else if (!args[i].startsWith("--")) {
        yaml = args[i];
      }
    }
    Map<String, Object> spec = loadYaml(Path.of(yaml));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Event seed = new Event(cid, "user", text == null ? "what is my balance?" : text, Map.of());
    DataStream<Event> source = env.fromData(List.of(seed), TypeInformation.of(Event.class));

    assemble(env, spec, source).print();
    env.execute("agentic-flink: " + yaml);
  }
}
