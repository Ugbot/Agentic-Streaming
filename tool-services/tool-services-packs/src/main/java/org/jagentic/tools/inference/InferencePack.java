package org.jagentic.tools.inference;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.ToolRegistry;
import org.jagentic.core.inference.Classification;
import org.jagentic.core.inference.Classifier;
import org.jagentic.core.inference.ClassifierGuardrail;
import org.jagentic.core.inference.EmbeddingClassifier;
import org.jagentic.core.inference.LexiconClassifier;
import org.jagentic.tools.ToolPack;

/** Inference pack — text classification, scoring, and a guardrail check, built on
 * jagentic-core's {@link Classifier} family (the Flink {@code InferenceToolAdapter} is
 * Flink-coupled, so this is rebuilt). Self-contained and model-free by default: the
 * classifier definition travels in the call — a {@code lexicon} (keyword sets) or
 * {@code examples} (fitted nearest-centroid over the hashing embedder) — so no model
 * download or pack state is needed. DJL-backed real models can plug in via the same SPI. */
public final class InferencePack implements ToolPack {

  @Override
  public String name() {
    return "inference";
  }

  @Override
  public List<String> register(ToolRegistry registry) {
    registry.register("classify_text",
        "Classify text into one of the provided labels (lexicon keywords or labelled examples).",
        classifySchema(), this::classify);
    registry.register("score_text",
        "Return the probability that text belongs to a target label.",
        scoreSchema(), this::score);
    registry.register("guardrail_check",
        "Check whether text trips a classifier guardrail (a blocked label above a threshold).",
        guardrailSchema(), this::guardrail);
    return List.of("classify_text", "score_text", "guardrail_check");
  }

  private Object classify(Map<String, Object> p) {
    Classification c = classifierFrom(p).classify(str(p, "text", ""));
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("label", c.label());
    out.put("score", c.score());
    out.put("scores", c.scores());
    return out;
  }

  private Object score(Map<String, Object> p) {
    String label = str(p, "label", null);
    if (label == null) {
      throw new IllegalArgumentException("score_text requires 'label'");
    }
    Classification c = classifierFrom(p).classify(str(p, "text", ""));
    double score = c.scores().getOrDefault(label, c.label().equals(label) ? c.score() : 0.0);
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("label", label);
    out.put("score", score);
    return out;
  }

  private Object guardrail(Map<String, Object> p) {
    List<String> blocked = stringList(p.get("blocked"));
    double threshold = doubleVal(p, "threshold", 0.5);
    ClassifierGuardrail guard = new ClassifierGuardrail(classifierFrom(p), blocked, threshold,
        str(p, "reason", "blocked by classifier policy"), false);
    String reason = guard.checkInput(str(p, "text", ""));
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("blocked", reason != null);
    if (reason != null) {
      out.put("reason", reason);
    }
    return out;
  }

  /** Build a classifier from the call: {@code examples} (embedding) wins over {@code lexicon}. */
  @SuppressWarnings("unchecked")
  private Classifier classifierFrom(Map<String, Object> p) {
    Object examples = p.get("examples");
    if (examples instanceof Map<?, ?> ex && !ex.isEmpty()) {
      return new EmbeddingClassifier().fit(toLabelLists((Map<String, Object>) examples));
    }
    Object lexicon = p.get("lexicon");
    if (lexicon instanceof Map<?, ?> lx && !lx.isEmpty()) {
      return new LexiconClassifier(toLabelLists((Map<String, Object>) lexicon),
          str(p, "default_label", "other"));
    }
    throw new IllegalArgumentException("provide a 'lexicon' (label -> keywords) or 'examples' (label -> texts)");
  }

  private static Map<String, List<String>> toLabelLists(Map<String, Object> raw) {
    Map<String, List<String>> out = new LinkedHashMap<>();
    for (Map.Entry<String, Object> e : raw.entrySet()) {
      out.put(e.getKey(), stringList(e.getValue()));
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  private static List<String> stringList(Object v) {
    List<String> out = new ArrayList<>();
    if (v instanceof List<?> list) {
      for (Object o : list) {
        out.add(String.valueOf(o));
      }
    } else if (v instanceof String s && !s.isBlank()) {
      for (String part : s.split("\\s*,\\s*")) {
        out.add(part);
      }
    }
    return out;
  }

  private static Map<String, Object> classifySchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("text", Map.of("type", "string", "description", "Text to classify."));
    props.put("lexicon", Map.of("type", "object", "description", "label -> list of keywords."));
    props.put("examples", Map.of("type", "object", "description", "label -> list of example texts."));
    props.put("default_label", Map.of("type", "string", "description", "Fallback label (lexicon mode)."));
    return objectSchema(props, List.of("text"));
  }

  private static Map<String, Object> scoreSchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("text", Map.of("type", "string", "description", "Text to score."));
    props.put("label", Map.of("type", "string", "description", "Target label to score for."));
    props.put("lexicon", Map.of("type", "object", "description", "label -> list of keywords."));
    props.put("examples", Map.of("type", "object", "description", "label -> list of example texts."));
    return objectSchema(props, List.of("text", "label"));
  }

  private static Map<String, Object> guardrailSchema() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("text", Map.of("type", "string", "description", "Text to screen."));
    props.put("blocked", Map.of("type", "array", "items", Map.of("type", "string"),
        "description", "Labels that should block."));
    props.put("threshold", Map.of("type", "number", "description", "Min confidence to block (default 0.5)."));
    props.put("lexicon", Map.of("type", "object", "description", "label -> list of keywords."));
    props.put("examples", Map.of("type", "object", "description", "label -> list of example texts."));
    props.put("reason", Map.of("type", "string", "description", "Block reason text."));
    return objectSchema(props, List.of("text", "blocked"));
  }

  private static Map<String, Object> objectSchema(Map<String, Object> properties, List<String> required) {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
    schema.put("required", required);
    return schema;
  }

  private static String str(Map<String, Object> m, String k, String def) {
    Object v = m == null ? null : m.get(k);
    return v == null ? def : String.valueOf(v);
  }

  private static double doubleVal(Map<String, Object> m, String k, double def) {
    Object v = m == null ? null : m.get(k);
    if (v instanceof Number n) {
      return n.doubleValue();
    }
    if (v != null) {
      try {
        return Double.parseDouble(String.valueOf(v));
      } catch (NumberFormatException ignored) {
        return def;
      }
    }
    return def;
  }
}
