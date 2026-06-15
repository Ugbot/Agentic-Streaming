package org.agentic.flink.cep;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * Translates the portable, declarative {@code cep:} rule (the same map the Flink-free cores'
 * {@code CepSpec} compiles — see {@code ports/jagentic-core/.../cep/CepSpec.java}) into a <b>native
 * Flink</b> {@link Pattern}. So one declarative pattern runs both as the portable NFA matcher on the
 * cores and as real, watermarked Flink CEP here.
 *
 * <p>Generic over the stream's event type {@code E}: callers supply how to read an event's text and
 * metadata, so it works for any POJO (e.g. an anomaly record). The {@code where} mini-language matches
 * the cores: {@code any} · {@code {text_contains: s|[..]}} · {@code {metadata_equals: {k: v}}} ·
 * {@code {metadata_gt: {k: n}}}. Contiguity {@code next} (strict) / {@code followedBy} (relaxed);
 * {@code within} → {@link Pattern#within(Duration)}. Wire the result with
 * {@code CEP.pattern(stream.keyBy(key), translate(...)).process(...)} as in {@code IncidentAgentExample}.</p>
 */
public final class CepSpecTranslator {

  /** A serializable extractor — Flink CEP conditions are shipped to task managers, so the text /
   * metadata accessors captured in them must be {@link Serializable}. */
  public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}

  private CepSpecTranslator() {}

  @SuppressWarnings("unchecked")
  public static <E> Pattern<E, E> toPattern(Map<String, Object> spec,
                                            SerializableFunction<E, String> textOf,
                                            SerializableFunction<E, Map<String, String>> metaOf) {
    List<Map<String, Object>> stages = (List<Map<String, Object>>) spec.get("pattern");
    if (stages == null || stages.isEmpty()) {
      throw new IllegalArgumentException("cep pattern needs at least one stage");
    }
    Pattern<E, E> pattern = null;
    for (Map<String, Object> st : stages) {
      String name = String.valueOf(st.getOrDefault("stage", "s"));
      SimpleCondition<E> cond = condition(st.get("where"), textOf, metaOf);
      if (pattern == null) {
        pattern = Pattern.<E>begin(name).where(cond);
      } else if ("next".equalsIgnoreCase(String.valueOf(st.getOrDefault("contiguity", "followedBy")))) {
        pattern = pattern.next(name).where(cond);
      } else {
        pattern = pattern.followedBy(name).where(cond);
      }
    }
    long within = ((Number) spec.getOrDefault("within", 0)).longValue();
    if (within > 0) {
      pattern = pattern.within(Duration.ofMillis(within));
    }
    return pattern;
  }

  @SuppressWarnings("unchecked")
  static <E> SimpleCondition<E> condition(Object where, SerializableFunction<E, String> textOf,
                                          SerializableFunction<E, Map<String, String>> metaOf) {
    if (where == null || "any".equals(where)) {
      return SimpleCondition.of(e -> true);
    }
    if (where instanceof Map<?, ?> m) {
      if (m.containsKey("text_contains")) {
        List<String> needles = asList(m.get("text_contains"));
        return SimpleCondition.of(e -> {
          String t = textOf.apply(e);
          return t != null && needles.stream().anyMatch(t::contains);
        });
      }
      if (m.containsKey("metadata_equals")) {
        Map<String, Object> kv = (Map<String, Object>) m.get("metadata_equals");
        return SimpleCondition.of(e -> {
          Map<String, String> meta = metaOf.apply(e);
          return kv.entrySet().stream()
              .allMatch(en -> String.valueOf(en.getValue()).equals(meta == null ? null : meta.get(en.getKey())));
        });
      }
      if (m.containsKey("metadata_gt")) {
        Map<String, Object> kv = (Map<String, Object>) m.get("metadata_gt");
        return SimpleCondition.of(e -> {
          Map<String, String> meta = metaOf.apply(e);
          return kv.entrySet().stream().allMatch(en -> {
            try {
              return Double.parseDouble(meta.get(en.getKey())) > ((Number) en.getValue()).doubleValue();
            } catch (RuntimeException ex) {
              return false;
            }
          });
        });
      }
    }
    throw new IllegalArgumentException("unknown cep where: " + where);
  }

  private static List<String> asList(Object v) {
    if (v instanceof List<?> l) {
      return l.stream().map(String::valueOf).toList();
    }
    return v == null ? List.of() : List.of(String.valueOf(v));
  }
}
