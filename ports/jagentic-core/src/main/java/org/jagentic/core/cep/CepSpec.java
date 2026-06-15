package org.jagentic.core.cep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.jagentic.core.Event;

/**
 * Compiles a pipeline's {@code cep:} section into runnable {@link CepWiring}s. The schema mirrors the
 * {@code kind}-dispatch style of {@code tools:}/{@code guardrails:}:
 *
 * <pre>
 * cep:
 *   - name: incident
 *     key: conversation_id            # conversation_id | metadata.&lt;field&gt;
 *     ts:  metadata.ts                 # metadata.&lt;field&gt; (else a per-rule arrival counter)
 *     within: 300000
 *     pattern:
 *       - { stage: first,  where: any }
 *       - { stage: second, where: { text_contains: anomaly }, contiguity: followedBy }
 *       - { stage: third,  where: any, contiguity: followedBy }
 *     on_match: { kind: submit, text: "incident on {key}" }   # or { kind: tool, tool: id, args: {...} }
 * </pre>
 *
 * {@code where}: {@code any} · {@code {text_contains: s|[..]}} · {@code {metadata_equals: {k: v}}} ·
 * {@code {metadata_gt: {k: n}}}.
 */
public final class CepSpec {

  private CepSpec() {}

  @SuppressWarnings("unchecked")
  public static List<CepWiring> compile(List<Map<String, Object>> specs) {
    List<CepWiring> wirings = new ArrayList<>();
    if (specs == null) {
      return wirings;
    }
    for (Map<String, Object> s : specs) {
      String name = String.valueOf(s.getOrDefault("name", "cep"));
      Pattern pattern = buildPattern((List<Map<String, Object>>) s.get("pattern"),
          ((Number) s.getOrDefault("within", 0)).longValue());
      Function<Event, String> keyFn = keyFn(String.valueOf(s.getOrDefault("key", "conversation_id")));
      ToLongFunction<Event> tsFn = tsFn(s.get("ts") == null ? null : String.valueOf(s.get("ts")));
      CepWiring.Action action = action((Map<String, Object>) s.get("on_match"));
      wirings.add(new CepWiring(name, new CepMatcher(pattern), keyFn, tsFn, action));
    }
    return wirings;
  }

  private static Pattern buildPattern(List<Map<String, Object>> stages, long within) {
    if (stages == null || stages.isEmpty()) {
      throw new IllegalArgumentException("cep pattern needs at least one stage");
    }
    Pattern pattern = null;
    for (Map<String, Object> st : stages) {
      String stage = String.valueOf(st.getOrDefault("stage", "s"));
      Condition cond = condition(st.get("where"));
      if (pattern == null) {
        pattern = Pattern.begin(stage, cond);
      } else if ("next".equalsIgnoreCase(String.valueOf(st.getOrDefault("contiguity", "followedBy")))) {
        pattern = pattern.next(stage, cond);
      } else {
        pattern = pattern.followedBy(stage, cond);
      }
    }
    return pattern.within(within);
  }

  @SuppressWarnings("unchecked")
  static Condition condition(Object where) {
    if (where == null || "any".equals(where)) {
      return Condition.any();
    }
    if (where instanceof Map<?, ?> m) {
      if (m.containsKey("text_contains")) {
        List<String> needles = asList(m.get("text_contains"));
        return Condition.of(e -> e.text() != null && needles.stream().anyMatch(e.text()::contains));
      }
      if (m.containsKey("metadata_equals")) {
        Map<String, Object> kv = (Map<String, Object>) m.get("metadata_equals");
        return Condition.of(e -> kv.entrySet().stream()
            .allMatch(en -> String.valueOf(en.getValue()).equals(meta(e, en.getKey()))));
      }
      if (m.containsKey("metadata_gt")) {
        Map<String, Object> kv = (Map<String, Object>) m.get("metadata_gt");
        return Condition.of(e -> kv.entrySet().stream().allMatch(en -> {
          try {
            return Double.parseDouble(meta(e, en.getKey())) > ((Number) en.getValue()).doubleValue();
          } catch (RuntimeException ex) {
            return false;
          }
        }));
      }
    }
    throw new IllegalArgumentException("unknown cep where: " + where);
  }

  private static Function<Event, String> keyFn(String key) {
    if (key.startsWith("metadata.")) {
      String field = key.substring("metadata.".length());
      return e -> meta(e, field);
    }
    return Event::conversationId; // conversation_id / conversationId / default
  }

  private static ToLongFunction<Event> tsFn(String ts) {
    if (ts != null && ts.startsWith("metadata.")) {
      String field = ts.substring("metadata.".length());
      return e -> {
        String v = meta(e, field);
        try {
          return v == null ? 0L : Long.parseLong(v);
        } catch (NumberFormatException ex) {
          return 0L;
        }
      };
    }
    AtomicLong counter = new AtomicLong(); // no ts → monotonic arrival order
    return e -> counter.getAndIncrement();
  }

  @SuppressWarnings("unchecked")
  private static CepWiring.Action action(Map<String, Object> onMatch) {
    if (onMatch == null) {
      return (match, key, runtime, tools) -> { /* detect-only */ };
    }
    String kind = String.valueOf(onMatch.getOrDefault("kind", "submit"));
    if ("tool".equals(kind)) {
      String toolId = String.valueOf(onMatch.get("tool"));
      Map<String, Object> args = (Map<String, Object>) onMatch.getOrDefault("args", Map.of());
      return (match, key, runtime, tools) -> tools.execute(toolId, args);
    }
    if ("submit".equals(kind)) {
      String text = String.valueOf(onMatch.getOrDefault("text", "cep match"));
      return (match, key, runtime, tools) -> {
        String body = text.replace("{key}", key == null ? "" : key);
        runtime.submit(new Event(key, "cep", body, Map.of(CepWiring.DERIVED, "true")));
      };
    }
    throw new IllegalArgumentException("unknown cep on_match kind: " + kind);
  }

  private static String meta(Event e, String field) {
    return e.metadata() == null ? null : e.metadata().get(field);
  }

  private static List<String> asList(Object v) {
    List<String> out = new ArrayList<>();
    if (v instanceof List<?> l) {
      for (Object o : l) {
        out.add(String.valueOf(o));
      }
    } else if (v != null) {
      out.add(String.valueOf(v));
    }
    return out;
  }
}
