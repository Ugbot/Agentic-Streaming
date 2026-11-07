package org.jagentic.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.pipeline.WorkflowValidator.WorkflowValidationException;

/** Opens the journal a workflow declares as {@code stores.effects} (spec section 10). */
public final class EffectJournals {
  /** Marks a {@code memory} journal as acceptable under a {@code non_idempotent} tool. */
  public static final String PROCESS_LOCAL_OK = "x-process-local";

  private static final Map<String, EffectJournal> NAMED = new ConcurrentHashMap<>();
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final ObjectMapper CANONICAL = new ObjectMapper()
      .configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  private EffectJournals() {}

  /**
   * The journal of {@code spec}, or a fresh {@link EffectJournal.InMemory} when it declares none,
   * which outlives a runtime object built over it and nothing else. A store that cannot be reached
   * fails here unless the block says {@code on_unavailable: degrade}.
   */
  @SuppressWarnings("unchecked")
  public static EffectJournal open(Map<String, Object> spec) {
    Map<String, Object> store = declared(spec);
    if (store == null) {
      return new EffectJournal.InMemory();
    }
    String kind = String.valueOf(store.getOrDefault("kind", "memory"));
    try {
      return switch (kind) {
        case "memory" -> store.get("name") == null ? new EffectJournal.InMemory()
            : NAMED.computeIfAbsent(String.valueOf(store.get("name")), k -> new EffectJournal.InMemory());
        case "postgres" -> new org.jagentic.core.store.PostgresEffectJournal(
            String.valueOf(store.getOrDefault("jdbc_url", store.get("url"))),
            (String) store.get("user"), (String) store.get("password"));
        case "kafka" -> new org.jagentic.core.store.KafkaEffectJournal(
            String.valueOf(store.get("url")),
            String.valueOf(store.getOrDefault("topic", "agentic.effects")));
        default -> throw new WorkflowValidationException("stores.effects.kind",
            "journal kind " + kind + " is not available on the JVM (memory|postgres|kafka)");
      };
    } catch (EffectJournal.Unavailable e) {
      // Degrading is for tools that can stand to run again. A call that must happen once does not
      // run with nothing durable behind it, whatever the block says.
      if ("degrade".equals(store.get("on_unavailable")) && firstNonIdempotent(spec) == null) {
        return new EffectJournal.InMemory();
      }
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> declared(Map<String, Object> spec) {
    return spec.get("stores") instanceof Map<?, ?> stores
        && ((Map<String, Object>) stores).get("effects") instanceof Map<?, ?> effects
        ? (Map<String, Object>) effects : null;
  }

  /**
   * A runtime whose log rolls back must refuse a {@code non_idempotent} tool without a journal that
   * outlives the rollback (spec section 10): without one, a re-run is a second payment.
   *
   * @return the first such tool's id, or null when the workflow is safe to run
   */
  public static String unsafeForRollback(Map<String, Object> spec) {
    Map<String, Object> store = declared(spec);
    boolean outlives = store != null && (!"memory".equals(store.getOrDefault("kind", "memory"))
        || Boolean.TRUE.equals(store.get(PROCESS_LOCAL_OK)));
    return outlives ? null : firstNonIdempotent(spec);
  }

  @SuppressWarnings("unchecked")
  private static String firstNonIdempotent(Map<String, Object> spec) {
    if (!(spec.get("tools") instanceof List<?> tools)) {
      return null;
    }
    for (Object t : tools) {
      Map<String, Object> tool = (Map<String, Object>) t;
      if (ToolEffect.NON_IDEMPOTENT.equals(tool.get("effect"))) {
        return String.valueOf(tool.get("id"));
      }
    }
    return null;
  }

  /** {@code value} as JSON with every map's keys sorted, at any depth; the text of an unserializable value. */
  static String canonicalJson(Object value) {
    try {
      return CANONICAL.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      return String.valueOf(value);
    }
  }

  /** An entry as the durable stores hold it. */
  public static String toJson(EffectJournal.Entry e) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("conversation_id", e.key().conversationId());
    m.put("turn_id", e.key().turnId());
    m.put("kind", e.key().kind());
    m.put("index", e.key().index());
    m.put("intent", e.intent());
    m.put("effect", e.effect());
    m.put("state", e.state().name().toLowerCase());
    m.put("attempts", e.attempts().stream()
        .map(a -> Map.of("attempt", (Object) a.attempt(), "error", String.valueOf(a.error()))).toList());
    m.put("result", e.result());
    m.put("error", e.error());
    m.put("released", e.released());
    try {
      return JSON.writeValueAsString(m);
    } catch (JsonProcessingException notJson) {
      // The call has been made by now, so the outcome is recorded in the form that can be.
      m.put("result", String.valueOf(e.result()));
      try {
        return JSON.writeValueAsString(m);
      } catch (JsonProcessingException ex) {
        throw new IllegalArgumentException("a journal entry must be serializable", ex);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static EffectJournal.Entry fromJson(String json) {
    try {
      Map<String, Object> m = JSON.readValue(json, Map.class);
      List<EffectJournal.Attempt> attempts = ((List<Map<String, Object>>) m.get("attempts")).stream()
          .map(a -> new EffectJournal.Attempt(((Number) a.get("attempt")).intValue(), (String) a.get("error")))
          .toList();
      return new EffectJournal.Entry(
          new EffectJournal.Key((String) m.get("conversation_id"), (String) m.get("turn_id"),
              (String) m.get("kind"), ((Number) m.get("index")).intValue()),
          (String) m.get("intent"), (String) m.get("effect"),
          EffectJournal.State.valueOf(((String) m.get("state")).toUpperCase()),
          attempts, m.get("result"), (String) m.get("error"), Boolean.TRUE.equals(m.get("released")));
    } catch (JsonProcessingException ex) {
      throw new EffectJournal.Unavailable("unreadable journal entry", ex);
    }
  }
}
