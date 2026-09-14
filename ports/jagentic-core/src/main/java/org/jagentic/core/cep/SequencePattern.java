package org.jagentic.core.cep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.jagentic.core.pipeline.WorkflowValidator;

/**
 * One {@code agentic/v1} sequence pattern (a {@code cep:} entry with {@code on_match.kind: tool}),
 * compiled from the workflow document. Matching is a pure fold over the conversation's
 * {@code turn_received} events ({@link #completesOn}); nothing about a partial match is stored
 * anywhere, so replaying the log always yields the same matches. This is a direct port of the
 * reference runtime's {@code _matches_on_last_turn}:
 *
 * <ul>
 *   <li>stages match in declaration order against the turn text ({@code where.text_contains},
 *       case-insensitive); a stage without {@code where} matches every turn;</li>
 *   <li>a stage with {@code contiguity: next} (the default) must match the turn right after the
 *       previous stage's turn, otherwise the partial match is dropped and that turn is consumed;
 *       {@code followedBy} skips non-matching turns;</li>
 *   <li>{@code within} bounds the event-time span from the first matched turn, read from the
 *       metadata key named by {@code ts}; a turn outside the window drops the partial match and
 *       may itself start a new one;</li>
 *   <li>a completed match consumes its turns, so matches never overlap.</li>
 * </ul>
 */
public final class SequencePattern implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The {@code on_match.kind} the turn graph evaluates in-turn. */
  public static final String TOOL_KIND = "tool";

  public enum Contiguity { NEXT, FOLLOWED_BY }

  /** One stage of the pattern; {@code textContains} is null for a stage without {@code where}. */
  public record Stage(String name, String textContains, Contiguity contiguity) implements Serializable {
    public boolean matches(String text) {
      return textContains == null
          || (text == null ? "" : text).toLowerCase(Locale.ROOT).contains(textContains.toLowerCase(Locale.ROOT));
    }
  }

  /** What the fold sees of one turn: the {@code turn_received} payload of the log. */
  public record Turn(String turnId, String text, Map<String, String> metadata) implements Serializable {
    public Turn {
      metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }

    /** Rebuilds a turn from a {@code turn_received} payload ({@code text}, optional {@code metadata}). */
    @SuppressWarnings("unchecked")
    public static Turn fromReceived(Map<String, Object> payload) {
      Map<String, String> meta = new java.util.LinkedHashMap<>();
      if (payload.get("metadata") instanceof Map<?, ?> m) {
        for (Map.Entry<?, ?> e : ((Map<Object, Object>) m).entrySet()) {
          meta.put(String.valueOf(e.getKey()), e.getValue() == null ? null : String.valueOf(e.getValue()));
        }
      }
      Object text = payload.get("text");
      return new Turn(String.valueOf(payload.get("turn_id")), text == null ? null : String.valueOf(text), meta);
    }
  }

  private final String name;
  private final String tsKey; // metadata key named by ts, or null
  private final Long withinMs; // null when unbounded
  private final List<Stage> stages;
  private final String tool;

  public SequencePattern(String name, String tsKey, Long withinMs, List<Stage> stages, String tool) {
    if (stages == null || stages.isEmpty()) {
      throw new WorkflowValidator.WorkflowValidationException("cep[" + name + "].pattern",
          "a sequence pattern needs at least one stage");
    }
    if (withinMs != null && tsKey == null) {
      throw new WorkflowValidator.WorkflowValidationException("cep[" + name + "].within",
          "within requires ts");
    }
    this.name = name;
    this.tsKey = tsKey;
    this.withinMs = withinMs;
    this.stages = List.copyOf(stages);
    this.tool = tool;
  }

  public String name() {
    return name;
  }

  /** The metadata key that carries event time ({@code ts: metadata.<key>}), or null. */
  public String tsKey() {
    return tsKey;
  }

  public Long withinMs() {
    return withinMs;
  }

  public List<Stage> stages() {
    return stages;
  }

  /** The tool invoked on the completing turn. */
  public String tool() {
    return tool;
  }

  /**
   * Compiles the {@code cep:} entries the turn graph evaluates: those with {@code on_match.kind:
   * tool}. Entries of other kinds (or without {@code on_match}) are left to the stream-level
   * wiring and skipped here.
   */
  @SuppressWarnings("unchecked")
  public static List<SequencePattern> compile(List<Map<String, Object>> specs) {
    List<SequencePattern> out = new ArrayList<>();
    if (specs == null) {
      return out;
    }
    for (Map<String, Object> s : specs) {
      if (hasToolAction(s)) {
        out.add(fromSpec(s));
      }
    }
    return Collections.unmodifiableList(out);
  }

  /** The {@code cep:} entries that are not {@link #compile compiled} into the turn graph. */
  public static List<Map<String, Object>> withoutToolActions(List<Map<String, Object>> specs) {
    List<Map<String, Object>> out = new ArrayList<>();
    if (specs != null) {
      for (Map<String, Object> s : specs) {
        if (!hasToolAction(s)) {
          out.add(s);
        }
      }
    }
    return out;
  }

  /** The {@code cep:} entries {@link #compile} turns into in-turn patterns. */
  public static List<Map<String, Object>> toolActions(List<Map<String, Object>> specs) {
    List<Map<String, Object>> out = new ArrayList<>();
    if (specs != null) {
      for (Map<String, Object> s : specs) {
        if (hasToolAction(s)) {
          out.add(s);
        }
      }
    }
    return out;
  }

  private static boolean hasToolAction(Map<String, Object> spec) {
    return spec.get("on_match") instanceof Map<?, ?> m && TOOL_KIND.equals(m.get("kind"));
  }

  @SuppressWarnings("unchecked")
  public static SequencePattern fromSpec(Map<String, Object> s) {
    String name = String.valueOf(s.get("name"));
    String where = "cep[" + name + "]";
    Object key = s.getOrDefault("key", "conversation_id");
    if (!"conversation_id".equals(key)) {
      throw new WorkflowValidator.WorkflowValidationException(where + ".key",
          "sequence patterns are keyed by conversation_id, got " + key);
    }
    String tsKey = null;
    if (s.get("ts") != null) {
      String ts = String.valueOf(s.get("ts"));
      if (!ts.startsWith("metadata.") || ts.length() == "metadata.".length()) {
        throw new WorkflowValidator.WorkflowValidationException(where + ".ts",
            "ts must be metadata.<key>, got " + ts);
      }
      tsKey = ts.substring("metadata.".length());
    }
    Long within = s.get("within") == null ? null : ((Number) s.get("within")).longValue();
    List<Stage> stages = new ArrayList<>();
    for (Map<String, Object> st : (List<Map<String, Object>>) s.getOrDefault("pattern", List.of())) {
      String needle = null;
      if (st.get("where") instanceof Map<?, ?> w && w.get("text_contains") != null) {
        needle = String.valueOf(w.get("text_contains"));
      }
      Contiguity contiguity = "followedBy".equals(st.getOrDefault("contiguity", "next"))
          ? Contiguity.FOLLOWED_BY : Contiguity.NEXT;
      stages.add(new Stage(String.valueOf(st.getOrDefault("stage", "s" + stages.size())), needle, contiguity));
    }
    Map<String, Object> onMatch = (Map<String, Object>) s.get("on_match");
    Object tool = onMatch == null ? null : onMatch.get("tool");
    if (tool == null) {
      throw new WorkflowValidator.WorkflowValidationException(where + ".on_match.tool",
          "on_match.kind tool needs a tool id");
    }
    return new SequencePattern(name, tsKey, within, stages, String.valueOf(tool));
  }

  /**
   * The fold: whether a match completes on the last of {@code turns} (this conversation's
   * {@code turn_received} turns in log order, the current turn last).
   */
  public boolean completesOn(List<Turn> turns) {
    int matchedAt = -1;
    int stage = 0;
    long startTs = 0;
    for (int i = 0; i < turns.size(); i++) {
      Turn turn = turns.get(i);
      if (stage > 0 && withinMs != null && timestamp(turn) - startTs > withinMs) {
        stage = 0;
      }
      Stage current = stages.get(stage);
      if (current.matches(turn.text())) {
        if (stage == 0 && tsKey != null) {
          startTs = timestamp(turn);
        }
        stage++;
        if (stage == stages.size()) {
          matchedAt = i;
          stage = 0;
        }
      } else if (stage > 0 && current.contiguity() == Contiguity.NEXT) {
        stage = 0;
      }
    }
    return !turns.isEmpty() && matchedAt == turns.size() - 1;
  }

  /** The event time of a turn under this pattern's {@code ts}. */
  public long timestamp(Turn turn) {
    String raw = turn.metadata().get(tsKey);
    if (raw == null) {
      throw new IllegalArgumentException("turn " + turn.turnId() + " lacks metadata." + tsKey
          + " needed by cep pattern " + name);
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("turn " + turn.turnId() + " metadata." + tsKey
          + " is not an integer: " + raw);
    }
  }

  @Override
  public String toString() {
    return "SequencePattern[" + name + ", stages=" + stages.size() + ", within=" + withinMs + "]";
  }
}
