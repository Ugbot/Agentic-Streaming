package org.jagentic.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One entry of the workflow's {@code timers} list ({@code spec/v1/workflow.schema.json}): scheduled
 * per conversation on its first turn, {@code afterMs} past the chosen clock's reading, firing the
 * named tool with {@code payload} on the first later turn delivered at or past the deadline.
 */
public record TimerSpec(String id, long afterMs, Clock clock, String tool, Map<String, Object> payload)
    implements Serializable {

  /** Which clock a timer reads: the runtime's logical processing clock or the conversation watermark. */
  public enum Clock {
    PROCESSING("processing"), EVENT("event");

    private final String wire;

    Clock(String wire) {
      this.wire = wire;
    }

    public String wire() {
      return wire;
    }

    public static Clock parse(Object o) {
      if (o == null) {
        return PROCESSING;
      }
      String s = String.valueOf(o);
      for (Clock c : values()) {
        if (c.wire.equals(s)) {
          return c;
        }
      }
      throw new IllegalArgumentException("unknown timer clock " + s + " (processing|event)");
    }
  }

  public TimerSpec {
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("timer requires an id");
    }
    if (afterMs < 0) {
      throw new IllegalArgumentException("timer " + id + ": after_ms must be >= 0");
    }
    clock = clock == null ? Clock.PROCESSING : clock;
    payload = payload == null ? Map.of()
        : Collections.unmodifiableMap(new LinkedHashMap<>(payload));
  }

  /** Parses the workflow's {@code timers} list; null or empty yields no timers. */
  @SuppressWarnings("unchecked")
  public static List<TimerSpec> fromSpecs(Object timers) {
    if (!(timers instanceof List<?> list) || list.isEmpty()) {
      return List.of();
    }
    List<TimerSpec> out = new ArrayList<>(list.size());
    for (Object o : list) {
      Map<String, Object> t = (Map<String, Object>) o;
      Object tool = t.get("tool");
      out.add(new TimerSpec(String.valueOf(t.get("id")), ((Number) t.get("after_ms")).longValue(),
          Clock.parse(t.get("clock")), tool == null ? null : String.valueOf(tool),
          (Map<String, Object>) t.get("payload")));
    }
    return List.copyOf(out);
  }
}
