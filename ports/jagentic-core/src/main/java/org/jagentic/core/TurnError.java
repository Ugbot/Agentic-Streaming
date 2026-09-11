package org.jagentic.core;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** The {@code error} object of a normalized turn result. */
public record TurnError(ErrorClass errorClass, String message) implements Serializable {

  /** The v1 error classes (spec/v1/primitives.md §5). */
  public enum ErrorClass {
    VALIDATION, GUARDRAIL, VERIFICATION, TOOL, TRANSIENT, FATAL;

    public String wire() {
      return name().toLowerCase();
    }

    public static ErrorClass parse(String wire) {
      return valueOf(wire.toUpperCase());
    }
  }

  public Map<String, Object> toMap() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("class", errorClass.wire());
    m.put("message", message == null ? "" : message);
    return m;
  }
}
