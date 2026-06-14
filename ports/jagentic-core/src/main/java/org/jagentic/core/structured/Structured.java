package org.jagentic.core.structured;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Structured output — validate an LLM's final answer against a small JSON-schema-lite
 * contract ({@code {type, required, properties}}). Portable analogue of the Flink
 * {@code OutputSchema}.
 */
public final class Structured {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Structured() {}

  /** Validation result: the parsed value (or null) + a list of errors (empty == valid). */
  public record Result(Map<String, Object> value, List<String> errors) {
    public boolean ok() {
      return errors.isEmpty();
    }
  }

  /** Validate a value against a schema; returns a list of error strings. */
  @SuppressWarnings("unchecked")
  public static List<String> validate(Object value, Map<String, Object> schema) {
    List<String> errors = new ArrayList<>();
    String expected = (String) schema.getOrDefault("type", "object");
    if (!typeMatches(expected, value)) {
      errors.add("expected " + expected + ", got " + (value == null ? "null" : value.getClass().getSimpleName()));
      return errors;
    }
    if ("object".equals(expected) && value instanceof Map<?, ?> obj) {
      for (Object req : (List<Object>) schema.getOrDefault("required", List.of())) {
        if (!obj.containsKey(req)) {
          errors.add("missing required field " + req);
        }
      }
      Map<String, Object> props = (Map<String, Object>) schema.get("properties");
      if (props != null) {
        for (Map.Entry<String, Object> e : props.entrySet()) {
          if (obj.containsKey(e.getKey()) && e.getValue() instanceof Map) {
            for (String err : validate(((Map<?, ?>) obj).get(e.getKey()), (Map<String, Object>) e.getValue())) {
              errors.add(e.getKey() + "." + err);
            }
          }
        }
      }
    }
    return errors;
  }

  private static boolean typeMatches(String expected, Object v) {
    return switch (expected) {
      case "string" -> v instanceof String;
      case "number", "integer" -> v instanceof Number;
      case "boolean" -> v instanceof Boolean;
      case "array" -> v instanceof List;
      case "object" -> v instanceof Map;
      default -> true;
    };
  }

  /** Parse {@code text} as one JSON object (tolerant of prose) and validate it. */
  @SuppressWarnings("unchecked")
  public static Result parse(String text, Map<String, Object> schema) {
    String s = text == null ? "" : text.strip();
    int start = s.indexOf('{'), end = s.lastIndexOf('}');
    if (start == -1 || end <= start) {
      return new Result(null, List.of("no JSON object found in output"));
    }
    try {
      Map<String, Object> obj = MAPPER.readValue(s.substring(start, end + 1), Map.class);
      return new Result(obj, validate(obj, schema));
    } catch (Exception e) {
      return new Result(null, List.of("invalid JSON: " + e.getMessage()));
    }
  }

  /** A system-prompt fragment telling the model to answer with conforming JSON. */
  public static String schemaInstruction(Map<String, Object> schema) {
    try {
      return "Respond with a single JSON object conforming to this schema: "
          + MAPPER.writeValueAsString(schema);
    } catch (Exception e) {
      return "Respond with a single JSON object.";
    }
  }
}
