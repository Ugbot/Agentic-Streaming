package org.agentic.flink.inference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses LLM tool-call argument payloads into the {@code Map<String, Object>} that {@link
 * org.agentic.flink.tools.ToolExecutor#execute} expects.
 *
 * <p>Nested objects become nested {@link Map}s and arrays become {@link List}s; numbers keep
 * their JSON numeric type. Values are never flattened or re-stringified.
 */
public final class ToolArguments {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
      new TypeReference<LinkedHashMap<String, Object>>() {};

  private ToolArguments() {}

  /** Parse a JSON object literal; a null/blank payload is an empty argument map. */
  public static Map<String, Object> parse(String json) {
    if (json == null || json.isBlank()) {
      return new LinkedHashMap<>();
    }
    try {
      Map<String, Object> parsed = MAPPER.readValue(json, MAP_TYPE);
      return parsed == null ? new LinkedHashMap<>() : parsed;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          "Tool arguments are not a JSON object: " + e.getOriginalMessage(), e);
    }
  }

  /**
   * Coerce an already-materialised argument payload: a JSON string is parsed, a map is copied,
   * anything else is rejected.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> coerce(Object arguments) {
    if (arguments == null) {
      return new LinkedHashMap<>();
    }
    if (arguments instanceof Map) {
      return new LinkedHashMap<>((Map<String, Object>) arguments);
    }
    if (arguments instanceof CharSequence) {
      return parse(arguments.toString());
    }
    throw new IllegalArgumentException(
        "Tool arguments must be a JSON string or a Map, got " + arguments.getClass().getName());
  }

  /** Serialise an argument map back to JSON, preserving nesting. */
  public static String toJson(Map<String, Object> arguments) {
    try {
      return MAPPER.writeValueAsString(arguments == null ? Map.of() : arguments);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Tool arguments are not JSON-serialisable", e);
    }
  }
}
