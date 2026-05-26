package org.agentic.flink.llm;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.Serializable;
import java.util.Objects;

/**
 * A typed contract for structured LLM output.
 *
 * <p>Wraps a target type plus an optional JSON Schema describing it. Implementations of {@link
 * ChatConnection} translate this to provider-specific structured-output features (OpenAI {@code
 * response_format=json_schema}, Ollama {@code format=json}, Anthropic tool-use schemas). On
 * providers without native support, the framework falls back to post-hoc parsing with retries.
 *
 * <p>The default {@link #of(Class)} constructs a schema by reflection on Jackson-deserializable
 * POJOs. Pass a hand-crafted {@code jsonSchema} string if you need finer control.
 */
public final class OutputSchema<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Class<T> type;
  private final String jsonSchema;
  private transient ObjectMapper mapper;

  private OutputSchema(Class<T> type, String jsonSchema) {
    this.type = Objects.requireNonNull(type, "type");
    this.jsonSchema = jsonSchema;
  }

  /** Build a schema by reflection over a Jackson-deserializable POJO. */
  public static <T> OutputSchema<T> of(Class<T> type) {
    return new OutputSchema<>(type, inferJsonSchema(type));
  }

  /** Build a schema with a hand-crafted JSON Schema string. */
  public static <T> OutputSchema<T> of(Class<T> type, String jsonSchema) {
    return new OutputSchema<>(type, jsonSchema);
  }

  public Class<T> getType() {
    return type;
  }

  public String getJsonSchema() {
    return jsonSchema;
  }

  /** Parse an LLM response into the schema's target type. */
  public T parse(String llmResponse) throws SchemaViolation {
    try {
      return mapper().readValue(extractJson(llmResponse), type);
    } catch (Exception e) {
      throw new SchemaViolation(
          "Failed to parse LLM response as " + type.getSimpleName() + ": " + e.getMessage(), e);
    }
  }

  private ObjectMapper mapper() {
    if (mapper == null) {
      mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.registerModule(new ParameterNamesModule());
      mapper.setVisibility(
          mapper
              .getSerializationConfig()
              .getDefaultVisibilityChecker()
              .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
              .withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
              .withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
              .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));
    }
    return mapper;
  }

  /**
   * Extract the JSON payload from an LLM response. Models often wrap JSON in markdown fences or
   * preamble text; we strip the obvious cases before parsing. Surgical, not clever.
   */
  private static String extractJson(String raw) {
    if (raw == null) return "{}";
    String s = raw.trim();
    int fence = s.indexOf("```");
    if (fence >= 0) {
      int start = s.indexOf('\n', fence);
      int end = s.lastIndexOf("```");
      if (start >= 0 && end > start) {
        s = s.substring(start + 1, end).trim();
      }
    }
    int braceStart = s.indexOf('{');
    int braceEnd = s.lastIndexOf('}');
    if (braceStart >= 0 && braceEnd > braceStart) {
      s = s.substring(braceStart, braceEnd + 1);
    }
    return s;
  }

  private static String inferJsonSchema(Class<?> type) {
    // Minimal reflective schema. We're not running a full schema generator — this is good
    // enough to send to providers that accept "any JSON" hints. Users wanting strict schemas
    // pass a hand-crafted one via OutputSchema.of(type, jsonSchema).
    StringBuilder b = new StringBuilder("{\"type\":\"object\",\"properties\":{");
    java.lang.reflect.Field[] fields = type.getDeclaredFields();
    boolean first = true;
    for (java.lang.reflect.Field f : fields) {
      if (java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
      if (!first) b.append(',');
      first = false;
      b.append('"').append(f.getName()).append("\":")
          .append(jsonTypeFor(f.getType()));
    }
    b.append("}}");
    return b.toString();
  }

  private static String jsonTypeFor(Class<?> c) {
    if (c == String.class) return "{\"type\":\"string\"}";
    if (c == int.class || c == Integer.class || c == long.class || c == Long.class
        || c == short.class || c == Short.class) return "{\"type\":\"integer\"}";
    if (c == float.class || c == Float.class || c == double.class || c == Double.class)
      return "{\"type\":\"number\"}";
    if (c == boolean.class || c == Boolean.class) return "{\"type\":\"boolean\"}";
    if (c.isArray() || java.util.Collection.class.isAssignableFrom(c))
      return "{\"type\":\"array\"}";
    return "{\"type\":\"object\"}";
  }

  /** Thrown when an LLM response cannot be parsed under this schema. */
  public static final class SchemaViolation extends Exception {
    private static final long serialVersionUID = 1L;

    public SchemaViolation(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
