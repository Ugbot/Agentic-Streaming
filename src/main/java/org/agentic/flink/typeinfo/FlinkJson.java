package org.agentic.flink.typeinfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * The canonical Jackson {@link ObjectMapper} used by {@link JsonTypeInfo} to serialize framework
 * value types through Flink as JSON instead of falling back to Kryo.
 *
 * <p>Configured to round-trip the project's value classes regardless of shape:
 *
 * <ul>
 *   <li>{@link ParameterNamesModule} — binds immutable, all-args-constructor types (the project
 *       compiles with {@code -parameters}); the A2A envelopes and {@code ChatMessage} rely on this.
 *   <li>Field visibility {@code ANY} (getters/setters off) — property names equal field/constructor
 *       parameter names, avoiding boolean-getter renaming (e.g. {@code isFinal()} → {@code "final"}).
 *   <li>{@code ALLOW_FINAL_FIELDS_AS_MUTATORS} — lets field-access deserialization populate final
 *       fields (e.g. {@code RoutingBudget}'s caps and its {@code ArrayDeque}) when a class is
 *       reconstructed via a no-arg creator rather than an all-args constructor.
 *   <li>Lenient on unknown/empty — tolerant across value-class evolution.
 * </ul>
 *
 * <p>{@link org.agentic.flink.a2a.A2AJson} delegates to this so there is a single mapper config.
 */
public final class FlinkJson {

  private static final ObjectMapper MAPPER = create();

  private FlinkJson() {}

  /** A fresh, fully-configured mapper. Use {@link #mapper()} unless you need to customize a copy. */
  public static ObjectMapper create() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new ParameterNamesModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, true);
    mapper.setVisibility(
        mapper
            .getSerializationConfig()
            .getDefaultVisibilityChecker()
            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
            .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withCreatorVisibility(JsonAutoDetect.Visibility.ANY));
    return mapper;
  }

  /** The shared, thread-safe mapper instance. */
  public static ObjectMapper mapper() {
    return MAPPER;
  }
}
