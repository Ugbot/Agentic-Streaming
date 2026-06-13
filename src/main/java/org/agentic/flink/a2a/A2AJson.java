package org.agentic.flink.a2a;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * The canonical Jackson {@link ObjectMapper} for A2A wire types.
 *
 * <p>The A2A value model and bridge envelopes ({@link A2ATask}, {@link A2AMessage}, {@link
 * org.agentic.flink.a2a.bridge.A2ARequest}, …) use immutable, all-args constructors. Binding JSON
 * back into them requires the {@link ParameterNamesModule} (the project already depends on it and
 * compiles with {@code -parameters}). Centralizing the configured mapper here — rather than calling
 * {@code new ObjectMapper()} ad hoc — means the bridge ({@code A2ABridge}), the gateway, and tests
 * all serialize identically, mirroring the {@code ParameterNamesModule} setup in {@code
 * PostgresConversationStore}.
 */
public final class A2AJson {

  private static final ObjectMapper MAPPER = create();

  private A2AJson() {}

  /** A fresh, fully-configured mapper. Use {@link #mapper()} unless you need to customize a copy. */
  public static ObjectMapper create() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new ParameterNamesModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    // Drive both directions off fields, not getters, so property names equal the constructor
    // parameter names the ParameterNamesModule binds against. This avoids boolean-getter renaming
    // (e.g. isFinal() -> "final") that would otherwise fail to bind the "isFinal" constructor arg.
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
