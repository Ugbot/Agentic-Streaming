package org.agentic.flink.a2a;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.agentic.flink.typeinfo.FlinkJson;

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

  private A2AJson() {}

  /**
   * A fresh, fully-configured mapper. Delegates to {@link FlinkJson#create()} so the A2A wire codec
   * and the framework's {@link org.agentic.flink.typeinfo.JsonTypeInfo} share one configuration.
   */
  public static ObjectMapper create() {
    return FlinkJson.create();
  }

  /** The shared, thread-safe mapper instance (the same one {@link FlinkJson#mapper()} returns). */
  public static ObjectMapper mapper() {
    return FlinkJson.mapper();
  }
}
