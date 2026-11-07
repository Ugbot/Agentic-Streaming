package org.agentic.flink.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.agentic.flink.annotation.Internal;
import org.agentic.flink.typeinfo.FlinkJson;
import org.jagentic.core.TurnResult;

/**
 * Encodes a {@link TurnResult} as its normalized {@code spec/v1/result.schema.json} document and
 * decodes it back. The wire form <em>is</em> the spec shape, so a Flink sink that writes these
 * bytes emits conformance-comparable results without a second mapping step.
 */
@Internal
public final class TurnResultCodec {
  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

  private TurnResultCodec() {}

  private static ObjectMapper json() {
    return FlinkJson.mapper();
  }

  public static byte[] encode(TurnResult r) throws IOException {
    return json().writeValueAsBytes(r.toMap());
  }

  public static TurnResult decode(byte[] bytes) throws IOException {
    return fromMap(json().readValue(bytes, MAP));
  }

  /** Inverse of {@link TurnResult#toMap()}. */
  @SuppressWarnings("unchecked")
  public static TurnResult fromMap(Map<String, Object> m) {
    return TurnResult.fromMap(m);
  }
}
