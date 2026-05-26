package org.agentic.flink.python;

import java.util.Base64;

/**
 * Encoding helpers for cloudpickle payloads carried in an {@code AgentPlan}.
 *
 * <p>Cloudpickle bytes themselves cannot be decoded purely from Java — unpickling Python closures
 * requires the Python runtime. This class only handles the base64 envelope used to put the bytes
 * inside JSON. {@link PythonExecutor#register(String)} does the actual deserialization inside the
 * embedded interpreter.
 */
public final class CloudpickleCodec {

  private CloudpickleCodec() {}

  public static String encode(byte[] cloudpickleBytes) {
    if (cloudpickleBytes == null) {
      throw new IllegalArgumentException("cloudpickleBytes must not be null");
    }
    return Base64.getEncoder().encodeToString(cloudpickleBytes);
  }

  public static byte[] decode(String b64) {
    if (b64 == null || b64.isEmpty()) {
      throw new IllegalArgumentException("base64 cloudpickle string must be non-empty");
    }
    return Base64.getDecoder().decode(b64);
  }
}
