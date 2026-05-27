package org.agentic.flink.embedding;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Deterministic, zero-infrastructure {@link EmbeddingConnection}.
 *
 * <p>Derives a fixed-length vector from a SHA-256 digest of the input text. The same text always
 * produces the same vector, and similar (identical) texts collide — this is NOT a semantic
 * embedder. It exists so tests, examples, and local development can exercise the ingest → store →
 * search → answer path with no embedding server. For real semantic search use
 * {@link OllamaEmbeddingConnection} or {@code DjlEmbeddingConnection}.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}; provider name {@code "hash"}. The vector
 * dimension comes from the {@link EmbeddingSetup} at each call.
 */
public final class HashEmbeddingConnection implements EmbeddingConnection {
  private static final long serialVersionUID = 1L;

  @Override
  public EmbeddingClient bind(RuntimeContext runtimeContext) {
    return new Client();
  }

  @Override
  public String providerName() {
    return "hash";
  }

  /** Stateless deterministic embedder. Serializable so it can be held by operators directly. */
  private static final class Client implements EmbeddingClient, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public String providerName() {
      return "hash";
    }

    @Override
    public float[] embed(String text, EmbeddingSetup setup) {
      int dim = setup.getDimension();
      float[] vector = new float[dim];
      byte[] hash = sha256(text == null ? "" : text);
      for (int i = 0; i < dim; i++) {
        // Normalize each hash byte to [0, 1]; cycle through the digest for dims > 32.
        vector[i] = (hash[i % hash.length] & 0xFF) / 255.0f;
      }
      if (setup.shouldNormalize()) {
        double norm = 0;
        for (float v : vector) norm += v * v;
        norm = Math.sqrt(norm);
        if (norm > 0) {
          for (int i = 0; i < dim; i++) vector[i] /= (float) norm;
        }
      }
      return vector;
    }

    private static byte[] sha256(String input) {
      try {
        return MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("SHA-256 not available", e);
      }
    }
  }
}
