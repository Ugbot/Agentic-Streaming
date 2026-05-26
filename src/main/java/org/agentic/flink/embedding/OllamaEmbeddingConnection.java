package org.agentic.flink.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.agentic.flink.config.ConfigKeys;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link EmbeddingConnection} that talks to a local or remote Ollama service.
 *
 * <p>Uses the {@code POST /api/embeddings} endpoint. The connection holds the HTTP client and
 * base URL; the {@link EmbeddingSetup} carries the model name (e.g. {@code
 * nomic-embed-text:latest}, {@code mxbai-embed-large}).
 */
public final class OllamaEmbeddingConnection implements EmbeddingConnection {
  private static final long serialVersionUID = 1L;

  private final String baseUrl;
  private final Duration timeout;

  /** No-arg constructor for {@link java.util.ServiceLoader}; targets default Ollama URL. */
  public OllamaEmbeddingConnection() {
    this(ConfigKeys.DEFAULT_OLLAMA_BASE_URL, Duration.ofSeconds(30));
  }

  public OllamaEmbeddingConnection(String baseUrl) {
    this(baseUrl, Duration.ofSeconds(30));
  }

  public OllamaEmbeddingConnection(String baseUrl, Duration timeout) {
    this.baseUrl = baseUrl;
    this.timeout = timeout == null ? Duration.ofSeconds(30) : timeout;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public Duration getTimeout() {
    return timeout;
  }

  @Override
  public EmbeddingClient bind(RuntimeContext runtimeContext) {
    return new Client(baseUrl, timeout);
  }

  @Override
  public String providerName() {
    return "ollama";
  }

  /** Transient runtime handle — not serialized; reconstructed per task in bind(). */
  private static final class Client implements EmbeddingClient {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private final String baseUrl;
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final Duration timeout;

    Client(String baseUrl, Duration timeout) {
      this.baseUrl = baseUrl;
      this.timeout = timeout;
      this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
      this.mapper = new ObjectMapper();
    }

    @Override
    public float[] embed(String text, EmbeddingSetup setup) {
      try {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("model", setup.getModelName());
        payload.put("prompt", text);

        HttpRequest req =
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/embeddings"))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(payload)))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
          throw new RuntimeException(
              "Ollama embeddings call failed: " + resp.statusCode() + " " + resp.body());
        }

        JsonNode root = mapper.readTree(resp.body());
        JsonNode embedding = root.get("embedding");
        if (embedding == null || !embedding.isArray()) {
          throw new RuntimeException("Ollama response missing 'embedding' array: " + resp.body());
        }

        int dim = embedding.size();
        if (setup.getDimension() != dim) {
          LOG.warn(
              "Embedding dimension mismatch for model {}: setup says {}, server returned {}",
              setup.getModelName(), setup.getDimension(), dim);
        }
        float[] out = new float[dim];
        for (int i = 0; i < dim; i++) {
          out[i] = (float) embedding.get(i).asDouble();
        }
        if (setup.shouldNormalize()) {
          normalize(out);
        }
        return out;
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to embed via Ollama at " + baseUrl + ": " + e.getMessage(), e);
      }
    }

    @Override
    public List<float[]> embedBatch(List<String> texts, EmbeddingSetup setup) {
      // Ollama doesn't batch natively — fall back to per-call. Sequential to keep ordering
      // deterministic; callers wanting parallelism should wrap with their own executor.
      return EmbeddingClient.super.embedBatch(texts, setup);
    }

    @Override
    public String providerName() {
      return "ollama";
    }

    private static void normalize(float[] v) {
      double sum = 0.0;
      for (float f : v) sum += f * f;
      double norm = Math.sqrt(sum);
      if (norm == 0.0) return;
      for (int i = 0; i < v.length; i++) {
        v[i] = (float) (v[i] / norm);
      }
    }
  }
}
