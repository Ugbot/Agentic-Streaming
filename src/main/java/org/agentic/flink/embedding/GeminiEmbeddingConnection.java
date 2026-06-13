package org.agentic.flink.embedding;

import dev.langchain4j.model.googleai.GoogleAiEmbeddingModel;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * {@link EmbeddingConnection} for Google Gemini embeddings (default model {@code
 * gemini-embedding-001}), via LangChain4J's {@code GoogleAiEmbeddingModel} with API-key auth
 * ({@code GOOGLE_API_KEY}).
 *
 * <p>This is the embedder the A2A hackathon mandates for marked runs; it's a config-only swap for
 * the local DJL embedder used in development. The {@link EmbeddingSetup} carries the model name and
 * the output dimensionality (gemini-embedding-001 supports reduced dimensions — set it to match the
 * vector index).
 */
public final class GeminiEmbeddingConnection implements EmbeddingConnection {
  private static final long serialVersionUID = 1L;

  private final String apiKey;
  private final Duration timeout;

  public GeminiEmbeddingConnection(String apiKey) {
    this(apiKey, Duration.ofSeconds(60));
  }

  public GeminiEmbeddingConnection(String apiKey, Duration timeout) {
    this.apiKey = apiKey;
    this.timeout = timeout == null ? Duration.ofSeconds(60) : timeout;
  }

  @Override
  public EmbeddingClient bind(RuntimeContext runtimeContext) {
    return new Client(apiKey, timeout);
  }

  @Override
  public String providerName() {
    return "gemini";
  }

  /** Transient runtime handle — the LangChain4J model is rebuilt per task in bind(). */
  private static final class Client implements EmbeddingClient {
    private final String apiKey;
    private final Duration timeout;
    private volatile GoogleAiEmbeddingModel model;
    private volatile int builtDim = -1;

    Client(String apiKey, Duration timeout) {
      this.apiKey = apiKey;
      this.timeout = timeout;
    }

    private GoogleAiEmbeddingModel model(EmbeddingSetup setup) {
      if (model == null || builtDim != setup.getDimension()) {
        synchronized (this) {
          if (model == null || builtDim != setup.getDimension()) {
            model =
                GoogleAiEmbeddingModel.builder()
                    .apiKey(apiKey)
                    .modelName(setup.getModelName())
                    .outputDimensionality(setup.getDimension())
                    .timeout(timeout)
                    .build();
            builtDim = setup.getDimension();
          }
        }
      }
      return model;
    }

    @Override
    public float[] embed(String text, EmbeddingSetup setup) {
      float[] vec = model(setup).embed(text).content().vector();
      if (setup.shouldNormalize()) {
        normalize(vec);
      }
      return vec;
    }

    @Override
    public List<float[]> embedBatch(List<String> texts, EmbeddingSetup setup) {
      return EmbeddingClient.super.embedBatch(texts, setup);
    }

    @Override
    public String providerName() {
      return "gemini";
    }

    private static void normalize(float[] v) {
      double sum = 0.0;
      for (float f : v) {
        sum += f * f;
      }
      double norm = Math.sqrt(sum);
      if (norm == 0.0) {
        return;
      }
      for (int i = 0; i < v.length; i++) {
        v[i] = (float) (v[i] / norm);
      }
    }
  }
}
