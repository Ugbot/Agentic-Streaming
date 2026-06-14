package org.agentic.flink.embedding;

import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * {@link EmbeddingConnection} for OpenAI embeddings (default model {@code text-embedding-3-small},
 * 1536-dim) via LangChain4J's {@code OpenAiEmbeddingModel} with {@code OPENAI_API_KEY} auth.
 *
 * <p>The semantic alternative to the keyword KB search when only an OpenAI key is available (the
 * hackathon mandates {@code gemini-embedding-001} for marked runs; this is a dev-parity option). The
 * {@link EmbeddingSetup} carries the model name and dimensionality — keep it at the model's native
 * size unless you also reduce it via the model's {@code dimensions}.
 */
public final class OpenAIEmbeddingConnection implements EmbeddingConnection {
  private static final long serialVersionUID = 1L;

  private final String apiKey;
  private final Duration timeout;

  public OpenAIEmbeddingConnection(String apiKey) {
    this(apiKey, Duration.ofSeconds(60));
  }

  public OpenAIEmbeddingConnection(String apiKey, Duration timeout) {
    this.apiKey = apiKey;
    this.timeout = timeout == null ? Duration.ofSeconds(60) : timeout;
  }

  @Override
  public EmbeddingClient bind(RuntimeContext runtimeContext) {
    return new Client(apiKey, timeout);
  }

  @Override
  public String providerName() {
    return "openai";
  }

  /** Transient runtime handle — the LangChain4J model is rebuilt per task in bind(). */
  private static final class Client implements EmbeddingClient {
    private final String apiKey;
    private final Duration timeout;
    private volatile OpenAiEmbeddingModel model;

    Client(String apiKey, Duration timeout) {
      this.apiKey = apiKey;
      this.timeout = timeout;
    }

    private OpenAiEmbeddingModel model(EmbeddingSetup setup) {
      if (model == null) {
        synchronized (this) {
          if (model == null) {
            model =
                OpenAiEmbeddingModel.builder()
                    .apiKey(apiKey)
                    .modelName(setup.getModelName())
                    .timeout(timeout)
                    .build();
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
      List<dev.langchain4j.data.segment.TextSegment> segs = new java.util.ArrayList<>(texts.size());
      for (String t : texts) {
        segs.add(dev.langchain4j.data.segment.TextSegment.from(t));
      }
      List<dev.langchain4j.data.embedding.Embedding> embs = model(setup).embedAll(segs).content();
      List<float[]> out = new java.util.ArrayList<>(embs.size());
      for (dev.langchain4j.data.embedding.Embedding e : embs) {
        float[] v = e.vector();
        if (setup.shouldNormalize()) {
          normalize(v);
        }
        out.add(v);
      }
      return out;
    }

    @Override
    public String providerName() {
      return "openai";
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
