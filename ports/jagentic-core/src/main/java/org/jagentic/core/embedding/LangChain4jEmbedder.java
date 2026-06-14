package org.jagentic.core.embedding;

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.ollama.OllamaEmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;

/** Real embeddings via LangChain4J (Ollama / OpenAI). Optional dep — only constructed
 * when a real embedder is requested. */
public final class LangChain4jEmbedder implements Embedder {

  private final EmbeddingModel model;
  private int dim;

  private LangChain4jEmbedder(EmbeddingModel model) {
    this.model = model;
  }

  /** provider = "ollama" | "openai". */
  public static LangChain4jEmbedder create(String provider, String modelName, String baseUrl) {
    EmbeddingModel m;
    if ("openai".equals(provider)) {
      OpenAiEmbeddingModel.OpenAiEmbeddingModelBuilder b = OpenAiEmbeddingModel.builder()
          .apiKey(envOr("OPENAI_API_KEY", "no-key"))
          .modelName(modelName == null || modelName.isBlank() ? "text-embedding-3-small" : modelName);
      if (baseUrl != null && !baseUrl.isBlank()) {
        b.baseUrl(baseUrl);
      }
      m = b.build();
    } else {
      m = OllamaEmbeddingModel.builder()
          .baseUrl(baseUrl == null || baseUrl.isBlank() ? "http://localhost:11434" : baseUrl)
          .modelName(modelName == null || modelName.isBlank() ? "nomic-embed-text" : modelName)
          .build();
    }
    return new LangChain4jEmbedder(m);
  }

  private static String envOr(String key, String fallback) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? fallback : v;
  }

  @Override
  public float[] embed(String text) {
    float[] v = model.embed(text).content().vector();
    if (dim == 0) {
      dim = v.length;
    }
    return v;
  }

  @Override
  public int dim() {
    if (dim == 0) {
      embed("dimension probe");
    }
    return dim;
  }
}
