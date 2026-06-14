package org.jagentic.core.embedding;

import java.util.Map;

/** Factory: build an {@link Embedder} from a {@code {kind|provider, dim, model, base_url}}
 * spec (the YAML {@code embeddings} section). kind = hashing (default) | ollama | openai. */
public final class Embedders {

  private Embedders() {}

  public static Embedder make(Map<String, Object> spec) {
    if (spec == null) {
      return new HashingEmbedder(256);
    }
    String kind = str(spec.getOrDefault("kind", spec.getOrDefault("provider", "hashing")));
    switch (kind) {
      case "hashing":
      case "memory":
        Object d = spec.get("dim");
        return new HashingEmbedder(d instanceof Number ? ((Number) d).intValue() : 256);
      case "ollama":
      case "openai":
      case "langchain4j":
        String provider = "langchain4j".equals(kind) ? str(spec.get("provider")) : kind;
        return LangChain4jEmbedder.create(provider, str(spec.get("model")), str(spec.get("base_url")));
      default:
        throw new IllegalArgumentException("unknown embedder kind " + kind + "; choose hashing|ollama|openai");
    }
  }

  private static String str(Object o) {
    return o == null ? null : o.toString();
  }
}
