package org.jagentic.core.embedding;

import java.util.List;

/**
 * Embedder SPI — turn text into a vector. The model-free {@link HashingEmbedder} is the
 * default (deterministic, offline); {@link LangChain4jEmbedder} calls a real provider
 * (Ollama/OpenAI) via LangChain4J. The retriever takes any Embedder, so swapping is a
 * one-line config change.
 */
public interface Embedder {
  float[] embed(String text);

  int dim();

  default float[][] embedBatch(List<String> texts) {
    float[][] out = new float[texts.size()][];
    for (int i = 0; i < texts.size(); i++) {
      out[i] = embed(texts.get(i));
    }
    return out;
  }
}
