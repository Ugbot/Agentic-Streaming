package org.agentic.flink.a2a.gateway.rag;

/**
 * Turns text into an embedding vector for the RAG proxy. Pluggable: the default
 * {@link HashingEmbedder} is deterministic and dependency-free (good for the demo + tests); a
 * production deployment swaps in a model-backed embedder (DJL / OpenAI / Gemini via the framework's
 * embedding SPI) by providing an alternative CDI bean.
 */
public interface Embedder {
  float[] embed(String text);

  int dimension();
}
