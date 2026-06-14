package org.jagentic.core.inference;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.Retrieval;
import org.jagentic.core.embedding.Embedder;
import org.jagentic.core.embedding.HashingEmbedder;

/** Nearest-centroid classifier over an {@link Embedder}. {@code fit} averages each label's
 * example embeddings into a centroid; {@code classify} returns the label whose centroid is
 * most cosine-similar, mapped to a softmax distribution. Real ML: with a real embedder this
 * is a genuine semantic classifier; with the hashing embedder it's a deterministic offline
 * default — the "real model opt-in" path (swap the embedder, keep the SPI). */
public final class EmbeddingClassifier implements Classifier {

  private final Embedder embedder;
  private final double temperature;
  private final Map<String, float[]> centroids = new LinkedHashMap<>();

  public EmbeddingClassifier(Embedder embedder, double temperature) {
    this.embedder = embedder == null ? new HashingEmbedder(256) : embedder;
    this.temperature = temperature <= 0 ? 10.0 : temperature;
  }

  public EmbeddingClassifier() {
    this(null, 10.0);
  }

  public EmbeddingClassifier fit(Map<String, List<String>> examples) {
    if (examples == null || examples.isEmpty()) {
      throw new IllegalArgumentException("need at least one labeled example set");
    }
    for (var e : examples.entrySet()) {
      List<String> texts = e.getValue();
      if (texts == null || texts.isEmpty()) {
        continue;
      }
      float[][] vecs = embedder.embedBatch(texts);
      int dim = vecs[0].length;
      float[] centroid = new float[dim];
      for (float[] v : vecs) {
        for (int i = 0; i < dim && i < v.length; i++) {
          centroid[i] += v[i];
        }
      }
      for (int i = 0; i < dim; i++) {
        centroid[i] /= vecs.length;
      }
      centroids.put(e.getKey(), centroid);
    }
    if (centroids.isEmpty()) {
      throw new IllegalArgumentException("no non-empty example sets");
    }
    return this;
  }

  @Override
  public Classification classify(String text) {
    if (centroids.isEmpty()) {
      throw new IllegalStateException("classifier not fitted; call fit(...) first");
    }
    float[] vec = embedder.embed(text);
    Map<String, Double> exps = new HashMap<>();
    double denom = 0.0;
    for (var e : centroids.entrySet()) {
      double ex = Math.exp(temperature * Retrieval.cosine(vec, e.getValue()));
      exps.put(e.getKey(), ex);
      denom += ex;
    }
    if (denom == 0.0) {
      denom = 1.0;
    }
    Map<String, Double> scores = new HashMap<>();
    String best = "other";
    double bestScore = -1.0;
    for (var e : exps.entrySet()) {
      double s = e.getValue() / denom;
      scores.put(e.getKey(), s);
      if (s > bestScore) {
        bestScore = s;
        best = e.getKey();
      }
    }
    return new Classification(best, bestScore, scores);
  }
}
