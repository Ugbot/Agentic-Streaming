package org.agentic.flink.context.relevancy;

import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.Scorer;
import org.agentic.flink.tools.rag.EmbeddingToolExecutor;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scores context items for relevancy to current intent Uses semantic similarity via embeddings
 */
public class RelevancyScorer implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RelevancyScorer.class);

  private final EmbeddingToolExecutor embeddingExecutor;
  private final Scorer injectedScorer;
  private final InferenceSetup injectedSetup;

  public RelevancyScorer(Map<String, String> config) {
    this.embeddingExecutor = new EmbeddingToolExecutor(config);
    this.injectedScorer = null;
    this.injectedSetup = null;
  }

  /**
   * Construct a scorer that delegates to an injected {@link Scorer} (e.g. a fine-tuned
   * cross-encoder ranking model) instead of the built-in heuristic + embedding path. Existing
   * callers that don't need this stay on the no-arg path.
   */
  public RelevancyScorer(Scorer scorer, InferenceSetup setup) {
    this.embeddingExecutor = null;
    this.injectedScorer = scorer;
    this.injectedSetup = setup;
  }

  /**
   * Score a context item for relevancy to intent Combines semantic similarity, temporal relevancy,
   * and access patterns
   *
   * @param item Context item to score
   * @param intent Current agent intent/goal
   * @return Relevancy score (0.0 - 1.0)
   */
  public CompletableFuture<Double> scoreRelevancy(ContextItem item, String intent) {
    if (injectedScorer != null) {
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              double pairScore =
                  injectedScorer.scorePair(item.getContent(), intent, injectedSetup);
              return Math.max(0.0, Math.min(1.0, pairScore));
            } catch (Exception e) {
              LOG.warn(
                  "Injected Scorer failed for item {}: {}", item.getItemId(), e.getMessage());
              return item.getPriority().getRetentionScore();
            }
          });
    }
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Component 1: Semantic similarity (via embeddings)
            double semanticScore = calculateSemanticSimilarity(item.getContent(), intent);

            // Component 2: Temporal relevancy (decay over time)
            double temporalScore = item.getTemporalRelevancy(System.currentTimeMillis());

            // Component 3: Access frequency
            double accessScore = calculateAccessScore(item);

            // Component 4: Priority-based boost
            double priorityBoost = item.getPriority().getRetentionScore();

            // Weighted combination
            double finalScore =
                (semanticScore * 0.5) // Semantic similarity is most important
                    + (temporalScore * 0.2) // Recency matters
                    + (accessScore * 0.15) // Frequently accessed = important
                    + (priorityBoost * 0.15); // Priority provides boost

            LOG.debug(
                "Relevancy score for item {}: semantic={:.2f}, temporal={:.2f}, "
                    + "access={:.2f}, priority={:.2f}, final={:.2f}",
                item.getItemId(),
                semanticScore,
                temporalScore,
                accessScore,
                priorityBoost,
                finalScore);

            return Math.min(1.0, finalScore);

          } catch (Exception e) {
            LOG.error("Error scoring relevancy for item: {}", item.getItemId(), e);
            // Fall back to priority-based score
            return item.getPriority().getRetentionScore();
          }
        });
  }

  /**
   * Calculate semantic similarity between item content and intent For now, uses simple heuristic
   * Can be enhanced with actual embedding comparison
   *
   * @param content Context content
   * @param intent Agent intent
   * @return Similarity score (0.0 - 1.0)
   */
  private double calculateSemanticSimilarity(String content, String intent) {
    if (content == null || intent == null) {
      return 0.0;
    }

    // Simple keyword-based similarity (placeholder)
    // In production, would use embedding cosine similarity
    String[] intentWords = intent.toLowerCase().split("\\s+");
    String contentLower = content.toLowerCase();

    int matches = 0;
    for (String word : intentWords) {
      if (contentLower.contains(word)) {
        matches++;
      }
    }

    return Math.min(1.0, (double) matches / intentWords.length);
  }

  /**
   * Calculate access score based on frequency Frequently accessed items are more likely to be
   * relevant
   *
   * @param item Context item
   * @return Access score (0.0 - 1.0)
   */
  private double calculateAccessScore(ContextItem item) {
    // Normalize access count (assume 10+ accesses = maximum relevance)
    return Math.min(1.0, item.getAccessCount() / 10.0);
  }

  /**
   * Batch score multiple items
   *
   * @param items Items to score
   * @param intent Current intent
   * @return Map of item ID to relevancy score
   */
  public CompletableFuture<Map<String, Double>> scoreAll(
      Iterable<ContextItem> items, String intent) {
    Map<String, CompletableFuture<Double>> scoreFutures = new HashMap<>();

    for (ContextItem item : items) {
      scoreFutures.put(item.getItemId(), scoreRelevancy(item, intent));
    }

    return CompletableFuture.allOf(scoreFutures.values().toArray(new CompletableFuture[0]))
        .thenApply(
            v -> {
              Map<String, Double> scores = new HashMap<>();
              scoreFutures.forEach(
                  (id, future) -> {
                    try {
                      scores.put(id, future.get());
                    } catch (Exception e) {
                      LOG.error("Error getting score for item: {}", id, e);
                      scores.put(id, 0.0);
                    }
                  });
              return scores;
            });
  }
}
