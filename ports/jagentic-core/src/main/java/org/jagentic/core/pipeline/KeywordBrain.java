package org.jagentic.core.pipeline;

import java.util.List;
import java.util.Map;

import org.jagentic.core.AgentContext;
import org.jagentic.core.Brain;
import org.jagentic.core.Retrieval;
import org.jagentic.core.embedding.Embedder;
import org.jagentic.core.embedding.HashingEmbedder;

/**
 * A generic, model-free brain built from a declarative spec: fire a tool when a trigger
 * keyword appears, else answer from retrieval, else echo. Reproduces the banking
 * RuleBrain behaviour without hardcoding it (the Java peer of pyagentic's KeywordBrain).
 *
 * <p>The embed function is threaded in from the builder (mirroring pyagentic's
 * {@code KeywordBrain(name, embed, ...)}): the deterministic hashing embedder by default,
 * a real provider when the {@code embeddings:} section selects one.
 */
public final class KeywordBrain implements Brain {
  private final String name;
  private final Embedder embedder;
  private final Map<String, String> toolTriggers;
  private final double threshold;

  /** Default to the hashing embedder at {@code dim} (byte-for-byte == Retrieval.embed). */
  public KeywordBrain(String name, int dim, Map<String, String> toolTriggers, double threshold) {
    this(name, new HashingEmbedder(dim), toolTriggers, threshold);
  }

  public KeywordBrain(String name, Embedder embedder, Map<String, String> toolTriggers, double threshold) {
    this.name = name;
    this.embedder = embedder == null ? new HashingEmbedder(256) : embedder;
    this.toolTriggers = toolTriggers == null ? Map.of() : toolTriggers;
    this.threshold = threshold;
  }

  @Override
  public String turn(String userText, AgentContext ctx) {
    String low = userText.toLowerCase();
    for (Map.Entry<String, String> e : toolTriggers.entrySet()) {
      if (low.contains(e.getKey().toLowerCase())) {
        Object result = ctx.callTool(e.getValue(), Map.of("user", ctx.userId));
        return "[" + name + "] " + e.getValue() + " returned " + result;
      }
    }
    if (ctx.retriever != null) {
      List<Retrieval.Scored> hits = ctx.retriever.retrieve(embedder.embed(userText), 1);
      if (!hits.isEmpty() && hits.get(0).score() > threshold) {
        return "[" + name + "] " + hits.get(0).text();
      }
    }
    return "[" + name + "] I can help with " + name + " questions. You said: \"" + userText + "\"";
  }
}
