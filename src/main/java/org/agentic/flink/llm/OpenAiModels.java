package org.agentic.flink.llm;

import java.util.Locale;

/**
 * Small shared helpers for OpenAI model quirks, so every place that builds an OpenAI chat model
 * applies them consistently.
 *
 * <p>The {@code gpt-5*} family and the {@code o1}/{@code o3}/{@code o4} reasoning series do not
 * accept {@code max_tokens} (they require {@code max_completion_tokens}, which langchain4j 0.35.0
 * cannot send) or a non-default {@code temperature} on the Chat Completions API — sending either is
 * a 400. Callers building an {@code OpenAiChatModel} must omit those params for reasoning models;
 * use {@link #isReasoning} to decide.
 */
public final class OpenAiModels {

  private OpenAiModels() {}

  /** True for OpenAI reasoning models that reject {@code max_tokens} / a custom {@code temperature}. */
  public static boolean isReasoning(String modelName) {
    if (modelName == null) {
      return false;
    }
    String m = modelName.toLowerCase(Locale.ROOT);
    return m.startsWith("gpt-5") || m.startsWith("o1") || m.startsWith("o3") || m.startsWith("o4");
  }
}
