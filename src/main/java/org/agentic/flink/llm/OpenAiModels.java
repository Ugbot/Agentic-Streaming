package org.agentic.flink.llm;

import org.agentic.flink.annotation.Public;
import org.jagentic.core.llm.ModelRules;

/**
 * Small shared helpers for OpenAI model quirks, so every place that builds an OpenAI chat model
 * applies them consistently.
 *
 * <p>The {@code o} series and {@code gpt-5} onwards take {@code max_completion_tokens} in place of
 * {@code max_tokens} and refuse a custom {@code temperature} on the Chat Completions API unless
 * reasoning effort is {@code none}. Callers building an {@code OpenAiChatModel} use {@link
 * #isReasoning} to decide.
 */
@Public
public final class OpenAiModels {

  private OpenAiModels() {}

  /**
   * True for OpenAI reasoning models that reject {@code max_tokens} / a custom {@code temperature}.
   */
  public static boolean isReasoning(String modelName) {
    return ModelRules.isOpenAiReasoning(modelName);
  }
}
