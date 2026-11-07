package org.jagentic.core.llm;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * What a provider's models accept on the wire, where that differs by model. Both tests are written
 * so an id this code has never seen gets the request that newer models need, since the failure
 * modes are unequal: an unsent temperature is a default, a sent one is a 400 on every call.
 */
public final class ModelRules {

  /** o-series, and {@code gpt-5} onwards. */
  private static final Pattern OPENAI_REASONING =
      Pattern.compile("^(o\\d.*|gpt-([5-9]|[1-9]\\d).*)$");

  /** The Claude models that still take sampling parameters; everything later refuses them. */
  private static final Pattern CLAUDE_SAMPLING =
      Pattern.compile("^claude-(\\d.*|(opus|sonnet|haiku)-[1-3].*|(opus|sonnet|haiku)-4(-[0-6].*)?)$");

  private ModelRules() {}

  /**
   * True for OpenAI models that take {@code max_completion_tokens} in place of {@code max_tokens}
   * and refuse a custom {@code temperature}. The {@code gpt-5} family and later accept one only at
   * reasoning effort {@code none}, which nothing here sets.
   */
  public static boolean isOpenAiReasoning(String model) {
    return model != null && OPENAI_REASONING.matcher(model.toLowerCase(Locale.ROOT)).matches();
  }

  /** False for current Claude models, which answer {@code temperature} with a 400. */
  public static boolean claudeAcceptsSampling(String model) {
    return model != null && CLAUDE_SAMPLING.matcher(model.toLowerCase(Locale.ROOT)).matches();
  }
}
