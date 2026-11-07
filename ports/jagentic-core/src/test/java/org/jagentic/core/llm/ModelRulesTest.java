package org.jagentic.core.llm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ModelRulesTest {

  @ParameterizedTest
  @ValueSource(strings = {"claude-fable-5-1", "claude-fable-5", "claude-opus-5-5", "claude-opus-5",
      "claude-opus-4-8", "claude-opus-4-7", "claude-sonnet-5-5", "claude-sonnet-5",
      "claude-haiku-5-5", "claude-mythos-5-1", "claude-something-new-9"})
  void currentClaudeModelsRefuseSampling(String model) {
    assertFalse(ModelRules.claudeAcceptsSampling(model), model);
  }

  @ParameterizedTest
  @ValueSource(strings = {"claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5",
      "claude-sonnet-4-5-20250929", "claude-opus-4", "claude-3-7-sonnet-latest"})
  void olderClaudeModelsAcceptIt(String model) {
    assertTrue(ModelRules.claudeAcceptsSampling(model), model);
  }

  @ParameterizedTest
  @ValueSource(strings = {"gpt-5", "gpt-5.4-mini", "gpt-6-luna", "gpt-6.1-terra", "gpt-10-x", "o1-mini",
      "o3", "O4-MINI"})
  void reasoningModels(String model) {
    assertTrue(ModelRules.isOpenAiReasoning(model), model);
  }

  @ParameterizedTest
  @ValueSource(strings = {"gpt-4.1", "gpt-4o-mini", "omni-moderation-latest", "qwen2.5:3b", ""})
  void otherModels(String model) {
    assertFalse(ModelRules.isOpenAiReasoning(model), model);
  }
}
