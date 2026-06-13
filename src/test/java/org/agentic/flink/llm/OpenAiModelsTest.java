package org.agentic.flink.llm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins which OpenAI models are treated as "reasoning" (no {@code max_tokens}/custom temperature) —
 * the bug that 400'd every gpt-5.4 call today until we omitted those params.
 */
class OpenAiModelsTest {

  @Test
  @DisplayName("gpt-5* and o-series are reasoning models")
  void reasoning() {
    assertTrue(OpenAiModels.isReasoning("gpt-5.4-nano"));
    assertTrue(OpenAiModels.isReasoning("gpt-5.4-mini"));
    assertTrue(OpenAiModels.isReasoning("gpt-5"));
    assertTrue(OpenAiModels.isReasoning("o1-mini"));
    assertTrue(OpenAiModels.isReasoning("o3"));
    assertTrue(OpenAiModels.isReasoning("O4-MINI")); // case-insensitive
  }

  @Test
  @DisplayName("classic chat models are NOT reasoning models")
  void notReasoning() {
    assertFalse(OpenAiModels.isReasoning("gpt-4o-mini"));
    assertFalse(OpenAiModels.isReasoning("gpt-4o"));
    assertFalse(OpenAiModels.isReasoning("gpt-3.5-turbo"));
    assertFalse(OpenAiModels.isReasoning(null));
    assertFalse(OpenAiModels.isReasoning(""));
  }
}
