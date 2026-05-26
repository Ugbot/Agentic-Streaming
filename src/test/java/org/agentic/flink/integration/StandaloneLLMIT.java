package org.agentic.flink.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.execution.LLMClient;
import org.agentic.flink.execution.LLMResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for LLM calls without Flink dependencies.
 *
 * <p>Tests real Ollama calls via LangChain4J.
 * Requires a running Ollama instance with qwen2.5:latest.
 */
@Tag("integration")
@Disabled("Requires running Ollama instance with qwen2.5:latest")
class StandaloneLLMIT {

  private LLMClient client;

  @BeforeEach
  void setUp() {
    client = LLMClient.builder()
        .withModel("qwen2.5:latest")
        .withTemperature(0.7)
        .withBaseUrl("http://localhost:11434")
        .build();
  }

  @Test
  void simpleLLMCallShouldReturnResponse() {
    String response = client.generate("What is Apache Flink in one sentence?");
    assertNotNull(response, "LLM should return a response");
    assertFalse(response.isBlank(), "Response should not be blank");
  }

  @Test
  void toolCallParsingShouldDetectToolCalls() {
    List<Map<String, Object>> messages = new ArrayList<>();
    messages.add(Map.of("role", "system", "content",
        "You are a calculator. When asked to calculate, respond with:\n"
            + "TOOL_CALL: calculator-add {\"a\": 5, \"b\": 3}\n"
            + "Then explain the result."));
    messages.add(Map.of("role", "user", "content", "What is 5 plus 3?"));

    LLMResponse response = client.chat(messages);
    assertNotNull(response.getText(), "Response should have text");
  }

  @Test
  void conversationHistoryShouldMaintainContext() {
    List<Map<String, Object>> messages = new ArrayList<>();
    messages.add(Map.of("role", "system", "content",
        "You are a helpful assistant. Keep responses brief."));
    messages.add(Map.of("role", "user", "content", "My favorite color is blue."));
    messages.add(Map.of("role", "assistant", "content", "That's nice! Blue is a great color."));
    messages.add(Map.of("role", "user", "content", "What was my favorite color?"));

    LLMResponse response = client.chat(messages);
    assertNotNull(response.getText(), "Response should have text");
    assertTrue(response.getText().toLowerCase().contains("blue"),
        "LLM should remember conversation context and mention 'blue'");
  }
}
