package org.jagentic.core.llm;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/** A {@link ChatClient} backed by a local Ollama {@code /api/chat} endpoint (JSON mode). */
public final class OllamaChatClient extends AbstractHttpChatClient {

  public OllamaChatClient(String model) {
    this(model, resolveBaseUrl());
  }

  public OllamaChatClient(String model, String baseUrl) {
    super(model, stripTrailingSlash(baseUrl) + "/api/chat", Map.of(), Duration.ofSeconds(10));
  }

  private static String resolveBaseUrl() {
    String env = System.getenv("AGENTIC_OLLAMA_URL");
    return env != null && !env.isBlank() ? env : "http://localhost:11434";
  }

  static String stripTrailingSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  @Override
  protected Map<String, Object> buildBody(List<Map<String, String>> messages) {
    return Map.of("model", model, "messages", messages, "stream", false, "format", "json");
  }

  @Override
  protected String extractContent(JsonNode response) {
    JsonNode msg = response.get("message");
    return msg != null && msg.hasNonNull("content") ? msg.get("content").asText() : "";
  }
}
