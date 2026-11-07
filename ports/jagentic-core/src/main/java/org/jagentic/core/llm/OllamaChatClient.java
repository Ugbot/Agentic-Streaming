package org.jagentic.core.llm;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/** A {@link ChatClient} backed by a local Ollama {@code /api/chat} endpoint (JSON mode). */
public final class OllamaChatClient extends AbstractHttpChatClient {

  private final Double temperature;
  private final Integer maxTokens;

  public OllamaChatClient(String model) {
    this(model, resolveBaseUrl());
  }

  public OllamaChatClient(String model, String baseUrl) {
    this(model, baseUrl, null, null);
  }

  /** @param baseUrl null for the default; @param temperature, maxTokens sent only when non-null */
  public OllamaChatClient(String model, String baseUrl, Double temperature, Integer maxTokens) {
    super(model, stripTrailingSlash(baseUrl == null ? resolveBaseUrl() : baseUrl) + "/api/chat",
        Map.of(), Duration.ofSeconds(10));
    this.temperature = temperature;
    this.maxTokens = maxTokens;
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
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("model", model);
    body.put("messages", messages);
    body.put("stream", false);
    body.put("format", "json");
    Map<String, Object> options = new LinkedHashMap<>();
    if (temperature != null) {
      options.put("temperature", temperature);
    }
    if (maxTokens != null) {
      options.put("num_predict", maxTokens);
    }
    if (!options.isEmpty()) {
      body.put("options", options);
    }
    return body;
  }

  @Override
  protected String extractContent(JsonNode response) {
    JsonNode msg = response.get("message");
    return msg != null && msg.hasNonNull("content") ? msg.get("content").asText() : "";
  }
}
