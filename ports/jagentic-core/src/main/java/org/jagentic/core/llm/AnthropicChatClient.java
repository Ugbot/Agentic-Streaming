package org.jagentic.core.llm;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A {@link ChatClient} on Anthropic's Messages API, over the same JSON protocol as the other HTTP
 * clients. The key comes from {@code ANTHROPIC_API_KEY}, never from a document. A sampling setting
 * is sent only when declared, since current models refuse a non-default one.
 */
public final class AnthropicChatClient extends AbstractHttpChatClient {

  /** The model a document that names none gets. */
  public static final String DEFAULT_MODEL = "claude-sonnet-5-5";
  private static final String VERSION = "2023-06-01";
  private static final int DEFAULT_MAX_TOKENS = 16_000;

  private final Double temperature;
  private final int maxTokens;

  /** The key comes from {@code ANTHROPIC_API_KEY}; {@code baseUrl} null for the default. */
  public AnthropicChatClient(String model, String baseUrl, Double temperature, Integer maxTokens) {
    this(model, System.getenv("ANTHROPIC_API_KEY"), baseUrl == null ? resolveBaseUrl() : baseUrl,
        temperature, maxTokens);
  }

  AnthropicChatClient(String model, String apiKey, String baseUrl, Double temperature, Integer maxTokens) {
    super(model, OllamaChatClient.stripTrailingSlash(baseUrl) + "/v1/messages",
        Map.of("x-api-key", require(apiKey), "anthropic-version", VERSION), Duration.ofSeconds(10));
    this.temperature = temperature;
    this.maxTokens = maxTokens == null ? DEFAULT_MAX_TOKENS : maxTokens;
  }

  private static String require(String key) {
    if (key == null || key.isBlank()) {
      throw new IllegalStateException("ANTHROPIC_API_KEY not set");
    }
    return key;
  }

  private static String resolveBaseUrl() {
    String env = System.getenv("ANTHROPIC_BASE_URL");
    return env != null && !env.isBlank() ? env : "https://api.anthropic.com";
  }

  @Override
  protected Map<String, Object> buildBody(List<Map<String, String>> messages) {
    List<String> system = new ArrayList<>();
    List<Map<String, String>> turns = new ArrayList<>();
    for (Map<String, String> m : messages) {
      if ("system".equals(m.get("role"))) {
        system.add(m.get("content"));
      } else {
        turns.add(Map.of("role", m.get("role"), "content", m.get("content")));
      }
    }
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("model", model);
    body.put("max_tokens", maxTokens);
    if (!system.isEmpty()) {
      body.put("system", String.join("\n", system));
    }
    body.put("messages", turns);
    if (temperature != null) {
      body.put("temperature", temperature);
    }
    return body;
  }

  /** A refusal arrives as a 200 and is the same next time, so it is not retried. */
  @Override
  protected String extractContent(JsonNode response) {
    if ("refusal".equals(response.path("stop_reason").asText())) {
      throw new IllegalStateException("the model refused the request");
    }
    StringBuilder text = new StringBuilder();
    for (JsonNode block : response.path("content")) {
      if ("text".equals(block.path("type").asText())) {
        text.append(block.path("text").asText());
      }
    }
    return text.toString();
  }
}
