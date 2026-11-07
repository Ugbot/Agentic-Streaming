package org.jagentic.core.llm;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/** A {@link ChatClient} backed by the OpenAI (or compatible) {@code /chat/completions}
 * endpoint in JSON-object mode. */
public final class OpenAIChatClient extends AbstractHttpChatClient {

  private final Double temperature;
  private final Integer maxTokens;

  public OpenAIChatClient(String model) {
    this(model, resolveKey(), resolveBaseUrl());
  }

  public OpenAIChatClient(String model, String apiKey, String baseUrl) {
    this(model, apiKey, baseUrl, null, null);
  }

  /** The key comes from {@code OPENAI_API_KEY}; {@code baseUrl} null for the default. */
  public OpenAIChatClient(String model, String baseUrl, Double temperature, Integer maxTokens) {
    this(model, resolveKey(), baseUrl == null ? resolveBaseUrl() : baseUrl, temperature, maxTokens);
  }

  private OpenAIChatClient(String model, String apiKey, String baseUrl, Double temperature,
                           Integer maxTokens) {
    super(model, OllamaChatClient.stripTrailingSlash(baseUrl) + "/chat/completions",
        Map.of("Authorization", "Bearer " + require(apiKey)), Duration.ofSeconds(10));
    this.temperature = temperature;
    this.maxTokens = maxTokens;
  }

  private static String require(String key) {
    if (key == null || key.isBlank()) {
      throw new IllegalStateException("OPENAI_API_KEY not set");
    }
    return key;
  }

  private static String resolveKey() {
    return System.getenv("OPENAI_API_KEY");
  }

  private static String resolveBaseUrl() {
    String env = System.getenv("OPENAI_BASE_URL");
    return env != null && !env.isBlank() ? env : "https://api.openai.com/v1";
  }

  @Override
  protected Map<String, Object> buildBody(List<Map<String, String>> messages) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("model", model);
    body.put("messages", messages);
    body.put("response_format", Map.of("type", "json_object"));
    if (temperature != null && !ModelRules.isOpenAiReasoning(model)) {
      body.put("temperature", temperature);
    }
    if (maxTokens != null) {
      body.put("max_completion_tokens", maxTokens);
    }
    return body;
  }

  @Override
  protected String extractContent(JsonNode response) {
    JsonNode choices = response.get("choices");
    if (choices != null && choices.isArray() && choices.size() > 0) {
      JsonNode msg = choices.get(0).get("message");
      if (msg != null && msg.hasNonNull("content")) {
        return msg.get("content").asText();
      }
    }
    return "";
  }
}
