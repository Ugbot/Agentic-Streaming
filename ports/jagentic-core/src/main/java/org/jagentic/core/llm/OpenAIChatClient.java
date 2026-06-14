package org.jagentic.core.llm;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/** A {@link ChatClient} backed by the OpenAI (or compatible) {@code /chat/completions}
 * endpoint in JSON-object mode. */
public final class OpenAIChatClient extends AbstractHttpChatClient {

  public OpenAIChatClient(String model) {
    this(model, resolveKey(), resolveBaseUrl());
  }

  public OpenAIChatClient(String model, String apiKey, String baseUrl) {
    super(model, OllamaChatClient.stripTrailingSlash(baseUrl) + "/chat/completions",
        Map.of("Authorization", "Bearer " + require(apiKey)), Duration.ofSeconds(10));
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
    return Map.of("model", model, "messages", messages,
        "response_format", Map.of("type", "json_object"));
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
