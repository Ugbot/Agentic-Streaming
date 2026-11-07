package org.jagentic.core.llm;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A {@link ChatClient} on Gemini's {@code generateContent}, over the same JSON protocol as the other
 * HTTP clients. The key comes from {@code GEMINI_API_KEY} and travels in a header, not the URL, so it
 * stays out of access logs. A document must name its model: this code does not guess one.
 */
public final class GeminiChatClient extends AbstractHttpChatClient {

  private final Double temperature;
  private final Integer maxTokens;

  /** The key comes from {@code GEMINI_API_KEY}; {@code baseUrl} null for the default. */
  public GeminiChatClient(String model, String baseUrl, Double temperature, Integer maxTokens) {
    this(model, System.getenv("GEMINI_API_KEY"), baseUrl == null ? resolveBaseUrl() : baseUrl,
        temperature, maxTokens);
  }

  GeminiChatClient(String model, String apiKey, String baseUrl, Double temperature, Integer maxTokens) {
    super(model, OllamaChatClient.stripTrailingSlash(baseUrl) + "/models/"
            + URLEncoder.encode(model, StandardCharsets.UTF_8) + ":generateContent",
        Map.of("x-goog-api-key", require(apiKey)), Duration.ofSeconds(10));
    this.temperature = temperature;
    this.maxTokens = maxTokens;
  }

  private static String require(String key) {
    if (key == null || key.isBlank()) {
      throw new IllegalStateException("GEMINI_API_KEY not set");
    }
    return key;
  }

  private static String resolveBaseUrl() {
    String env = System.getenv("GEMINI_BASE_URL");
    return env != null && !env.isBlank() ? env : "https://generativelanguage.googleapis.com/v1beta";
  }

  private static Map<String, Object> parts(String text) {
    return Map.of("parts", List.of(Map.of("text", text)));
  }

  @Override
  protected Map<String, Object> buildBody(List<Map<String, String>> messages) {
    List<String> system = new ArrayList<>();
    List<Map<String, Object>> contents = new ArrayList<>();
    for (Map<String, String> m : messages) {
      if ("system".equals(m.get("role"))) {
        system.add(m.get("content"));
      } else {
        Map<String, Object> turn = new LinkedHashMap<>(parts(m.get("content")));
        turn.put("role", "assistant".equals(m.get("role")) ? "model" : "user");
        contents.add(turn);
      }
    }
    Map<String, Object> config = new LinkedHashMap<>();
    config.put("responseMimeType", "application/json");
    if (temperature != null) {
      config.put("temperature", temperature);
    }
    if (maxTokens != null) {
      config.put("maxOutputTokens", maxTokens);
    }
    Map<String, Object> body = new LinkedHashMap<>();
    if (!system.isEmpty()) {
      body.put("systemInstruction", parts(String.join("\n", system)));
    }
    body.put("contents", contents);
    body.put("generationConfig", config);
    return body;
  }

  /** A blocked prompt or answer arrives as a 200 and is the same next time, so it is not retried. */
  @Override
  protected String extractContent(JsonNode response) {
    if (response.path("promptFeedback").hasNonNull("blockReason")) {
      throw new IllegalStateException("the model refused the request: "
          + response.path("promptFeedback").path("blockReason").asText());
    }
    JsonNode candidate = response.path("candidates").path(0);
    if ("SAFETY".equals(candidate.path("finishReason").asText())) {
      throw new IllegalStateException("the model refused the request: SAFETY");
    }
    StringBuilder text = new StringBuilder();
    for (JsonNode part : candidate.path("content").path("parts")) {
      text.append(part.path("text").asText());
    }
    return text.toString();
  }
}
