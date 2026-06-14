package org.jagentic.core.llm;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared JSON-mode HTTP chat client. The model must reply with a single JSON object —
 * {@code {"tool": "...", "args": {...}}} to call a tool or {@code {"text": "..."}} to
 * answer — so no provider-specific function-calling API is needed. Subclasses supply
 * the request body and pull the assistant content out of the provider's response.
 */
abstract class AbstractHttpChatClient implements ChatClient {

  static final ObjectMapper MAPPER = new ObjectMapper();

  protected final String model;
  protected final URI uri;
  protected final Map<String, String> headers;
  private final HttpClient http;

  protected AbstractHttpChatClient(String model, String url, Map<String, String> headers, Duration timeout) {
    this.model = model;
    this.uri = URI.create(url);
    this.headers = headers == null ? Map.of() : headers;
    this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
  }

  protected abstract Map<String, Object> buildBody(java.util.List<Map<String, String>> messages);

  protected abstract String extractContent(JsonNode response);

  @Override
  public ChatResult chat(java.util.List<Map<String, String>> messages, java.util.List<Map<String, String>> tools) {
    try {
      String body = MAPPER.writeValueAsString(buildBody(messages));
      HttpRequest.Builder b = HttpRequest.newBuilder(uri)
          .timeout(Duration.ofSeconds(60))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(body));
      headers.forEach(b::header);
      HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() / 100 != 2) {
        throw new RuntimeException("chat HTTP " + resp.statusCode() + ": " + resp.body());
      }
      return parseChatJson(extractContent(MAPPER.readTree(resp.body())));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("chat call failed: " + e.getMessage(), e);
    }
  }

  /** Parse a model reply that should be one JSON object; tolerate fences/prose. */
  static ChatResult parseChatJson(String content) {
    if (content == null) {
      return ChatResult.text("");
    }
    String s = content.strip();
    int start = s.indexOf('{'), end = s.lastIndexOf('}');
    if (start != -1 && end > start) {
      try {
        JsonNode obj = MAPPER.readTree(s.substring(start, end + 1));
        if (obj.hasNonNull("tool")) {
          Map<String, Object> args = obj.has("args") && obj.get("args").isObject()
              ? MAPPER.convertValue(obj.get("args"), Map.class)
              : new LinkedHashMap<>();
          return ChatResult.toolCall(obj.get("tool").asText(), args);
        }
        if (obj.has("text")) {
          return ChatResult.text(obj.get("text").asText());
        }
      } catch (Exception ignore) {
        // fall through to freeform
      }
    }
    return ChatResult.text(s);
  }
}
