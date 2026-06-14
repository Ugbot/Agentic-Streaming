package org.jagentic.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

/** A2A (Agent-to-Agent) client — call a peer agent's HTTP gateway (/agent +
 * /.well-known/agent-card.json) with bounded retries. {@link #peerTool} wraps a peer as a
 * ToolRegistry tool so a path can delegate to another agent. */
public final class A2AClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String base;
  private final int retries;
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  public A2AClient(String baseUrl, int retries) {
    this.base = baseUrl.replaceAll("/+$", "");
    this.retries = Math.max(0, retries);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> card() {
    try {
      HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base + "/.well-known/agent-card.json"))
          .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
      return MAPPER.readValue(r.body(), Map.class);
    } catch (Exception e) {
      throw new RuntimeException("A2A card: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> send(String conversationId, String text, String userId) {
    RuntimeException last = null;
    for (int attempt = 0; attempt <= retries; attempt++) {
      try {
        String body = MAPPER.writeValueAsString(
            Map.of("conversation_id", conversationId, "text", text, "user_id", userId));
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base + "/agent"))
            .header("Content-Type", "application/json").timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        return MAPPER.readValue(r.body(), Map.class);
      } catch (Exception e) {
        last = new RuntimeException(e);
        try {
          Thread.sleep(200L * (1L << attempt));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    throw new RuntimeException("A2A send to " + base + "/agent failed", last);
  }

  /** A ToolRegistry tool that delegates a turn to a peer agent. */
  public static Function<Map<String, Object>, Object> peerTool(String baseUrl, int retries) {
    A2AClient client = new A2AClient(baseUrl, retries);
    return params -> client.send(
        String.valueOf(params.getOrDefault("conversation_id", "a2a")),
        String.valueOf(params.getOrDefault("text", "")),
        String.valueOf(params.getOrDefault("user_id", "anonymous")));
  }
}
