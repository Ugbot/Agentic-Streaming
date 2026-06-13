package org.agentic.flink.example.banking.env;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializable client for the A2A hackathon harness env-tools API.
 *
 * <p>Mirrors the template's {@code env_toolset.py}: lists and calls the bank/user environment tools
 * for a session, keyed by the A2A {@code contextId}.
 *
 * <ul>
 *   <li>{@code GET  {baseUrl}/sessions/{contextId}/tools} → OpenAI-style tool schemas for the
 *       caller's scope.
 *   <li>{@code POST {baseUrl}/sessions/{contextId}/tools/{name}} body {@code {"arguments": {...}}}
 *       → the tool result.
 * </ul>
 *
 * <p>Authenticated with a bearer token. Ships in the Flink job graph (Serializable); the JDK {@link
 * HttpClient} and mapper are {@code transient} and rebuilt lazily on the task side.
 */
public final class EnvApiClient implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(EnvApiClient.class);

  private final String baseUrl;
  private final String token;
  private final long requestTimeoutMs;

  private transient volatile HttpClient http;
  private transient volatile ObjectMapper mapper;

  public EnvApiClient(String baseUrl, String token, long requestTimeoutMs) {
    this.baseUrl = stripTrailingSlash(Objects.requireNonNull(baseUrl, "baseUrl"));
    this.token = token;
    this.requestTimeoutMs = requestTimeoutMs <= 0 ? 30_000 : requestTimeoutMs;
  }

  /** Build from the harness-injected {@code ENV_API_URL} / {@code ENV_API_TOKEN} env vars. */
  public static EnvApiClient fromEnv() {
    String url = System.getenv("ENV_API_URL");
    if (url == null || url.isBlank()) {
      throw new IllegalStateException("ENV_API_URL is not set");
    }
    return new EnvApiClient(url, System.getenv("ENV_API_TOKEN"), 30_000);
  }

  public String baseUrl() {
    return baseUrl;
  }

  /** Fetch the OpenAI-style tool schemas available to this session's scope. */
  public List<Map<String, Object>> listTools(String contextId) {
    String url = baseUrl + "/sessions/" + enc(contextId) + "/tools";
    HttpRequest req = authed(HttpRequest.newBuilder(URI.create(url))).GET().build();
    Map<String, Object> body = send(req, url);
    Object tools = body.get("tools");
    if (tools instanceof List) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> typed = (List<Map<String, Object>>) tools;
      return typed;
    }
    return List.of();
  }

  /** Execute an env tool, returning its result map (typically {@code {content, error}}). */
  public Map<String, Object> callTool(String contextId, String name, Map<String, Object> arguments) {
    String url = baseUrl + "/sessions/" + enc(contextId) + "/tools/" + enc(name);
    byte[] payload = writeJson(Map.of("arguments", arguments == null ? Map.of() : arguments));
    HttpRequest req =
        authed(HttpRequest.newBuilder(URI.create(url)))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
            .build();
    return send(req, url);
  }

  // ==================== internals ====================

  private Map<String, Object> send(HttpRequest req, String url) {
    try {
      HttpResponse<byte[]> resp = http().send(req, HttpResponse.BodyHandlers.ofByteArray());
      if (resp.statusCode() != 200) {
        String text = new String(resp.body(), StandardCharsets.UTF_8);
        LOG.warn("env API {} -> HTTP {}: {}", url, resp.statusCode(), text);
        return Map.of("error", true, "content", "HTTP " + resp.statusCode() + ": " + text);
      }
      return mapper().readValue(resp.body(), Map.class);
    } catch (Exception e) {
      LOG.warn("env API call to {} failed: {}", url, e.getMessage());
      return Map.of("error", true, "content", "env API call failed: " + e.getMessage());
    }
  }

  private HttpRequest.Builder authed(HttpRequest.Builder b) {
    b.timeout(Duration.ofMillis(requestTimeoutMs));
    if (token != null && !token.isBlank()) {
      b.header("Authorization", "Bearer " + token);
    }
    return b;
  }

  private byte[] writeJson(Object value) {
    try {
      return mapper().writeValueAsBytes(value);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize env API request", e);
    }
  }

  private HttpClient http() {
    HttpClient c = http;
    if (c == null) {
      synchronized (this) {
        if (http == null) {
          http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }
        c = http;
      }
    }
    return c;
  }

  private ObjectMapper mapper() {
    ObjectMapper m = mapper;
    if (m == null) {
      synchronized (this) {
        if (mapper == null) {
          mapper = new ObjectMapper();
        }
        m = mapper;
      }
    }
    return m;
  }

  private static String enc(String s) {
    // contextIds and tool names are path-safe tokens; guard against null.
    return s == null ? "" : s;
  }

  private static String stripTrailingSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }
}
