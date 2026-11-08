package org.jagentic.core;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.function.BiFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.net.PinnedHttp;

/** A2A (Agent-to-Agent) client — call a peer agent's HTTP gateway (/agent +
 * /.well-known/agent-card.json) with bounded retries. {@link #peerTool} wraps a peer as a
 * ToolRegistry tool so a path can delegate to another agent. */
public final class A2AClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String base;
  private final int retries;
  private final String bearer; // a reference; resolved on each call

  public A2AClient(String baseUrl, int retries) {
    this(baseUrl, retries, null);
  }

  /** @param bearer a secret reference for the peer's gateway ({@link Secrets}), or null */
  public A2AClient(String baseUrl, int retries, String bearer) {
    this.base = baseUrl.replaceAll("/+$", "");
    this.retries = Math.max(0, retries);
    this.bearer = bearer;
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> card() {
    try {
      PinnedHttp.Response r = PinnedHttp.send("GET", URI.create(base + "/.well-known/agent-card.json"), Map.of(),
          null, Duration.ofSeconds(10), PinnedHttp.egress());
      return MAPPER.readValue(r.text(), Map.class);
    } catch (Exception e) {
      throw new RuntimeException("A2A card: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> send(String conversationId, String text, String userId) {
    return send(conversationId, text, userId, java.util.UUID.randomUUID().toString());
  }

  /** As above under {@code idempotencyKey}, the same on every retry so the peer can deduplicate. */
  @SuppressWarnings("unchecked")
  public Map<String, Object> send(String conversationId, String text, String userId, String idempotencyKey) {
    org.jagentic.core.net.Egress.check(base);
    Exception last = null;
    for (int attempt = 0; attempt <= retries; attempt++) {
      try {
        String body = MAPPER.writeValueAsString(
            Map.of("conversation_id", conversationId, "text", text, "user_id", userId));
        Map<String, String> headers = new java.util.LinkedHashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Idempotency-Key", idempotencyKey);
        if (bearer != null) {
          headers.put("Authorization", "Bearer " + Secrets.resolve(bearer));
        }
        // Resolved once, checked, and connected to as checked: a rebinding name cannot move it.
        PinnedHttp.Response r = PinnedHttp.send("POST", URI.create(base + "/agent"), headers,
            body.getBytes(java.nio.charset.StandardCharsets.UTF_8), Duration.ofSeconds(30), PinnedHttp.egress());
        if (r.status() >= 500) {
          throw new java.io.IOException("HTTP " + r.status());
        }
        return MAPPER.readValue(r.text(), Map.class);
      } catch (org.jagentic.core.net.Egress.Blocked e) {
        throw e; // nothing was sent, so it is a definite failure, not an unknown one
      } catch (Exception e) {
        last = e;
        if (attempt == retries) {
          break;
        }
        try {
          Thread.sleep(200L * (1L << attempt));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    // The request may have landed: no answer is not a failure, for a peer as for an http tool.
    throw new OutcomeUnknown("A2A send to " + base + "/agent failed: " + last, last);
  }

  /** A ToolRegistry tool that delegates a turn to a peer agent. */
  public static BiFunction<Map<String, Object>, ToolRegistry.CallIdentity, Object> peerTool(
      String baseUrl, int retries) {
    return peerTool(baseUrl, retries, null);
  }

  /** As above, authenticating with the secret {@code bearer} refers to. */
  public static BiFunction<Map<String, Object>, ToolRegistry.CallIdentity, Object> peerTool(
      String baseUrl, int retries, String bearer) {
    A2AClient client = new A2AClient(baseUrl, retries, bearer);
    return (params, call) -> client.send(
        String.valueOf(params.getOrDefault("conversation_id", call.conversationId())),
        String.valueOf(params.getOrDefault("text", "")),
        String.valueOf(params.getOrDefault("user_id", "anonymous")),
        call.idempotencyKey());
  }
}
