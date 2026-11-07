package org.jagentic.core;

import java.io.IOException;
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
 * Asks an Open Policy Agent about each tool call, over its REST data API. The document's policy is
 * the floor: this runs after it and can only narrow it. Anything but a clear yes is a no, so an OPA
 * that is down, slow, undefined or confused refuses the call.
 *
 * <p>The input is {@code {tool, origin, path, effect, principal, args}}. The answer is a boolean at
 * the policy's path, or an object with {@code allow} and optionally {@code reason}, or {@code
 * approval} true to send the call to a person.
 */
public final class OpaToolPolicy implements ToolPolicy {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final URI uri;
  private final Duration timeout;
  private final HttpClient http;

  /**
   * @param baseUrl where OPA is, such as {@code http://127.0.0.1:8181}
   * @param policyPath the document to ask for, such as {@code agentic/tools}
   */
  public OpaToolPolicy(String baseUrl, String policyPath, Duration timeout) {
    this.uri = URI.create(baseUrl.replaceAll("/+$", "") + "/v1/data/" + policyPath.replaceAll("^/+", ""));
    this.timeout = timeout;
    this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
  }

  /** The policy the operator configured with {@code AGENTIC_OPA_URL}, or null when there is none. */
  public static OpaToolPolicy fromEnvironment(java.util.function.UnaryOperator<String> env) {
    String url = env.apply("AGENTIC_OPA_URL");
    if (url == null || url.isBlank()) {
      return null;
    }
    String path = env.apply("AGENTIC_OPA_POLICY");
    String ms = env.apply("AGENTIC_OPA_TIMEOUT_MS");
    return new OpaToolPolicy(url, path == null ? "agentic/tools" : path,
        Duration.ofMillis(ms == null ? 2000 : Long.parseLong(ms)));
  }

  @Override
  public boolean outsideThisProcess() {
    return true;
  }

  @Override
  public Decision decide(Request r) {
    try {
      Map<String, Object> input = new LinkedHashMap<>();
      input.put("tool", r.tool());
      input.put("origin", r.origin().wire());
      input.put("path", r.path());
      input.put("effect", r.effect());
      input.put("principal", r.principal() == null ? null : r.principal().toMap());
      input.put("args", r.args());
      HttpRequest request = HttpRequest.newBuilder(uri).timeout(timeout)
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of("input", input)))).build();
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        return Decision.deny("opa answered HTTP " + response.statusCode() + " for tool " + r.tool());
      }
      return read(JSON.readTree(response.body()).path("result"), r.tool());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Decision.deny("opa was interrupted for tool " + r.tool());
    } catch (IOException | RuntimeException e) {
      return Decision.deny("opa unavailable for tool " + r.tool());
    }
  }

  private static Decision read(JsonNode result, String tool) {
    boolean allowed = result.isBoolean() ? result.booleanValue()
        : result.path("allow").isBoolean() && result.path("allow").booleanValue();
    if (allowed) {
      return result.path("approval").asBoolean(false) ? Decision.ASK_A_PERSON : Decision.ALLOW;
    }
    String reason = result.path("reason").asText("");
    return Decision.deny(reason.isBlank() ? "opa refused tool " + tool : reason);
  }
}
