package org.jagentic.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.gateway.auth.StaticTokens;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Attacks on the gateway that found something. Each test failed before its fix. */
class GatewayAttackTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String ALICE = "alice-token-0123456789";
  private static final String MALLORY = "mallory-token-0123456789";
  private static final StaticTokens TOKENS =
      StaticTokens.parse(ALICE + "=alice|acme;" + MALLORY + "=mallory|acme");

  private final HttpClient http = HttpClient.newHttpClient();
  private Gateway gateway;

  private static Map<String, Object> workflow() {
    return Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "keyword", "default", "main",
                "rules", Map.of("held", List.of("refund"))),
            "paths", Map.of("main", Map.of("brain", "rule"),
                "held", Map.of("brain", "rule", "x-suspend-until", "approval")),
            "verifier", Map.of("kind", "none")),
        "policies", Map.of("approvers", Map.of("roles", List.of("approver"))));
  }

  private static LocalRuntime runtime() {
    GraphBuilder.Built built = GraphBuilder.build(workflow(), null);
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), null, new ConversationLog.InMemory());
  }

  @AfterEach
  void stop() {
    if (gateway != null) {
      gateway.close();
    }
  }

  private HttpResponse<String> call(String method, String path, String token, Object body) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + gateway.port() + path))
        .timeout(java.time.Duration.ofSeconds(20));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    b.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
        : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> turn(String token, String cid, String text) throws Exception {
    return call("POST", "/agent", token, Map.of("conversation_id", cid, "text", text));
  }

  /**
   * A runtime whose log the gateway sees only through the results, the way it sees a Flink job over
   * Kafka, and whose turns wait until released.
   */
  private static final class LaggingBackend implements TurnBackend {
    final LocalRuntime runtime = runtime();
    final ConversationView view = new ConversationView();
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch submitted = new CountDownLatch(1);

    @Override
    public CompletionStage<TurnResult> submit(Event event) {
      submitted.countDown();
      return CompletableFuture.supplyAsync(() -> {
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        TurnResult r = runtime.submit(event);
        view.add(r);
        return r;
      });
    }

    @Override
    public CompletionStage<List<LogEvent>> conversation(String key) {
      return CompletableFuture.completedFuture(view.events(key));
    }
  }

  /**
   * The owner check read the backend's log, which for a Flink job is the results it has seen. Until
   * the first turn's result came back, the conversation looked empty, so anyone in the tenant could
   * put a turn into it, and then the transcript the model saw was someone else's.
   */
  @Test
  void aConversationWhoseFirstTurnIsStillRunningIsNotOpenToTheTenant() throws Exception {
    LaggingBackend backend = new LaggingBackend();
    gateway = Gateway.start(GatewayOptions.loopback(TOKENS), backend, Map.of());
    String cid = "c-" + UUID.randomUUID();
    CompletableFuture<HttpResponse<String>> first = CompletableFuture.supplyAsync(() -> {
      try {
        return turn(ALICE, cid, "my secret plan");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    assertTrue(backend.submitted.await(10, TimeUnit.SECONDS));
    CompletableFuture<HttpResponse<String>> second = CompletableFuture.supplyAsync(() -> {
      try {
        return turn(MALLORY, cid, "what did alice say?");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    Thread.sleep(300);
    backend.release.countDown();
    assertEquals(200, first.get(20, TimeUnit.SECONDS).statusCode());
    assertEquals(404, second.get(20, TimeUnit.SECONDS).statusCode());
    for (LogEvent e : backend.runtime.log().events(Gateway.scopedKey("acme", cid))) {
      if (e.is(EventType.TURN_RECEIVED)) {
        assertEquals("alice", e.payload().get("user_id"), "nobody else's turn is in alice's conversation");
      }
    }
  }

  /**
   * A resume of a turn that is not there was handed to the runtime, which answered 200 with the
   * conversation's state, so anyone in the tenant could tell a conversation existed and read its turn
   * count. A refused signal answered with the whole result.
   */
  @Test
  void aResumeOfSomeoneElsesConversationSaysNothingAboutIt() throws Exception {
    gateway = Gateway.start(GatewayOptions.loopback(TOKENS), new RuntimeBackend(runtime()), Map.of());
    String cid = "c-" + UUID.randomUUID();
    assertEquals(200, turn(ALICE, cid, "hi").statusCode());
    HttpResponse<String> nothing = call("POST", "/conversations/c-none/turns/t/resume", MALLORY, Map.of());
    HttpResponse<String> unknownTurn = call("POST", "/conversations/" + cid + "/turns/t-none/resume", MALLORY, Map.of());
    assertEquals(404, nothing.statusCode());
    assertEquals(nothing.statusCode(), unknownTurn.statusCode());
    assertEquals(nothing.body(), unknownTurn.body());

    JsonNode held = JSON.readTree(turn(ALICE, cid, "refund me").body());
    assertEquals("suspended", held.path("status").asText());
    HttpResponse<String> refused = call("POST",
        "/conversations/" + cid + "/turns/" + held.path("turn_id").asText() + "/resume", MALLORY, Map.of());
    assertEquals(403, refused.statusCode());
    JsonNode body = JSON.readTree(refused.body());
    assertFalse(body.has("state"), refused.body());
    assertFalse(body.has("events"), refused.body());
  }

  /** A {@code +} in a path read as a space, so a conversation started as {@code a+b} could not be read. */
  @Test
  void aConversationIdInThePathMeansWhatItSays() throws Exception {
    gateway = Gateway.start(GatewayOptions.loopback(TOKENS), new RuntimeBackend(runtime()), Map.of());
    assertTrue(raw("GET /conversations/a%zz HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer " + ALICE
        + "\r\nConnection: close\r\n\r\n").startsWith("HTTP/1.1 400"));
    assertEquals(200, turn(ALICE, "a+b", "hi").statusCode());
    assertEquals(200, call("GET", "/conversations/a+b", ALICE, null).statusCode());
    assertEquals(404, call("GET", "/conversations/a%20b", ALICE, null).statusCode());
  }

  private String raw(String request) throws Exception {
    try (java.net.Socket socket = new java.net.Socket("127.0.0.1", gateway.port())) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(request.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      return new String(socket.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
  }

  /** The issuer's keys over plain HTTP can be swapped by anyone on the path, and every token with them. */
  @Test
  void theIssuersKeysAreFetchedOverHttpsOnly() {
    assertThrows(IllegalArgumentException.class, () -> GatewayOptions.fromEnvironment(Map.of(
        "AGENTIC_OIDC_ISSUER", "http://idp.example/realms/a", "AGENTIC_OIDC_AUDIENCE", "gw")));
    assertThrows(IllegalArgumentException.class, () -> GatewayOptions.fromEnvironment(Map.of(
        "AGENTIC_OIDC_ISSUER", "https://idp.example/realms/a", "AGENTIC_OIDC_AUDIENCE", "gw",
        "AGENTIC_OIDC_JWKS_URL", "http://idp.example/realms/a/protocol/openid-connect/certs")));
    assertThrows(IllegalArgumentException.class, () -> GatewayOptions.fromEnvironment(Map.of(
        "AGENTIC_OIDC_ISSUER", " ", "AGENTIC_OIDC_AUDIENCE", "gw")));
    GatewayOptions.fromEnvironment(Map.of(
        "AGENTIC_OIDC_ISSUER", "https://idp.example/realms/a", "AGENTIC_OIDC_AUDIENCE", "gw"));
    GatewayOptions.fromEnvironment(Map.of(
        "AGENTIC_OIDC_ISSUER", "http://127.0.0.1:18080/realms/a", "AGENTIC_OIDC_AUDIENCE", "gw"));
  }
}
