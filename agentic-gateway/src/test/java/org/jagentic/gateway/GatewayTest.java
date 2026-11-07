package org.jagentic.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.gateway.auth.StaticTokens;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Real HTTP against a real runtime: who can do what, and what a caller cannot claim. */
class GatewayTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String ALICE = "alice-token-0123456789";
  private static final String BOB = "bob-token-0123456789";
  private static final String MALLORY = "mallory-token-0123456789";
  private static final String OPS = "ops-token-0123456789";
  private static final String AUDITOR = "auditor-token-0123456789";
  private static final String NO_TENANT = "no-tenant-token-012345";

  private final HttpClient http = HttpClient.newHttpClient();
  private Gateway gateway;
  private LocalRuntime runtime;

  private static Map<String, Object> workflow() {
    return Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "pay", "router", Map.of("kind", "keyword", "default", "main",
                "rules", Map.of("held", List.of("refund"))),
            "paths", Map.of(
                "main", Map.of("brain", "rule", "tools", List.of("whoami"), "tool_triggers", Map.of("who", "whoami")),
                "held", Map.of("brain", "rule", "x-suspend-until", "approval")),
            "verifier", Map.of("kind", "none")),
        "policies", Map.of("approvers", Map.of("roles", List.of("approver")),
            "limits", Map.of("tool_calls_per_day", Map.of("whoami", 2))),
        "tools", List.of(Map.of("id", "whoami", "kind", "constant", "effect", "read", "value", "ok")));
  }

  @BeforeEach
  void start() throws Exception {
    GraphBuilder.Built built = GraphBuilder.build(workflow(), null);
    runtime = new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), null, new ConversationLog.InMemory());
    StaticTokens tokens = StaticTokens.parse(ALICE + "=alice|acme;" + BOB + "=bob|beta;" + MALLORY + "=mallory|acme;"
        + OPS + "=ops|acme|approver;" + AUDITOR + "=auditor|acme|audit;" + NO_TENANT + "=x|");
    gateway = Gateway.start(GatewayOptions.loopback(tokens).withReaderRoles("audit"),
        new RuntimeBackend(runtime), Map.of("name", "test"));
  }

  @AfterEach
  void stop() {
    gateway.close();
  }

  private HttpResponse<String> call(String method, String path, String token, Object body) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + gateway.port() + path));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    String raw = body instanceof String s ? s : body == null ? "" : JSON.writeValueAsString(body);
    b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(raw));
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> turn(String token, String cid, String text) throws Exception {
    return call("POST", "/agent", token, Map.of("conversation_id", cid, "text", text));
  }

  private static JsonNode json(HttpResponse<String> r) throws Exception {
    return JSON.readTree(r.body());
  }

  private static String cid() {
    return "c-" + UUID.randomUUID();
  }

  @Test
  void noTokenIsRefusedEverywhereThatMatters() throws Exception {
    assertEquals(401, turn(null, cid(), "hi").statusCode());
    assertEquals(401, call("GET", "/conversations/" + cid(), null, null).statusCode());
    assertEquals(401, call("POST", "/conversations/c/turns/t/resume", null, Map.of()).statusCode());
    assertEquals("Bearer", turn(null, cid(), "hi").headers().firstValue("WWW-Authenticate").orElse(""));
    assertEquals(401, turn("wrong-token-0123456789", cid(), "hi").statusCode());
  }

  @Test
  void onlyHealthAndTheCardAreOpen() throws Exception {
    assertEquals(200, call("GET", "/healthz", null, null).statusCode());
    assertEquals(200, call("GET", "/.well-known/agent-card.json", null, null).statusCode());
    assertEquals(404, call("GET", "/anything-else", null, null).statusCode());
  }

  @Test
  void aTokenWithNoTenantGetsNothing() throws Exception {
    assertEquals(403, turn(NO_TENANT, cid(), "hi").statusCode());
  }

  @Test
  void aSpoofedUserAndPrincipalInTheBodyMeanNothing() throws Exception {
    String cid = cid();
    Map<String, Object> body = new HashMap<>(Map.of("conversation_id", cid, "text", "who am i", "user_id", "mallory",
        "tenant", "beta", "principal", Map.of("subject", "root", "tenant", "beta", "roles", List.of("approver"))));
    JsonNode r = json(call("POST", "/agent", ALICE, body));
    assertEquals("completed", r.path("status").asText());
    assertEquals(cid, r.path("conversation_id").asText());
    JsonNode log = json(call("GET", "/conversations/" + cid, ALICE, null)).path("events");
    JsonNode received = log.get(0).path("payload");
    assertEquals("alice", received.path("user_id").asText());
    assertEquals("alice", received.path("principal").path("subject").asText());
    assertEquals("acme", received.path("principal").path("tenant").asText());
    assertEquals("alice", json(call("POST", "/agent", ALICE, Map.of("conversation_id", cid, "text", "who")))
        .path("tool_calls").get(0).path("args").path("user").asText());
  }

  @Test
  void anotherTenantCannotReachAConversationByItsName() throws Exception {
    String cid = cid();
    assertEquals(200, turn(ALICE, cid, "hi").statusCode());
    assertEquals(404, call("GET", "/conversations/" + cid, BOB, null).statusCode());
    assertEquals(200, turn(BOB, cid, "mine now").statusCode(), "the same name is a new conversation in beta");
    assertTrue(call("GET", "/conversations/" + cid, ALICE, null).body().contains("\"hi\""));
    assertFalse(call("GET", "/conversations/" + cid, BOB, null).body().contains("\"hi\""));
  }

  @Test
  void inATenantOnlyTheOwnerOrAReaderGetsIn() throws Exception {
    String cid = cid();
    turn(ALICE, cid, "hi");
    HttpResponse<String> read = call("GET", "/conversations/" + cid, MALLORY, null);
    assertEquals(404, read.statusCode());
    assertEquals(call("GET", "/conversations/" + cid(), MALLORY, null).body(), read.body(),
        "someone else's conversation looks like no conversation");
    assertEquals(404, turn(MALLORY, cid, "let me in").statusCode());
    assertEquals(200, call("GET", "/conversations/" + cid, AUDITOR, null).statusCode());
    assertEquals(404, turn(AUDITOR, cid, "a reader cannot write").statusCode());
  }

  @Test
  void anUnauthorisedApproverIsRefusedAndAnApproverIsNot() throws Exception {
    String cid = cid();
    JsonNode held = json(turn(ALICE, cid, "refund me"));
    assertEquals("suspended", held.path("status").asText());
    String path = "/conversations/" + cid + "/turns/" + held.path("turn_id").asText() + "/resume";

    assertEquals(403, call("POST", path, MALLORY, Map.of("kind", "approval")).statusCode());
    assertEquals(403, call("POST", path, ALICE, Map.of("kind", "approval")).statusCode(), "not your own approver");
    assertEquals(404, call("POST", path, BOB, Map.of("kind", "approval")).statusCode(), "another tenant's name");
    assertTrue(call("GET", "/conversations/" + cid, ALICE, null).body().contains("signal_refused"));

    HttpResponse<String> approved = call("POST", path, OPS, Map.of("kind", "approval"));
    assertEquals(200, approved.statusCode());
    assertEquals("completed", json(approved).path("status").asText());
    String log = call("GET", "/conversations/" + cid, ALICE, null).body();
    assertTrue(log.contains("\"signal_sender\":\"ops\""), log);
  }

  @Test
  void aLimitIsA429AndIsOnTheLog() throws Exception {
    String cid = cid();
    assertEquals(200, turn(ALICE, cid, "who").statusCode());
    assertEquals(200, turn(ALICE, cid, "who").statusCode());
    assertEquals(429, turn(ALICE, cid, "who").statusCode());
    assertTrue(call("GET", "/conversations/" + cid, ALICE, null).body().contains("limit_exceeded"));
  }

  @Test
  void aMalformedOrOversizedBodyIsRefused() throws Exception {
    assertEquals(400, call("POST", "/agent", ALICE, "not json").statusCode());
    assertEquals(400, call("POST", "/agent", ALICE, "[1,2]").statusCode());
    assertEquals(400, call("POST", "/agent", ALICE, Map.of("text", "no conversation")).statusCode());
    assertEquals(400, call("POST", "/agent", ALICE, Map.of("conversation_id", "c")).statusCode());
    assertEquals(413, call("POST", "/agent", ALICE, "x".repeat((1 << 20) + 1)).statusCode());
  }

  @Test
  void conversationKeysCannotCollide() {
    assertNotEquals(Gateway.scopedKey("a/b", "c"), Gateway.scopedKey("a", "b/c"));
    assertNotEquals(Gateway.scopedKey("a", "b"), Gateway.scopedKey("b", "a"));
  }

  @Test
  void itDoesNotStartWithoutAuthentication() {
    assertThrows(IllegalArgumentException.class, () -> GatewayOptions.fromEnvironment(Map.of()));
  }

  @Test
  void staticTokensNeedTheirOwnSwitchAndLoopback() {
    String spec = ALICE + "=alice|acme";
    assertThrows(IllegalArgumentException.class,
        () -> GatewayOptions.fromEnvironment(Map.of("AGENTIC_GATEWAY_STATIC_TOKENS", spec)));
    assertThrows(IllegalArgumentException.class, () -> GatewayOptions.fromEnvironment(Map.of(
        "AGENTIC_GATEWAY_STATIC_TOKENS", spec, "AGENTIC_GATEWAY_ALLOW_STATIC_TOKENS", "true",
        "AGENTIC_GATEWAY_HOST", "0.0.0.0", "AGENTIC_GATEWAY_BEHIND_TLS_PROXY", "true")));
    assertEquals("127.0.0.1", GatewayOptions.fromEnvironment(Map.of(
        "AGENTIC_GATEWAY_STATIC_TOKENS", spec, "AGENTIC_GATEWAY_ALLOW_STATIC_TOKENS", "true")).host());
    assertThrows(IllegalArgumentException.class, () -> StaticTokens.parse("short=alice|acme"));
    assertThrows(IllegalArgumentException.class, () -> StaticTokens.parse(" ; "));
  }

  @Test
  void anythingButLoopbackNeedsATlsProxyInFront() {
    Map<String, String> oidc = new HashMap<>(Map.of("AGENTIC_OIDC_ISSUER", "https://idp.example",
        "AGENTIC_OIDC_AUDIENCE", "gw", "AGENTIC_GATEWAY_HOST", "0.0.0.0"));
    assertThrows(IllegalArgumentException.class, () -> GatewayOptions.fromEnvironment(oidc));
    oidc.put("AGENTIC_GATEWAY_BEHIND_TLS_PROXY", "true");
    assertEquals("0.0.0.0", GatewayOptions.fromEnvironment(oidc).host());
    oidc.remove("AGENTIC_OIDC_AUDIENCE");
    assertThrows(IllegalArgumentException.class, () -> GatewayOptions.fromEnvironment(oidc));
    assertFalse(oidc.containsKey("AGENTIC_OIDC_AUDIENCE"));
  }
}
