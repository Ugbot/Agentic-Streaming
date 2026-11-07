package org.jagentic.pekko.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.jagentic.gateway.Gateway;
import org.jagentic.gateway.GatewayOptions;
import org.jagentic.gateway.auth.StaticTokens;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.runtime.TurnWire;
import org.jagentic.pekko.testing.CountingGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The gateway over real conversation entities: the front door the Pekko routes used to leave open. */
class PekkoGatewayTest {

  private static final String ALICE = "alice-token-0123456789";
  private static final String MALLORY = "mallory-token-0123456789";
  private static final String OPS = "ops-token-0123456789";

  private PekkoSystem sys;
  private Gateway gateway;
  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void start() throws Exception {
    sys = new PekkoSystem(new CountingGraph().deps());
    StaticTokens tokens = StaticTokens.parse(ALICE + "=alice|acme;" + MALLORY + "=mallory|acme;" + OPS + "=ops|acme|approver");
    gateway = Gateway.start(GatewayOptions.loopback(tokens), new PekkoBackend(sys.system(), Duration.ofSeconds(10)),
        AgentCard.defaultCard("http://test").toJson());
  }

  @AfterEach
  void stop() throws Exception {
    gateway.close();
    sys.close();
  }

  private HttpResponse<String> call(String method, String path, String token, Map<String, Object> body) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + gateway.port() + path));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    return client.send(b.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
        : HttpRequest.BodyPublishers.ofString(TurnWire.write(body))).build(), BodyHandlers.ofString());
  }

  @Test
  void theDoorIsShutWithoutAToken() throws Exception {
    assertEquals(401, call("POST", "/agent", null, Map.of("conversation_id", "c", "text", "hi")).statusCode());
    assertEquals(401, call("GET", "/conversations/c", null, null).statusCode());
    assertEquals(200, call("GET", "/.well-known/agent-card.json", null, null).statusCode());
  }

  @Test
  void aTurnRunsAsTheTokenAndOnlyItsOwnerReadsTheLog() throws Exception {
    String cid = "c-" + UUID.randomUUID();
    HttpResponse<String> turn = call("POST", "/agent", ALICE,
        Map.of("conversation_id", cid, "text", "hello", "user_id", "mallory"));
    assertEquals(200, turn.statusCode());
    assertEquals(cid, TurnWire.readObject(turn.body()).get("conversation_id"));
    HttpResponse<String> log = call("GET", "/conversations/" + cid, ALICE, null);
    assertEquals(200, log.statusCode());
    assertTrue(log.body().contains("\"user_id\":\"alice\""), log.body());
    assertEquals(404, call("GET", "/conversations/" + cid, MALLORY, null).statusCode());
  }

  @Test
  void onlyAnApproverResumesAndTheApproverIsOnTheLog() throws Exception {
    String cid = "c-" + UUID.randomUUID();
    Map<String, Object> held = TurnWire.readObject(call("POST", "/agent", ALICE,
        Map.of("conversation_id", cid, "text", "refund me")).body());
    assertEquals("suspended", held.get("status"));
    String path = "/conversations/" + cid + "/turns/" + held.get("turn_id") + "/resume";
    assertEquals(403, call("POST", path, MALLORY, Map.of("kind", "approval")).statusCode());
    HttpResponse<String> done = call("POST", path, OPS, Map.of("kind", "approval"));
    assertEquals(200, done.statusCode());
    assertEquals("completed", TurnWire.readObject(done.body()).get("status"));
    String log = call("GET", "/conversations/" + cid, ALICE, null).body();
    assertTrue(log.contains("\"signal_sender\":\"ops\"") && log.contains("signal_refused"), log);
  }
}
