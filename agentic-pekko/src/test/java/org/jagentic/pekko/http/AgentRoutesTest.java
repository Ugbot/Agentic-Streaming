package org.jagentic.pekko.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.pekko.http.javadsl.ServerBinding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.runtime.TurnWire;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * End-to-end HTTP front door against a real bound server on an ephemeral port, driven by the JDK
 * HttpClient: the request carries an explicit {@code turn_id}, the response is the normalized
 * result document, redelivery is a {@code duplicate}, and malformed input is a 400 with a reason.
 */
class AgentRoutesTest {

  private CountingGraph graph;
  private PekkoSystem sys;
  private ServerBinding binding;
  private String base;
  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void start() throws Exception {
    graph = new CountingGraph();
    sys = new PekkoSystem(graph.deps());
    binding = HttpFrontDoor.start(sys.system(), "127.0.0.1", 0, AgentCard.defaultCard("http://test"),
        Duration.ofSeconds(10)).toCompletableFuture().get();
    base = "http://127.0.0.1:" + binding.localAddress().getPort();
  }

  @AfterEach
  void stop() throws Exception {
    binding.unbind().toCompletableFuture().get();
    sys.close();
  }

  private HttpResponse<String> post(String body) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(base + "/agent"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body)).build(), BodyHandlers.ofString());
  }

  private HttpResponse<String> get(String path) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(base + path)).GET().build(), BodyHandlers.ofString());
  }

  @Test
  void servesAgentCardAndHealth() throws Exception {
    HttpResponse<String> card = get("/.well-known/agent-card.json");
    assertEquals(200, card.statusCode());
    assertTrue(card.body().contains("agentic-pekko"), card.body());
    assertEquals(200, get("/healthz").statusCode());
  }

  @Test
  @SuppressWarnings("unchecked")
  void turnsReturnTheNormalizedResultAndRedeliveryIsADuplicate() throws Exception {
    String cid = "http-" + UUID.randomUUID();
    String t1 = "t-" + UUID.randomUUID();
    String body = TurnWire.write(Map.of("conversation_id", cid, "turn_id", t1, "user_id", "u",
        "text", "what is my balance?", "metadata", Map.of("channel", "web")));

    HttpResponse<String> first = post(body);
    assertEquals(200, first.statusCode());
    Map<String, Object> doc = TurnWire.readObject(first.body());
    assertEquals(List.of("conversation_id", "turn_id", "status", "path", "reply", "state", "tool_calls", "events",
        "error"), List.copyOf(doc.keySet()));
    assertEquals(cid, doc.get("conversation_id"));
    assertEquals(t1, doc.get("turn_id"));
    assertEquals("completed", doc.get("status"));
    assertEquals("payments", doc.get("path"));
    assertTrue(String.valueOf(doc.get("reply")).contains(Double.toString(graph.balance)));
    assertNull(doc.get("error"));
    List<Map<String, Object>> calls = (List<Map<String, Object>>) doc.get("tool_calls");
    assertEquals(1, calls.size());
    assertEquals("lookup", calls.get(0).get("tool"));
    assertEquals(Map.of("user", "u"), calls.get(0).get("args"));
    List<Map<String, Object>> events = (List<Map<String, Object>>) doc.get("events");
    assertEquals("turn_received", events.get(0).get("type"));
    assertEquals(0, events.get(0).get("sequence"));
    assertEquals(1, ((Map<String, Object>) doc.get("state")).get("turn_count"));

    HttpResponse<String> second = post(body);
    assertEquals(200, second.statusCode());
    Map<String, Object> dup = TurnWire.readObject(second.body());
    assertEquals("duplicate", dup.get("status"));
    assertEquals(doc.get("reply"), dup.get("reply"));
    assertEquals(List.of(), dup.get("events"));
    assertEquals(1, graph.brainCalls.get());

    HttpResponse<String> state = get("/conversations/" + cid);
    assertEquals(200, state.statusCode());
    Map<String, Object> view = TurnWire.readObject(state.body());
    assertEquals(cid, view.get("conversation_id"));
    assertEquals(events.size(), ((List<?>) view.get("events")).size());
    assertEquals(List.of(), view.get("suspended"));
  }

  @Test
  void resumeSignalsAreStructuredAndSuspendedTurnsShowInTheConversationView() throws Exception {
    String cid = "http-" + UUID.randomUUID();
    String t1 = "t-" + UUID.randomUUID();
    Map<String, Object> suspended = TurnWire.readObject(post(TurnWire.write(
        Map.of("conversation_id", cid, "turn_id", t1, "text", "refund me"))).body());
    assertEquals("suspended", suspended.get("status"));
    assertEquals(List.of(t1), TurnWire.readObject(get("/conversations/" + cid).body()).get("suspended"));

    Map<String, Object> resumed = TurnWire.readObject(post(TurnWire.write(
        Map.of("conversation_id", cid, "turn_id", t1, "signal", Map.of("kind", "approval", "approved", true)))).body());
    assertEquals("completed", resumed.get("status"));
    assertEquals(t1, resumed.get("turn_id"));
  }

  @Test
  void malformedRequestsAre400WithAReason() throws Exception {
    HttpResponse<String> noCid = post("{\"text\":\"hi\"}");
    assertEquals(400, noCid.statusCode());
    assertTrue(noCid.body().contains("conversation_id"), noCid.body());
    HttpResponse<String> notJson = post("not json at all");
    assertEquals(400, notJson.statusCode());
    HttpResponse<String> badMetadata = post("{\"conversation_id\":\"c\",\"metadata\":[1,2]}");
    assertEquals(400, badMetadata.statusCode());
    assertTrue(badMetadata.body().contains("metadata"), badMetadata.body());
    assertEquals(0, graph.brainCalls.get());
  }
}
