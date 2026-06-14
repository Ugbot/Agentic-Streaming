package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Test;

/** Phase D — saga rollback, MoSCoW context compaction, A2A peer-as-tool (against an
 * in-process HttpServer stub). All offline/deterministic. */
class OrchestrationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void sagaRollsBackInReverseOnFailure() {
    List<String> log = new ArrayList<>();
    Saga saga = new Saga();
    saga.step("charge", () -> {
      log.add("charge");
      return "ch_1";
    }, () -> log.add("refund"));
    saga.step("ship", () -> {
      log.add("ship");
      return "sh_1";
    }, () -> log.add("cancel-ship"));

    RuntimeException boom = assertThrows(RuntimeException.class, () ->
        saga.step("reserve", () -> {
          throw new IllegalStateException("inventory gone");
        }, () -> log.add("unreserve")));
    assertEquals("inventory gone", boom.getMessage());

    // reserve's do failed (no undo recorded); ship + charge compensate in reverse
    assertEquals(List.of("charge", "ship", "cancel-ship", "refund"), log);
  }

  @Test
  void contextCompactionKeepsHighestPriorityWithinBudget() {
    List<ContextItem> items = List.of(
        new ContextItem("M".repeat(40), Priority.MUST),
        new ContextItem("S".repeat(40), Priority.SHOULD),
        new ContextItem("C".repeat(40), Priority.COULD),
        new ContextItem("W".repeat(40), Priority.WONT));
    // each item ~ (40+3)/4 = 10 tokens; budget 22 keeps two highest
    List<ContextItem> kept = new ContextWindowManager(22).compact(items);
    List<Priority> prios = kept.stream().map(ContextItem::priority).toList();
    assertTrue(prios.contains(Priority.MUST));
    assertTrue(prios.contains(Priority.SHOULD));
    assertFalse(prios.contains(Priority.COULD));
    assertFalse(prios.contains(Priority.WONT));
  }

  @Test
  void a2aClientCardSendAndPeerTool() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/.well-known/agent-card.json", ex -> {
      byte[] body = "{\"name\":\"Stub Agent\"}".getBytes(StandardCharsets.UTF_8);
      ex.getResponseHeaders().add("Content-Type", "application/json");
      ex.sendResponseHeaders(200, body.length);
      try (OutputStream os = ex.getResponseBody()) {
        os.write(body);
      }
    });
    server.createContext("/agent", ex -> {
      Map<?, ?> req = MAPPER.readValue(ex.getRequestBody(), Map.class);
      byte[] body = MAPPER.writeValueAsBytes(
          Map.of("reply", "echo: " + req.get("text"), "ok", true));
      ex.getResponseHeaders().add("Content-Type", "application/json");
      ex.sendResponseHeaders(200, body.length);
      try (OutputStream os = ex.getResponseBody()) {
        os.write(body);
      }
    });
    server.start();
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort();
      A2AClient client = new A2AClient(base, 2);
      assertEquals("Stub Agent", client.card().get("name"));
      assertEquals("echo: hello", client.send("c1", "hello", "alice").get("reply"));

      var tool = A2AClient.peerTool(base, 2);
      @SuppressWarnings("unchecked")
      Map<String, Object> out = (Map<String, Object>) tool.apply(
          Map.of("conversation_id", "c1", "text", "delegate this"));
      assertNotNull(out);
      assertEquals("echo: delegate this", out.get("reply"));
    } finally {
      server.stop(0);
    }
  }
}
