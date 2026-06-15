package org.jagentic.pekko.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

import org.apache.pekko.http.javadsl.ServerBinding;
import org.junit.jupiter.api.Test;

import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoSystem;

/** End-to-end HTTP test against a real bound server on an ephemeral port, driven by the JDK
 * HttpClient (keeps it JUnit 5, no JUnit4 route-testkit). */
class AgentRoutesTest {

  @Test
  void servesAgentCardAndProcessesAgentTurn() throws Exception {
    PekkoSystem sys = new PekkoSystem(AgentDeps.banking());
    ServerBinding binding = HttpFrontDoor.start(
            sys.system(), "127.0.0.1", 0, AgentCard.defaultCard("http://test"), Duration.ofSeconds(10))
        .toCompletableFuture().get();
    int port = binding.localAddress().getPort();
    String base = "http://127.0.0.1:" + port;
    HttpClient client = HttpClient.newHttpClient();
    try {
      var card = client.send(
          HttpRequest.newBuilder(URI.create(base + "/.well-known/agent-card.json")).GET().build(),
          BodyHandlers.ofString());
      assertEquals(200, card.statusCode());
      assertTrue(card.body().contains("agentic-pekko"), card.body());

      var turn = client.send(
          HttpRequest.newBuilder(URI.create(base + "/agent"))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(
                  "{\"conversation_id\":\"c1\",\"user_id\":\"u\",\"text\":\"what is my balance?\"}"))
              .build(),
          BodyHandlers.ofString());
      assertEquals(200, turn.statusCode());
      assertTrue(turn.body().contains("\"path\":\"payments\""), turn.body());
      assertTrue(turn.body().contains("1234.56"), turn.body());
    } finally {
      binding.unbind().toCompletableFuture().get();
      sys.close();
    }
  }
}
