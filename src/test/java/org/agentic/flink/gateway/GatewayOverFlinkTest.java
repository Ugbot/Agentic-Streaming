package org.agentic.flink.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.agentic.flink.runtime.FlinkRuntimeOptions;
import org.agentic.flink.runtime.LocalWorkflowSession;
import org.agentic.flink.runtime.testkit.Workflows;
import org.jagentic.core.Event;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.gateway.ConversationView;
import org.jagentic.gateway.Gateway;
import org.jagentic.gateway.GatewayOptions;
import org.jagentic.gateway.TurnBackend;
import org.jagentic.gateway.auth.StaticTokens;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The primary runtime behind the front door: a real Flink job, reached by real HTTP, with the
 * turn's principal coming from a token and surviving a stop-with-savepoint before the approver
 * answers.
 */
class GatewayOverFlinkTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String ALICE = "alice-token-0123456789";
  private static final String MALLORY = "mallory-token-0123456789";
  private static final String OPS = "ops-token-0123456789";

  private final HttpClient http = HttpClient.newHttpClient();

  /** What {@code KafkaBackend} does over topics, over the in-process session. */
  private static final class SessionBackend implements TurnBackend {
    private final LocalWorkflowSession session;
    private final ConversationView view = new ConversationView();

    SessionBackend(LocalWorkflowSession session) {
      this.session = session;
    }

    @Override
    public CompletionStage<TurnResult> submit(Event event) {
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              TurnResult r = session.submit(event);
              view.add(r);
              return r;
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
          });
    }

    @Override
    public CompletionStage<List<LogEvent>> conversation(String key) {
      return CompletableFuture.completedFuture(view.events(key));
    }
  }

  private HttpResponse<String> call(int port, String path, String token, Object body)
      throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .header("Authorization", "Bearer " + token);
    return http.send(
        body == null
            ? b.GET().build()
            : b.POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void aTurnThroughTheGatewayKeepsItsPrincipalAcrossARestartOfTheJob(@TempDir Path dir)
      throws Exception {
    Map<String, Object> wf = Workflows.approval();
    StaticTokens tokens =
        StaticTokens.parse(
            ALICE + "=alice|acme;" + MALLORY + "=mallory|acme;" + OPS + "=ops|acme|approver");
    try (LocalWorkflowSession session =
        new LocalWorkflowSession(
            wf,
            FlinkRuntimeOptions.fromSpec(wf),
            1,
            null,
            dir,
            Duration.ofSeconds(60),
            "gateway-" + UUID.randomUUID())) {
      session.start();
      try (Gateway gateway =
          Gateway.start(
              GatewayOptions.loopback(tokens),
              new SessionBackend(session),
              Map.of("name", "flink"))) {
        int port = gateway.port();
        String cid = "c-" + UUID.randomUUID();
        JsonNode held =
            JSON.readTree(
                call(
                        port,
                        "/agent",
                        ALICE,
                        Map.of("conversation_id", cid, "text", "refund me", "user_id", "mallory"))
                    .body());
        assertEquals("suspended", held.path("status").asText());

        session.restart();

        String resume =
            "/conversations/" + cid + "/turns/" + held.path("turn_id").asText() + "/resume";
        assertEquals(403, call(port, resume, MALLORY, Map.of("kind", "approval")).statusCode());
        HttpResponse<String> done = call(port, resume, OPS, Map.of("kind", "approval"));
        assertEquals(200, done.statusCode(), done.body());
        assertEquals("completed", JSON.readTree(done.body()).path("status").asText());

        String log = call(port, "/conversations/" + cid, ALICE, null).body();
        assertTrue(log.contains("\"user_id\":\"alice\""), log);
        assertTrue(log.contains("\"signal_sender\":\"ops\""), log);
        assertTrue(log.contains("signal_refused"), log);
        assertEquals(404, call(port, "/conversations/" + cid, MALLORY, null).statusCode());
      }
    }
  }
}
