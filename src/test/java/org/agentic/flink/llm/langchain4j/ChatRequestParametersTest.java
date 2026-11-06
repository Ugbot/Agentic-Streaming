package org.agentic.flink.llm.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection.Provider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What actually goes on the wire. Sampling parameters are a 400 on current Claude models, and on
 * OpenAI reasoning models outside reasoning effort {@code none}, so these read the request body
 * instead of trusting the builder.
 */
class ChatRequestParametersTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private final List<String> bodies = new CopyOnWriteArrayList<>();
  private HttpServer server;

  @BeforeEach
  void start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        e -> {
          bodies.add(new String(e.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] body = "{\"error\":{\"message\":\"refused by the test\"}}".getBytes();
          e.sendResponseHeaders(400, body.length);
          e.getResponseBody().write(body);
          e.close();
        });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private JsonNode requestFrom(Provider provider, String path, String model, Double temperature)
      throws Exception {
    String base = "http://127.0.0.1:" + server.getAddress().getPort() + path;
    var connection =
        new LangChain4jChatConnection(provider, base, "test-key", Duration.ofSeconds(5));
    try {
      connection.buildModel(model, temperature, 256).chat("hello");
    } catch (RuntimeException refusedByTheTestServer) {
      // only the request matters here
    }
    return JSON.readTree(bodies.get(0));
  }

  @Test
  void currentClaudeModelsAreNeverSentTemperature() throws Exception {
    for (String model :
        List.of("claude-fable-5-1", "claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5")) {
      bodies.clear();
      JsonNode body = requestFrom(Provider.ANTHROPIC, "/v1/", model, 0.3);
      assertFalse(body.has("temperature"), model + ": " + body);
      assertEquals(256, body.path("max_tokens").asInt(), model);
    }
  }

  @Test
  void anOlderClaudeModelKeepsADeclaredTemperature() throws Exception {
    JsonNode body = requestFrom(Provider.ANTHROPIC, "/v1/", "claude-sonnet-4-6", 0.3);
    assertEquals(0.3, body.path("temperature").asDouble(), 1e-9);
  }

  @Test
  void aTemperatureNobodyDeclaredIsNotSentToAnyone() throws Exception {
    assertFalse(
        requestFrom(Provider.ANTHROPIC, "/v1/", "claude-sonnet-4-6", null).has("temperature"));
    bodies.clear();
    assertFalse(requestFrom(Provider.OPENAI, "/v1/", "gpt-4.1", null).has("temperature"));
  }

  @Test
  void aChatSetupDoesNotDeclareATemperatureByDefault() {
    ChatSetup setup = ChatSetup.builder().withModel("m").build();
    assertNull(setup.getDeclaredTemperature());
    assertEquals(
        0.25,
        ChatSetup.builder().withModel("m").withTemperature(0.25).build().getDeclaredTemperature());
  }

  @Test
  void gpt6IsAReasoningRequest() throws Exception {
    JsonNode body = requestFrom(Provider.OPENAI, "/v1/", "gpt-6-luna", 0.3);
    assertFalse(body.has("temperature"), body.toString());
    assertFalse(body.has("max_tokens"), body.toString());
    assertEquals(256, body.path("max_completion_tokens").asInt(), body.toString());
  }

  @Test
  void aModelThatTakesSamplingKeepsIt() throws Exception {
    JsonNode body = requestFrom(Provider.OPENAI, "/v1/", "gpt-4.1", 0.3);
    assertTrue(body.has("temperature"), body.toString());
  }
}
