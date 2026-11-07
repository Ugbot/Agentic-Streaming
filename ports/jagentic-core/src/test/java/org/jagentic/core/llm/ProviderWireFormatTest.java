package org.jagentic.core.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.jagentic.core.AgentContext;
import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Policies;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TransientFailure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Each provider's client against a local server that checks what that provider's API requires and
 * refuses what it refuses. This proves the wire format as the tests state it, not any live model.
 */
class ProviderWireFormatTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String KEY = "stub-key-" + System.nanoTime();
  private static final List<Map<String, String>> MESSAGES = List.of(
      Map.of("role", "system", "content", "be brief"),
      Map.of("role", "user", "content", "hello"),
      Map.of("role", "assistant", "content", "{\"tool\":\"look\"}"),
      Map.of("role", "user", "content", "Result of look: 42"));

  private HttpServer server;
  private final List<JsonNode> bodies = new ArrayList<>();
  private final List<HttpExchange> exchanges = new ArrayList<>();
  private int status = 200;
  private String reply;

  @BeforeEach
  void start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", e -> {
      bodies.add(JSON.readTree(e.getRequestBody().readAllBytes()));
      exchanges.add(e);
      byte[] out = reply.getBytes(StandardCharsets.UTF_8);
      e.sendResponseHeaders(status, out.length);
      e.getResponseBody().write(out);
      e.close();
    });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private String base() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @Test
  void anthropicIsSentItsOwnShapeAndAnswersThroughTextBlocks() {
    reply = "{\"stop_reason\":\"end_turn\",\"content\":[{\"type\":\"thinking\",\"thinking\":\"\"},"
        + "{\"type\":\"text\",\"text\":\"{\\\"text\\\":\\\"it is 42\\\"}\"}]}";
    ChatResult r = new AnthropicChatClient("claude-sonnet-5-5", KEY, base(), null, null).chat(MESSAGES, List.of());
    assertEquals("it is 42", r.text());
    HttpExchange e = exchanges.get(0);
    assertEquals("/v1/messages", e.getRequestURI().getPath());
    assertEquals(KEY, e.getRequestHeaders().getFirst("x-api-key"));
    assertEquals("2023-06-01", e.getRequestHeaders().getFirst("anthropic-version"));
    JsonNode body = bodies.get(0);
    assertEquals("claude-sonnet-5-5", body.get("model").asText());
    assertTrue(body.get("max_tokens").asInt() > 0, "required");
    assertEquals("be brief", body.get("system").asText());
    assertEquals(3, body.get("messages").size(), "the system prompt is not a message");
    assertEquals("user", body.get("messages").get(0).get("role").asText());
    assertEquals("user", body.get("messages").get(2).get("role").asText(), "no prefill");
    assertFalse(body.has("temperature"), "not sent unless declared");
  }

  @Test
  void anthropicToolCallsParse() {
    reply = "{\"content\":[{\"type\":\"text\",\"text\":\"{\\\"tool\\\":\\\"pay\\\",\\\"args\\\":{\\\"to\\\":\\\"x\\\"}}\"}]}";
    ChatResult r = new AnthropicChatClient("m", KEY, base(), 0.5, 100).chat(MESSAGES, List.of());
    assertEquals("pay", r.tool());
    assertEquals(Map.of("to", "x"), r.args());
    assertEquals(100, bodies.get(0).get("max_tokens").asInt());
    assertEquals(0.5, bodies.get(0).get("temperature").asDouble());
  }

  @Test
  void anAnthropicRefusalIsAnErrorNotAnEmptyAnswerAndIsNotRetried() {
    reply = "{\"stop_reason\":\"refusal\",\"content\":[]}";
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> new AnthropicChatClient("m", KEY, base(), null, null).chat(MESSAGES, List.of()));
    assertFalse(e instanceof TransientFailure);
  }

  @Test
  void anOverloadedProviderIsTransientAndACredentialInAnErrorBodyDoesNotGetOut() {
    status = 529;
    reply = "{\"error\":\"overloaded for key " + KEY + "\"}" + "x".repeat(500);
    TransientFailure e = assertThrows(TransientFailure.class,
        () -> new AnthropicChatClient("m", KEY, base(), null, null).chat(MESSAGES, List.of()));
    assertFalse(e.getMessage().contains(KEY), e.getMessage());
    assertTrue(e.getMessage().length() < 300, e.getMessage());
  }

  @Test
  void aRejectedKeyIsNotRetried() {
    status = 401;
    reply = "{\"error\":\"invalid x-api-key\"}";
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> new AnthropicChatClient("m", KEY, base(), null, null).chat(MESSAGES, List.of()));
    assertFalse(e instanceof TransientFailure);
  }

  @Test
  void geminiIsSentItsOwnShapeWithTheKeyInAHeader() {
    reply = "{\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"role\":\"model\",\"parts\":"
        + "[{\"text\":\"{\\\"text\\\":\"},{\"text\":\"\\\"it is 42\\\"}\"}]}}]}";
    ChatResult r = new GeminiChatClient("some-gemini", KEY, base() + "/v1beta", 0.25, 64).chat(MESSAGES, List.of());
    assertEquals("it is 42", r.text());
    HttpExchange e = exchanges.get(0);
    assertEquals("/v1beta/models/some-gemini:generateContent", e.getRequestURI().getPath());
    assertEquals(null, e.getRequestURI().getQuery(), "the key stays out of the URL");
    assertEquals(KEY, e.getRequestHeaders().getFirst("x-goog-api-key"));
    JsonNode body = bodies.get(0);
    assertEquals("be brief", body.at("/systemInstruction/parts/0/text").asText());
    assertEquals(3, body.get("contents").size());
    assertEquals("model", body.at("/contents/1/role").asText());
    assertEquals("application/json", body.at("/generationConfig/responseMimeType").asText());
    assertEquals(64, body.at("/generationConfig/maxOutputTokens").asInt());
    assertEquals(0.25, body.at("/generationConfig/temperature").asDouble());
  }

  @Test
  void aBlockedGeminiPromptIsAnErrorNotAnEmptyAnswer() {
    reply = "{\"promptFeedback\":{\"blockReason\":\"SAFETY\"}}";
    assertThrows(IllegalStateException.class,
        () -> new GeminiChatClient("m", KEY, base(), null, null).chat(MESSAGES, List.of()));
  }

  @Test
  void aMissingKeySaysWhichOneAndNoValue() {
    assertTrue(assertThrows(IllegalStateException.class,
        () -> new AnthropicChatClient("m", null, base(), null, null)).getMessage().contains("ANTHROPIC_API_KEY"));
    assertTrue(assertThrows(IllegalStateException.class,
        () -> new GeminiChatClient("m", "", base(), null, null)).getMessage().contains("GEMINI_API_KEY"));
  }

  /** OpenAI answers 400 to a {@code tool} message with no {@code tool_call_id}. */
  @Test
  void theBrainNeverSendsTheToolRoleToARealProvider() {
    List<List<Map<String, String>>> seen = new ArrayList<>();
    ChatClient strict = (messages, tools) -> {
      seen.add(List.copyOf(messages));
      for (Map<String, String> m : messages) {
        assertFalse("tool".equals(m.get("role")), "role tool would be a 400: " + messages);
      }
      return seen.size() == 1 ? ChatResult.toolCall("look", Map.of()) : ChatResult.text("done");
    };
    ToolRegistry tools = new ToolRegistry().registerKeyed("look", "looks", null, "read", (a, c) -> 42);
    AgentContext ctx = new AgentContext("c", "t", "u", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), tools, null, new ConversationLog.InMemory(), Policies.DEFAULTS);
    String reply = new LlmBrain(strict, "p").turn("hello", ctx);
    assertTrue(reply.contains("done"), reply);
    Map<String, String> last = seen.get(1).get(seen.get(1).size() - 1);
    assertEquals("user", last.get("role"));
    assertEquals("Result of look: 42", last.get("content"));
  }
}
