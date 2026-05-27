package org.agentic.flink.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.llm.langchain4j.LangChain4jChatClient;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@link ChatClient} contract: every implementation produces a {@link ChatResponse} with the
 * same fields filled in, regardless of transport. We can't talk to a real LLM in unit tests, so
 * we drive the stub {@link EchoChatConnection} and the LangChain4J connection's bind path (no
 * network) side-by-side and assert response-shape equivalence on the stub.
 */
class ChatClientContractTest {

  @Test
  @DisplayName("EchoChatConnection produces a non-null response with model + finish reason")
  void echoProducesShapedResponse() {
    ChatClient client = new EchoChatConnection("!").bind(null);
    ChatSetup setup = ChatSetup.builder().withModel("echo-1").withTemperature(0.0).build();
    ChatResponse response =
        client.chat(List.of(ChatMessage.user("hello"), ChatMessage.user("world")), setup);

    assertNotNull(response);
    assertEquals("hello\nworld!", response.getText());
    assertEquals("echo-1", response.getModelName());
    assertEquals(ChatResponse.FinishReason.STOP, response.getFinishReason());
    assertFalse(response.hasToolCalls());
    assertNotNull(response.getTokensUsed());
    assertEquals("echo", client.providerName());
  }

  @Test
  @DisplayName("LangChain4jChatClient exposes the underlying model via the escape-hatch interface")
  void langChainEscapeHatch() throws Exception {
    LangChain4jChatConnection connection = LangChain4jChatConnection.ollama("http://localhost:11434");
    ChatClient client = connection.bind(null);

    // The cast is documented as implementation-coupled. We verify it works without making a
    // real LLM call by building the model directly via the connection.
    assertInstanceOf(LangChain4jChatClient.class, client);
    LangChain4jChatClient lc = (LangChain4jChatClient) client;

    // No model built yet — getUnderlyingModel() must signal that explicitly.
    assertThrows(IllegalStateException.class, lc::getUnderlyingModel);

    // Direct build is allowed (and used internally on the first chat() call).
    Object model = connection.buildModel("qwen2.5:3b", 0.5, 1024);
    assertNotNull(model);
    assertTrue(model.getClass().getName().startsWith("dev.langchain4j"));
  }

  @Test
  @DisplayName("Anthropic provider builds a Claude model without making an API call")
  void anthropicProviderBuildsModel() {
    LangChain4jChatConnection connection = LangChain4jChatConnection.anthropic("test-key");
    assertEquals(
        LangChain4jChatConnection.Provider.ANTHROPIC, connection.getProvider());
    assertEquals("langchain4j:anthropic", connection.providerName());
    // Construction is offline; no network call until chat().
    Object model = connection.buildModel("claude-sonnet-4-6", 0.3, 2048);
    assertNotNull(model);
    assertTrue(model.getClass().getName().contains("anthropic"));
  }
}
