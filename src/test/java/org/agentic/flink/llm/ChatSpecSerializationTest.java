package org.agentic.flink.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Anything that ships in the Flink job graph must round-trip Java serialization. {@link
 * ChatConnection} and {@link ChatSetup} both travel with the operator, so each provider
 * implementation has to survive serialize-deserialize without losing fields.
 */
class ChatSpecSerializationTest {

  @Test
  @DisplayName("ChatSetup round-trips with randomized fields")
  void chatSetupRoundTrips() throws Exception {
    String model = "qwen2.5:" + ThreadLocalRandom.current().nextInt(1, 100) + "b";
    double temp = ThreadLocalRandom.current().nextDouble(0.0, 2.0);
    int maxTokens = ThreadLocalRandom.current().nextInt(64, 8192);
    long seed = ThreadLocalRandom.current().nextLong();

    ChatSetup original =
        ChatSetup.builder()
            .withModel(model)
            .withTemperature(temp)
            .withMaxResponseTokens(maxTokens)
            .withSeed(seed)
            .withStopSequences(java.util.List.of("STOP", "END"))
            .build();

    Object restored = roundTrip(original);
    assertInstanceOf(ChatSetup.class, restored);
    ChatSetup r = (ChatSetup) restored;
    assertEquals(model, r.getModelName());
    assertEquals(temp, r.getTemperature(), 1e-9);
    assertEquals(maxTokens, r.getMaxResponseTokens());
    assertEquals(seed, r.getSeed());
    assertEquals(java.util.List.of("STOP", "END"), r.getStopSequences());
  }

  @Test
  @DisplayName("ChatSetup builder rejects empty modelName")
  void chatSetupBuilderRequiresModel() {
    assertThrows(IllegalStateException.class, () -> ChatSetup.builder().build());
    assertThrows(
        IllegalStateException.class, () -> ChatSetup.builder().withModel("").build());
  }

  @Test
  @DisplayName("LangChain4jChatConnection (Ollama) round-trips")
  void ollamaConnectionRoundTrips() throws Exception {
    String baseUrl = "http://" + UUID.randomUUID() + ":11434";
    LangChain4jChatConnection original = LangChain4jChatConnection.ollama(baseUrl);

    Object restored = roundTrip(original);
    assertInstanceOf(LangChain4jChatConnection.class, restored);
    LangChain4jChatConnection r = (LangChain4jChatConnection) restored;
    assertEquals(LangChain4jChatConnection.Provider.OLLAMA, r.getProvider());
    assertEquals(baseUrl, r.getBaseUrl());
    assertEquals(Duration.ofSeconds(60), r.getTimeout());
    assertTrue(r.providerName().contains("ollama"));
  }

  @Test
  @DisplayName("LangChain4jChatConnection (OpenAI) round-trips without leaking the API key string")
  void openAiConnectionRoundTrips() throws Exception {
    String fakeKey = "sk-test-" + UUID.randomUUID();
    LangChain4jChatConnection original = LangChain4jChatConnection.openai(fakeKey);

    Object restored = roundTrip(original);
    assertInstanceOf(LangChain4jChatConnection.class, restored);
    LangChain4jChatConnection r = (LangChain4jChatConnection) restored;
    assertEquals(LangChain4jChatConnection.Provider.OPENAI, r.getProvider());
    assertNull(r.getBaseUrl());
    assertTrue(r.providerName().contains("openai"));
  }

  @Test
  @DisplayName("Default ChatConnection is discovered via ServiceLoader")
  void serviceLoaderFindsDefault() {
    java.util.ServiceLoader<ChatConnection> loader =
        java.util.ServiceLoader.load(ChatConnection.class);
    ChatConnection first = null;
    for (ChatConnection c : loader) {
      first = c;
      break;
    }
    assertNotNull(first, "Expected at least one ChatConnection registered via ServiceLoader");
    assertInstanceOf(LangChain4jChatConnection.class, first);
  }

  private static Object roundTrip(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(obj);
    }
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return ois.readObject();
    }
  }
}
