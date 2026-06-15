package org.agentic.flink.llm.langchain4j;

import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import dev.langchain4j.model.chat.ChatModel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default {@link org.agentic.flink.llm.ChatClient} backed by LangChain4J.
 *
 * <p>Caches LangChain4J {@link ChatModel} instances per {@code (modelName, temperature,
 * maxTokens)} signature so repeated calls reuse the same client. The cache lives for the
 * lifetime of the Flink task; cleanup happens automatically when the task closes.
 *
 * <p>Implements {@link LangChain4jChatClient} so power users can downcast and reach the
 * underlying model — see {@link #getUnderlyingModel()} for the contract.
 */
final class ChatClientImpl implements LangChain4jChatClient {

  private final LangChain4jChatConnection connection;
  private final ConcurrentMap<String, ChatModel> modelCache = new ConcurrentHashMap<>();
  private volatile ChatModel lastModel;

  ChatClientImpl(LangChain4jChatConnection connection) {
    this.connection = connection;
  }

  @Override
  public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
    ChatModel model = modelFor(setup);
    List<dev.langchain4j.data.message.ChatMessage> lcMessages = new ArrayList<>(messages.size());
    for (ChatMessage m : messages) {
      lcMessages.add(LangChain4jChatConnection.toLangChainMessage(m));
    }
    dev.langchain4j.model.chat.response.ChatResponse response = model.chat(lcMessages);
    return LangChain4jChatConnection.fromLangChainResponse(response, setup.getModelName());
  }

  @Override
  public String providerName() {
    return connection.providerName();
  }

  @Override
  public ChatModel getUnderlyingModel() {
    ChatModel m = lastModel;
    if (m == null) {
      throw new IllegalStateException(
          "No model has been built yet — call chat() at least once before reaching for the "
              + "underlying model. Alternatively, build one explicitly via connection.buildModel(...).");
    }
    return m;
  }

  @Override
  public void close() {
    modelCache.clear();
    lastModel = null;
  }

  private ChatModel modelFor(ChatSetup setup) {
    String signature =
        setup.getModelName() + "|" + setup.getTemperature() + "|" + setup.getMaxResponseTokens();
    ChatModel m =
        modelCache.computeIfAbsent(
            signature,
            sig ->
                connection.buildModel(
                    setup.getModelName(), setup.getTemperature(), setup.getMaxResponseTokens()));
    lastModel = m;
    return m;
  }
}
