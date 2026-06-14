package org.jagentic.core.llm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;

/**
 * Real chat via LangChain4J (Ollama / OpenAI / Anthropic / Gemini), behind the framework
 * {@link ChatClient} SPI. Uses the same JSON-mode ReAct protocol as the rest of the
 * framework ({@code {"tool": ...}} / {@code {"text": ...}}), so {@code LlmBrain} works
 * unchanged. Optional dep — the model-free {@code StubChatClient} needs none of it.
 */
public final class LangChain4jChatClient implements ChatClient {

  private final ChatLanguageModel model;

  private LangChain4jChatClient(ChatLanguageModel model) {
    this.model = model;
  }

  /** provider = "ollama" | "openai". */
  public static LangChain4jChatClient create(String provider, String modelName, String baseUrl) {
    if ("openai".equals(provider)) {
      OpenAiChatModel.OpenAiChatModelBuilder b = OpenAiChatModel.builder()
          .apiKey(envOr("OPENAI_API_KEY", "no-key"))
          .modelName(modelName == null || modelName.isBlank() ? "gpt-4o-mini" : modelName)
          .responseFormat("json_object");
      if (baseUrl != null && !baseUrl.isBlank()) {
        b.baseUrl(baseUrl);
      }
      return new LangChain4jChatClient(b.build());
    }
    ChatLanguageModel m = OllamaChatModel.builder()
        .baseUrl(baseUrl == null || baseUrl.isBlank() ? "http://localhost:11434" : baseUrl)
        .modelName(modelName == null || modelName.isBlank() ? "llama3.2" : modelName)
        .format("json")
        .build();
    return new LangChain4jChatClient(m);
  }

  private static String envOr(String key, String fallback) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? fallback : v;
  }

  @Override
  public ChatResult chat(List<Map<String, String>> messages, List<Map<String, String>> tools) {
    List<ChatMessage> lc = new ArrayList<>(messages.size());
    for (Map<String, String> m : messages) {
      String role = m.getOrDefault("role", "user");
      String content = m.getOrDefault("content", "");
      switch (role) {
        case "system" -> lc.add(SystemMessage.from(content));
        case "assistant" -> lc.add(AiMessage.from(content));
        default -> lc.add(UserMessage.from(content)); // user + tool observations
      }
    }
    String text = model.generate(lc).content().text();
    return AbstractHttpChatClient.parseChatJson(text);
  }
}
