package org.agentic.flink.example.banking;

import java.io.Serializable;
import java.util.Locale;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;

/**
 * Chooses the chat model for the banking agents from the environment, so the same job runs on a
 * local OpenAI key during development and swaps to the hackathon-mandated {@code gemini-3.5-flash}
 * for marked runs with no code change.
 *
 * <ul>
 *   <li>{@code LLM_PROVIDER} — {@code openai} (default, for local testing) | {@code gemini} |
 *       {@code ollama}.
 *   <li>{@code MODEL} — model name; sensible per-provider default.
 *   <li>{@code OPENAI_API_KEY} / {@code GOOGLE_API_KEY} — credential for the chosen provider.
 *   <li>{@code OLLAMA_BASE_URL} — for the local-LLM path.
 * </ul>
 */
public final class BankingModel implements Serializable {
  private static final long serialVersionUID = 1L;

  private final ChatConnection connection;
  private final ChatSetup setup;

  public BankingModel(ChatConnection connection, ChatSetup setup) {
    this.connection = connection;
    this.setup = setup;
  }

  public ChatConnection connection() {
    return connection;
  }

  public ChatSetup setup() {
    return setup;
  }

  /** Resolve the model from environment variables (see class doc). */
  public static BankingModel fromEnv() {
    String provider = env("LLM_PROVIDER", "openai").toLowerCase(Locale.ROOT);
    String model = System.getenv("MODEL");
    double temperature = 0.2;
    int maxTokens = 1024;

    switch (provider) {
      case "gemini": {
        ChatConnection c =
            LangChain4jChatConnection.gemini(require("GOOGLE_API_KEY"));
        return new BankingModel(
            c, setup(model == null ? "gemini-3.5-flash" : model, temperature, maxTokens));
      }
      case "ollama": {
        ChatConnection c =
            LangChain4jChatConnection.ollama(env("OLLAMA_BASE_URL", "http://localhost:11434"));
        return new BankingModel(c, setup(model == null ? "qwen2.5:3b" : model, temperature, maxTokens));
      }
      case "openai":
      default: {
        ChatConnection c = LangChain4jChatConnection.openai(require("OPENAI_API_KEY"));
        return new BankingModel(c, setup(model == null ? "gpt-4o-mini" : model, temperature, maxTokens));
      }
    }
  }

  private static ChatSetup setup(String model, double temperature, int maxTokens) {
    return ChatSetup.builder()
        .withModel(model)
        .withTemperature(temperature)
        .withMaxResponseTokens(maxTokens)
        .build();
  }

  private static String env(String key, String def) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? def : v;
  }

  private static String require(String key) {
    String v = System.getenv(key);
    if (v == null || v.isBlank()) {
      throw new IllegalStateException(key + " is required for the selected LLM provider");
    }
    return v;
  }
}
