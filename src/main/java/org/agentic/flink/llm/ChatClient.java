package org.agentic.flink.llm;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Runtime handle for a chat-style LLM, returned by {@link ChatConnection#bind}.
 *
 * <p>A {@code ChatClient} represents a live connection to a provider (HTTP client + credentials
 * + retry policy) and accepts a {@link ChatSetup} per call to vary model name, temperature, and
 * response shape. The same client serves many agents.
 *
 * <p>Lives inside a Flink {@code RichFunction}: {@link ChatConnection} ships in the job graph,
 * {@code bind()} produces this client in {@code open()}.
 */
public interface ChatClient extends AutoCloseable {

  /** Blocking chat call. */
  ChatResponse chat(List<ChatMessage> messages, ChatSetup setup);

  /** Async chat call. Default implementation runs {@link #chat} on the common pool. */
  default CompletableFuture<ChatResponse> chatAsync(List<ChatMessage> messages, ChatSetup setup) {
    return CompletableFuture.supplyAsync(() -> chat(messages, setup));
  }

  /**
   * The connection's reported provider name (e.g. "ollama", "openai", "langchain4j:ollama").
   * Used for logging and metrics.
   */
  String providerName();

  /** Optional: implementations may release pooled resources here. Default is no-op. */
  @Override
  default void close() {
    // no-op
  }
}
