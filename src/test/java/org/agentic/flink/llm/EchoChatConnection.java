package org.agentic.flink.llm;

import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * Deterministic stub {@link ChatConnection} for tests.
 *
 * <p>Echoes the concatenated user-message contents as the assistant reply, plus a configurable
 * suffix. Used to verify that the framework path produces shape-equivalent {@link ChatResponse}s
 * regardless of which transport backs them.
 */
public final class EchoChatConnection implements ChatConnection {
  private static final long serialVersionUID = 1L;

  private final String suffix;

  public EchoChatConnection() {
    this("");
  }

  public EchoChatConnection(String suffix) {
    this.suffix = suffix == null ? "" : suffix;
  }

  @Override
  public ChatClient bind(RuntimeContext runtimeContext) {
    return new ChatClient() {
      @Override
      public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
        StringBuilder sb = new StringBuilder();
        for (ChatMessage m : messages) {
          if (m.getRole() == ChatRole.USER) {
            if (sb.length() > 0) sb.append('\n');
            sb.append(m.getContent());
          }
        }
        sb.append(suffix);
        return new ChatResponse(
            sb.toString(),
            setup.getModelName(),
            java.util.Collections.emptyList(),
            (long) sb.length(),
            ChatResponse.FinishReason.STOP);
      }

      @Override
      public String providerName() {
        return "echo";
      }
    };
  }

  @Override
  public String providerName() {
    return "echo";
  }
}
