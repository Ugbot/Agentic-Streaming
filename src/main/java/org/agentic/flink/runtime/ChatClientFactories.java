package org.agentic.flink.runtime;

import java.io.Serializable;
import java.util.Map;
import org.jagentic.core.llm.ChatClient;
import org.jagentic.core.llm.ScriptedChatClient;
import org.jagentic.core.pipeline.GraphBuilder;

/**
 * Serializable {@link GraphBuilder.ChatClientFactory} implementations for
 * {@link WorkflowTurnFunction}. The factory is shipped with the operator, so it must be
 * {@link Serializable}; the {@link ChatClient} it creates is built inside {@code open()} and
 * never serialized.
 */
public final class ChatClientFactories {

  private ChatClientFactories() {}

  /** A {@link GraphBuilder.ChatClientFactory} that Flink can ship with the operator. */
  @FunctionalInterface
  public interface SerializableChatClientFactory
      extends GraphBuilder.ChatClientFactory, Serializable {}

  /**
   * Default factory: refuses every {@code llm:} spec. {@link WorkflowTurnFunction} detects this
   * factory at construction and rejects workflows with an {@code llm} brain there, so a missing
   * LLM configuration fails when the job graph is built, not when the operator opens.
   */
  public static SerializableChatClientFactory failFast() {
    return FailFast.INSTANCE;
  }

  /**
   * Factory for the spec's deterministic {@code provider: stub}: the {@code llm.script} steps
   * become a {@link ScriptedChatClient}. Any other provider is rejected with the provider name.
   * {@link GraphBuilder} resolves the stub provider itself, so this factory is only needed when a
   * caller wants to be explicit about it.
   */
  public static SerializableChatClientFactory stub() {
    return Stub.INSTANCE;
  }

  static boolean isFailFast(GraphBuilder.ChatClientFactory factory) {
    return factory instanceof FailFast;
  }

  private enum FailFast implements SerializableChatClientFactory {
    INSTANCE;

    @Override
    public ChatClient create(Map<String, Object> llmSpec) {
      throw new IllegalStateException(
          "no ChatClientFactory configured for llm spec " + llmSpec
              + "; pass one to WorkflowTurnFunction");
    }
  }

  private enum Stub implements SerializableChatClientFactory {
    INSTANCE;

    @Override
    public ChatClient create(Map<String, Object> llmSpec) {
      if (!ScriptedChatClient.accepts(llmSpec)) {
        throw new IllegalArgumentException(
            "ChatClientFactories.stub() only supports llm.provider: stub, got "
                + (llmSpec == null ? null : llmSpec.get("provider")));
      }
      return ScriptedChatClient.fromSpec(llmSpec);
    }
  }
}
