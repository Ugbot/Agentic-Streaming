package org.agentic.flink.runtime;

import java.io.Serializable;
import java.util.Map;
import org.agentic.flink.annotation.Internal;
import org.jagentic.core.llm.ChatClient;
import org.jagentic.core.llm.ScriptedChatClient;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.core.pipeline.PipelineLoader;

/**
 * Serializable {@link GraphBuilder.ChatClientFactory} implementations for {@link
 * WorkflowTurnFunction}. The factory is shipped with the operator, so it must be {@link
 * Serializable}; the {@link ChatClient} it creates is built inside {@code open()} and never
 * serialized.
 */
@Internal
public final class ChatClientFactories {

  private ChatClientFactories() {}

  /** A {@link GraphBuilder.ChatClientFactory} that Flink can ship with the operator. */
  @FunctionalInterface
  public interface SerializableChatClientFactory
      extends GraphBuilder.ChatClientFactory, Serializable {}

  /**
   * Default factory: refuses every {@code llm:} spec. {@link WorkflowTurnFunction} detects this
   * factory at construction and rejects workflows with an {@code llm} brain there, so a missing LLM
   * configuration fails when the job graph is built, not when the operator opens.
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

  /**
   * The production factory: builds the client the document's {@code llm} block declares, with the
   * same provider code the cores use ({@code PipelineLoader.chatClient}). It holds no state, so
   * nothing but the document ships with the operator. Credentials are read from the environment of
   * the TaskManager when the operator opens; {@link WorkflowTurnFunction} refuses a document that
   * carries one, or an unknown provider, when the job is built.
   */
  public static SerializableChatClientFactory fromWorkflow() {
    return FromWorkflow.INSTANCE;
  }

  static boolean isFromWorkflow(GraphBuilder.ChatClientFactory factory) {
    return factory instanceof FromWorkflow;
  }

  static boolean isFailFast(GraphBuilder.ChatClientFactory factory) {
    return factory instanceof FailFast;
  }

  private enum FailFast implements SerializableChatClientFactory {
    INSTANCE;

    @Override
    public ChatClient create(Map<String, Object> llmSpec) {
      throw new IllegalStateException(
          "no ChatClientFactory configured for llm provider "
              + (llmSpec == null ? null : llmSpec.get("provider"))
              + "; pass one to WorkflowTurnFunction");
    }
  }

  private enum FromWorkflow implements SerializableChatClientFactory {
    INSTANCE;

    @Override
    public ChatClient create(Map<String, Object> llmSpec) {
      return PipelineLoader.chatClient(llmSpec);
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
