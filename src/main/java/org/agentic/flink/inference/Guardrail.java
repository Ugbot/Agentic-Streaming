package org.agentic.flink.inference;

import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import java.io.Serializable;
import java.util.List;

/**
 * Pre/post-LLM interceptor that can block or rewrite a chat interaction.
 *
 * <p>Guardrails fire from inside {@link org.agentic.flink.execution.LLMClient#chat}:
 * {@link #beforeChat} runs against the outgoing messages, and {@link #afterChat} runs against
 * the response. Both can return {@link GuardrailDecision#allow()},
 * {@link GuardrailDecision#block(String, String)}, or
 * {@link GuardrailDecision#rewrite(String, String, String)}.
 *
 * <p>The canonical implementation, {@link ClassifierGuardrail}, runs a {@link Classifier} over
 * the messages and blocks based on the predicted label.
 */
public interface Guardrail extends Serializable {

  /** Apply before the chat call. */
  default GuardrailDecision beforeChat(String agentId, List<ChatMessage> messages) {
    return GuardrailDecision.allow();
  }

  /** Apply after the chat call. */
  default GuardrailDecision afterChat(String agentId, ChatResponse response) {
    return GuardrailDecision.allow();
  }

  /** Human-readable name for logging and listener events. */
  default String name() {
    return getClass().getSimpleName();
  }
}
