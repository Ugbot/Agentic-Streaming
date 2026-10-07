package org.agentic.flink.inference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.agentic.flink.annotation.Internal;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatRole;

/**
 * Applies a chain of {@link Guardrail}s around one chat call.
 *
 * <p>Both the {@link org.agentic.flink.execution.LLMClient} path and the operators that keep a
 * transcript in Flink state ({@code ReActProcessFunction}) run the same two steps: {@link
 * #beforeChat} may block or rewrite the outgoing messages, and {@link #afterChat} may block or
 * rewrite the reply. Callers persist only what comes back from these methods, so a blocked reply is
 * stored as its redacted text ({@code reason} or {@value #DEFAULT_BLOCK_TEXT}) and the raw model
 * output never reaches the transcript or the next prompt.
 */
@Internal
public final class Guardrails {

  public static final String DEFAULT_BLOCK_TEXT = "Blocked by guardrail";

  private Guardrails() {}

  /** Result of the pre-chat pass: either the (possibly rewritten) messages or a block. */
  public static final class PreChat {
    private final List<ChatMessage> messages;
    private final GuardrailDecision block;

    private PreChat(List<ChatMessage> messages, GuardrailDecision block) {
      this.messages = messages;
      this.block = block;
    }

    /** Messages to send; unchanged unless a guardrail rewrote the last user message. */
    public List<ChatMessage> messages() {
      return messages;
    }

    public boolean isBlocked() {
      return block != null;
    }

    /** The blocking decision, or {@code null} when the chat may proceed. */
    public GuardrailDecision block() {
      return block;
    }

    /** Redacted text to persist and return in place of a reply when blocked. */
    public String blockedText() {
      return Guardrails.blockedText(block);
    }
  }

  /** Result of the post-chat pass: the (possibly rewritten or redacted) reply plus a block flag. */
  public static final class PostChat {
    private final ChatResponse response;
    private final GuardrailDecision block;

    private PostChat(ChatResponse response, GuardrailDecision block) {
      this.response = response;
      this.block = block;
    }

    /** Reply to persist and return; carries only redacted text when {@link #isBlocked()}. */
    public ChatResponse response() {
      return response;
    }

    public boolean isBlocked() {
      return block != null;
    }

    /** The blocking decision, or {@code null} when the reply passed. */
    public GuardrailDecision block() {
      return block;
    }
  }

  /** Runs every {@link Guardrail#beforeChat} in order; the first block wins. */
  public static PreChat beforeChat(
      List<Guardrail> guardrails,
      String agentId,
      List<ChatMessage> messages,
      AgentEventListener listener) {
    List<ChatMessage> current = messages;
    for (Guardrail g : nonNull(guardrails)) {
      GuardrailDecision d = g.beforeChat(agentId, current);
      if (d.isBlock()) {
        listener(listener).onGuardrailBlock(agentId, d.getModelName(), d.getReason());
        return new PreChat(current, d);
      }
      if (d.isRewrite() && d.getRewrittenPayload() != null) {
        listener(listener).onGuardrailRewrite(agentId, d.getModelName(), d.getReason());
        current = replaceLastUserMessage(current, d.getRewrittenPayload());
      }
    }
    return new PreChat(current, null);
  }

  /**
   * Runs every {@link Guardrail#afterChat} in order. A block replaces the reply with its redacted
   * text and drops tool calls; a rewrite substitutes the payload and keeps the rest of the reply.
   */
  public static PostChat afterChat(
      List<Guardrail> guardrails,
      String agentId,
      ChatResponse response,
      AgentEventListener listener) {
    ChatResponse current = response;
    for (Guardrail g : nonNull(guardrails)) {
      GuardrailDecision d = g.afterChat(agentId, current);
      if (d.isBlock()) {
        listener(listener).onGuardrailBlock(agentId, d.getModelName(), d.getReason());
        return new PostChat(blockedResponse(d, current), d);
      }
      if (d.isRewrite() && d.getRewrittenPayload() != null) {
        listener(listener).onGuardrailRewrite(agentId, d.getModelName(), d.getReason());
        current =
            new ChatResponse(
                d.getRewrittenPayload(),
                current.getModelName(),
                current.getToolCalls(),
                current.getTokensUsed(),
                current.getFinishReason());
      }
    }
    return new PostChat(current, null);
  }

  /** The text stored and returned for a blocked interaction. */
  public static String blockedText(GuardrailDecision decision) {
    return decision == null || decision.getReason() == null
        ? DEFAULT_BLOCK_TEXT
        : decision.getReason();
  }

  /** A reply carrying only the redacted text; model name and usage come from the original. */
  public static ChatResponse blockedResponse(GuardrailDecision decision, ChatResponse original) {
    return new ChatResponse(
        blockedText(decision),
        original == null ? null : original.getModelName(),
        Collections.emptyList(),
        original == null ? null : original.getTokensUsed(),
        original == null ? ChatResponse.FinishReason.STOP : original.getFinishReason());
  }

  /**
   * Replaces the most recent user message with the guardrail's rewritten payload, keeping the rest
   * of the conversation (system prompt, earlier turns, tool results) intact. When the conversation
   * has no user message the rewrite is appended as one.
   */
  public static List<ChatMessage> replaceLastUserMessage(
      List<ChatMessage> messages, String rewritten) {
    List<ChatMessage> out = new ArrayList<>(messages);
    for (int i = out.size() - 1; i >= 0; i--) {
      if (out.get(i).getRole() == ChatRole.USER) {
        out.set(i, ChatMessage.user(rewritten));
        return out;
      }
    }
    out.add(ChatMessage.user(rewritten));
    return out;
  }

  private static List<Guardrail> nonNull(List<Guardrail> guardrails) {
    return guardrails == null ? Collections.emptyList() : guardrails;
  }

  private static AgentEventListener listener(AgentEventListener listener) {
    return listener == null ? new AgentEventListener() {} : listener;
  }
}
