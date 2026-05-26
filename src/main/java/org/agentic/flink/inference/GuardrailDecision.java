package org.agentic.flink.inference;

import java.io.Serializable;
import java.util.Objects;

/**
 * What a {@link Guardrail} wants to happen to an LLM interaction.
 *
 * <p>Three outcomes:
 *
 * <ul>
 *   <li>{@link Action#ALLOW} — pass through unchanged.
 *   <li>{@link Action#BLOCK} — short-circuit; the LLM is not called (or the response is
 *       suppressed) and a blocked response with {@link #getReason()} is returned to callers.
 *   <li>{@link Action#REWRITE} — replace the payload with {@link #getRewrittenPayload()} and
 *       continue.
 * </ul>
 */
public final class GuardrailDecision implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum Action {
    ALLOW,
    BLOCK,
    REWRITE
  }

  private final Action action;
  private final String reason;
  private final String rewrittenPayload;
  private final String modelName;

  private GuardrailDecision(
      Action action, String reason, String rewrittenPayload, String modelName) {
    this.action = Objects.requireNonNull(action, "action");
    this.reason = reason;
    this.rewrittenPayload = rewrittenPayload;
    this.modelName = modelName;
  }

  public static GuardrailDecision allow() {
    return new GuardrailDecision(Action.ALLOW, null, null, null);
  }

  public static GuardrailDecision block(String reason, String modelName) {
    return new GuardrailDecision(Action.BLOCK, reason, null, modelName);
  }

  public static GuardrailDecision rewrite(String payload, String reason, String modelName) {
    return new GuardrailDecision(Action.REWRITE, reason, payload, modelName);
  }

  public Action getAction() {
    return action;
  }

  public String getReason() {
    return reason;
  }

  public String getRewrittenPayload() {
    return rewrittenPayload;
  }

  public String getModelName() {
    return modelName;
  }

  public boolean isBlock() {
    return action == Action.BLOCK;
  }

  public boolean isAllow() {
    return action == Action.ALLOW;
  }

  public boolean isRewrite() {
    return action == Action.REWRITE;
  }

  @Override
  public String toString() {
    return "GuardrailDecision[" + action + (reason == null ? "" : ", reason=" + reason) + "]";
  }
}
