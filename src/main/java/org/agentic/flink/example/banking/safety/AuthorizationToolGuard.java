package org.agentic.flink.example.banking.safety;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.agentic.flink.example.banking.env.EnvSession;
import org.agentic.flink.tools.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorization guardrail at the tool layer: decorates a {@link ToolExecutor} to block unsafe or
 * unauthorized actions before they reach the bank's environment.
 *
 * <p>Two protections, both keyed by the turn's A2A {@code contextId} (via {@link EnvSession}):
 *
 * <ul>
 *   <li><b>verification gate</b> — high-risk tools (money movement, data release, referral submit)
 *       are refused until {@link SessionAuthState} shows the customer verified for this session. A
 *       designated verify tool marks the session verified on success.
 *   <li><b>placeholder guard</b> — refuses calls whose arguments contain obvious placeholders (e.g.
 *       {@code customer_name="User"}, empty strings), which the harness rejects and which signal a
 *       hallucinated call.
 * </ul>
 *
 * <p>Returns a tool-error result (not an exception) on refusal, so the agent can recover by asking
 * the user for the missing detail or completing verification first.
 */
public final class AuthorizationToolGuard implements ToolExecutor {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationToolGuard.class);

  private static final Set<String> PLACEHOLDERS =
      Set.of("", "user", "placeholder", "unknown", "n/a", "na", "tbd", "string", "example", "test");

  private final ToolExecutor delegate;
  private final boolean requiresVerification;
  private final boolean verifyTool;
  private final SessionAuthState authState;

  public AuthorizationToolGuard(
      ToolExecutor delegate,
      boolean requiresVerification,
      boolean verifyTool,
      SessionAuthState authState) {
    this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
    this.requiresVerification = requiresVerification;
    this.verifyTool = verifyTool;
    this.authState = java.util.Objects.requireNonNull(authState, "authState");
  }

  @Override
  public String getToolId() {
    return delegate.getToolId();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    String contextId = EnvSession.contextId();

    String placeholder = findPlaceholder(parameters);
    if (placeholder != null) {
      return CompletableFuture.completedFuture(
          error(
              "Refusing "
                  + getToolId()
                  + ": argument '"
                  + placeholder
                  + "' looks like a placeholder. Ask the user for the real value first."));
    }

    if (requiresVerification && !authState.isVerified(contextId)) {
      LOG.debug("Blocked high-risk tool {} — identity not verified (ctx {})", getToolId(), contextId);
      return CompletableFuture.completedFuture(
          error(
              "Refusing "
                  + getToolId()
                  + ": the customer's identity must be verified before this action. "
                  + "Complete identity verification first."));
    }

    return delegate
        .execute(parameters)
        .thenApply(
            result -> {
              if (verifyTool && !isError(result)) {
                authState.markVerified(contextId);
                LOG.debug("Session {} marked verified via {}", contextId, getToolId());
              }
              return result;
            });
  }

  private static String findPlaceholder(Map<String, Object> params) {
    if (params == null) {
      return null;
    }
    for (Map.Entry<String, Object> e : params.entrySet()) {
      if (e.getValue() instanceof String) {
        String v = ((String) e.getValue()).trim().toLowerCase(java.util.Locale.ROOT);
        if (PLACEHOLDERS.contains(v)) {
          return e.getKey();
        }
      }
    }
    return null;
  }

  private static boolean isError(Object result) {
    return result instanceof Map && Boolean.TRUE.equals(((Map<?, ?>) result).get("error"));
  }

  private Map<String, Object> error(String message) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("error", true);
    m.put("content", message);
    return m;
  }
}
