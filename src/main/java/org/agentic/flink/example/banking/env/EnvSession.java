package org.agentic.flink.example.banking.env;

import java.util.function.Supplier;

/**
 * Thread-scoped holder for the current A2A {@code contextId} while an agent turn runs.
 *
 * <p>The A2A banking harness keys every env session on the inbound {@code contextId} and requires
 * it on every env-tool call (it's the session id in the URL path). {@link
 * org.agentic.flink.tools.ToolExecutor#execute} takes only a parameter map, so the agent operator
 * binds the turn's {@code contextId} here (via {@link #withContext}) before driving the agent, and
 * {@link EnvApiToolExecutor} reads it back — the {@code contextId} is never invented or passed
 * through the LLM. Mirrors the {@code ToolInvocationChannel.CURRENT_CONTEXT} ThreadLocal pattern.
 */
public final class EnvSession {

  private static final ThreadLocal<String> CONTEXT_ID = new ThreadLocal<>();

  private EnvSession() {}

  /** The contextId bound to the current thread, or {@code null} if none is active. */
  public static String contextId() {
    return CONTEXT_ID.get();
  }

  /** Bind {@code contextId} for the current thread. Prefer {@link #withContext} for scoping. */
  public static void set(String contextId) {
    CONTEXT_ID.set(contextId);
  }

  /** Clear the current thread's binding. */
  public static void clear() {
    CONTEXT_ID.remove();
  }

  /** Run {@code body} with {@code contextId} bound, restoring the previous binding afterward. */
  public static <T> T withContext(String contextId, Supplier<T> body) {
    String previous = CONTEXT_ID.get();
    CONTEXT_ID.set(contextId);
    try {
      return body.get();
    } finally {
      if (previous == null) {
        CONTEXT_ID.remove();
      } else {
        CONTEXT_ID.set(previous);
      }
    }
  }
}
