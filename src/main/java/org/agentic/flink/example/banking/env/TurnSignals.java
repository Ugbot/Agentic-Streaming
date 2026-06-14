package org.agentic.flink.example.banking.env;

import java.util.Locale;
import java.util.function.Supplier;

/**
 * Thread-scoped signals a path operator collects while its brain runs one turn — chiefly "did this
 * turn perform a real environment <em>action</em>" (a mutating env tool succeeded), which the
 * verifier uses to advance the workflow phase (e.g. {@code READY_TO_ACT → ACTED}).
 *
 * <p>Set by {@link EnvApiToolExecutor} on a successful non-read tool call, read by the path
 * operator after the brain returns. Scoped like {@link EnvSession} (the brain runs synchronously on
 * the operator thread), so the flag never leaks across turns.
 */
public final class TurnSignals {

  private static final ThreadLocal<boolean[]> ACTION = new ThreadLocal<>();

  private TurnSignals() {}

  /** Run {@code body} with a fresh signal scope and return whether an env action was performed. */
  public static <T> Result<T> capture(Supplier<T> body) {
    boolean[] flag = new boolean[] {false};
    ACTION.set(flag);
    try {
      T value = body.get();
      return new Result<>(value, flag[0]);
    } finally {
      ACTION.remove();
    }
  }

  /** Record that {@code toolName} ran successfully; mutating tools flip the action flag. */
  public static void recordEnvToolCall(String toolName, boolean error) {
    boolean[] flag = ACTION.get();
    if (flag != null && !error && isMutating(toolName)) {
      flag[0] = true;
    }
  }

  /** Read-only tools don't change the world; everything else is treated as an action. */
  private static boolean isMutating(String toolName) {
    if (toolName == null) {
      return false;
    }
    String n = toolName.toLowerCase(Locale.ROOT);
    if (n.startsWith("list") || n.startsWith("get") || n.startsWith("lookup") || n.startsWith("search")
        || n.startsWith("find") || n.startsWith("view") || n.startsWith("check")
        || n.contains("_search") || n.equals("call_env_tool") || n.equals("list_env_tools")) {
      return false;
    }
    return true;
  }

  /** A captured result plus whether an env action was performed during the scope. */
  public static final class Result<T> {
    public final T value;
    public final boolean actionPerformed;

    Result(T value, boolean actionPerformed) {
      this.value = value;
      this.actionPerformed = actionPerformed;
    }
  }
}
