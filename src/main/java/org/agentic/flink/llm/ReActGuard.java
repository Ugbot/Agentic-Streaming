package org.agentic.flink.llm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Action-adherence guard for hand-rolled ReAct loops, shared by the core {@link
 * org.agentic.flink.function.ReActProcessFunction} operator and the banking {@code ReActTurnBrain}.
 *
 * <p>Small models frequently <b>stall</b>: they end a turn with a {@code final} step that merely
 * <em>narrates</em> a tool need ("I need to inspect the available tools first", "I don't have access
 * to the required tools") instead of emitting the {@code action} step that actually calls the tool —
 * so the tool is never called and the task silently fails. A loop should detect that case and push
 * back (re-prompt) a bounded number of times before accepting such a final.
 *
 * <p>Pure and Flink-free so both the Flink operator and the plain-JVM brain can reuse it. Use it as:
 * track tool calls per run; on a {@code final}, if the model has tools, has called none, hasn't been
 * nudged {@link #MAX_STALL_NUDGES} times, and {@link #looksLikeToolStall} matches, append {@link
 * #stallNudge} as a user turn and continue instead of finishing.
 */
public final class ReActGuard {

  /** Max times a loop pushes back on a "I need to use a tool" non-action final before accepting it. */
  public static final int MAX_STALL_NUDGES = 3;

  private ReActGuard() {}

  /**
   * Does a {@code final} answer read like the model stalling — narrating that it needs to
   * inspect/access tools it actually has, instead of emitting the action step? Deliberately
   * conservative so a genuine answer (or a plain knowledge reply) is never mistaken for a stall.
   */
  public static boolean looksLikeToolStall(String answer) {
    if (answer == null) {
      return false;
    }
    String t = answer.toLowerCase(Locale.ROOT);
    return t.contains("inspect the available tool")
        || t.contains("inspect the tool")
        || t.contains("inspect available tool")
        || t.contains("need to inspect")
        || t.contains("don't have access to")
        || t.contains("do not have access to")
        || t.contains("no access to")
        || t.contains("need access to")
        || t.contains("tool access")
        || t.contains("once i have access")
        || t.contains("until i have access")
        || (t.contains("unable to") && t.contains("tool"));
  }

  /** The push-back message instructing the model to emit an action step now, listing its tools. */
  public static String stallNudge(Collection<String> toolNames) {
    String names = toolNames == null ? "" : String.join(", ", new ArrayList<>(toolNames));
    return "You DO have these tools available and can call them right now this turn: "
        + names
        + ". Do not say you need to inspect them or that you lack access. Emit an ACTION step now —"
        + " e.g. {\"type\":\"action\",\"tool\":\"<one of the tools above>\",\"arguments\":{...}} —"
        + " then use the observation. Do not return a final answer until you have actually performed"
        + " the action.";
  }

  /**
   * Should the loop reject this {@code final} and re-prompt instead of finishing? True when the
   * model can act, hasn't acted this run, is within the nudge budget, and the final looks like a
   * stall.
   */
  public static boolean shouldNudge(
      String finalAnswer, boolean hasTools, int toolCallsThisRun, int nudgesSoFar) {
    return hasTools
        && toolCallsThisRun == 0
        && nudgesSoFar < MAX_STALL_NUDGES
        && looksLikeToolStall(finalAnswer);
  }

  /** Convenience: empty-safe list copy of tool names (for callers that build the set ad hoc). */
  public static List<String> names(Collection<String> toolNames) {
    return toolNames == null ? new ArrayList<>() : new ArrayList<>(toolNames);
  }
}
