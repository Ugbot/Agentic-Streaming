package org.jagentic.core.cep;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.AgentContext;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;

/**
 * Evaluates a workflow's {@link SequencePattern}s inside a turn, after {@code routed} and before
 * the brain. The turns are read back from the conversation log ({@code turn_received} events,
 * the current turn included), so the result is a function of the log alone; a pattern that
 * completes on this turn invokes its tool through the context, which records the call on this
 * turn like any other.
 */
public final class TurnPatterns {

  private TurnPatterns() {}

  /** The conversation's turns in log order, as the fold sees them. */
  public static List<SequencePattern.Turn> turnsOf(List<LogEvent> log) {
    List<SequencePattern.Turn> turns = new ArrayList<>();
    for (LogEvent e : log) {
      if (e.eventType().orElse(null) == EventType.TURN_RECEIVED) {
        turns.add(SequencePattern.Turn.fromReceived(e.payload()));
      }
    }
    return turns;
  }

  /** The {@code on_match.kind: tool} arguments: the pattern name and the conversation key. */
  public static Map<String, Object> matchArgs(SequencePattern pattern, String conversationId) {
    Map<String, Object> args = new LinkedHashMap<>();
    args.put("pattern", pattern.name());
    args.put("key", conversationId);
    return args;
  }

  /** Runs every pattern against the log and fires the tool of each one completing on this turn. */
  public static void evaluate(List<SequencePattern> patterns, AgentContext ctx) {
    if (patterns.isEmpty()) {
      return;
    }
    List<SequencePattern.Turn> turns = turnsOf(ctx.log.events(ctx.conversationId));
    for (SequencePattern pattern : patterns) {
      if (pattern.completesOn(turns)) {
        ctx.callTool(pattern.tool(), matchArgs(pattern, ctx.conversationId));
      }
    }
  }
}
