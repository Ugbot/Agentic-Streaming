package org.jagentic.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.cep.EventTime;

/**
 * The turn-time half of the spec's timers (section 8): what {@link RoutedGraph#handle} does with a
 * workflow's {@link TimerSpec}s around {@code turn_received}. Everything here is a pure function of
 * the folded {@link TimerState}, the clock readings, and the turn; the log is the only memory, so a
 * runtime that restarts and folds the same log fires the same timers exactly once.
 *
 * <ul>
 *   <li>{@link #eventTimeOf} reads {@code metadata.event_time_ms} (a decimal string).
 *   <li>{@link #fireDue} appends {@code timer_fired {timer_id, due_ms}} for every pending timer
 *       whose clock reads at or past {@code due_ms}, in {@code (due_ms, timer_id)} order, and
 *       invokes the timer's tool with its payload as that turn's first tool calls.
 *   <li>{@link #schedule} appends {@code timer_scheduled {timer_id, clock, due_ms}} for each
 *       declared timer, {@code after_ms} past the chosen clock's reading, on a conversation's
 *       first turn.
 * </ul>
 */
public final class WorkflowTimers {

  private WorkflowTimers() {}

  /**
   * {@code metadata.event_time_ms} as an integer, or null when the turn carries none. Shares the
   * one reader with the CEP fold ({@link EventTime#eventTimeMs}) so timers and sequence patterns
   * agree on a turn's event time.
   */
  public static Long eventTimeOf(Event event) {
    return EventTime.eventTimeMs(event);
  }

  /**
   * The processing clock the turn uses. A workflow that declares timers needs one; a runtime that
   * cannot supply it declares {@code timers} unsupported rather than guessing.
   */
  public static long processingNow(List<TimerSpec> timers, AgentContext ctx) {
    if (ctx.clock == null) {
      throw new IllegalStateException("workflow declares timers " + ids(timers)
          + " but the runtime supplied no LogicalClock (AgentContext.clock)");
    }
    return ctx.clock.nowMs();
  }

  /** The event clock reads the watermark; like the reference it reads 0 before any event time was seen. */
  static long reading(TimerSpec.Clock clock, long processingMs, Long watermarkMs) {
    if (clock == TimerSpec.Clock.EVENT) {
      return watermarkMs == null ? 0L : watermarkMs;
    }
    return processingMs;
  }

  /**
   * Fires every pending timer that is due at these readings, before the turn's own events.
   *
   * @param watermarkMs the conversation watermark including this turn's event time, or null
   */
  public static void fireDue(List<TimerSpec> timers, TimerState folded, AgentContext ctx,
                             long processingMs, Long watermarkMs) {
    List<TimerState.Pending> due = new ArrayList<>();
    for (TimerState.Pending p : folded.pending().values()) {
      if (reading(p.clock(), processingMs, watermarkMs) >= p.dueMs()) {
        due.add(p);
      }
    }
    due.sort(Comparator.comparingLong(TimerState.Pending::dueMs).thenComparing(TimerState.Pending::timerId));
    for (TimerState.Pending p : due) {
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("timer_id", p.timerId());
      payload.put("due_ms", p.dueMs());
      ctx.record(EventType.TIMER_FIRED, payload);
      TimerSpec spec = find(timers, p.timerId());
      if (spec != null && spec.tool() != null) {
        ctx.callTool(spec.tool(), new LinkedHashMap<>(spec.payload()));
      }
    }
  }

  /** Schedules every declared timer for this conversation; called once, on its first turn. */
  public static void schedule(List<TimerSpec> timers, AgentContext ctx, long processingMs, Long watermarkMs) {
    for (TimerSpec t : timers) {
      long now = reading(t.clock(), processingMs, watermarkMs);
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("timer_id", t.id());
      payload.put("clock", t.clock().wire());
      payload.put("due_ms", now + t.afterMs());
      ctx.record(EventType.TIMER_SCHEDULED, payload);
    }
  }

  private static TimerSpec find(List<TimerSpec> timers, String id) {
    for (TimerSpec t : timers) {
      if (t.id().equals(id)) {
        return t;
      }
    }
    return null;
  }

  private static List<String> ids(List<TimerSpec> timers) {
    List<String> out = new ArrayList<>();
    for (TimerSpec t : timers) {
      out.add(t.id());
    }
    return out;
  }
}
