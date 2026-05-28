package org.agentic.flink.operator;

import java.util.Map;
import java.util.Objects;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.ControlState;
import org.agentic.flink.control.DebugControl;
import org.agentic.flink.control.DebugEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Base class for every framework process function. Wraps a {@link KeyedBroadcastProcessFunction}
 * that consumes a keyed input alongside a broadcast stream of {@link ControlMessage}s, hiding
 * the boilerplate so subclasses only need to implement {@link #onElement}.
 *
 * <p>For free, subclasses get:
 *
 * <ul>
 *   <li>A predefined {@link OperatorDebug#SIDE_OUT} side output that the framework's debug sink
 *       consumes — call {@link #emitDebug} from the hot path.
 *   <li>Passive TTL expiry of {@link DebugControl} directives — no Flink timers required.
 *   <li>Targeting by stable {@link #operatorId} and the {@code "*"} broadcast wildcard.
 * </ul>
 *
 * <p>The hot path overhead when debug is OFF is one {@link ReadOnlyBroadcastState#get} call plus
 * two comparisons. When debug is ON, {@link #emitDebug} pays for the {@link DebugEvent}
 * allocation plus a side-output emission.
 *
 * @param <K> key type
 * @param <IN> keyed input element type
 * @param <OUT> output element type
 */
public abstract class AgenticKeyedProcessFunction<K, IN, OUT>
    extends KeyedBroadcastProcessFunction<K, IN, ControlMessage, OUT> {
  private static final long serialVersionUID = 1L;

  private final String operatorId;

  protected AgenticKeyedProcessFunction(String operatorId) {
    this.operatorId = Objects.requireNonNull(operatorId, "operatorId");
  }

  /** Stable operator name — must match what the control plane targets. */
  public final String operatorId() {
    return operatorId;
  }

  /**
   * Implement the per-element behaviour. Identical contract to
   * {@link KeyedBroadcastProcessFunction#processElement} but with the broadcast plumbing handled.
   */
  protected abstract void onElement(IN value, ReadOnlyContext ctx, Collector<OUT> out)
      throws Exception;

  @Override
  public final void processElement(IN value, ReadOnlyContext ctx, Collector<OUT> out)
      throws Exception {
    onElement(value, ctx, out);
  }

  @Override
  public final void processBroadcastElement(ControlMessage msg, Context ctx, Collector<OUT> out)
      throws Exception {
    if (msg == null) {
      return;
    }
    if (msg instanceof DebugControl dbg) {
      applyDebugControl(dbg, ctx.getBroadcastState(ControlState.DIRECTIVES), ctx);
    }
    // Future ControlMessage types: dispatch here.
  }

  private void applyDebugControl(
      DebugControl dbg, BroadcastState<String, ControlState.Directive> state, Context ctx)
      throws Exception {
    String target = dbg.operatorId();
    if (!ControlMessage.ALL_OPERATORS.equals(target) && !target.equals(operatorId)) {
      // Different operator — but we still store under the targeted id so peers (which share the
      // broadcast state, since broadcast state replicates across all subtasks) all converge on
      // the same map. The hot path filters by ours-or-wildcard at read time.
    }
    if (!dbg.enabled()) {
      state.remove(target);
      return;
    }
    long now = ctx.currentProcessingTime();
    long ttl = dbg.ttlMillis();
    long expiresAt;
    if (ttl == ControlMessage.PERMANENT) {
      expiresAt = Long.MAX_VALUE;
    } else if (ttl <= 0L) {
      expiresAt = now + ControlMessage.DEFAULT_TTL_MILLIS;
    } else {
      expiresAt = now + ttl;
    }
    state.put(target, new ControlState.Directive(true, expiresAt));
  }

  /**
   * True if the debug side-output is currently active for this operator. Cheap; safe to call on
   * every record.
   */
  protected final boolean debugEnabled(ReadOnlyContext ctx) throws Exception {
    long now = ctx.currentProcessingTime();
    ReadOnlyBroadcastState<String, ControlState.Directive> state =
        ctx.getBroadcastState(ControlState.DIRECTIVES);
    ControlState.Directive mine = state.get(operatorId);
    if (mine != null && mine.active(now)) {
      return true;
    }
    ControlState.Directive all = state.get(ControlMessage.ALL_OPERATORS);
    return all != null && all.active(now);
  }

  /**
   * Emit a debug event to the framework's predefined side output, but only if debug is currently
   * enabled. No-op (returns immediately) when off.
   */
  protected final void emitDebug(ReadOnlyContext ctx, String kind, Map<String, Object> payload)
      throws Exception {
    if (!debugEnabled(ctx)) {
      return;
    }
    ctx.output(
        OperatorDebug.SIDE_OUT,
        new DebugEvent(operatorId, ctx.currentProcessingTime(), kind, payload));
  }
}
