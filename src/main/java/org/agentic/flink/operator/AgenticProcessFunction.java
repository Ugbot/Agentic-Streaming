package org.agentic.flink.operator;

import java.util.Map;
import java.util.Objects;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.ControlState;
import org.agentic.flink.control.DebugControl;
import org.agentic.flink.control.DebugEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Non-keyed sibling of {@link AgenticKeyedProcessFunction}. Subclasses get the same predefined
 * debug side-output and broadcast control input semantics, but the input stream is not keyed.
 *
 * <p>Wraps a {@link BroadcastProcessFunction}. Subclasses implement {@link #onElement} only;
 * broadcast plumbing, TTL expiry, and {@link #emitDebug} are inherited.
 *
 * @param <IN> input element type
 * @param <OUT> output element type
 */
public abstract class AgenticProcessFunction<IN, OUT>
    extends BroadcastProcessFunction<IN, ControlMessage, OUT> {
  private static final long serialVersionUID = 1L;

  private final String operatorId;

  protected AgenticProcessFunction(String operatorId) {
    this.operatorId = Objects.requireNonNull(operatorId, "operatorId");
  }

  /** Stable operator name — must match what the control plane targets. */
  public final String operatorId() {
    return operatorId;
  }

  /** Per-element behaviour. Identical contract to the standard process function. */
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

  /** True if the debug side-output is currently active for this operator. */
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
   * enabled. No-op when off — cheap to call on the hot path.
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
