package org.agentic.flink.operator.wiring;

import java.util.Arrays;
import org.agentic.flink.channel.Channel;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.ControlState;
import org.agentic.flink.control.DebugEvent;
import org.agentic.flink.operator.AgenticKeyedProcessFunction;
import org.agentic.flink.operator.OperatorDebug;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.agentic.flink.channel.source.PollingSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The framework's stream-graph wiring helper. Three responsibilities:
 *
 * <ol>
 *   <li>Build a {@link BroadcastStream} of {@link ControlMessage} from any {@link Channel}
 *       so every operator in the job listens on the same control plane.
 *   <li>Wire any {@link AgenticKeyedProcessFunction} into the graph in one call, threading the
 *       broadcast control and setting the operator name from {@link
 *       AgenticKeyedProcessFunction#operatorId()}.
 *   <li>Tap the predefined {@link OperatorDebug#SIDE_OUT} side output from one or more operators
 *       into a single {@link DataStream} that callers sink to whatever debug channel they like.
 * </ol>
 */
public final class AgenticPipeline {
  private AgenticPipeline() {}

  /** Open the supplied control channel and broadcast it for downstream operators. */
  public static BroadcastStream<ControlMessage> controlInput(
      StreamExecutionEnvironment env, Channel<ControlMessage> control) throws Exception {
    return control.open(env).broadcast(ControlState.DIRECTIVES);
  }

  /**
   * Wire {@code fn} as a {@link AgenticKeyedProcessFunction} into the graph: connects the keyed
   * input with the broadcast control, processes, and names the operator from
   * {@link AgenticKeyedProcessFunction#operatorId()}.
   */
  public static <K, IN, OUT> SingleOutputStreamOperator<OUT> wire(
      KeyedStream<IN, K> in,
      BroadcastStream<ControlMessage> control,
      AgenticKeyedProcessFunction<K, IN, OUT> fn) {
    return in.connect(control).process(fn).name(fn.operatorId()).uid(fn.operatorId());
  }

  /** Non-keyed overload — same shape as {@link #wire} for a plain {@link DataStream} input. */
  public static <IN, OUT> SingleOutputStreamOperator<OUT> wire(
      DataStream<IN> in,
      BroadcastStream<ControlMessage> control,
      org.agentic.flink.operator.AgenticProcessFunction<IN, OUT> fn) {
    return in.connect(control).process(fn).name(fn.operatorId()).uid(fn.operatorId());
  }

  /**
   * Build a broadcast control stream from a fixed seed of directives. Convenience for examples
   * and tests that want to run with the debug side-output on at start without setting up an
   * external control channel. Pass {@link DebugControl#everywhere()} to enable everywhere.
   */
  public static BroadcastStream<ControlMessage> seededControl(
      StreamExecutionEnvironment env, ControlMessage... directives) {
    java.util.List<ControlMessage> seq = java.util.Arrays.asList(directives);
    return env.fromSource(
            new PollingSource<>(new SeededControlPollFn(seq)),
            WatermarkStrategy.noWatermarks(),
            "seeded-control",
            org.apache.flink.api.common.typeinfo.TypeInformation.of(ControlMessage.class))
        .setParallelism(1)
        .broadcast(ControlState.DIRECTIVES);
  }

  /** Empty broadcast control — operator wires up but never sees a flip. */
  public static BroadcastStream<ControlMessage> emptyControl(StreamExecutionEnvironment env) {
    return seededControl(env);
  }

  /**
   * Tiny source that emits a fixed sequence of {@link ControlMessage}s then idles for a few
   * seconds so the broadcast lands before the data sources finish. Used by
   * {@link #seededControl}.
   */
  static final class SeededControlPollFn implements PollingSource.PollFn<ControlMessage> {
    private static final long serialVersionUID = 1L;
    private final java.util.List<ControlMessage> seq;
    private transient java.util.Iterator<ControlMessage> it;

    SeededControlPollFn(java.util.List<ControlMessage> seq) {
      this.seq = new java.util.ArrayList<>(seq);
    }

    @Override
    public void open(int subtaskIndex) {
      it = new java.util.ArrayList<>(seq).iterator();
    }

    @Override
    public ControlMessage poll(long timeoutMs) throws InterruptedException {
      if (it.hasNext()) {
        return it.next();
      }
      // Emitted the seed; idle (the broadcast stays open for the lifetime of the job).
      Thread.sleep(Math.min(Math.max(1, timeoutMs), 200));
      return null;
    }
  }

  /**
   * Union the predefined debug side-outputs from every supplied operator into a single
   * {@link DataStream}. Cheap; downstream call sites add whatever sink they want (Kafka, Fluss,
   * ZMQ, etc.).
   */
  public static DataStream<DebugEvent> debugStream(SingleOutputStreamOperator<?>... operators) {
    if (operators == null || operators.length == 0) {
      throw new IllegalArgumentException("at least one operator required");
    }
    DataStream<DebugEvent> merged = operators[0].getSideOutput(OperatorDebug.SIDE_OUT);
    for (int i = 1; i < operators.length; i++) {
      merged = merged.union(operators[i].getSideOutput(OperatorDebug.SIDE_OUT));
    }
    return merged;
  }

  /**
   * Convenience: union debug side-outputs and immediately sink them. Returns the union stream so
   * callers can chain further (e.g. {@code .filter(...)}, alternate sinks).
   */
  public static DataStream<DebugEvent> attachDebugSink(
      Sink<DebugEvent> sink, SingleOutputStreamOperator<?>... operators) {
    DataStream<DebugEvent> debug = debugStream(operators);
    debug.sinkTo(sink).name("agentic-debug-sink");
    return debug;
  }

  /** Same as {@link #debugStream(SingleOutputStreamOperator[])} but with an iterable input. */
  public static DataStream<DebugEvent> debugStream(
      Iterable<? extends SingleOutputStreamOperator<?>> operators) {
    return debugStream(toArray(operators));
  }

  private static SingleOutputStreamOperator<?>[] toArray(
      Iterable<? extends SingleOutputStreamOperator<?>> ops) {
    java.util.List<SingleOutputStreamOperator<?>> list = new java.util.ArrayList<>();
    ops.forEach(list::add);
    return list.toArray(new SingleOutputStreamOperator<?>[0]);
  }

  /** Echo of {@link Arrays#asList} for callers that prefer varargs at the call site. */
  public static <T> java.util.List<T> ops(T... operators) {
    return Arrays.asList(operators);
  }
}
