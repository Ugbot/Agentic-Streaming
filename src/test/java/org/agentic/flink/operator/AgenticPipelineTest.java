package org.agentic.flink.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.ControlState;
import org.agentic.flink.control.DebugControl;
import org.agentic.flink.control.DebugEvent;
import org.agentic.flink.operator.wiring.AgenticPipeline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the framework control plane. Uses a local Flink environment with
 * parallelism=1 and finite sources; the data source sleeps briefly to let broadcast state from
 * the control source propagate before the first keyed record arrives.
 */
final class AgenticPipelineTest {

  // Static collectors are the simplest way to capture side-output + main output across the
  // serialized boundary; tests run sequentially so this is safe.
  private static final ConcurrentLinkedQueue<DebugEvent> DEBUG_EVENTS =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<String> MAIN_OUTPUT = new ConcurrentLinkedQueue<>();

  @Test
  @DisplayName("debug OFF by default — no DebugEvents emitted")
  void noControlNoDebug() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();

    runJob(List.of(), List.of(1, 2, 3, 4, 5));

    assertEquals(5, MAIN_OUTPUT.size());
    assertTrue(DEBUG_EVENTS.isEmpty(), "expected no debug events, got " + DEBUG_EVENTS.size());
  }

  @Test
  @DisplayName("on(opId) enables debug for that operator only")
  void enableSingleOperator() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();

    runJob(List.of(DebugControl.on("test-op")), List.of(10, 20, 30));

    assertEquals(3, MAIN_OUTPUT.size());
    assertEquals(3, DEBUG_EVENTS.size(), "expected 3 debug events");
    for (DebugEvent e : DEBUG_EVENTS) {
      assertEquals("test-op", e.operatorId());
      assertEquals("seen", e.kind());
    }
  }

  @Test
  @DisplayName("pinned(opId) keeps debug on with effectively unlimited TTL")
  void pinnedOperator() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();

    runJob(List.of(DebugControl.pinned("test-op")), List.of(7, 8, 9));

    assertEquals(3, DEBUG_EVENTS.size());
  }

  @Test
  @DisplayName("off(opId) cancels an existing on(opId)")
  void offCancelsOn() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();

    // First on, then off — data arrives after both.
    runJob(List.of(DebugControl.on("test-op"), DebugControl.off("test-op")), List.of(1, 2, 3));

    assertEquals(0, DEBUG_EVENTS.size(), "expected no events after off");
  }

  @Test
  @DisplayName("everywhere() enables debug across all operators")
  void everywhereEnablesAll() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();

    runJob(List.of(DebugControl.everywhere()), List.of(100, 200));

    assertEquals(2, DEBUG_EVENTS.size());
  }

  @Test
  @DisplayName("on(opId, 1ms) expires before data — passive TTL works")
  void shortTtlExpires() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();

    // Send a 1ms-TTL directive then sleep well past it; data should not see debug enabled.
    runJob(
        List.of(DebugControl.on("test-op", 1L)),
        List.of(1, 2, 3),
        /* preDataSleepMs = */ 250L);

    assertEquals(0, DEBUG_EVENTS.size(), "expected no events after TTL expiry");
  }

  // ---- harness ----

  private void runJob(List<DebugControl> controlSeq, List<Integer> dataSeq) throws Exception {
    runJob(controlSeq, dataSeq, /* preDataSleepMs = */ 600L);
  }

  private void runJob(List<DebugControl> controlSeq, List<Integer> dataSeq, long preDataSleepMs)
      throws Exception {
    Configuration cfg = new Configuration();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, cfg);

    // Control source: emits the supplied directives then idles. The control-plane types are
    // declared as Flink POJOs (no-arg ctor + getters/setters) so the serializer never falls
    // back to Kryo — Kryo + records is fragile and we explicitly avoid it.
    List<ControlMessage> controls = new ArrayList<>(controlSeq);
    BroadcastStream<ControlMessage> control =
        env.addSource(new ControlSeqSource(controls), TypeInformation.of(ControlMessage.class))
            .broadcast(ControlState.DIRECTIVES);

    // Data source: delays the first emission so the broadcast control has time to land.
    DelayedListSource data = new DelayedListSource(preDataSleepMs, dataSeq);
    KeyedStream<Integer, String> keyed =
        env.addSource(data, TypeInformation.of(Integer.class)).keyBy(i -> "k");

    TestOp op = new TestOp("test-op");
    SingleOutputStreamOperator<String> out = AgenticPipeline.wire(keyed, control, op);
    out.addSink(new CollectMain());

    DataStream<DebugEvent> debug = AgenticPipeline.debugStream(out);
    debug.addSink(new CollectDebug());

    env.execute("agentic-pipeline-test");
  }

  /** Trivial operator under test — emits a debug event per input when debug is on. */
  static final class TestOp extends AgenticKeyedProcessFunction<String, Integer, String> {
    private static final long serialVersionUID = 1L;

    TestOp(String operatorId) {
      super(operatorId);
    }

    @Override
    protected void onElement(Integer value, ReadOnlyContext ctx, Collector<String> out)
        throws Exception {
      Map<String, Object> p = new HashMap<>();
      p.put("v", value);
      emitDebug(ctx, "seen", p);
      out.collect("out:" + value);
    }
  }

  /**
   * Source that emits a fixed sequence of {@link ControlMessage}s, then idles (so the data
   * source has time to start) until the job is cancelled.
   */
  static final class ControlSeqSource implements SourceFunction<ControlMessage> {
    private static final long serialVersionUID = 1L;
    private final List<ControlMessage> seq;
    private volatile boolean running = true;

    ControlSeqSource(List<ControlMessage> seq) {
      this.seq = Collections.unmodifiableList(new ArrayList<>(seq));
    }

    @Override
    public void run(SourceContext<ControlMessage> ctx) throws Exception {
      for (ControlMessage m : seq) {
        if (!running) {
          return;
        }
        ctx.collect(m);
      }
      // Hold long enough for the data source's preDataSleepMs window to elapse and for the
      // small set of records to drain. The test pre-sleep is 600ms (250 for short-TTL case);
      // 2 seconds is comfortably longer than either.
      long until = System.currentTimeMillis() + 2_000L;
      while (running && System.currentTimeMillis() < until) {
        Thread.sleep(50);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  /** Source that yields a fixed list of integers after an initial sleep. */
  static final class DelayedListSource implements SourceFunction<Integer> {
    private static final long serialVersionUID = 1L;
    private final long sleepMs;
    private final List<Integer> values;
    private volatile boolean running = true;

    DelayedListSource(long sleepMs, List<Integer> values) {
      this.sleepMs = sleepMs;
      this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
      Thread.sleep(sleepMs);
      for (Integer v : values) {
        if (!running) {
          return;
        }
        ctx.collect(v);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  /** Side-effect collectors. Live as static fields so they survive serialization. */
  static final class CollectMain implements SinkFunction<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(String value, Context context) {
      MAIN_OUTPUT.add(value);
    }
  }

  static final class CollectDebug implements SinkFunction<DebugEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(DebugEvent value, Context context) {
      DEBUG_EVENTS.add(value);
    }
  }

  // Suppresses an "unused" warning if the AgenticPipeline ever gets a new entry point that this
  // test should pick up via a later edit; pinning the import here keeps the test honest.
  @SuppressWarnings("unused")
  private static final Class<?> PIN = AgenticPipeline.class;

  static {
    assertNotNull(OperatorDebug.SIDE_OUT, "guard");
  }
}
