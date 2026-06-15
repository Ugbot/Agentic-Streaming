package org.agentic.flink.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.ControlState;
import org.agentic.flink.control.DebugControl;
import org.agentic.flink.control.DebugEvent;
import org.agentic.flink.operator.wiring.AgenticPipeline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Mirrors {@link AgenticPipelineTest} but exercises the non-keyed
 * {@link AgenticProcessFunction} base. Asserts (1) debug stays silent without control,
 * (2) {@code on()} enables emissions, (3) the seeded-control helper works end-to-end via
 * {@link AgenticPipeline#seededControl}.
 */
final class AgenticProcessFunctionTest {

  private static final ConcurrentLinkedQueue<DebugEvent> DEBUG_EVENTS = new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<String> MAIN_OUTPUT = new ConcurrentLinkedQueue<>();

  @Test
  @DisplayName("non-keyed: no control sent → no debug events")
  void silentByDefault() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();
    runJob(List.of(), List.of(1, 2, 3));
    assertEquals(3, MAIN_OUTPUT.size());
    assertTrue(DEBUG_EVENTS.isEmpty(), "expected silent; got " + DEBUG_EVENTS.size());
  }

  @Test
  @DisplayName("non-keyed: on(opId) enables debug")
  void enableSingle() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();
    runJob(List.of(DebugControl.on("nk-op")), List.of(10, 20));
    assertEquals(2, DEBUG_EVENTS.size());
    for (DebugEvent e : DEBUG_EVENTS) {
      assertEquals("nk-op", e.operatorId());
    }
  }

  @Test
  @DisplayName("seededControl helper enables debug at job startup")
  void seededHelper() throws Exception {
    DEBUG_EVENTS.clear();
    MAIN_OUTPUT.clear();
    runJobSeeded(DebugControl.everywhere(), List.of(5, 6, 7));
    assertEquals(3, DEBUG_EVENTS.size());
  }

  // ---- harness ----

  private void runJob(List<DebugControl> controlSeq, List<Integer> dataSeq) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());

    List<ControlMessage> controls = new ArrayList<>(controlSeq);
    BroadcastStream<ControlMessage> control =
        env.addSource(new ControlSeqSource(controls), TypeInformation.of(ControlMessage.class))
            .broadcast(ControlState.DIRECTIVES);
    DataStream<Integer> data = env.addSource(new DelayedListSource(600L, dataSeq));

    SingleOutputStreamOperator<String> out =
        AgenticPipeline.wire(data, control, new NkTestOp("nk-op"));
    out.addSink(new CollectMain());
    AgenticPipeline.debugStream(out).addSink(new CollectDebug());

    env.execute("agentic-process-function-test");
  }

  private void runJobSeeded(ControlMessage directive, List<Integer> dataSeq) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());

    BroadcastStream<ControlMessage> control = AgenticPipeline.seededControl(env, directive);
    DataStream<Integer> data = env.addSource(new DelayedListSource(600L, dataSeq));

    SingleOutputStreamOperator<String> out =
        AgenticPipeline.wire(data, control, new NkTestOp("nk-op"));
    out.addSink(new CollectMain());
    AgenticPipeline.debugStream(out).addSink(new CollectDebug());

    // AgenticPipeline.seededControl is a CONTINUOUS_UNBOUNDED control channel — it stays open
    // for the job's lifetime (as in production, so later control flips can land), so the job
    // never finishes on its own. Run it asynchronously, wait for the expected debug events to
    // arrive, then cancel — the canonical way to test a continuous streaming job. (A blocking
    // env.execute() here would hang forever.)
    JobClient client = env.executeAsync("agentic-process-function-test-seeded");
    try {
      int expected = dataSeq.size();
      long deadline = System.currentTimeMillis() + 30_000L;
      while (DEBUG_EVENTS.size() < expected && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
    } finally {
      try {
        client.cancel().get(30, TimeUnit.SECONDS);
      } catch (Exception ignore) {
        // best-effort cancel; the minicluster tears down with the env
      }
    }
  }

  static final class NkTestOp extends AgenticProcessFunction<Integer, String> {
    private static final long serialVersionUID = 1L;

    NkTestOp(String id) {
      super(id);
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
        if (!running) return;
        ctx.collect(m);
      }
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
        if (!running) return;
        ctx.collect(v);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

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
}
