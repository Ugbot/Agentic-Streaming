package org.agentic.flink.pipeline;

import java.util.Map;

import org.agentic.flink.runtime.WorkflowTurnFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;

/**
 * Pre-spec keyed operator that emitted one summary line per turn from a per-task in-memory
 * runtime. It never held conversation state in Flink: a restart lost every conversation, and a
 * redelivered turn ran the brain again.
 *
 * @deprecated use {@link WorkflowTurnFunction} (emits normalized {@link TurnResult}s and keeps the
 *     conversation event log in checkpointed keyed state) or
 *     {@link FlinkPipelineRunner#assembleResults}. This class now delegates to
 *     {@link WorkflowTurnFunction} so existing jobs gain durable state, and formats the same summary
 *     line; it will be removed with the DSL it served.
 */
@Deprecated
public final class FlinkGraphFunction extends KeyedProcessFunction<String, Event, String> {
  private static final long serialVersionUID = 2L;

  private final WorkflowTurnFunction delegate;

  public FlinkGraphFunction(Map<String, Object> spec) {
    this.delegate = new WorkflowTurnFunction(spec);
  }

  @Override
  public void setRuntimeContext(org.apache.flink.api.common.functions.RuntimeContext t) {
    super.setRuntimeContext(t);
    delegate.setRuntimeContext(t);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    delegate.open(openContext);
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {
    delegate.processElement(event, new DelegatingContext(ctx), new SummaryCollector(out));
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    delegate.onTimer(timestamp, new DelegatingTimerContext(ctx), new SummaryCollector(out));
  }

  static String summary(TurnResult r) {
    return r.conversationId + " | path=" + r.path + " | ok=" + r.ok + " | " + r.reply;
  }

  private static final class SummaryCollector implements Collector<TurnResult> {
    private final Collector<String> out;

    SummaryCollector(Collector<String> out) {
      this.out = out;
    }

    @Override
    public void collect(TurnResult record) {
      out.collect(summary(record));
    }

    @Override
    public void close() {
      out.close();
    }
  }

  private final class DelegatingContext extends WorkflowTurnFunction.Context {
    private final Context inner;

    DelegatingContext(Context inner) {
      delegate.super();
      this.inner = inner;
    }

    @Override
    public Long timestamp() {
      return inner.timestamp();
    }

    @Override
    public org.apache.flink.streaming.api.TimerService timerService() {
      return inner.timerService();
    }

    @Override
    public <X> void output(org.apache.flink.util.OutputTag<X> outputTag, X value) {
      inner.output(outputTag, value);
    }

    @Override
    public String getCurrentKey() {
      return inner.getCurrentKey();
    }
  }

  private final class DelegatingTimerContext extends WorkflowTurnFunction.OnTimerContext {
    private final OnTimerContext inner;

    DelegatingTimerContext(OnTimerContext inner) {
      delegate.super();
      this.inner = inner;
    }

    @Override
    public org.apache.flink.streaming.api.TimeDomain timeDomain() {
      return inner.timeDomain();
    }

    @Override
    public String getCurrentKey() {
      return inner.getCurrentKey();
    }

    @Override
    public Long timestamp() {
      return inner.timestamp();
    }

    @Override
    public org.apache.flink.streaming.api.TimerService timerService() {
      return inner.timerService();
    }

    @Override
    public <X> void output(org.apache.flink.util.OutputTag<X> outputTag, X value) {
      inner.output(outputTag, value);
    }
  }
}
