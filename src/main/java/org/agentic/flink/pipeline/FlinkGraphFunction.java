package org.agentic.flink.pipeline;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.PipelineLoader;

// Emits a readable summary line per turn (also keeps the stream element a plain String — trivially
// serializable across the keyBy/sink, unlike the record types).

/**
 * Runs the portable agent graph inside a Flink keyed operator — one conversation per key, so Flink's
 * {@code keyBy} provides the single-writer-per-conversation ordering the agent needs (the same role
 * the {@code LocalRuntime} lock / a Pekko sharded entity plays elsewhere).
 *
 * <p>The serializable {@code pipeline.yaml} spec map is shipped to each task manager; the graph,
 * tools and stores are (re)built in {@link #open} via the Flink-free {@link PipelineLoader}. The
 * {@code cep:} section is stripped here — native Flink CEP is wired in {@link FlinkPipelineRunner},
 * so it must not also run in-operator. Per-conversation memory uses the portable in-memory store
 * (not Flink-checkpointed state — a documented limitation of this runner).</p>
 */
public final class FlinkGraphFunction extends KeyedProcessFunction<String, Event, String> {

  private final Map<String, Object> spec;
  private transient PipelineLoader.PipelineSystem system;

  public FlinkGraphFunction(Map<String, Object> spec) {
    Map<String, Object> copy = new HashMap<>(spec);
    copy.remove("cep"); // native CEP is wired in the job graph, not in this operator
    this.spec = copy;
  }

  @Override
  public void open(OpenContext openContext) {
    system = PipelineLoader.buildSystem(spec, "local");
  }

  @Override
  public void processElement(Event event, Context ctx, Collector<String> out) {
    TurnResult r = system.submit(event);
    out.collect(event.conversationId() + " | path=" + r.path + " | ok=" + r.ok + " | " + r.reply);
  }
}
