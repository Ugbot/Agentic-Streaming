package org.agentic.flink.example.feedback;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.DebugControl;
import org.agentic.flink.control.DebugEvent;
import org.agentic.flink.feedback.KeywordQualityCheck;
import org.agentic.flink.feedback.RefinementLoop;
import org.agentic.flink.feedback.RefinementResult;
import org.agentic.flink.operator.AgenticProcessFunction;
import org.agentic.flink.operator.wiring.AgenticPipeline;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Streaming self-refinement: each task is generated, checked, and — if it falls short — regenerated
 * with the critique fed back, up to an attempt budget. Per record it prints whether the answer was
 * accepted, how many attempts it took, and the best score.
 *
 * <p>Wired on the new framework instrumentation: the inner operator extends
 * {@link AgenticProcessFunction} so its per-task refinement details (attempts, scores, accepted
 * flag) flow through the debug side-output when the broadcast control plane enables it. Pass
 * {@code --debug} or set {@code AGENTIC_DEBUG=1} to see the trace.
 *
 * <p>Refinement inherently needs a generator LLM, so this example requires {@code ANTHROPIC_API_KEY}
 * (the loop's offline behaviour is covered by {@code RefinementLoopTest} with a scripted generator).
 */
public final class SelfRefinementExample {

  /** task prompt + the terms a good answer must mention (drives the deterministic check). */
  public record Task(String prompt, List<String> mustMention) {}

  private static final Task[] SAMPLE = {
    new Task("Explain what Apache Flink is in two sentences.", List.of("stream", "state")),
    new Task("Describe retrieval-augmented generation briefly.", List.of("retrieval", "context")),
    new Task("Summarize why checkpoints matter in Flink.", List.of("checkpoint", "recovery")),
  };

  public static void main(String[] args) throws Exception {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    if (apiKey == null || apiKey.isBlank()) {
      System.out.println(
          "SelfRefinementExample needs a generator LLM. Set ANTHROPIC_API_KEY and re-run.\n"
              + "(The loop's logic is covered offline by RefinementLoopTest.)");
      return;
    }
    boolean debug = hasFlag(args, "--debug") || "1".equals(System.getenv("AGENTIC_DEBUG"));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Task> tasks = env.fromElements(SAMPLE);
    BroadcastStream<ControlMessage> control =
        debug ? AgenticPipeline.seededControl(env, DebugControl.everywhere())
              : AgenticPipeline.emptyControl(env);

    SingleOutputStreamOperator<String> verdicts =
        AgenticPipeline.wire(tasks, control, new RefineFunction(apiKey));
    verdicts.name("self-refinement").print();

    AgenticPipeline.debugStream(verdicts)
        .map(e -> String.format("[debug] %s.%s %s", e.operatorId(), e.kind(), e.payload()))
        .print();

    System.out.println(
        "\n=== Self-refinement (generate → check → feed back → retry, Claude"
            + (debug ? "; debug ON" : "; debug OFF — pass --debug to enable")
            + ") ===\n");
    env.execute("Self-Refinement Loop");
  }

  private static boolean hasFlag(String[] args, String flag) {
    if (args == null) return false;
    for (String a : args) {
      if (flag.equals(a)) return true;
    }
    return false;
  }

  static final class RefineFunction extends AgenticProcessFunction<Task, String> {
    private static final long serialVersionUID = 1L;

    private final String apiKey;

    RefineFunction(String apiKey) {
      super("feedback.refine");
      this.apiKey = apiKey;
    }

    @Override
    protected void onElement(Task task, ReadOnlyContext ctx, Collector<String> out)
        throws Exception {
      RefinementLoop loop =
          RefinementLoop.builder()
              .withClaude(apiKey)
              .withCheck(new KeywordQualityCheck(task.mustMention(), 40, 1.0))
              .withMaxAttempts(3)
              .build();
      RefinementResult r = loop.refine(task.prompt());

      if (debugEnabled(ctx)) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("prompt", task.prompt());
        payload.put("accepted", r.accepted);
        payload.put("attempts", r.attemptsUsed);
        payload.put("finalScore", r.finalScore());
        payload.put("mustMention", task.mustMention());
        emitDebug(ctx, "refine", payload);
      }

      out.collect(
          String.format(
              "%-9s attempts=%d score=%.2f | %s",
              r.accepted ? "ACCEPTED" : "BEST-SO-FAR", r.attemptsUsed, r.finalScore(), task.prompt()));
    }
  }

  /** Suppresses an "unused" complaint if {@link DebugEvent} is removed from imports later. */
  @SuppressWarnings("unused")
  private static final Class<?> PIN = DebugEvent.class;
}
