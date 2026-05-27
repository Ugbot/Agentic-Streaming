package org.agentic.flink.example.feedback;

import org.agentic.flink.feedback.KeywordQualityCheck;
import org.agentic.flink.feedback.RefinementLoop;
import org.agentic.flink.feedback.RefinementResult;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Streaming self-refinement: each task is generated, checked, and — if it falls short — regenerated
 * with the critique fed back, up to an attempt budget. Per record it prints whether the answer was
 * accepted, how many attempts it took, and the best score.
 *
 * <p>Refinement inherently needs a generator LLM, so this example requires {@code ANTHROPIC_API_KEY}
 * (the loop's offline behaviour is covered by {@code RefinementLoopTest} with a scripted generator).
 * The default check here is a deterministic {@link KeywordQualityCheck} (each task must mention some
 * required terms); swap in {@code LlmCriticQualityCheck} for an LLM critic. Runs via {@code flink run}.
 */
public final class SelfRefinementExample {

  /** task prompt + the terms a good answer must mention (drives the deterministic check). */
  private record Task(String prompt, List<String> mustMention) {}

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

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Task> tasks = env.fromElements(SAMPLE);
    tasks.process(new RefineFunction(apiKey)).name("self-refinement").print();

    System.out.println("\n=== Self-refinement (generate → check → feed back → retry, Claude) ===\n");
    env.execute("Self-Refinement Loop");
  }

  static final class RefineFunction extends ProcessFunction<Task, String> {
    private final String apiKey;

    RefineFunction(String apiKey) {
      this.apiKey = apiKey;
    }

    @Override
    public void processElement(Task task, Context ctx, Collector<String> out) {
      RefinementLoop loop =
          RefinementLoop.builder()
              .withClaude(apiKey)
              .withCheck(new KeywordQualityCheck(task.mustMention(), 40, 1.0))
              .withMaxAttempts(3)
              .build();
      RefinementResult r = loop.refine(task.prompt());
      out.collect(
          String.format(
              "%-9s attempts=%d score=%.2f | %s",
              r.accepted ? "ACCEPTED" : "BEST-SO-FAR", r.attemptsUsed, r.finalScore(), task.prompt()));
    }
  }
}
