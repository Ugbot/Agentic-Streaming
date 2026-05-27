package org.agentic.flink.example.cascade;

import org.agentic.flink.cascade.EscalationPipeline;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Streaming three-tier escalation cascade over a flow of messages:
 *
 * <pre>
 *   cheap filter  ──▶  ML classifier  ──▶  LLM adjudicator
 *   "looks            "confirm            "act
 *    suspicious?"      suspicious"          accordingly"
 * </pre>
 *
 * <p>Most messages are cleared by the substring filter for ~free; only filter hits run the ML
 * classifier; only ML-confirmed cases reach the (expensive) LLM. The job prints each decision
 * tagged with the tier that made it, so you can see the funnel narrow at each stage.
 *
 * <p>Runnable with no external infra (built-in lexicon classifier). Set {@code ANTHROPIC_API_KEY}
 * to let the LLM tier adjudicate confirmed cases with Claude; without it, confirmed cases route to
 * human {@code REVIEW}.
 *
 * <pre>
 *   mvn exec:java -Dexec.mainClass=org.agentic.flink.example.cascade.SuspiciousActivityCascadeExample \
 *                 -Dexec.classpathScope=test
 * </pre>
 */
public final class SuspiciousActivityCascadeExample {

  private static final String[] SAMPLE_MESSAGES = {
    "Hi team, lunch moved to 1pm in the usual room.",
    "Your package is out for delivery and arrives today.",
    "Can you review the Q3 design doc before Friday?",
    "Hello, I'd like to request a refund for order #4471.",
    "Reminder: standup is at 9:30 tomorrow.",
    "URGENT: your account will be suspended — verify your account now via this link.",
    "Please send a wire transfer and three gift cards immediately, this is time sensitive.",
    "Confirm your social security number and routing number to release your refund.",
    "Act now! Limited time crypto offer, send bitcoin to double your money.",
    "The deployment finished and all checks are green.",
  };

  public static void main(String[] args) throws Exception {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> messages = env.fromElements(SAMPLE_MESSAGES);

    messages
        .process(new CascadeFunction(apiKey))
        .name("escalation-cascade")
        .print();

    System.out.println(
        "\n=== Suspicious-activity cascade ("
            + (apiKey == null ? "LLM tier disabled — set ANTHROPIC_API_KEY" : "LLM tier: Claude")
            + ") ===\n");
    env.execute("Suspicious Activity Escalation Cascade");
  }

  /** Builds the cascade in open() (clients are not serializable) and evaluates each message. */
  static final class CascadeFunction extends ProcessFunction<String, String> {
    private final String apiKey;
    private transient EscalationPipeline pipeline;

    CascadeFunction(String apiKey) {
      this.apiKey = apiKey;
    }

    @Override
    public void open(OpenContext openContext) {
      EscalationPipeline.Builder b = EscalationPipeline.builder().withMlThreshold(0.5);
      if (apiKey != null && !apiKey.isBlank()) {
        b = b.withClaude(apiKey);
      }
      this.pipeline = b.build();
    }

    @Override
    public void processElement(String message, Context ctx, Collector<String> out) {
      EscalationPipeline.Decision d = pipeline.evaluate(message);
      out.collect(
          String.format(
              "%-6s %-6s | %s%s",
              d.decidedBy,
              d.verdict,
              truncate(message, 70),
              d.decidedBy == EscalationPipeline.Tier.LLM ? "  >> " + d.llmRationale : ""));
    }

    private static String truncate(String s, int n) {
      return s.length() <= n ? s : s.substring(0, n - 1) + "…";
    }
  }
}
