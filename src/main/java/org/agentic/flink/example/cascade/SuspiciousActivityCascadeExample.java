package org.agentic.flink.example.cascade;

import java.util.HashMap;
import java.util.Map;
import org.agentic.flink.cascade.EscalationPipeline;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.DebugControl;
import org.agentic.flink.control.DebugEvent;
import org.agentic.flink.operator.AgenticProcessFunction;
import org.agentic.flink.operator.wiring.AgenticPipeline;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * <p>Wired on the new framework instrumentation: the inner operator extends
 * {@link AgenticProcessFunction} so its debug side-output is observable from the broadcast
 * control plane. Pass {@code --debug} (or set {@code AGENTIC_DEBUG=1}) to seed a
 * {@link DebugControl#everywhere()} at startup; per-message debug events are then printed
 * with a {@code [debug]} prefix alongside the main verdict line.
 *
 * <p>Runnable with no external infra (built-in lexicon classifier). Set {@code ANTHROPIC_API_KEY}
 * to let the LLM tier adjudicate confirmed cases with Claude; without it, confirmed cases route to
 * human {@code REVIEW}.
 *
 * <pre>
 *   mvn exec:java -Dexec.mainClass=org.agentic.flink.example.cascade.SuspiciousActivityCascadeExample \
 *                 -Dexec.classpathScope=test -Dexec.args="--debug"
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
    boolean debug = hasFlag(args, "--debug") || "1".equals(System.getenv("AGENTIC_DEBUG"));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> messages = env.fromElements(SAMPLE_MESSAGES);

    // Control plane: seed an "everywhere" directive when --debug is on so debug events flow
    // immediately. Otherwise wire an empty broadcast — operator behaves the same, just silent.
    BroadcastStream<ControlMessage> control =
        debug ? AgenticPipeline.seededControl(env, DebugControl.everywhere())
              : AgenticPipeline.emptyControl(env);

    SingleOutputStreamOperator<String> verdicts =
        AgenticPipeline.wire(messages, control, new CascadeFunction(apiKey));
    verdicts.name("escalation-cascade").print();

    // Tap the debug side-output and print it inline. No-op when debug is off.
    AgenticPipeline.debugStream(verdicts).map(DebugFormatter::format).print();

    System.out.println(
        "\n=== Suspicious-activity cascade ("
            + (apiKey == null ? "LLM tier disabled — set ANTHROPIC_API_KEY" : "LLM tier: Claude")
            + (debug ? "; debug ON" : "; debug OFF — pass --debug to enable")
            + ") ===\n");
    env.execute("Suspicious Activity Escalation Cascade");
  }

  private static boolean hasFlag(String[] args, String flag) {
    if (args == null) return false;
    for (String a : args) {
      if (flag.equals(a)) return true;
    }
    return false;
  }

  /** Builds the cascade in open() and evaluates each message; emits per-stage debug events. */
  static final class CascadeFunction extends AgenticProcessFunction<String, String> {
    private static final long serialVersionUID = 1L;

    private final String apiKey;
    private transient EscalationPipeline pipeline;

    CascadeFunction(String apiKey) {
      super("cascade.escalation");
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
    protected void onElement(String message, ReadOnlyContext ctx, Collector<String> out)
        throws Exception {
      EscalationPipeline.Decision d = pipeline.evaluate(message);
      if (debugEnabled(ctx)) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("decidedBy", String.valueOf(d.decidedBy));
        payload.put("verdict", d.verdict);
        payload.put("messagePreview", truncate(message, 80));
        if (d.llmRationale != null) {
          payload.put("llmRationale", d.llmRationale);
        }
        emitDebug(ctx, "tier-decision", payload);
      }
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

  /** Pretty-prints {@link DebugEvent}s with a {@code [debug]} prefix. */
  static final class DebugFormatter {
    static String format(DebugEvent e) {
      return String.format("[debug] %s.%s %s", e.operatorId(), e.kind(), e.payload());
    }
  }
}
