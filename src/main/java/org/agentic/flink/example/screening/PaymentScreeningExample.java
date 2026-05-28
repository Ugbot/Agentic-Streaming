package org.agentic.flink.example.screening;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.DebugControl;
import org.agentic.flink.control.DebugEvent;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.LexiconInferenceConnection;
import org.agentic.flink.operator.AgenticKeyedProcessFunction;
import org.agentic.flink.operator.wiring.AgenticPipeline;
import org.agentic.flink.screening.BandPassDetector;
import org.agentic.flink.screening.RepeatDetector;
import org.agentic.flink.screening.ScreenItem;
import org.agentic.flink.screening.ScreeningPipeline;
import org.agentic.flink.screening.ScreeningResult;
import org.agentic.flink.screening.VelocityDetector;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Multiphase payment screening over a keyed transaction stream: a band-pass amount check, a
 * "same payment three times in a row" repeat screen, and an account velocity screen are layered
 * into a combined risk; flagged payments escalate to an ML classifier and then an LLM that decides
 * ALLOW / REVIEW / BLOCK.
 *
 * <p>Wired on the new framework instrumentation: the inner keyed operator extends
 * {@link AgenticKeyedProcessFunction} so per-account decisions emit through the debug side-output
 * when {@code --debug} (or {@code AGENTIC_DEBUG=1}) is set.
 *
 * <p>Keyed by account so the stateful detectors see each account's full history on one subtask.
 * Set {@code ANTHROPIC_API_KEY} to enable the LLM tier; otherwise flagged payments route to REVIEW.
 */
public final class PaymentScreeningExample {

  /** account, amount, merchant, event-time millis. */
  public record Payment(String account, double amount, String merchant, long ts) {}

  private static final Payment[] SAMPLE = {
    new Payment("acct-1", 12.50, "Coffee Hut", 0),
    new Payment("acct-1", 80.00, "Grocers", 60_000),
    new Payment("acct-2", 4999.00, "Wire Transfer Co", 5_000), // out-of-band amount
    // acct-3 submits the SAME payment three times in a row → repeat screen.
    new Payment("acct-3", 250.00, "GiftCardz", 1_000),
    new Payment("acct-3", 250.00, "GiftCardz", 2_000),
    new Payment("acct-3", 250.00, "GiftCardz", 3_000),
    // acct-4 bursts: 5 charges inside a minute → velocity screen.
    new Payment("acct-4", 9.00, "AppStore", 0),
    new Payment("acct-4", 9.00, "AppStore", 5_000),
    new Payment("acct-4", 9.00, "AppStore", 10_000),
    new Payment("acct-4", 9.00, "AppStore", 15_000),
    new Payment("acct-4", 9.00, "AppStore", 20_000),
  };

  public static void main(String[] args) throws Exception {
    String apiKey = System.getenv("ANTHROPIC_API_KEY");
    boolean debug = hasFlag(args, "--debug") || "1".equals(System.getenv("AGENTIC_DEBUG"));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Payment> payments = env.fromElements(SAMPLE);
    BroadcastStream<ControlMessage> control =
        debug ? AgenticPipeline.seededControl(env, DebugControl.everywhere())
              : AgenticPipeline.emptyControl(env);

    KeyedStream<Payment, String> keyed = payments.keyBy(Payment::account);
    SingleOutputStreamOperator<String> verdicts =
        AgenticPipeline.wire(keyed, control, new ScreenFunction(apiKey));
    verdicts.name("payment-screening").print();

    AgenticPipeline.debugStream(verdicts)
        .map(e -> String.format("[debug] %s.%s %s", e.operatorId(), e.kind(), e.payload()))
        .print();

    System.out.println(
        "\n=== Payment screening (band-pass + repeat + velocity → ML → "
            + (apiKey == null ? "REVIEW; set ANTHROPIC_API_KEY for LLM" : "Claude")
            + (debug ? "; debug ON" : "; debug OFF — pass --debug to enable")
            + ") ===\n");
    env.execute("Payment Screening Cascade");
  }

  private static boolean hasFlag(String[] args, String flag) {
    if (args == null) return false;
    for (String a : args) {
      if (flag.equals(a)) return true;
    }
    return false;
  }

  static final class ScreenFunction extends AgenticKeyedProcessFunction<String, Payment, String> {
    private static final long serialVersionUID = 1L;

    private final String apiKey;
    private transient ScreeningPipeline pipeline;

    ScreenFunction(String apiKey) {
      super("screening.payments");
      this.apiKey = apiKey;
    }

    @Override
    public void open(OpenContext openContext) {
      ScreeningPipeline.Builder b =
          ScreeningPipeline.builder()
              .addDetector(new BandPassDetector(0, 1000, 0.6)) // amounts should be < $1000
              .addDetector(
                  new RepeatDetector(
                      3, 0.8, (a, c) -> a.value() == c.value() && a.label().equals(c.label())))
              .addDetector(new VelocityDetector(5, Duration.ofMinutes(1), 0.7))
              .withClassifier(
                  new LexiconInferenceConnection(),
                  InferenceSetup.builder()
                      .withModelName("lexicon")
                      .withModelUri("lexicon://default")
                      .build())
              .withReviewThreshold(0.5)
              .withBlockThreshold(2.0);
      if (apiKey != null && !apiKey.isBlank()) {
        b = b.withClaude(apiKey);
      }
      this.pipeline = b.build();
    }

    @Override
    protected void onElement(Payment p, ReadOnlyContext ctx, Collector<String> out)
        throws Exception {
      ScreenItem item =
          new ScreenItem(
              p.account(),
              p.amount(),
              p.merchant(),
              p.ts(),
              Map.of("merchant", p.merchant()));
      ScreeningResult r = pipeline.screen(item);

      if (debugEnabled(ctx)) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("account", p.account());
        payload.put("amount", p.amount());
        payload.put("merchant", p.merchant());
        payload.put("decidedBy", String.valueOf(r.decidedBy));
        payload.put("verdict", String.valueOf(r.verdict));
        payload.put("combinedRisk", r.combinedRisk);
        emitDebug(ctx, "screen", payload);
      }

      out.collect(
          String.format(
              "%-6s %-6s risk=%.2f | %s $%.2f @ %s",
              r.decidedBy, r.verdict, r.combinedRisk, p.account(), p.amount(), p.merchant()));
    }
  }

  /** Suppresses an "unused" complaint if {@link DebugEvent} is removed from imports later. */
  @SuppressWarnings("unused")
  private static final Class<?> PIN = DebugEvent.class;
}
