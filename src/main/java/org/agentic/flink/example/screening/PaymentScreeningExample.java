package org.agentic.flink.example.screening;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.LexiconInferenceConnection;
import org.agentic.flink.screening.BandPassDetector;
import org.agentic.flink.screening.RepeatDetector;
import org.agentic.flink.screening.ScreenItem;
import org.agentic.flink.screening.ScreeningPipeline;
import org.agentic.flink.screening.ScreeningResult;
import org.agentic.flink.screening.VelocityDetector;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Multiphase payment screening over a keyed transaction stream: a band-pass amount check, a
 * "same payment three times in a row" repeat screen, and an account velocity screen are layered
 * into a combined risk; flagged payments escalate to an ML classifier and then an LLM that decides
 * ALLOW / REVIEW / BLOCK.
 *
 * <p>Keyed by account so the stateful detectors see each account's full history on one subtask.
 * The {@link ScreeningPipeline} keeps bounded per-key history internally; for checkpoint-durable
 * state use Flink keyed {@code ValueState} (see {@code AgentLoopProcessFunction}). Runs via
 * {@code flink run} (the MiniCluster fails under {@code mvn exec:java}). Set {@code ANTHROPIC_API_KEY}
 * to enable the LLM tier; otherwise flagged payments route to REVIEW.
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

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Payment> payments = env.fromElements(SAMPLE);
    payments
        .keyBy(Payment::account)
        .process(new ScreenFunction(apiKey))
        .name("payment-screening")
        .print();

    System.out.println(
        "\n=== Payment screening (band-pass + repeat + velocity → ML → "
            + (apiKey == null ? "REVIEW; set ANTHROPIC_API_KEY for LLM" : "Claude") + ") ===\n");
    env.execute("Payment Screening Cascade");
  }

  static final class ScreenFunction extends KeyedProcessFunction<String, Payment, String> {
    private final String apiKey;
    private transient ScreeningPipeline pipeline;

    ScreenFunction(String apiKey) {
      this.apiKey = apiKey;
    }

    @Override
    public void open(OpenContext openContext) {
      ScreeningPipeline.Builder b =
          ScreeningPipeline.builder()
              .addDetector(new BandPassDetector(0, 1000, 0.6)) // amounts should be < $1000
              .addDetector(
                  new RepeatDetector(3, 0.8,
                      (a, c) -> a.value() == c.value() && a.label().equals(c.label())))
              .addDetector(new VelocityDetector(5, Duration.ofMinutes(1), 0.7))
              .withClassifier(
                  new LexiconInferenceConnection(),
                  InferenceSetup.builder().withModelName("lexicon").withModelUri("lexicon://default").build())
              .withReviewThreshold(0.5)
              .withBlockThreshold(2.0);
      if (apiKey != null && !apiKey.isBlank()) {
        b = b.withClaude(apiKey);
      }
      this.pipeline = b.build();
    }

    @Override
    public void processElement(Payment p, Context ctx, Collector<String> out) {
      ScreenItem item =
          new ScreenItem(p.account(), p.amount(), p.merchant(), p.ts(), Map.of("merchant", p.merchant()));
      ScreeningResult r = pipeline.screen(item);
      out.collect(
          String.format(
              "%-6s %-6s risk=%.2f | %s $%.2f @ %s",
              r.decidedBy, r.verdict, r.combinedRisk, p.account(), p.amount(), p.merchant()));
    }
  }
}
