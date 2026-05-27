package org.agentic.flink.example.screening;

import org.agentic.flink.screening.BandPassDetector;
import org.agentic.flink.screening.RepeatDetector;
import org.agentic.flink.screening.ScreenItem;
import org.agentic.flink.screening.ScreeningPipeline;
import org.agentic.flink.screening.ScreeningResult;
import org.agentic.flink.screening.VelocityDetector;
import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Sensor/telemetry band-pass screening — the same {@link ScreeningPipeline} as
 * {@link PaymentScreeningExample}, different domain and detector config:
 *
 * <ul>
 *   <li><b>band-pass</b> — readings must stay within an expected operating band; out-of-band fires;</li>
 *   <li><b>repeat</b> — the identical reading N times in a row signals a stuck sensor;</li>
 *   <li><b>velocity</b> — a burst of readings signals a flapping/chattering sensor.</li>
 * </ul>
 *
 * <p>No ML/LLM tier here (numeric telemetry needs no language model) — it stops at the rules
 * verdict. Keyed by sensor id; runs via {@code flink run}.
 */
public final class TelemetryScreeningExample {

  /** sensorId, reading value, event-time millis. */
  public record Reading(String sensorId, double value, long ts) {}

  private static final Reading[] SAMPLE = {
    new Reading("temp-1", 21.4, 0),
    new Reading("temp-1", 22.1, 1_000),
    new Reading("temp-1", 98.7, 2_000), // out-of-band spike
    // temp-2 stuck: identical reading 4× in a row.
    new Reading("temp-2", 20.0, 0),
    new Reading("temp-2", 20.0, 1_000),
    new Reading("temp-2", 20.0, 2_000),
    new Reading("temp-2", 20.0, 3_000),
    // temp-3 flapping: many readings in a short burst.
    new Reading("temp-3", 19.0, 0),
    new Reading("temp-3", 25.0, 200),
    new Reading("temp-3", 18.0, 400),
    new Reading("temp-3", 27.0, 600),
    new Reading("temp-3", 17.0, 800),
  };

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Reading> readings = env.fromElements(SAMPLE);
    readings
        .keyBy(Reading::sensorId)
        .process(new ScreenFunction())
        .name("telemetry-screening")
        .print();

    System.out.println("\n=== Telemetry band-pass screening (band-pass + stuck + flapping) ===\n");
    env.execute("Telemetry Screening");
  }

  static final class ScreenFunction extends KeyedProcessFunction<String, Reading, String> {
    private transient ScreeningPipeline pipeline;

    @Override
    public void open(OpenContext openContext) {
      this.pipeline =
          ScreeningPipeline.builder()
              .addDetector(new BandPassDetector(-40, 85, 0.7)) // expected operating range
              .addDetector(RepeatDetector.sameValue(4, 0.8)) // stuck sensor
              .addDetector(new VelocityDetector(5, Duration.ofSeconds(1), 0.6)) // flapping
              .withReviewThreshold(0.5)
              .withBlockThreshold(1.5)
              .build();
    }

    @Override
    public void processElement(Reading rd, Context ctx, Collector<String> out) {
      ScreenItem item = ScreenItem.of(rd.sensorId(), rd.value(), rd.sensorId(), rd.ts());
      ScreeningResult r = pipeline.screen(item);
      out.collect(
          String.format(
              "%-6s %-6s risk=%.2f | %s = %.1f", r.decidedBy, r.verdict, r.combinedRisk,
              rd.sensorId(), rd.value()));
    }
  }
}
