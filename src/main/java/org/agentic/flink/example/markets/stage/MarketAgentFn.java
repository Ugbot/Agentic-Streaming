package org.agentic.flink.example.markets.stage;

import org.agentic.flink.example.markets.model.MarketRecords.AlertEvent;
import org.agentic.flink.example.markets.model.MarketRecords.MarketFeatures;
import org.agentic.flink.screening.BandPassDetector;
import org.agentic.flink.screening.Phase;
import org.agentic.flink.screening.ScreenItem;
import org.agentic.flink.screening.ScreeningPipeline;
import org.agentic.flink.screening.ScreeningResult;
import org.agentic.flink.screening.ZScoreDetector;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stage 5 — the INLINE AGENTIC operator. Consumes the upstream {@link MarketFeatures} stream and
 * runs each row through a {@link ScreeningPipeline} that layers a band-pass on bid-offer spread, a
 * rolling z-score on spread, and a rolling z-score on bid depth; flagged windows escalate to an LLM
 * (Claude when {@code ANTHROPIC_API_KEY} is set) which adjudicates the action.
 *
 * <p>This is the "classic Flink upstream + agentic inline downstream" pattern in one place — the
 * pipeline is built once per task in {@link #open}, then called per record. Key is
 * {@code instrumentId} so each instrument has its own per-key rolling history inside the pipeline.
 */
public final class MarketAgentFn
    extends KeyedProcessFunction<String, MarketFeatures, AlertEvent> {
  private static final long serialVersionUID = 1L;

  private final String apiKey; // nullable
  private final double spreadBandLow;
  private final double spreadBandHigh;
  private final double zThreshold;
  private final int warmup;
  private transient ScreeningPipeline pipeline;

  public MarketAgentFn(String apiKey) {
    this(apiKey, 0.0, 50.0, 3.0, 5);
  }

  public MarketAgentFn(
      String apiKey, double spreadBandLow, double spreadBandHigh, double zThreshold, int warmup) {
    this.apiKey = apiKey;
    this.spreadBandLow = spreadBandLow;
    this.spreadBandHigh = spreadBandHigh;
    this.zThreshold = zThreshold;
    this.warmup = warmup;
  }

  @Override
  public void open(OpenContext openContext) {
    ScreeningPipeline.Builder b =
        ScreeningPipeline.builder()
            .addDetector(new BandPassDetector(spreadBandLow, spreadBandHigh, 0.6))
            .addDetector(new ZScoreDetector("z-spread", Phase.CLASSIFIER, zThreshold, warmup, 0.7))
            .addDetector(
                ZScoreDetector.onAttr("totalBidVolume", Phase.CLASSIFIER, zThreshold, warmup, 0.5))
            .addDetector(
                ZScoreDetector.onAttr(
                    "totalOfferVolume", Phase.CLASSIFIER, zThreshold, warmup, 0.5))
            .withReviewThreshold(0.6)
            .withBlockThreshold(2.0);
    if (apiKey != null && !apiKey.isBlank()) {
      b = b.withClaude(apiKey);
    }
    this.pipeline = b.build();
  }

  @Override
  public void processElement(MarketFeatures features, Context ctx, Collector<AlertEvent> out) {
    if (Double.isNaN(features.bidOfferSpread())) return; // not enough data this window
    ScreenItem item =
        new ScreenItem(
            Long.toString(features.instrumentId()),
            features.bidOfferSpread(),
            features.isin(),
            features.windowEnd(),
            features.asAttrs());
    ScreeningResult r = pipeline.screen(item);
    out.collect(new AlertEvent(features, r));
  }
}
