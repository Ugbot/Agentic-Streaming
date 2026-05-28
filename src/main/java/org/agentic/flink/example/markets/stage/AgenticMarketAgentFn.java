package org.agentic.flink.example.markets.stage;

import java.util.HashMap;
import java.util.Map;
import org.agentic.flink.example.markets.model.MarketRecords.AlertEvent;
import org.agentic.flink.example.markets.model.MarketRecords.MarketFeatures;
import org.agentic.flink.operator.AgenticKeyedProcessFunction;
import org.agentic.flink.screening.BandPassDetector;
import org.agentic.flink.screening.Phase;
import org.agentic.flink.screening.ScreenItem;
import org.agentic.flink.screening.ScreeningPipeline;
import org.agentic.flink.screening.ScreeningResult;
import org.agentic.flink.screening.ZScoreDetector;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.util.Collector;

/**
 * Control-plane-aware sibling of {@link MarketAgentFn}. Same screening pipeline (band-pass +
 * rolling z-score + Claude), but extends {@link AgenticKeyedProcessFunction} so it picks up the
 * framework debug side-output and broadcast control input for free.
 *
 * <p>Used by the session-cluster launcher's L5 stage. The original {@link MarketAgentFn} stays
 * in place for the {@code flink run}-style bond/crypto examples whose Flink graphs predate the
 * control plane.
 */
public final class AgenticMarketAgentFn
    extends AgenticKeyedProcessFunction<String, MarketFeatures, AlertEvent> {
  private static final long serialVersionUID = 1L;

  private final String apiKey; // nullable
  private final double spreadBandLow;
  private final double spreadBandHigh;
  private final double zThreshold;
  private final int warmup;
  private transient ScreeningPipeline pipeline;

  public AgenticMarketAgentFn(String operatorId, String apiKey) {
    this(operatorId, apiKey, 0.0, 50.0, 3.0, 5);
  }

  public AgenticMarketAgentFn(
      String operatorId,
      String apiKey,
      double spreadBandLow,
      double spreadBandHigh,
      double zThreshold,
      int warmup) {
    super(operatorId);
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
  protected void onElement(MarketFeatures features, ReadOnlyContext ctx, Collector<AlertEvent> out)
      throws Exception {
    if (Double.isNaN(features.bidOfferSpread())) {
      return; // not enough data this window
    }
    ScreenItem item =
        new ScreenItem(
            Long.toString(features.instrumentId()),
            features.bidOfferSpread(),
            features.isin(),
            features.windowEnd(),
            features.asAttrs());
    ScreeningResult r = pipeline.screen(item);

    // Cheap volatile-read fast path; emitDebug is a no-op when off.
    if (debugEnabled(ctx)) {
      Map<String, Object> payload = new HashMap<>();
      payload.put("instrumentId", features.instrumentId());
      payload.put("spread", features.bidOfferSpread());
      payload.put("verdict", String.valueOf(r.verdict));
      payload.put("decidedBy", String.valueOf(r.decidedBy));
      payload.put("combinedRisk", r.combinedRisk);
      emitDebug(ctx, "screen", payload);
    }
    out.collect(new AlertEvent(features, r));
  }
}
