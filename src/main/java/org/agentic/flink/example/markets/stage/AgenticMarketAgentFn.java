package org.agentic.flink.example.markets.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
    extends AgenticKeyedProcessFunction<String, MarketFeatures, String> {
  private static final long serialVersionUID = 1L;

  private final String apiKey; // nullable
  private final double spreadBandLow;
  private final double spreadBandHigh;
  private final double zThreshold;
  private final int warmup;
  private transient ScreeningPipeline pipeline;
  private transient ObjectMapper mapper;

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
    this.mapper = new ObjectMapper().registerModule(new ParameterNamesModule());
  }

  @Override
  protected void onElement(MarketFeatures features, ReadOnlyContext ctx, Collector<String> out)
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

    // Emit a JSON envelope (rather than a typed AlertEvent record) so the chain serializer
    // stays on Flink's built-in StringSerializer — record types in the screening + markets
    // packages would otherwise force a Kryo fallback that can't handle their final fields.
    out.collect(toJson(features, r));
  }

  private String toJson(MarketFeatures features, ScreeningResult r) throws Exception {
    Map<String, Object> alert = new LinkedHashMap<>();
    alert.put("instrumentId", features.instrumentId());
    alert.put("isin", features.isin());
    alert.put("securityName", features.securityName());
    alert.put("sector", features.sector());
    alert.put("windowEnd", features.windowEnd());
    alert.put("bidOfferSpread", features.bidOfferSpread());
    alert.put("totalBidVolume", features.totalBidVolume());
    alert.put("totalOfferVolume", features.totalOfferVolume());
    Map<String, Object> decision = new LinkedHashMap<>();
    decision.put("verdict", String.valueOf(r.verdict));
    decision.put("decidedBy", String.valueOf(r.decidedBy));
    decision.put("combinedRisk", r.combinedRisk);
    decision.put(
        "firedPhases",
        r.fired.stream().map(s -> s.phase().name()).distinct().toList());
    if (r.llmRationale != null) {
      decision.put("llmRationale", r.llmRationale);
    }
    alert.put("result", decision);
    return mapper.writeValueAsString(alert);
  }

  // Suppresses "unused" — these types are referenced through factories above but the
  // compiler can lose track of them when the imports get touched.
  @SuppressWarnings("unused")
  private static final Class<?>[] PIN = {AlertEvent.class, List.class};
}
