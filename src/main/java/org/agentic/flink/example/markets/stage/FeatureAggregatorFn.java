package org.agentic.flink.example.markets.stage;

import org.agentic.flink.example.markets.model.MarketRecords.EnrichedInventory;
import org.agentic.flink.example.markets.model.MarketRecords.MarketFeatures;
import org.agentic.flink.example.markets.model.MarketRecords.RankedQuote;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stage 4 — windowed feature aggregation per instrumentId. Mirrors {@code 4_feature_aggregation.sql}
 * (per-rank prices/sizes, top-3/5 averages, spreads, volumes, counts) but implemented manually
 * with a {@code KeyedProcessFunction} + processing-time timer (no Table API in the repo).
 *
 * <p>Key: instrumentId (as String for state-backend consistency with the rest of the pipeline).
 * On each {@link RankedQuote}: overwrite the (rank, side) slot in a small accumulator and register
 * a window-end timer if not already pending. On timer: compute and emit {@link MarketFeatures},
 * then clear state.
 */
public final class FeatureAggregatorFn
    extends KeyedProcessFunction<String, RankedQuote, MarketFeatures> {
  private static final long serialVersionUID = 1L;

  private final long windowMillis;
  private final int topN;
  private transient ValueState<Accumulator> acc;

  public FeatureAggregatorFn(long windowMillis, int topN) {
    this.windowMillis = windowMillis;
    this.topN = topN;
  }

  @Override
  public void open(OpenContext openContext) {
    acc =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("feature-accumulator", Accumulator.class));
  }

  @Override
  public void processElement(RankedQuote rq, Context ctx, Collector<MarketFeatures> out)
      throws Exception {
    Accumulator a = acc.value();
    long now = ctx.timerService().currentProcessingTime();
    if (a == null) {
      a = new Accumulator(topN);
      a.windowStart = now;
      ctx.timerService().registerProcessingTimeTimer(now + windowMillis);
    }
    a.absorb(rq);
    acc.update(a);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<MarketFeatures> out)
      throws Exception {
    Accumulator a = acc.value();
    if (a == null) return;
    out.collect(MarketSignals.toFeatures(a, a.windowStart, timestamp));
    acc.clear();
  }

  /**
   * Small mutable accumulator held in keyed state. {@link MarketSignals#toFeatures} reduces it to
   * the immutable {@link MarketFeatures} on window close.
   */
  public static final class Accumulator implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public final int topN;
    public long windowStart;
    public long instrumentId;
    public String isin;
    public String securityName;
    public String sector;
    public String isInvestmentGrade;
    public double[] bidPrices;
    public double[] offerPrices;
    public long[] bidSizes;
    public long[] offerSizes;
    public int bidCount;
    public int offerCount;
    public double totalBidVolume;
    public double totalOfferVolume;

    public Accumulator() { // for Flink Pojo serialization
      this(5);
    }

    public Accumulator(int topN) {
      this.topN = topN;
      this.bidPrices = new double[topN];
      this.offerPrices = new double[topN];
      this.bidSizes = new long[topN];
      this.offerSizes = new long[topN];
      java.util.Arrays.fill(this.bidPrices, Double.NaN);
      java.util.Arrays.fill(this.offerPrices, Double.NaN);
    }

    /** Apply one ranked quote to the accumulator (idempotent per slot — last write wins). */
    public void absorb(RankedQuote rq) {
      int idx = rq.rank() - 1;
      if (idx < 0 || idx >= topN) return;
      EnrichedInventory q = rq.quote();
      this.instrumentId = q.inventory().instrumentId();
      this.isin = q.isin();
      if (q.security() != null) {
        this.securityName = q.security().name();
        this.sector = q.security().sector();
        this.isInvestmentGrade = q.security().isInvestmentGrade();
      }
      if ("BID".equalsIgnoreCase(q.inventory().side())) {
        if (Double.isNaN(bidPrices[idx])) bidCount++;
        else totalBidVolume -= bidSizes[idx]; // replacing same slot
        bidPrices[idx] = q.inventory().price();
        bidSizes[idx] = q.inventory().size();
        totalBidVolume += q.inventory().size();
      } else {
        if (Double.isNaN(offerPrices[idx])) offerCount++;
        else totalOfferVolume -= offerSizes[idx];
        offerPrices[idx] = q.inventory().price();
        offerSizes[idx] = q.inventory().size();
        totalOfferVolume += q.inventory().size();
      }
    }
  }
}
