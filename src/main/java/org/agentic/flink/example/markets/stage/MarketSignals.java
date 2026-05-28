package org.agentic.flink.example.markets.stage;

import org.agentic.flink.example.markets.model.MarketRecords.MarketFeatures;

/** Pure, side-effect-free math used by {@link FeatureAggregatorFn} — split out so it's testable. */
public final class MarketSignals {

  private MarketSignals() {}

  /** Reduce an accumulator to an immutable {@link MarketFeatures} for a closed window. */
  public static MarketFeatures toFeatures(
      FeatureAggregatorFn.Accumulator a, long windowStart, long windowEnd) {
    double[] bidPricesOut = a.bidPrices.clone();
    double[] offerPricesOut = a.offerPrices.clone();
    double[] bidSizesOut = toDoubleArray(a.bidSizes);
    double[] offerSizesOut = toDoubleArray(a.offerSizes);

    double avgBidTop3 = avgValid(a.bidPrices, 3);
    double avgBidTop5 = avgValid(a.bidPrices, a.topN);
    double avgOfferTop3 = avgValid(a.offerPrices, 3);
    double avgOfferTop5 = avgValid(a.offerPrices, a.topN);

    double bestBid = a.bidCount == 0 ? Double.NaN : a.bidPrices[0];
    double bestOffer = a.offerCount == 0 ? Double.NaN : a.offerPrices[0];
    double spread =
        Double.isNaN(bestBid) || Double.isNaN(bestOffer) ? Double.NaN : bestOffer - bestBid;

    return new MarketFeatures(
        a.instrumentId,
        a.isin == null ? "" : a.isin,
        a.securityName,
        a.sector,
        a.isInvestmentGrade,
        windowStart,
        windowEnd,
        bidPricesOut,
        offerPricesOut,
        bidSizesOut,
        offerSizesOut,
        avgBidTop3,
        avgOfferTop3,
        avgBidTop5,
        avgOfferTop5,
        bestBid,
        bestOffer,
        spread,
        a.totalBidVolume,
        a.totalOfferVolume,
        a.bidCount,
        a.offerCount);
  }

  /** Mean of the first {@code n} non-NaN entries; NaN if none. */
  public static double avgValid(double[] xs, int n) {
    double sum = 0;
    int cnt = 0;
    for (int i = 0; i < xs.length && cnt < n; i++) {
      if (!Double.isNaN(xs[i])) {
        sum += xs[i];
        cnt++;
      }
    }
    return cnt == 0 ? Double.NaN : sum / cnt;
  }

  private static double[] toDoubleArray(long[] xs) {
    double[] out = new double[xs.length];
    for (int i = 0; i < xs.length; i++) out[i] = xs[i];
    return out;
  }
}
