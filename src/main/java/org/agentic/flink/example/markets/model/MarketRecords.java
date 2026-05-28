package org.agentic.flink.example.markets.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.agentic.flink.screening.ScreeningResult;

/**
 * Record schemas for the market data pipeline. Field shapes mirror
 * {@code /Users/bengamble/mrkaxis copy/invenory_rows_synthesiser} (anonymised firm/platform names
 * in the producers) so the Java DataStream stages map 1:1 onto the original SQL pipeline.
 */
public final class MarketRecords {

  private MarketRecords() {}

  /** Dealer inventory quote (bid or offer at some price/size for an instrument). */
  public record Inventory(
      String companyShortName,
      long instrumentId,
      String side, // "BID" | "OFFER"
      double price,
      long size,
      double spread,
      int level, // dealer-supplied 1..5
      int tier,
      String marketSegment,
      String productCD,
      String quoteType,
      String action, // "UPDATE" | "DELETE"
      long processedTs)
      implements Serializable {}

  /** Static-ish security master row (looked up by id). */
  public record Security(
      long id,
      String isin,
      String cusip,
      String name,
      String issuer,
      String sector,
      String industry,
      double coupon,
      String maturity,
      String fitchRating,
      String snpRating,
      String moodyRating,
      String isInvestmentGrade)
      implements Serializable {}

  /** Trade event from the (anonymised) execution reporting platform. */
  public record Trade(
      long id,
      String isin,
      long tradeTs,
      String side, // "BUY" | "SELL"
      double dealPrice,
      double quantity,
      double yieldVal,
      double iSpread,
      double zSpread,
      double marketSpread,
      String sector,
      String firmPrincipal,
      String firmCpty,
      String placeOfTrade)
      implements Serializable {}

  /** Inventory enriched with the security-master fields (stage 1 output). */
  public record EnrichedInventory(Inventory inventory, Security security) implements Serializable {
    public String isin() {
      return security == null ? "" : security.isin();
    }
  }

  /** A top-N row per instrument and side (stage 2 output). */
  public record RankedQuote(EnrichedInventory quote, int rank) implements Serializable {
    public long instrumentId() {
      return quote.inventory().instrumentId();
    }

    public String side() {
      return quote.inventory().side();
    }
  }

  /** The rank-1 quote on each side plus the most recent trade for the same ISIN (stage 3). */
  public record BestQuoteWithTrade(
      long instrumentId,
      String isin,
      String securityName,
      String sector,
      String isInvestmentGrade,
      double bestBidPrice,
      long bestBidSize,
      double bestOfferPrice,
      long bestOfferSize,
      double bidOfferSpread,
      // last trade (nullable fields)
      Long lastTradeId,
      Double lastTradePrice,
      Double lastTradeYield,
      Double lastTradeISpread,
      Double lastTradeZSpread,
      long ts)
      implements Serializable {}

  /** Per-window aggregated market features over an instrument (stage 4 output). */
  public record MarketFeatures(
      long instrumentId,
      String isin,
      String securityName,
      String sector,
      String isInvestmentGrade,
      long windowStart,
      long windowEnd,
      // top-N prices per side (rank 1..5 if present, else NaN)
      double[] bidRankPrices,
      double[] offerRankPrices,
      double[] bidRankSizes,
      double[] offerRankSizes,
      double avgBidTop3,
      double avgOfferTop3,
      double avgBidTop5,
      double avgOfferTop5,
      double bestBidPrice,
      double bestOfferPrice,
      double bidOfferSpread,
      double totalBidVolume,
      double totalOfferVolume,
      int bidCount,
      int offerCount)
      implements Serializable {

    public Map<String, String> asAttrs() {
      return Map.of(
          "spread", Double.toString(bidOfferSpread),
          "totalBidVolume", Double.toString(totalBidVolume),
          "totalOfferVolume", Double.toString(totalOfferVolume),
          "avgBidTop3", Double.toString(avgBidTop3),
          "avgOfferTop3", Double.toString(avgOfferTop3),
          "bidCount", Integer.toString(bidCount),
          "offerCount", Integer.toString(offerCount));
    }
  }

  /** Final agentic alert: the feature row plus the screening decision. */
  public record AlertEvent(MarketFeatures features, ScreeningResult result)
      implements Serializable {

    public List<String> firedPhases() {
      return result == null
          ? Collections.emptyList()
          : result.fired.stream().map(s -> s.phase().name()).distinct().toList();
    }

    @Override
    public String toString() {
      String phases = String.join(",", firedPhases());
      return String.format(
          "ALERT instr=%d isin=%s spread=%.2f vol(b/o)=%.0f/%.0f -> [%s] %s risk=%.2f signals=[%s]",
          features.instrumentId(),
          features.isin(),
          features.bidOfferSpread(),
          features.totalBidVolume(),
          features.totalOfferVolume(),
          result == null ? "-" : result.decidedBy.name(),
          result == null ? "-" : result.verdict,
          result == null ? 0.0 : result.combinedRisk,
          phases.isEmpty() ? "-" : phases);
    }
  }
}
