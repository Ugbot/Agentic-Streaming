package org.agentic.flink.example.markets.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.example.markets.model.MarketRecords.EnrichedInventory;
import org.agentic.flink.example.markets.model.MarketRecords.Inventory;
import org.agentic.flink.example.markets.model.MarketRecords.MarketFeatures;
import org.agentic.flink.example.markets.model.MarketRecords.RankedQuote;
import org.agentic.flink.example.markets.model.MarketRecords.Security;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Pure-function tests for the market-signal math (no Flink runtime needed). */
class MarketSignalsTest {

  private static Inventory inv(String dealer, String side, double price, long size) {
    return new Inventory(dealer, 42L, side, price, size, 0.0, 1, 1, "HG", "USHG", "F", "UPDATE", 0L);
  }

  private static Security sec() {
    return new Security(42L, "US1234567890", "123456789", "ALPHACORE", "ALPHACORE", "Financials",
        "Banks", 0.045, "2035-07-12", "A+", "A", "A2", "Y");
  }

  private static EnrichedInventory ei(String dealer, String side, double price, long size) {
    return new EnrichedInventory(inv(dealer, side, price, size), sec());
  }

  @Test
  void topNComparatorOrdersBidsDescendingAndOffersAscending() {
    Comparator<EnrichedInventory> bid = TopNRankerFn.comparator("BID");
    Comparator<EnrichedInventory> ofr = TopNRankerFn.comparator("OFFER");
    List<EnrichedInventory> bids = new java.util.ArrayList<>(List.of(
        ei("d1", "BID", 100.10, 100),
        ei("d2", "BID", 100.50, 100),
        ei("d3", "BID", 100.25, 100)));
    bids.sort(bid);
    assertEquals(100.50, bids.get(0).inventory().price(), 1e-9);
    assertEquals(100.10, bids.get(2).inventory().price(), 1e-9);

    List<EnrichedInventory> ofs = new java.util.ArrayList<>(List.of(
        ei("d1", "OFFER", 101.10, 100),
        ei("d2", "OFFER", 100.80, 100),
        ei("d3", "OFFER", 101.50, 100)));
    ofs.sort(ofr);
    assertEquals(100.80, ofs.get(0).inventory().price(), 1e-9);
    assertEquals(101.50, ofs.get(2).inventory().price(), 1e-9);
  }

  @Test
  void accumulatorAndToFeaturesProduceExpectedAggregates() {
    FeatureAggregatorFn.Accumulator a = new FeatureAggregatorFn.Accumulator(5);
    // 3 bids ranks 1..3
    a.absorb(new RankedQuote(ei("d1", "BID", 100.50, 500), 1));
    a.absorb(new RankedQuote(ei("d2", "BID", 100.25, 300), 2));
    a.absorb(new RankedQuote(ei("d3", "BID", 100.10, 200), 3));
    // 2 offers ranks 1..2
    a.absorb(new RankedQuote(ei("d4", "OFFER", 101.00, 400), 1));
    a.absorb(new RankedQuote(ei("d5", "OFFER", 101.25, 600), 2));

    MarketFeatures f = MarketSignals.toFeatures(a, 1000L, 2000L);
    assertEquals(42L, f.instrumentId());
    assertEquals(3, f.bidCount());
    assertEquals(2, f.offerCount());
    assertEquals(100.50, f.bestBidPrice(), 1e-9);
    assertEquals(101.00, f.bestOfferPrice(), 1e-9);
    assertEquals(0.50, f.bidOfferSpread(), 1e-9);
    assertEquals(1000.0, f.totalBidVolume(), 1e-9);
    assertEquals(1000.0, f.totalOfferVolume(), 1e-9);
    assertEquals((100.50 + 100.25 + 100.10) / 3.0, f.avgBidTop3(), 1e-9);
    assertEquals((101.00 + 101.25) / 2.0, f.avgOfferTop3(), 1e-9, "top-3 avg over 2 present");
  }

  @Test
  void emptyAccumulatorYieldsNaNs() {
    FeatureAggregatorFn.Accumulator a = new FeatureAggregatorFn.Accumulator(5);
    MarketFeatures f = MarketSignals.toFeatures(a, 0L, 1L);
    assertTrue(Double.isNaN(f.bestBidPrice()));
    assertTrue(Double.isNaN(f.bidOfferSpread()));
    assertEquals(0, f.bidCount());
  }

  @Test
  void replacingSlotKeepsCountStableAndUpdatesVolume() {
    FeatureAggregatorFn.Accumulator a = new FeatureAggregatorFn.Accumulator(5);
    a.absorb(new RankedQuote(ei("d1", "BID", 100.50, 500), 1));
    a.absorb(new RankedQuote(ei("d1", "BID", 100.75, 700), 1)); // replace slot 1
    assertEquals(1, a.bidCount);
    assertEquals(700.0, a.totalBidVolume, 1e-9);
  }
}
