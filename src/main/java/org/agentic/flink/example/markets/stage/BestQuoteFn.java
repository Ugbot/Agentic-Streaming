package org.agentic.flink.example.markets.stage;

import org.agentic.flink.example.markets.model.MarketRecords.BestQuoteWithTrade;
import org.agentic.flink.example.markets.model.MarketRecords.EnrichedInventory;
import org.agentic.flink.example.markets.model.MarketRecords.RankedQuote;
import org.agentic.flink.example.markets.model.MarketRecords.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stage 3 — derive best bid + best offer and join with the most recent trade by ISIN. Mirrors
 * {@code 3_best_quotes.sql}: filters {@link RankedQuote}s where {@code rank=1} on each side, holds
 * the latest of each in keyed state, and joins the most recent {@link Trade} for the same ISIN.
 *
 * <p>Key: ISIN. Inputs are the rank-1-filtered ranked quotes (left) and the trade stream (right).
 * Emits a fresh {@link BestQuoteWithTrade} on either side update or trade arrival once both sides
 * are present.
 */
public final class BestQuoteFn
    extends KeyedCoProcessFunction<String, RankedQuote, Trade, BestQuoteWithTrade> {
  private static final long serialVersionUID = 1L;

  private transient ValueState<EnrichedInventory> bestBid;
  private transient ValueState<EnrichedInventory> bestOffer;
  private transient ValueState<Trade> lastTrade;

  @Override
  public void open(OpenContext openContext) {
    bestBid =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("bestBid", EnrichedInventory.class));
    bestOffer =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("bestOffer", EnrichedInventory.class));
    lastTrade = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTrade", Trade.class));
  }

  @Override
  public void processElement1(RankedQuote rq, Context ctx, Collector<BestQuoteWithTrade> out)
      throws Exception {
    if (rq.rank() != 1) return; // only the best on each side
    if ("BID".equalsIgnoreCase(rq.side())) {
      bestBid.update(rq.quote());
    } else {
      bestOffer.update(rq.quote());
    }
    emitIfReady(out);
  }

  @Override
  public void processElement2(Trade trade, Context ctx, Collector<BestQuoteWithTrade> out)
      throws Exception {
    lastTrade.update(trade);
    emitIfReady(out);
  }

  private void emitIfReady(Collector<BestQuoteWithTrade> out) throws Exception {
    EnrichedInventory bid = bestBid.value();
    EnrichedInventory ofr = bestOffer.value();
    if (bid == null || ofr == null) return;
    Trade tr = lastTrade.value();
    out.collect(
        new BestQuoteWithTrade(
            bid.inventory().instrumentId(),
            bid.isin(),
            bid.security() == null ? null : bid.security().name(),
            bid.security() == null ? null : bid.security().sector(),
            bid.security() == null ? null : bid.security().isInvestmentGrade(),
            bid.inventory().price(),
            bid.inventory().size(),
            ofr.inventory().price(),
            ofr.inventory().size(),
            ofr.inventory().price() - bid.inventory().price(),
            tr == null ? null : tr.id(),
            tr == null ? null : tr.dealPrice(),
            tr == null ? null : tr.yieldVal(),
            tr == null ? null : tr.iSpread(),
            tr == null ? null : tr.zSpread(),
            Math.max(bid.inventory().processedTs(), ofr.inventory().processedTs())));
  }
}
