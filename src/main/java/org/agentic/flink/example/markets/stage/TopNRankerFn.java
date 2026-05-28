package org.agentic.flink.example.markets.stage;

import org.agentic.flink.example.markets.model.MarketRecords.EnrichedInventory;
import org.agentic.flink.example.markets.model.MarketRecords.RankedQuote;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stage 2 — running top-N ranking per (instrumentId, side). Mirrors {@code 2_topn_ranking.sql}
 * (ROW_NUMBER() within window) but emits the latest top-N on every update (no closing window
 * delay), which is what downstream best-quote and feature-aggregation operators want.
 *
 * <p>State: latest quote per dealer (deduped by {@code companyShortName}), kept sorted by price —
 * descending for BID (highest is rank 1), ascending for OFFER (lowest is rank 1) — capped at
 * {@link #topN}. Emits one {@link RankedQuote} per current slot on each tick.
 *
 * <p>Key: {@code instrumentId + "|" + side}.
 */
public final class TopNRankerFn
    extends KeyedProcessFunction<String, EnrichedInventory, RankedQuote> {
  private static final long serialVersionUID = 1L;

  private final int topN;
  private transient ValueState<ArrayList<EnrichedInventory>> ranking;

  public TopNRankerFn(int topN) {
    if (topN < 1) throw new IllegalArgumentException("topN must be >= 1");
    this.topN = topN;
  }

  @Override
  public void open(OpenContext openContext) {
    ValueStateDescriptor<ArrayList<EnrichedInventory>> desc =
        new ValueStateDescriptor<>(
            "top-" + topN, TypeInformation.of(new TypeHint<ArrayList<EnrichedInventory>>() {}));
    this.ranking = getRuntimeContext().getState(desc);
  }

  @Override
  public void processElement(
      EnrichedInventory inv, Context ctx, Collector<RankedQuote> out) throws Exception {
    ArrayList<EnrichedInventory> list = ranking.value();
    if (list == null) list = new ArrayList<>(topN + 1);

    // Dedup by dealer for this (instrument, side) — the latest quote wins.
    String dealer = inv.inventory().companyShortName();
    list.removeIf(e -> dealer.equals(e.inventory().companyShortName()));
    list.add(inv);
    list.sort(comparator(inv.inventory().side()));
    while (list.size() > topN) list.remove(list.size() - 1);
    ranking.update(list);

    for (int i = 0; i < list.size(); i++) {
      out.collect(new RankedQuote(list.get(i), i + 1));
    }
  }

  /** Comparator that puts the best price first: BID descending, OFFER ascending. */
  static Comparator<EnrichedInventory> comparator(String side) {
    return "BID".equalsIgnoreCase(side)
        ? Comparator.comparingDouble((EnrichedInventory e) -> e.inventory().price()).reversed()
        : Comparator.comparingDouble((EnrichedInventory e) -> e.inventory().price());
  }
}
