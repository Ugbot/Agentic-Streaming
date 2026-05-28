package org.agentic.flink.example.markets.stage;

import org.agentic.flink.example.markets.model.MarketRecords.EnrichedInventory;
import org.agentic.flink.example.markets.model.MarketRecords.Inventory;
import org.agentic.flink.example.markets.model.MarketRecords.Security;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stage 1 — enrich inventory rows with the security master (broadcast join).
 *
 * <p>Mirrors {@code 1_base_enrichment.sql}: looks up the security by {@code MOD(instrumentId, 50_000)}
 * and emits {@link EnrichedInventory}. Filters out {@code DELETE} actions and non-positive prices.
 */
public final class EnrichmentFn
    extends BroadcastProcessFunction<Inventory, Security, EnrichedInventory> {
  private static final long serialVersionUID = 1L;

  /** Broadcast state descriptor for the security master, keyed by id. */
  public static final MapStateDescriptor<Long, Security> SECURITIES =
      new MapStateDescriptor<>("securities", Long.class, Security.class);

  private static final long ID_MODULUS = 50_000L;

  @Override
  public void processElement(
      Inventory inv, ReadOnlyContext ctx, Collector<EnrichedInventory> out) throws Exception {
    if (inv == null || "DELETE".equalsIgnoreCase(inv.action()) || inv.price() <= 0) {
      return;
    }
    Security sec = ctx.getBroadcastState(SECURITIES).get(inv.instrumentId() % ID_MODULUS);
    out.collect(new EnrichedInventory(inv, sec));
  }

  @Override
  public void processBroadcastElement(
      Security sec, Context ctx, Collector<EnrichedInventory> out) throws Exception {
    if (sec == null) return;
    BroadcastState<Long, Security> state = ctx.getBroadcastState(SECURITIES);
    state.put(sec.id(), sec);
  }
}
