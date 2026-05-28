package org.agentic.flink.example.markets.producer;

import org.agentic.flink.example.markets.model.MarketRecords.Trade;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Java twin of {@code examples-bin/markets/bond_trades_producer.py}. Synthesizes anonymised trades
 * and publishes JSON to the {@code fnd-trades} topic.
 *
 * <pre>
 *   java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
 *     org.agentic.flink.example.markets.producer.BondTradesProducer --rate 200
 * </pre>
 */
public final class BondTradesProducer {

  private static final List<String> FIRMS =
      List.of("NORTH", "OMNI", "KAPI", "ZENI", "AXIS", "HALO", "VERT", "QORE");
  private static final List<String> SIDES = List.of("BUY", "SELL");
  private static final List<String> PLATFORMS = List.of("VXOF", "VSNT", "VOFF");
  private static final List<String> SECTORS = List.of("CORP", "SOVN", "COVER", "AGCY");

  public static void main(String[] args) throws Exception {
    int rate = BondInventoryProducer.parseInt(args, "--rate", 200);
    long sleepNanos = Math.max(1_000_000L, 1_000_000_000L / Math.max(rate, 1));

    try (MarketProducerSupport mp =
        new MarketProducerSupport(MarketProducerSupport.defaultBootstrap(), "bond-trades")) {
      System.out.printf(Locale.ROOT, "producing trades to fnd-trades at ~%d/s%n", rate);
      long seq = 0;
      while (!Thread.currentThread().isInterrupted()) {
        mp.send("fnd-trades", randomTrade(seq++));
        java.util.concurrent.locks.LockSupport.parkNanos(sleepNanos);
      }
    }
  }

  static Trade randomTrade(long seq) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    long instr = rng.nextLong(10_000_000L, 40_000_000L);
    String isin = String.format(Locale.ROOT, "FN%010d", instr);
    double price = clamp(rng.nextGaussian() * 5.0 + 99.5, 60.0, 130.0);
    double qty = sampleQuantity(rng);
    double yld = clamp(rng.nextGaussian() * 1.5 + 4.25, 0.5, 9.5);
    return new Trade(
        2_100_000_000L + seq, isin, System.currentTimeMillis(),
        SIDES.get(rng.nextInt(SIDES.size())),
        round3(price), qty, round4(yld),
        round2(rng.nextGaussian() * 40 + 150), round2(rng.nextGaussian() * 40 + 145),
        round2(rng.nextGaussian() * 40 + 155),
        SECTORS.get(rng.nextInt(SECTORS.size())),
        FIRMS.get(rng.nextInt(FIRMS.size())),
        FIRMS.get(rng.nextInt(FIRMS.size())),
        PLATFORMS.get(rng.nextInt(PLATFORMS.size())));
  }

  private static double sampleQuantity(ThreadLocalRandom rng) {
    double r = rng.nextDouble();
    if (r < 0.30) return 1_000;
    if (r < 0.60) return 25_000;
    if (r < 0.80) return 100_000;
    if (r < 0.92) return 500_000;
    if (r < 0.98) return 1_000_000;
    return 2_500_000;
  }

  private static double clamp(double v, double lo, double hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  private static double round2(double v) {
    return Math.round(v * 100.0) / 100.0;
  }

  private static double round3(double v) {
    return Math.round(v * 1000.0) / 1000.0;
  }

  private static double round4(double v) {
    return Math.round(v * 10_000.0) / 10_000.0;
  }
}
