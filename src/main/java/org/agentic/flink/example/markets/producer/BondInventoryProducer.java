package org.agentic.flink.example.markets.producer;

import org.agentic.flink.example.markets.model.MarketRecords.Inventory;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Java twin of {@code examples-bin/markets/bond_inventory_producer.py}. Synthesizes anonymised
 * dealer bid/offer quotes and publishes JSON to the {@code fnd-inventory} Kafka topic.
 *
 * <p>Run with:
 * <pre>
 *   mvn -DskipTests package
 *   java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
 *     org.agentic.flink.example.markets.producer.BondInventoryProducer --rate 500
 * </pre>
 */
public final class BondInventoryProducer {

  private static final List<String> DEALER_POOL = buildDealerPool();
  private static final List<String> SIDES = List.of("BID", "OFFER");
  private static final List<String> ACTIONS = List.of("UPDATE", "UPDATE", "UPDATE", "DELETE");
  private static final List<String> QUOTE_TYPES = List.of("I", "F");
  private static final List<String> MARKET_SEGMENTS = List.of("IG", "AA", "HY");
  private static final List<String> PRODUCT_CDS =
      List.of("FNDIG", "FNDAA", "FNDSC", "FNDHS", "FNDHY", "FNDPG");
  private static final int[] LEVEL_CDF = {15, 35, 70, 90, 100}; // probs 0.15/0.20/0.35/0.20/0.10 cumulative
  private static final Map<String, double[]> SEG_ANCHOR =
      Map.of("IG", new double[] {110, 8}, "AA", new double[] {112, 7}, "HY", new double[] {92, 10});
  private static final Map<String, double[]> SEG_BOUNDS =
      Map.of("IG", new double[] {85, 140}, "AA", new double[] {90, 140}, "HY", new double[] {60, 120});

  public static void main(String[] args) throws Exception {
    int rate = parseInt(args, "--rate", 500);
    int batch = parseInt(args, "--batch", 50);
    long sleepNanos = Math.max(1_000_000L, 1_000_000_000L * batch / Math.max(rate, 1));

    try (MarketProducerSupport mp =
        new MarketProducerSupport(MarketProducerSupport.defaultBootstrap(), "bond-inventory")) {
      System.out.printf(Locale.ROOT,
          "producing ~%d rows/s to fnd-inventory (batch %d)%n", rate, batch);
      while (!Thread.currentThread().isInterrupted()) {
        for (int i = 0; i < batch; i++) {
          mp.send("fnd-inventory", randomRow());
        }
        LockSupport.parkNanos(sleepNanos);
      }
    }
  }

  static Inventory randomRow() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    String segment = MARKET_SEGMENTS.get(rng.nextInt(MARKET_SEGMENTS.size()));
    String side = SIDES.get(rng.nextInt(SIDES.size()));
    double[] anchor = SEG_ANCHOR.get(segment);
    double[] bounds = SEG_BOUNDS.get(segment);
    double nudge = "BID".equals(side) ? -0.6 : 0.6;
    double price = clamp(round3(anchor[0] + nudge + rng.nextGaussian() * anchor[1]), bounds[0], bounds[1]);
    long instrumentId = rng.nextLong(10_000_000L, 40_000_000L);
    long size = sizeBucket(rng);
    double spread = rng.nextDouble() < 0.15 ? Math.max(0, rng.nextGaussian() * 30 + 120) : 0.0;
    int level = sampleLevel(rng);
    int tier = 1 + rng.nextInt(4);
    String productCD = PRODUCT_CDS.get(rng.nextInt(PRODUCT_CDS.size()));
    String quoteType = QUOTE_TYPES.get(rng.nextInt(QUOTE_TYPES.size()));
    String action = ACTIONS.get(rng.nextInt(ACTIONS.size()));
    return new Inventory(
        DEALER_POOL.get(rng.nextInt(DEALER_POOL.size())),
        instrumentId, side, price, size, spread, level, tier, segment,
        productCD, quoteType, action, System.currentTimeMillis());
  }

  private static long sizeBucket(ThreadLocalRandom rng) {
    double r = rng.nextDouble();
    if (r < 0.25) return rng.nextInt(1, 20);
    if (r < 0.60) return rng.nextInt(20, 250);
    if (r < 0.85) return rng.nextInt(250, 1200);
    if (r < 0.97) return 1000L + 1000L * rng.nextInt(0, 5);
    return 10_000L + 5_000L * rng.nextInt(0, 2);
  }

  private static int sampleLevel(ThreadLocalRandom rng) {
    int r = rng.nextInt(100);
    for (int i = 0; i < LEVEL_CDF.length; i++) if (r < LEVEL_CDF[i]) return i + 1;
    return 5;
  }

  private static double round3(double v) {
    return Math.round(v * 1000.0) / 1000.0;
  }

  private static double clamp(double v, double lo, double hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  private static List<String> buildDealerPool() {
    String[] prefixes = {"NORTH", "OMNI", "KAPI", "ZENI", "AXIS", "HALO", "VERT", "QORE"};
    String[] countries = {"US", "UK"};
    java.util.ArrayList<String> out = new java.util.ArrayList<>(prefixes.length * 5 * countries.length);
    for (String p : prefixes) {
      for (int i = 1; i <= 5; i++) {
        for (String c : countries) {
          out.add(p + String.format(Locale.ROOT, "%02d", i) + "_" + c);
        }
      }
    }
    return List.copyOf(out);
  }

  static int parseInt(String[] args, String flag, int def) {
    for (int i = 0; i + 1 < args.length; i++) {
      if (flag.equals(args[i])) {
        try {
          return Integer.parseInt(args[i + 1]);
        } catch (NumberFormatException ignore) {
          // keep default
        }
      }
    }
    return def;
  }

  /** Tiny shim to avoid bringing in java.util.concurrent.locks just for one parkNanos. */
  static final class LockSupport {
    static void parkNanos(long n) {
      java.util.concurrent.locks.LockSupport.parkNanos(n);
    }
  }
}
