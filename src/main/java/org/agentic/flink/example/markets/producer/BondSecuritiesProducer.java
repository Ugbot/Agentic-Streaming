package org.agentic.flink.example.markets.producer;

import org.agentic.flink.example.markets.model.MarketRecords.Security;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Java twin of {@code examples-bin/markets/bond_securities_producer.py}. Publishes a slow stream of
 * security-master rows to the {@code fnd-securities} topic. The Flink job broadcasts these into
 * the inventory enrichment stage.
 *
 * <pre>
 *   java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
 *     org.agentic.flink.example.markets.producer.BondSecuritiesProducer --count 50000 --rate 500
 * </pre>
 */
public final class BondSecuritiesProducer {

  private static final List<String> SECTORS =
      List.of("Financials", "Industrials", "Utilities", "Energy", "Healthcare", "Tech", "Consumer");
  private static final Map<String, List<String>> INDUSTRIES =
      Map.of(
          "Financials", List.of("Banks", "Insurance", "AssetMgmt"),
          "Industrials", List.of("Machinery", "Aerospace", "Logistics"),
          "Utilities", List.of("Electric", "Gas", "Water"),
          "Energy", List.of("Oil", "Renewables"),
          "Healthcare", List.of("Pharma", "Devices"),
          "Tech", List.of("Software", "Semiconductors"),
          "Consumer", List.of("Retail", "Apparel"));
  private static final List<String> FITCH =
      List.of("AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "NR");
  private static final List<String> MOODY =
      List.of("Aaa", "Aa1", "Aa2", "A1", "A2", "A3", "Baa1", "Baa2", "Baa3", "Ba1", "Ba2", "NR");
  private static final java.util.Set<String> IG =
      java.util.Set.of("AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-");

  public static void main(String[] args) throws Exception {
    int count = BondInventoryProducer.parseInt(args, "--count", 50_000);
    int rate = BondInventoryProducer.parseInt(args, "--rate", 500);
    long sleepNanos = Math.max(1_000_000L, 1_000_000_000L / Math.max(rate, 1));

    try (MarketProducerSupport mp =
        new MarketProducerSupport(MarketProducerSupport.defaultBootstrap(), "bond-securities")) {
      System.out.printf(Locale.ROOT,
          "producing %d securities to fnd-securities at ~%d/s%n", count, rate);
      for (int i = 1; i <= count && !Thread.currentThread().isInterrupted(); i++) {
        mp.send("fnd-securities", randomSecurity(i));
        java.util.concurrent.locks.LockSupport.parkNanos(sleepNanos);
      }
    }
  }

  static Security randomSecurity(int id) {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    String sector = SECTORS.get(rng.nextInt(SECTORS.size()));
    String fitch = FITCH.get(rng.nextInt(FITCH.size()));
    String isin = String.format(Locale.ROOT, "FN%010d", id);
    String maturity =
        String.format(
            Locale.ROOT, "203%d-%02d-%02d",
            rng.nextInt(0, 6), 1 + rng.nextInt(9), 10 + rng.nextInt(18));
    return new Security(
        id, isin, String.format(Locale.ROOT, "%09d", id),
        String.format(Locale.ROOT, "FNDP_%05d", id),
        String.format(Locale.ROOT, "FNDP_%05d", id),
        sector, INDUSTRIES.get(sector).get(rng.nextInt(INDUSTRIES.get(sector).size())),
        Math.round(rng.nextDouble(0.005, 0.085) * 10_000.0) / 10_000.0,
        maturity, fitch, FITCH.get(rng.nextInt(FITCH.size())),
        MOODY.get(rng.nextInt(MOODY.size())),
        IG.contains(fitch) ? "Y" : "N");
  }
}
