package org.agentic.flink.example.markets;

import org.agentic.flink.example.markets.model.MarketRecords.AlertEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The same classic-Flink-then-agentic graph as {@link BondMarketAgentExample}, but pointed at the
 * live Coinbase market data feed (via the Python bridge at
 * {@code examples-bin/markets/coinbase_producer.py}, which subscribes to the public Coinbase
 * WebSocket and writes per-product bid/offer levels + trades to Kafka in the same JSON shape).
 *
 * <p>Crypto has no security master, so the {@code coinbase-securities} topic carries a small
 * static product list (one row per BTC-USD, ETH-USD, SOL-USD …) and is broadcast like the bond
 * security master — the pipeline operator graph is unchanged.
 *
 * <p>Run with:
 * <pre>
 *   podman compose -f docker-compose-kafka.yml up -d
 *   python3 examples-bin/markets/coinbase_producer.py &amp;   # requires internet
 *   flink run target/agentic-flink-1.0.0-SNAPSHOT.jar \
 *     org.agentic.flink.example.markets.CryptoMarketAgentExample
 * </pre>
 */
public final class CryptoMarketAgentExample {

  public static void main(String[] args) throws Exception {
    String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
    MarketPipeline.KafkaConfig kafka =
        new MarketPipeline.KafkaConfig(
            bootstrap,
            "coinbase-inventory",
            "coinbase-securities",
            "coinbase-trades",
            "crypto-market-agent");
    // Crypto: tighter spread band, shorter z-score warmup, smaller feature window — the feed is
    // continuous and high-rate compared to the bond producers.
    MarketPipeline.AgentConfig agent =
        new MarketPipeline.AgentConfig(
            System.getenv("ANTHROPIC_API_KEY"),
            0.0, // spreadBandLow
            100.0, // spreadBandHigh (price units)
            2.5, // zThreshold
            4, // zWarmup
            5_000L, // featureWindowMillis (5s)
            5);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<AlertEvent> alerts = MarketPipeline.wireFromKafka(env, kafka, agent);
    alerts.print().name("alerts-stdout");

    System.out.println(
        "\n=== Crypto market agent (Coinbase via "
            + bootstrap
            + ", LLM "
            + (agent.anthropicApiKey() == null ? "disabled" : "Claude")
            + ") ===\n");
    env.execute("Crypto Market Agent");
  }
}
