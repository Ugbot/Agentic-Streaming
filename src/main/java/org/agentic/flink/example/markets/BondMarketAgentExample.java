package org.agentic.flink.example.markets;

import org.agentic.flink.example.markets.model.MarketRecords.AlertEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Classic Flink upstream + inline agentic downstream over a (anonymised) fixed-income bond market
 * data feed. Reads three Kafka topics produced by the anonymised Python synthesizers under
 * {@code examples-bin/markets/}:
 *
 * <ul>
 *   <li>{@code fnd-inventory} — dealer bid/offer quotes (5K msg/s when synthesizers run)</li>
 *   <li>{@code fnd-securities} — security master (issuer, sector, ratings)</li>
 *   <li>{@code fnd-trades} — execution reports from the (anonymised) "VertexFi" platform</li>
 * </ul>
 *
 * <p>Pipeline: enrich → top-5 per (instrument, side) → best-quote ⨝ latest trade → windowed
 * features → {@code MarketAgentFn} (band-pass on spread + rolling z-score + Claude adjudication).
 *
 * <p>Run with:
 * <pre>
 *   podman compose -f docker-compose-kafka.yml up -d
 *   python3 examples-bin/markets/bond_securities_producer.py &amp;
 *   python3 examples-bin/markets/bond_inventory_producer.py &amp;
 *   python3 examples-bin/markets/bond_trades_producer.py &amp;
 *   flink run target/agentic-flink-1.0.0-SNAPSHOT.jar \
 *     org.agentic.flink.example.markets.BondMarketAgentExample
 * </pre>
 */
public final class BondMarketAgentExample {

  public static void main(String[] args) throws Exception {
    String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
    MarketPipeline.KafkaConfig kafka =
        new MarketPipeline.KafkaConfig(
            bootstrap, "fnd-inventory", "fnd-securities", "fnd-trades", "bond-market-agent");
    MarketPipeline.AgentConfig agent = MarketPipeline.AgentConfig.defaults();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<AlertEvent> alerts = MarketPipeline.wireFromKafka(env, kafka, agent);
    alerts.print().name("alerts-stdout");

    System.out.println(
        "\n=== Bond market agent (Kafka topics on "
            + bootstrap
            + ", LLM "
            + (agent.anthropicApiKey() == null ? "disabled" : "Claude")
            + ") ===\n");
    env.execute("Bond Market Agent");
  }
}
