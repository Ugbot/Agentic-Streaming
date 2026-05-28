package org.agentic.flink.example.markets;

import org.agentic.flink.channel.KafkaChannel;
import org.agentic.flink.example.markets.model.MarketRecords.AlertEvent;
import org.agentic.flink.example.markets.model.MarketRecords.EnrichedInventory;
import org.agentic.flink.example.markets.model.MarketRecords.Inventory;
import org.agentic.flink.example.markets.model.MarketRecords.RankedQuote;
import org.agentic.flink.example.markets.model.MarketRecords.Security;
import org.agentic.flink.example.markets.model.MarketRecords.Trade;
import org.agentic.flink.example.markets.stage.BestQuoteFn;
import org.agentic.flink.example.markets.stage.EnrichmentFn;
import org.agentic.flink.example.markets.stage.FeatureAggregatorFn;
import org.agentic.flink.example.markets.stage.MarketAgentFn;
import org.agentic.flink.example.markets.stage.TopNRankerFn;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Composes the full market-data Flink graph: classic-Flink upstream (enrich → top-N → best-quote →
 * features) feeding an inline agentic operator (band-pass + z-score + LLM) downstream. Two domain
 * examples — {@link BondMarketAgentExample} and {@link CryptoMarketAgentExample} — both call
 * {@link #wire} with their own Kafka topic configuration.
 *
 * <p>Use {@link #wire} when you have three already-constructed streams (e.g. from
 * {@code env.fromElements} in a test) and {@link #wireFromKafka} when you want stock
 * {@link KafkaSource}s reading newline-delimited JSON.
 */
public final class MarketPipeline {

  private MarketPipeline() {}

  /** Configuration object for the Kafka-backed entry point. */
  public record KafkaConfig(
      String bootstrap,
      String inventoryTopic,
      String securityTopic,
      String tradeTopic,
      String groupId) {}

  /** Tunables for the agentic stage. */
  public record AgentConfig(
      String anthropicApiKey, // nullable
      double spreadBandLow,
      double spreadBandHigh,
      double zThreshold,
      int zWarmup,
      long featureWindowMillis,
      int topN) {

    public static AgentConfig defaults() {
      return new AgentConfig(System.getenv("ANTHROPIC_API_KEY"), 0.0, 50.0, 3.0, 5, 10_000L, 5);
    }
  }

  /** Build the full graph from three already-constructed input streams. */
  public static DataStream<AlertEvent> wire(
      StreamExecutionEnvironment env,
      DataStream<Inventory> inventory,
      DataStream<Security> securities,
      DataStream<Trade> trades,
      AgentConfig cfg) {

    // Stage 1 — broadcast enrichment.
    BroadcastStream<Security> securityBroadcast = securities.broadcast(EnrichmentFn.SECURITIES);
    DataStream<EnrichedInventory> enriched =
        inventory.connect(securityBroadcast).process(new EnrichmentFn()).name("enrich-inventory");

    // Stage 2 — running top-N per (instrument, side).
    DataStream<RankedQuote> ranked =
        enriched
            .keyBy(e -> e.inventory().instrumentId() + "|" + e.inventory().side())
            .process(new TopNRankerFn(cfg.topN()))
            .name("top-" + cfg.topN());

    // Stage 3 — best-quote (rank-1) joined with the latest trade by ISIN. Emitted as a side
    // sink so consumers can subscribe to best-bid/best-offer updates with trade context.
    org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<
            org.agentic.flink.example.markets.model.MarketRecords.BestQuoteWithTrade>
        bestQuoteWithTrade =
            ranked
                .filter(rq -> rq.rank() == 1)
                .keyBy(rq -> rq.quote().isin())
                .connect(trades.keyBy(Trade::isin))
                .process(new BestQuoteFn())
                .name("best-quote-join");
    bestQuoteWithTrade.print().name("best-quote-stdout");

    // Stage 4 — windowed feature aggregation per instrument from the top-N stream (so we see all
    // ranks, not just the best).
    DataStream<org.agentic.flink.example.markets.model.MarketRecords.MarketFeatures> features =
        ranked
            .keyBy(rq -> Long.toString(rq.quote().inventory().instrumentId()))
            .process(new FeatureAggregatorFn(cfg.featureWindowMillis(), cfg.topN()))
            .name("feature-aggregator");

    // Stage 5 — INLINE AGENTIC: band-pass + rolling z-score + LLM.
    return features
        .keyBy(f -> Long.toString(f.instrumentId()))
        .process(
            new MarketAgentFn(
                cfg.anthropicApiKey(),
                cfg.spreadBandLow(),
                cfg.spreadBandHigh(),
                cfg.zThreshold(),
                cfg.zWarmup()))
        .name("market-agent");
  }

  /** Build the graph with stock {@link KafkaSource}s (newline-delimited JSON per record). */
  public static DataStream<AlertEvent> wireFromKafka(
      StreamExecutionEnvironment env, KafkaConfig kafka, AgentConfig agent) {
    DataStream<Inventory> inv = kafkaSource(env, kafka, kafka.inventoryTopic(), Inventory.class);
    DataStream<Security> sec = kafkaSource(env, kafka, kafka.securityTopic(), Security.class);
    DataStream<Trade> trd = kafkaSource(env, kafka, kafka.tradeTopic(), Trade.class);
    return wire(env, inv, sec, trd, agent);
  }

  private static <T> DataStream<T> kafkaSource(
      StreamExecutionEnvironment env, KafkaConfig cfg, String topic, Class<T> type) {
    KafkaSource<T> source =
        KafkaSource.<T>builder()
            .setBootstrapServers(cfg.bootstrap())
            .setTopics(topic)
            .setGroupId(cfg.groupId())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new KafkaChannel.JsonSchema<>(type, TypeInformation.of(type)))
            .build();
    return env.fromSource(
        source,
        WatermarkStrategy.<T>noWatermarks().withIdleness(Duration.ofSeconds(30)),
        "kafka[" + topic + "]");
  }
}
