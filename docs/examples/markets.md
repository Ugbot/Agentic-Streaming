# Markets: classic Flink upstream + inline agentic downstream

> **Flink-runtime showcase**.**Kafka + Flink streaming** (Coinbase/bond feeds → agentic enrichment).
> Genuinely streaming-native; not the portable baseline. For the agent that runs unchanged on every
> runtime see [the banking agent on every runtime](banking-everywhere.md).

A single Flink job that wires four classic DataStream operators feeding an inline agentic
operator. Two domain examples share the same operator graph: an **anonymised bond markets**
pipeline and a **live Coinbase crypto** pipeline.

## The composition

```
       inventory ┐
                 ├─▶  EnrichmentFn (broadcast: inventory ⨝ securities)
       securities┘                │
                                  ▼
                          TopNRankerFn (keyed by instrument|side, running top-5 by price)
                                  │
                       ┌──────────┴──────────┐
                       ▼                     ▼
   filter(rank==1)─▶ BestQuoteFn      FeatureAggregatorFn
                       ▲    (keyed by ISIN,            (windowed, per instrument:
   trades──────────────┘     joins latest trade)        spread / depth / counts)
                                                              │
                                                              ▼
                                                MarketAgentFn  ◀── inline AGENTIC
                                          (BandPass on spread
                                         + ZScoreDetector(spread)
                                         + ZScoreDetector.onAttr(volumes)
                                         + Claude LLM tier)
                                                              │
                                                              ▼
                                                     AlertEvent stream
                                                  (decidedBy / verdict /
                                                   fired phases / signals)
```

## Files

- `src/main/java/org/agentic/flink/example/markets/`
  - `MarketPipeline.java`, composes the graph from Inventory/Security/Trade streams
  - `BondMarketAgentExample.java`, main: reads anonymised `fnd-*` topics
  - `CryptoMarketAgentExample.java`, main: reads `coinbase-*` topics
  - `model/MarketRecords.java`. Inventory / Security / Trade / EnrichedInventory / RankedQuote /
    BestQuoteWithTrade / MarketFeatures / AlertEvent
  - `stage/EnrichmentFn.java`, broadcast enrichment
  - `stage/TopNRankerFn.java`, running top-N per (instrument, side)
  - `stage/BestQuoteFn.java`, best-quote ⨝ latest trade (KeyedCoProcessFunction)
  - `stage/FeatureAggregatorFn.java`, windowed aggregation
  - `stage/MarketSignals.java`, pure-function math
  - `stage/MarketAgentFn.java`, inline agentic operator
  - `producer/`. Java producers:
    `BondInventoryProducer`, `BondSecuritiesProducer`, `BondTradesProducer`, `CoinbaseProducer`
    (built-in JDK WebSocket client, no extra deps), plus `MarketProducerSupport`

## Running

```bash
podman network create agentic-flink-network                     # once
podman compose -f docker-compose-markets.yml up -d                # Kafka on localhost:9092, Flink 2.2.1 session cluster (REST on :8081)
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw package -DskipTests                                        # target/agentic-flink-1.0.0-SNAPSHOT-uber.jar
```

Then start the producers in `example.markets.producer` and submit
`org.agentic.flink.example.markets.BondMarketAgentExample` or `CryptoMarketAgentExample` with
`flink run -c <main class> target/agentic-flink-1.0.0-SNAPSHOT-uber.jar`, which needs a Flink 2.2.x
CLI on the PATH. Inside the compose network the job must read Kafka at `kafka:29092`, so set
`KAFKA_BOOTSTRAP=kafka:29092` on the JobManager and TaskManager; the default, `localhost:9092`, fails
there with a metadata timeout.

Prerequisites: JDK 21, the Maven wrapper, Podman for the stack. No API key; `ANTHROPIC_API_KEY`
enables the optional LLM tier and the rule tiers alert without it. The jobs are unbounded streaming
jobs fed from Kafka, so they are submitted to the session cluster rather than run in an embedded
MiniCluster. Alerts print to the TaskManager stdout log.

## What the agentic operator does

`MarketAgentFn` builds a `ScreeningPipeline` in `open()` with:

| Detector | Triggers on |
|---|---|
| `BandPassDetector` | bid-offer spread outside the expected band |
| `ZScoreDetector` (value) | rolling z-score on spread > threshold (window per instrument) |
| `ZScoreDetector.onAttr("totalBidVolume")` | bid-volume z-score (depth collapse / spike) |
| `ZScoreDetector.onAttr("totalOfferVolume")` | offer-volume z-score |

Then the `ScreeningPipeline` escalates flagged windows to Claude (when `ANTHROPIC_API_KEY` is set)
for `ALLOW` / `REVIEW` / `BLOCK`, with auto-block on overwhelming combined risk.

## Why the same job works for crypto

Coinbase has no "security master", so `CoinbaseProducer` publishes a small static seed
(one row per `BTC-USD` etc.) to the broadcast input topic and translates level2 changes + matches
into the same Inventory / Trade JSON shapes the bond pipeline already understands. The Java
operator graph is unchanged.
