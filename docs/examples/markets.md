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
  - `producer/`. Java producers (same wire format as the Python flavour):
    `BondInventoryProducer`, `BondSecuritiesProducer`, `BondTradesProducer`, `CoinbaseProducer`
    (built-in JDK WebSocket client, no extra deps), plus `MarketProducerSupport`
- `examples-bin/markets/`. Python producers (anonymised + Coinbase)
- `examples-bin/run-markets-stack.sh`, `examples-bin/run-bond-market.sh`,
  `examples-bin/run-crypto-market.sh`
- `notebooks/07_market_depth_agents.ipynb`, drives the agentic operator on a deterministic
  feature stream so the funnel is demonstrable without Kafka

## Running

```bash
bash examples-bin/run-markets-stack.sh     # Kafka on localhost:9092 plus a Flink 2.2.1 session cluster (REST on :8081) in Podman
bash examples-bin/run-bond-market.sh       # checks prerequisites, builds target/agentic-flink-1.0.0-SNAPSHOT-uber.jar, prints the commands
bash examples-bin/run-crypto-market.sh     # same for the Coinbase feeds (outbound internet to wss://ws-feed.exchange.coinbase.com)
```

Prerequisites: JDK 21, the Maven wrapper, python3 (for the Python producers; `pip install
kafka-python numpy websockets`), Podman for the stack. No API key; `ANTHROPIC_API_KEY` enables
the optional LLM tier and the rule tiers alert without it. Both wrappers stop with an exact
message when Kafka is not listening. They print the producer commands and
`flink run -c <main class> "$JAR"`; with `--submit` they also run `flink run`, which needs a
Flink 2.2.x CLI on the PATH. The jobs are unbounded streaming jobs fed from Kafka, so they are
submitted to the session cluster rather than run in an embedded MiniCluster. Alerts print to
the TaskManager stdout log. `examples-bin/markets/README.md` lists the Java producer flavour
and the Kafka topics.

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

Coinbase has no "security master", so `coinbase_producer.py` publishes a small static seed
(one row per `BTC-USD` etc.) to the broadcast input topic and translates level2 changes + matches
into the same Inventory / Trade JSON shapes the bond pipeline already understands. The Java
operator graph is unchanged.
