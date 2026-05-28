# Markets — classic Flink upstream + inline agentic downstream

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
  - `MarketPipeline.java` — composes the graph from Inventory/Security/Trade streams
  - `BondMarketAgentExample.java` — main: reads anonymised `fnd-*` topics
  - `CryptoMarketAgentExample.java` — main: reads `coinbase-*` topics
  - `model/MarketRecords.java` — Inventory / Security / Trade / EnrichedInventory / RankedQuote /
    BestQuoteWithTrade / MarketFeatures / AlertEvent
  - `stage/EnrichmentFn.java` — broadcast enrichment
  - `stage/TopNRankerFn.java` — running top-N per (instrument, side)
  - `stage/BestQuoteFn.java` — best-quote ⨝ latest trade (KeyedCoProcessFunction)
  - `stage/FeatureAggregatorFn.java` — windowed aggregation
  - `stage/MarketSignals.java` — pure-function math
  - `stage/MarketAgentFn.java` — inline agentic operator
- `examples-bin/markets/` — Python producers (anonymised + Coinbase)
- `examples-bin/run-bond-market.sh`, `examples-bin/run-crypto-market.sh`
- `notebooks/07_market_depth_agents.ipynb` — drives the agentic operator on a deterministic
  feature stream so the funnel is demonstrable without Kafka

## Running

See `examples-bin/markets/README.md` for the full quick-start. The Flink job runs via `flink run`
(not `mvn exec:java`) — the streaming MiniCluster classpath is incomplete under exec:java in this
repo, same limitation as the other streaming examples.

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
