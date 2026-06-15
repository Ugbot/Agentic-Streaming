# Market data producers (bond + crypto)

JSON producers that feed the markets Flink example
(`org.agentic.flink.example.markets.{Bond,Crypto}MarketAgentExample`) on a local Kafka cluster.

Each producer ships in **two flavours** with identical wire format (the canonical
Java-record JSON shape): a Python script that's quickest to launch and a Java main class that
runs from the shaded jar with no extra dependencies. Pick whichever fits your environment.

## Quick start

```bash
# 1. Local Kafka (one-time)
podman compose -f docker-compose-kafka.yml up -d
# 2. Build the shaded jar (only needed for the Java producers).
mvn -DskipTests package
```

### Bond pipeline — Python flavour

```bash
pip install kafka-python numpy websockets
python examples-bin/markets/bond_securities_producer.py &
python examples-bin/markets/bond_inventory_producer.py  &
python examples-bin/markets/bond_trades_producer.py      &
```

### Bond pipeline — Java flavour

```bash
JAR=target/agentic-flink-1.0.0-SNAPSHOT-uber.jar
java -cp "$JAR" org.agentic.flink.example.markets.producer.BondSecuritiesProducer --count 50000 --rate 500 &
java -cp "$JAR" org.agentic.flink.example.markets.producer.BondInventoryProducer  --rate 500 --batch 50 &
java -cp "$JAR" org.agentic.flink.example.markets.producer.BondTradesProducer     --rate 200 &
```

### Crypto pipeline (needs internet)

```bash
# Python:
python examples-bin/markets/coinbase_producer.py --products BTC-USD,ETH-USD,SOL-USD

### Submit the Flink job

```bash
flink run "$JAR" org.agentic.flink.example.markets.BondMarketAgentExample
# or
flink run "$JAR" org.agentic.flink.example.markets.CryptoMarketAgentExample
# Optional — enables the LLM tier in MarketAgentFn:
export ANTHROPIC_API_KEY=sk-ant-...
```

## Topics

| Producer | Topic | Schema |
|---|---|---|
| `BondInventoryProducer` / `bond_inventory_producer.py`   | `fnd-inventory`       | dealer bid/offer quotes |
| `BondSecuritiesProducer` / `bond_securities_producer.py` | `fnd-securities`      | issuer / sector / ratings master |
| `BondTradesProducer` / `bond_trades_producer.py`         | `fnd-trades`          | execution reports |
| `CoinbaseProducer` / `coinbase_producer.py`              | `coinbase-inventory`  | Coinbase level2 changes mapped into Inventory |
|                                                          | `coinbase-securities` | one row per subscribed product (BTC-USD etc.) |
|                                                          | `coinbase-trades`     | Coinbase matches mapped into Trade |

Both flavours emit the **same JSON shape** — the keys match the Java records exactly
(`org.agentic.flink.example.markets.model.MarketRecords.{Inventory,Security,Trade}`). The Flink
job's deserializer (`KafkaChannel.JsonSchema`) ignores unknown extras, so adding fields on the
producer side is non-breaking.

## Anonymisation

The bond producers deliberately do NOT use any real-world firm or platform names. Firm codes
(`NORTH/OMNI/KAPI/ZENI/AXIS/HALO/VERT/QORE`), bond prefixes (`FNDP_*`), and the venue code
(`VXOF/VSNT/VOFF`) are placeholders. Schemas mirror the user's mrkaxis pipeline so the same Flink
stages work on the same field names, but no field carries real-vendor strings.

## Coinbase WebSocket — network access

`coinbase_producer.py` needs outbound access to `wss://ws-feed.exchange.coinbase.com` (no API key
required — the public market-data feed is open). The same JSON shape is published to Kafka, so the
crypto Flink job runs the identical operator graph with different topic names.
