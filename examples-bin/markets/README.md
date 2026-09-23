# Market data producers (bond + crypto)

JSON producers that feed the markets Flink example
(`org.agentic.flink.example.markets.{Bond,Crypto}MarketAgentExample`) on a local Kafka cluster.

Each producer ships in **two flavours** with identical wire format (the canonical
Java-record JSON shape): a Python script that's quickest to launch and a Java main class that
runs from the shaded jar with no extra dependencies. Pick whichever fits your environment.

## Quick start

```bash
# 1. Local Kafka plus a Flink 2.2.1 session cluster in Podman (one-time)
bash examples-bin/run-markets-stack.sh
# 2. Build the shaded jar (needed for the Java producers and for the Flink job). Install the
#    shared core first; it is outside the root reactor.
./mvnw -f ports/jagentic-core/pom.xml install -DskipTests
./mvnw -DskipTests package
```

`bash examples-bin/run-bond-market.sh` and `bash examples-bin/run-crypto-market.sh` check the
prerequisites (JDK 21, python3, Kafka on `localhost:9092`), build the jar when it is missing and
print the exact producer and `flink run` commands below; add `--submit` to run `flink run`.

### Bond pipeline: Python flavour

```bash
pip install kafka-python numpy websockets
python3 examples-bin/markets/bond_securities_producer.py &
python3 examples-bin/markets/bond_inventory_producer.py  &
python3 examples-bin/markets/bond_trades_producer.py      &
```

### Bond pipeline: Java flavour

```bash
JAR=target/agentic-flink-1.0.0-SNAPSHOT-uber.jar
java -cp "$JAR" org.agentic.flink.example.markets.producer.BondSecuritiesProducer --count 50000 --rate 500 &
java -cp "$JAR" org.agentic.flink.example.markets.producer.BondInventoryProducer  --rate 500 --batch 50 &
java -cp "$JAR" org.agentic.flink.example.markets.producer.BondTradesProducer     --rate 200 &
```

### Crypto pipeline (needs internet)

```bash
# Python:
python3 examples-bin/markets/coinbase_producer.py --products BTC-USD,ETH-USD,SOL-USD
# Java:
java -cp "$JAR" org.agentic.flink.example.markets.producer.CoinbaseProducer --products BTC-USD,ETH-USD,SOL-USD
```

### Submit the Flink job

```bash
JAR=target/agentic-flink-1.0.0-SNAPSHOT-uber.jar
flink run -c org.agentic.flink.example.markets.BondMarketAgentExample "$JAR"
# or
flink run -c org.agentic.flink.example.markets.CryptoMarketAgentExample "$JAR"
# Optional: an Anthropic key enables the LLM tier in MarketAgentFn; without it the rule tiers still alert.
export ANTHROPIC_API_KEY=...
```

The `flink` CLI must be a 2.2.x build (the session cluster from the compose file is Flink
2.2.1). Without the CLI, upload the jar to the REST API on `http://localhost:8081` as shown by
`bash examples-bin/run-session-cluster.sh`.

## Topics

| Producer | Topic | Schema |
|---|---|---|
| `BondInventoryProducer` / `bond_inventory_producer.py`   | `fnd-inventory`       | dealer bid/offer quotes |
| `BondSecuritiesProducer` / `bond_securities_producer.py` | `fnd-securities`      | issuer / sector / ratings master |
| `BondTradesProducer` / `bond_trades_producer.py`         | `fnd-trades`          | execution reports |
| `CoinbaseProducer` / `coinbase_producer.py`              | `coinbase-inventory`  | Coinbase level2 changes mapped into Inventory |
|                                                          | `coinbase-securities` | one row per subscribed product (BTC-USD etc.) |
|                                                          | `coinbase-trades`     | Coinbase matches mapped into Trade |

Both flavours emit the **same JSON shape** - the keys match the Java records exactly
(`org.agentic.flink.example.markets.model.MarketRecords.{Inventory,Security,Trade}`). The Flink
job's deserializer (`KafkaChannel.JsonSchema`) ignores unknown extras, so adding fields on the
producer side is non-breaking.

## Anonymisation

The bond producers deliberately do NOT use any real-world firm or platform names. Firm codes
(`NORTH/OMNI/KAPI/ZENI/AXIS/HALO/VERT/QORE`), bond prefixes (`FNDP_*`), and the venue code
(`VXOF/VSNT/VOFF`) are placeholders. Schemas mirror the user's mrkaxis pipeline so the same Flink
stages work on the same field names, but no field carries real-vendor strings.

## Coinbase WebSocket - network access

`coinbase_producer.py` needs outbound access to `wss://ws-feed.exchange.coinbase.com` (no API key
required - the public market-data feed is open). The same JSON shape is published to Kafka, so the
crypto Flink job runs the identical operator graph with different topic names.
