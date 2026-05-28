# Market data producers (bond + crypto)

JSON producers that feed the markets Flink example
(`org.agentic.flink.example.markets.{Bond,Crypto}MarketAgentExample`) on a local Kafka cluster.

## Quick start

```bash
# 1. Local Kafka (one-time)
podman compose -f docker-compose-kafka.yml up -d

# 2. Python deps
pip install kafka-python numpy websockets

# 3a. Bond pipeline — three producers feed enrichment, top-N, and best-quote join.
python examples-bin/markets/bond_securities_producer.py &
python examples-bin/markets/bond_inventory_producer.py  &
python examples-bin/markets/bond_trades_producer.py      &

# 3b. OR crypto pipeline — one bridge consuming the public Coinbase WebSocket.
python examples-bin/markets/coinbase_producer.py --products BTC-USD,ETH-USD,SOL-USD

# 4. Build + run the Flink job. Pick the matching example main class.
mvn -DskipTests package
flink run target/agentic-flink-1.0.0-SNAPSHOT.jar \
    org.agentic.flink.example.markets.BondMarketAgentExample
#   or  org.agentic.flink.example.markets.CryptoMarketAgentExample

# 5. (optional) export ANTHROPIC_API_KEY=sk-ant-... to enable the LLM tier in MarketAgentFn.
```

## Topics

| Producer | Topic | Schema |
|---|---|---|
| `bond_inventory_producer.py`  | `fnd-inventory`       | dealer bid/offer quotes (anonymised firms) |
| `bond_securities_producer.py` | `fnd-securities`      | issuer / sector / ratings master |
| `bond_trades_producer.py`     | `fnd-trades`          | execution reports (anonymised "VertexFi" platform) |
| `coinbase_producer.py`        | `coinbase-inventory`  | Coinbase level2 changes mapped into the Inventory shape |
|                                | `coinbase-securities` | one row per subscribed product (BTC-USD etc.) |
|                                | `coinbase-trades`     | Coinbase matches mapped into the Trade shape |

## Anonymisation

The bond producers deliberately do NOT use any real-world firm or platform names. Firm codes
(`NORTH/OMNI/KAPI/ZENI/AXIS/HALO/VERT/QORE`), bond prefixes (`FNDP_*`), and the venue code
(`VXOF/VSNT/VOFF`) are placeholders. Schemas mirror the user's mrkaxis pipeline so the same Flink
stages work on the same field names, but no field carries real-vendor strings.

## Coinbase WebSocket — network access

`coinbase_producer.py` needs outbound access to `wss://ws-feed.exchange.coinbase.com` (no API key
required — the public market-data feed is open). The same JSON shape is published to Kafka, so the
crypto Flink job runs the identical operator graph with different topic names.
