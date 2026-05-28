#!/usr/bin/env python3
"""Live Coinbase Exchange WebSocket → Kafka bridge for the crypto markets example.

Subscribes to the public Coinbase WebSocket feed (no auth required) and translates each event into
the SAME JSON shape the bond producers use, so the Java Flink job needs no domain-specific code —
the same operator graph processes both.

Channels & translation:
    * ``level2_batch``       → ``coinbase-inventory``  (one Inventory row per (product, side, price level))
    * ``matches``            → ``coinbase-trades``      (one Trade row per match)
    * (a small static set)   → ``coinbase-securities``  (one Security row per product at startup)

    pip install kafka-python websockets
    python examples-bin/markets/coinbase_producer.py --products BTC-USD,ETH-USD,SOL-USD

Coinbase feed: wss://ws-feed.exchange.coinbase.com  (no API key for public market data).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone

import websockets  # type: ignore
from kafka import KafkaProducer  # type: ignore

import kafka_config

COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"


def _ts_to_millis(ts: str) -> int:
    """Coinbase timestamps look like '2026-05-28T12:34:56.789012Z'."""
    if not ts:
        return int(time.time() * 1000)
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)


def _product_to_instrument(product_id: str) -> int:
    """Deterministic int id from a product string (e.g. BTC-USD → stable int)."""
    return abs(hash(product_id)) % (10**9)


def _emit_security_seed(producer: KafkaProducer, products: list[str]) -> None:
    """Publish a tiny synthetic 'security master' so the Flink enrichment broadcast has rows."""
    topic = kafka_config.TOPIC_CRYPTO_SECURITIES
    for p in products:
        producer.send(
            topic,
            value=dict(
                id=_product_to_instrument(p),
                isin=p,
                cusip=p.replace("-", ""),
                name=p,
                issuer="Coinbase",
                sector="Crypto",
                industry=p.split("-")[0],
                coupon=0.0,
                maturity="2099-12-31",
                fitchRating="NR",
                snpRating="NR",
                moodyRating="NR",
                isInvestmentGrade="N",
                timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            ),
        )
    producer.flush()


def _l2_change_to_inventory(product: str, change: list, ts_ms: int) -> dict:
    """Coinbase level2 change is ['buy'|'sell', '<price>', '<size>']."""
    side_raw, price_s, size_s = change[0], change[1], change[2]
    size = float(size_s)
    return dict(
        companyShortName="COINBASE",
        instrumentId=_product_to_instrument(product),
        side="BID" if side_raw == "buy" else "OFFER",
        price=float(price_s),
        size=int(max(size, 0) * 10_000),  # scale fractional crypto sizes to integers
        spread=0.0,
        level=1,
        tier=1,
        marketSegment="CRYPTO",
        productCD=product,
        quoteType="F",
        action="UPDATE" if size > 0 else "DELETE",
        processedTs=ts_ms,
    )


def _match_to_trade(msg: dict) -> dict:
    """Field names mirror the Java Trade record so the Flink job deserializes consistently."""
    product = msg.get("product_id", "")
    price = float(msg.get("price", 0.0))
    size = float(msg.get("size", 0.0))
    ts_iso = msg.get("time", "")
    try:
        ts_ms = int(datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        ts_ms = int(time.time() * 1000)
    return dict(
        id=int(msg.get("trade_id", 0)),
        isin=product,
        tradeTs=ts_ms,
        side="BUY" if msg.get("side") == "buy" else "SELL",
        dealPrice=price,
        quantity=size,
        yieldVal=0.0,
        iSpread=0.0,
        zSpread=0.0,
        marketSpread=0.0,
        sector="CRYPTO",
        firmPrincipal=msg.get("maker_order_id", "")[:8] or "CBMAKER",
        firmCpty=msg.get("taker_order_id", "")[:8] or "CBTAKER",
        placeOfTrade="COINBASE",
    )


async def run(products: list[str]) -> None:
    producer = KafkaProducer(**kafka_config.producer_kwargs())
    _emit_security_seed(producer, products)
    sub = json.dumps(
        {
            "type": "subscribe",
            "product_ids": products,
            "channels": ["level2_batch", "matches", "heartbeat"],
        }
    )
    print(f"connecting to {COINBASE_WS} for products {products}")
    async with websockets.connect(COINBASE_WS, ping_interval=20) as ws:
        await ws.send(sub)
        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")
            if mtype == "snapshot":
                ts_ms = _ts_to_millis(msg.get("time", ""))
                prod = msg.get("product_id", "")
                for side_key, side_label in (("bids", "buy"), ("asks", "sell")):
                    for level in msg.get(side_key, [])[:25]:  # cap snapshot depth
                        producer.send(
                            kafka_config.TOPIC_CRYPTO_INVENTORY,
                            value=_l2_change_to_inventory(prod, [side_label, level[0], level[1]], ts_ms),
                        )
            elif mtype == "l2update":
                ts_ms = _ts_to_millis(msg.get("time", ""))
                prod = msg.get("product_id", "")
                for change in msg.get("changes", []):
                    producer.send(
                        kafka_config.TOPIC_CRYPTO_INVENTORY,
                        value=_l2_change_to_inventory(prod, change, ts_ms),
                    )
            elif mtype == "match":
                producer.send(kafka_config.TOPIC_CRYPTO_TRADES, value=_match_to_trade(msg))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--products", default="BTC-USD,ETH-USD,SOL-USD")
    args = ap.parse_args()
    products = [p.strip() for p in args.products.split(",") if p.strip()]
    try:
        asyncio.run(run(products))
    except KeyboardInterrupt:
        print("\nstopping")


if __name__ == "__main__":
    main()
