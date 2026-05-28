#!/usr/bin/env python3
"""Anonymised dealer-inventory bid/offer producer for the bond markets example.

Mirrors the field shapes of the user's mrkaxis ``kafka_inventory_producer.py`` — but with
**anonymised** firm codes / market segments and no Aiven SASL config. Writes JSON rows to
``fnd-inventory`` on the local broker brought up by ``docker-compose-kafka.yml``.

Fields:
    companyShortName, instrumentId, side, price, size, spread, level, tier,
    marketSegment, productCD, quoteType, action, processedTs

Run with:
    pip install kafka-python numpy
    python examples-bin/markets/bond_inventory_producer.py
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer  # type: ignore

import kafka_config

SEED = 42
rng = np.random.default_rng(SEED)

# --- Anonymised pools (different from the source pipeline) -------------------
# Firm codes (replacing JPMG / CITI / BOFA / MSCO / UBSW etc.)
DEALER_POOL = np.array(
    [f"{p}{i:02d}_{cc}" for p in ("NORTH", "OMNI", "KAPI", "ZENI", "AXIS", "HALO", "VERT", "QORE")
     for i in range(1, 6) for cc in ("US", "UK")]
)
SIDES = np.array(["BID", "OFFER"])
ACTIONS = np.array(["UPDATE", "DELETE"])
QUOTE_TYPES = np.array(["I", "F"])
TIERS = np.array([1, 2, 3, 4])
MARKET_SEGMENTS = np.array(["IG", "AA", "HY"])   # investment-grade / AA-tier / high-yield
PRODUCT_CDS = np.array(["FNDIG", "FNDAA", "FNDSC", "FNDHS", "FNDHY", "FNDPG"])

LEVEL_PROBS = np.array([0.15, 0.20, 0.35, 0.20, 0.10])
LEVELS = np.array([1, 2, 3, 4, 5])

# Price anchors per segment (mid, std).
SEG_ANCHOR = {"IG": (110.0, 8.0), "AA": (112.0, 7.0), "HY": (92.0, 10.0)}
SEG_BOUNDS = {"IG": (85.0, 140.0), "AA": (90.0, 140.0), "HY": (60.0, 120.0)}


def gen_sizes(n: int) -> np.ndarray:
    bucket = rng.choice(
        ["tiny", "small", "med", "big", "huge"], size=n, p=[0.25, 0.35, 0.25, 0.12, 0.03])
    out = np.empty(n, dtype=np.int64)
    out[bucket == "tiny"] = rng.integers(1, 20, size=(bucket == "tiny").sum())
    out[bucket == "small"] = rng.integers(20, 250, size=(bucket == "small").sum())
    out[bucket == "med"] = rng.integers(250, 1200, size=(bucket == "med").sum())
    out[bucket == "big"] = rng.choice([1000, 2000, 5000], size=(bucket == "big").sum())
    out[bucket == "huge"] = rng.choice([10000, 15000], size=(bucket == "huge").sum())
    return out


def gen_prices(n: int, segs: np.ndarray, sides: np.ndarray) -> np.ndarray:
    out = np.zeros(n, dtype=float)
    for seg in np.unique(segs):
        idx = np.where(segs == seg)[0]
        mean, std = SEG_ANCHOR[seg]
        nudge = np.where(sides[idx] == "BID", -0.6, 0.6)
        out[idx] = rng.normal(mean + nudge, std)
    lo = np.vectorize(lambda s: SEG_BOUNDS[s][0])(segs)
    hi = np.vectorize(lambda s: SEG_BOUNDS[s][1])(segs)
    return np.round(np.clip(out, lo, hi), 3)


def gen_batch(batch: int) -> list[dict]:
    now = datetime.now(timezone.utc)
    iso = now.isoformat().replace("+00:00", "Z")
    companies = rng.choice(DEALER_POOL, size=batch)
    sides = rng.choice(SIDES, size=batch)
    segs = rng.choice(MARKET_SEGMENTS, size=batch)
    instruments = rng.integers(10_000_000, 40_000_000, size=batch, dtype=np.int64)
    prices = gen_prices(batch, segs, sides)
    sizes = gen_sizes(batch)
    spreads = np.where(rng.random(batch) < 0.15, rng.normal(120, 30, size=batch), 0.0).clip(min=0)
    levels = rng.choice(LEVELS, size=batch, p=LEVEL_PROBS)
    tiers = rng.choice(TIERS, size=batch)
    products = rng.choice(PRODUCT_CDS, size=batch)
    quote_types = rng.choice(QUOTE_TYPES, size=batch)
    actions = rng.choice(ACTIONS, size=batch, p=[0.97, 0.03])

    rows = []
    for i in range(batch):
        rows.append(
            dict(
                companyShortName=str(companies[i]),
                instrumentId=int(instruments[i]),
                side=str(sides[i]),
                price=float(prices[i]),
                size=int(sizes[i]),
                spread=float(spreads[i]),
                level=int(levels[i]),
                tier=int(tiers[i]),
                marketSegment=str(segs[i]),
                productCD=str(products[i]),
                quoteType=str(quote_types[i]),
                action=str(actions[i]),
                processedTs=iso,
            )
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=500, help="rows per second")
    ap.add_argument("--batch", type=int, default=50, help="rows per Kafka send batch")
    args = ap.parse_args()

    producer = KafkaProducer(**kafka_config.producer_kwargs())
    topic = kafka_config.TOPIC_BOND_INVENTORY
    interval = args.batch / max(args.rate, 1)
    sent = 0
    started = time.time()
    print(f"producing {args.rate} rows/s to '{topic}' (batch={args.batch})")
    try:
        while True:
            for row in gen_batch(args.batch):
                producer.send(topic, value=row)
            sent += args.batch
            if sent % (args.batch * 20) == 0:
                rate = sent / max(time.time() - started, 1e-6)
                print(f"  sent {sent} rows; effective {rate:.0f}/s")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
