#!/usr/bin/env python3
"""Anonymised trade execution producer for the bond markets example.

Mirrors the user's mrkaxis ``kafka_trades_producer.py`` schema (TRAX → "VertexFi") with
anonymised counterparty / firm codes. Writes JSON rows to ``fnd-trades``.

    pip install kafka-python numpy
    python examples-bin/markets/bond_trades_producer.py
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer  # type: ignore

import kafka_config

rng = np.random.default_rng(123)

# Anonymised firm IDs and roles.
FIRMS = ["NORTH", "OMNI", "KAPI", "ZENI", "AXIS", "HALO", "VERT", "QORE"]
FIRM_ROLES = ["BRKR-DLR", "BUY-SIDE", "BANK", "IDB"]
DENOMS = ["USD", "EUR", "GBP"]
SIDES = ["BUYR", "SELL"]
PLATFORMS = ["VXOF", "VSNT", "VOFF"]   # "VertexFi" venue codes (replacing TRAX)
TRADE_QUALITY = ["NPM", "PMT", "APA-NON-PUB"]
SECTORS = ["CORP", "SOVN", "COVER", "AGCY"]


def gen_trade(seed_id: int) -> dict:
    qty = float(rng.choice([1_000, 25_000, 100_000, 500_000, 1_000_000, 2_500_000],
                           p=[0.30, 0.30, 0.20, 0.12, 0.06, 0.02]))
    price = float(np.clip(rng.normal(99.5, 5.0), 60.0, 130.0))
    yld = float(np.clip(rng.normal(4.25, 1.5), 0.5, 9.5))
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    instr = rng.integers(10_000_000, 40_000_000)
    return dict(
        id=int(2_100_000_000 + seed_id),
        icmainstrumentidentifierisin=f"FN{instr:010d}",
        tradedateandtime=now,
        reporteddatetime=now,
        purchasesellindicator=str(rng.choice(SIDES)),
        dealprice=round(price, 3),
        quantityoffinancialinstrument=qty,
        quantityineuro=round(qty * 0.98, 2),
        quantityinusd=round(qty * 1.07, 2),
        denomination=str(rng.choice(DENOMS)),
        yield_=round(yld, 4),
        ispread=round(float(rng.normal(150.0, 40.0)), 2),
        zspread=round(float(rng.normal(145.0, 40.0)), 2),
        marketspread=round(float(rng.normal(155.0, 40.0)), 2),
        counterpartycode=f"LEI_{rng.integers(10**18, 10**19)}",
        firm_id_principal=str(rng.choice(FIRMS)),
        firm_role_principal=str(rng.choice(FIRM_ROLES)),
        firm_id_cpty=str(rng.choice(FIRMS)),
        firm_role_cpty=str(rng.choice(FIRM_ROLES)),
        placeoftrade=str(rng.choice(PLATFORMS)),
        trade_quality=str(rng.choice(TRADE_QUALITY)),
        sector=str(rng.choice(SECTORS)),
        trade_pair_id=int(rng.integers(100_000_000_000, 999_999_999_999)),
        ccyeurfxrate=0.98,
        ccyusdfxrate=1.07,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=200, help="trades per second")
    args = ap.parse_args()

    producer = KafkaProducer(**kafka_config.producer_kwargs())
    topic = kafka_config.TOPIC_BOND_TRADES
    interval = 1.0 / max(args.rate, 1)
    print(f"producing trades to '{topic}' at ~{args.rate}/s")
    sent = 0
    try:
        while True:
            producer.send(topic, value=gen_trade(sent))
            sent += 1
            if sent % 500 == 0:
                print(f"  sent {sent} trades")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
