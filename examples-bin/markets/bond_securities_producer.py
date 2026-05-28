#!/usr/bin/env python3
"""Anonymised security-master producer (issuer, sector, ratings) for the bond markets example.

Writes a slow, low-rate stream of records to ``fnd-securities`` — the Flink job broadcasts these
into the inventory enrichment stage. Fields mirror the user's mrkaxis ``kafka_security_producer.py``
schema with anonymised issuer/sector names.

    pip install kafka-python numpy
    python examples-bin/markets/bond_securities_producer.py
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer  # type: ignore

import kafka_config

rng = np.random.default_rng(2026)

SECTORS = ["Financials", "Industrials", "Utilities", "Energy", "Healthcare", "Tech", "Consumer"]
INDUSTRIES = {
    "Financials": ["Banks", "Insurance", "AssetMgmt"],
    "Industrials": ["Machinery", "Aerospace", "Logistics"],
    "Utilities": ["Electric", "Gas", "Water"],
    "Energy": ["Oil", "Renewables"],
    "Healthcare": ["Pharma", "Devices"],
    "Tech": ["Software", "Semiconductors"],
    "Consumer": ["Retail", "Apparel"],
}
FITCH = ["AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "NR"]
SNP = FITCH
MOODY = ["Aaa", "Aa1", "Aa2", "A1", "A2", "A3", "Baa1", "Baa2", "Baa3", "Ba1", "Ba2", "NR"]
COUNTRIES = ["US", "UK", "DE", "FR", "CA"]
CCY = ["USD", "EUR", "GBP", "CHF"]


def gen_security(idx: int) -> dict:
    sec = rng.choice(SECTORS)
    rating = rng.choice(FITCH)
    investment_grade = "Y" if rating in {"AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-"} else "N"
    return dict(
        id=int(idx),
        isin=f"FN{idx:010d}",
        cusip=f"{idx:09d}",
        name=f"FNDP_{idx:05d}",
        issuer=f"FNDP_{idx:05d}",
        sector=str(sec),
        industry=str(rng.choice(INDUSTRIES[sec])),
        coupon=round(float(rng.uniform(0.005, 0.085)), 4),
        maturity=f"203{rng.integers(0, 6)}-0{rng.integers(1, 10)}-{rng.integers(10, 28):02d}",
        fitchRating=str(rating),
        snpRating=str(rng.choice(SNP)),
        moodyRating=str(rng.choice(MOODY)),
        isInvestmentGrade=investment_grade,
        callable_="Y" if rng.random() < 0.3 else "N",
        paymentFrequency=str(rng.choice(["Q", "S", "A"])),
        countryCode=str(rng.choice(COUNTRIES)),
        currency=str(rng.choice(CCY)),
        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=50_000, help="number of securities to publish")
    ap.add_argument("--rate", type=float, default=500, help="rows per second")
    args = ap.parse_args()

    producer = KafkaProducer(**kafka_config.producer_kwargs())
    topic = kafka_config.TOPIC_BOND_SECURITIES
    interval = 1.0 / max(args.rate, 1)
    print(f"producing {args.count} securities to '{topic}' at ~{args.rate}/s")
    try:
        for i in range(1, args.count + 1):
            producer.send(topic, value=gen_security(i))
            if i % 1000 == 0:
                print(f"  sent {i} securities")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
