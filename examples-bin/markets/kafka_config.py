"""Local-Kafka config for the markets example producers.

Targets the broker brought up by ``docker-compose-kafka.yml`` at ``localhost:9092``.
No SASL / no TLS — these are dev producers feeding the on-host Flink job. Set
``KAFKA_BOOTSTRAP`` if you point the cluster elsewhere.

Topic names are deliberately anonymised: ``fnd-*`` for the fintech (bond) side
and ``coinbase-*`` for the crypto side.
"""

from __future__ import annotations

import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# Anonymised fintech bond pipeline topics.
TOPIC_BOND_INVENTORY = "fnd-inventory"
TOPIC_BOND_SECURITIES = "fnd-securities"
TOPIC_BOND_TRADES = "fnd-trades"

# Crypto pipeline topics (Coinbase live feed).
TOPIC_CRYPTO_INVENTORY = "coinbase-inventory"
TOPIC_CRYPTO_SECURITIES = "coinbase-securities"
TOPIC_CRYPTO_TRADES = "coinbase-trades"


def producer_kwargs() -> dict:
    """kafka-python KafkaProducer kwargs for local plaintext Kafka."""
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": 1,
        "linger_ms": 10,
        "compression_type": "gzip",
        "value_serializer": lambda v: __import__("json").dumps(v).encode("utf-8"),
    }
