"""Long-term store SPI — conversation resumption + a per-user fact archive, the portable
analogue of the Flink ``LongTermMemoryStore``. ``InMemoryLongTermStore`` is the default;
``PostgresLongTermStore`` is the real reference impl (persists across process restarts so
a conversation can resume and facts survive).
"""

from __future__ import annotations

from typing import Dict, List, Optional, Protocol, Tuple


class LongTermStore(Protocol):
    def save_turn(self, conversation_id: str, user_id: str, role: str, content: str) -> None: ...

    def load_history(self, conversation_id: str) -> List[Tuple[str, str]]: ...  # [(role, content)]

    def save_fact(self, user_id: str, key: str, value: str) -> None: ...

    def facts(self, user_id: str) -> Dict[str, str]: ...

    def conversations_for_user(self, user_id: str) -> List[str]: ...


class InMemoryLongTermStore:
    def __init__(self) -> None:
        self._turns: Dict[str, List[Tuple[str, str]]] = {}
        self._owner: Dict[str, str] = {}
        self._facts: Dict[str, Dict[str, str]] = {}

    def save_turn(self, conversation_id, user_id, role, content) -> None:
        self._turns.setdefault(conversation_id, []).append((role, content))
        self._owner[conversation_id] = user_id

    def load_history(self, conversation_id) -> List[Tuple[str, str]]:
        return list(self._turns.get(conversation_id, []))

    def save_fact(self, user_id, key, value) -> None:
        self._facts.setdefault(user_id, {})[key] = value

    def facts(self, user_id) -> Dict[str, str]:
        return dict(self._facts.get(user_id, {}))

    def conversations_for_user(self, user_id) -> List[str]:
        return sorted(c for c, u in self._owner.items() if u == user_id)


class PostgresLongTermStore:
    """Postgres-backed long-term store (conversation transcript + per-user facts). Creates
    its tables on first use; the reference durable impl."""

    def __init__(self, url: str = "postgresql://agentic:agentic@localhost:5432/agentic", schema: str = "agentic"):
        import psycopg

        self._psycopg = psycopg
        self._url = url
        self._schema = schema
        with self._conn() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.turns (
                id BIGSERIAL PRIMARY KEY, conversation_id TEXT NOT NULL, user_id TEXT NOT NULL,
                role TEXT NOT NULL, content TEXT NOT NULL, ts TIMESTAMPTZ DEFAULT now())""")
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.facts (
                user_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
                PRIMARY KEY (user_id, key))""")
            conn.commit()

    def _conn(self):
        return self._psycopg.connect(self._url)

    def save_turn(self, conversation_id, user_id, role, content) -> None:
        with self._conn() as conn:
            conn.execute(f"INSERT INTO {self._schema}.turns (conversation_id, user_id, role, content) "
                         "VALUES (%s, %s, %s, %s)", (conversation_id, user_id, role, content))
            conn.commit()

    def load_history(self, conversation_id) -> List[Tuple[str, str]]:
        with self._conn() as conn:
            rows = conn.execute(f"SELECT role, content FROM {self._schema}.turns "
                                "WHERE conversation_id = %s ORDER BY id", (conversation_id,)).fetchall()
        return [(r[0], r[1]) for r in rows]

    def save_fact(self, user_id, key, value) -> None:
        with self._conn() as conn:
            conn.execute(f"INSERT INTO {self._schema}.facts (user_id, key, value) VALUES (%s, %s, %s) "
                         "ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value",
                         (user_id, key, value))
            conn.commit()

    def facts(self, user_id) -> Dict[str, str]:
        with self._conn() as conn:
            rows = conn.execute(f"SELECT key, value FROM {self._schema}.facts WHERE user_id = %s",
                                (user_id,)).fetchall()
        return {r[0]: r[1] for r in rows}

    def conversations_for_user(self, user_id) -> List[str]:
        with self._conn() as conn:
            rows = conn.execute(f"SELECT DISTINCT conversation_id FROM {self._schema}.turns "
                                "WHERE user_id = %s ORDER BY conversation_id", (user_id,)).fetchall()
        return [r[0] for r in rows]


def make_long_term_store(spec: Optional[dict]) -> Optional[LongTermStore]:
    """Build a LongTermStore from a ``{kind, url}`` spec. kind = memory | postgres."""
    if not spec:
        return None
    kind = (spec.get("kind") or "memory").lower()
    if kind == "memory":
        return InMemoryLongTermStore()
    if kind == "postgres":
        import os
        url = spec.get("url") or os.environ.get("AGENTIC_POSTGRES_URL") \
            or "postgresql://agentic:agentic@localhost:5432/agentic"
        return PostgresLongTermStore(url=url, schema=spec.get("schema", "agentic"))
    raise ValueError(f"unknown long-term store kind {kind!r}; choose memory|postgres")
