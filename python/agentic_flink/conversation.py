"""Per-conversation store wrappers — the cross-operator / cross-turn state spine.

Factory functions returning live Java :class:`ConversationStore` instances: the
in-JVM shared default (embedded), Redis, or the Fluss PK table. Pass to
``AgentBuilder.with_conversation_store(...)`` or to an A2A stateful step.
"""

from __future__ import annotations

from ._jvm import jclass


def discover():
    """The default store: the first ServiceLoader-registered impl, else the shared
    in-JVM store (correct for the embedded single-JVM deployment)."""
    return jclass("org.agentic.flink.memory.conversation.ConversationStores").discover()


def in_memory_shared():
    """The process-wide shared in-JVM store (same instance for every operator in
    this JVM; survives the job-graph round trip)."""
    return jclass("org.agentic.flink.memory.conversation.InMemoryConversationStore").shared()


def redis(host: str = "localhost", port: int = 6379):
    """Redis-backed conversation store (cross-process state spine)."""
    Store = jclass("org.agentic.flink.memory.conversation.redis.RedisConversationStore")
    return Store(host, int(port))


def fluss(bootstrap_servers: str = "localhost:9123", database: str = "agentic", table: str = "conversations"):
    """Fluss PK-table conversation store (cross-process state spine)."""
    Store = jclass("org.agentic.flink.memory.conversation.fluss.FlussConversationStore")
    return Store(bootstrap_servers, database, table)


__all__ = ["discover", "in_memory_shared", "redis", "fluss"]
