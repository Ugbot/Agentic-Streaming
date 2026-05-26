"""Channel wrappers — the unified continuous-input primitive.

The Java :class:`Channel` is generic; we surface the concrete reference
implementations with Pythonic helpers. For tool-driven channels see
:func:`tool_invocation_side_output` / :func:`tool_invocation_in_jvm`.
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

from ._jvm import jclass
from ._proxy import to_py


def static_seed(seeds: Sequence[Any], java_type=None):
    """In-memory seed channel. ``seeds`` is a Python iterable of any
    JPype-compatible value; ``java_type`` (optional) is a JPype
    :class:`Class` for the element type (defaults to the runtime class of the
    first seed)."""
    SS = jclass("org.agentic.flink.channel.StaticSeedChannel")
    TypeInformation = jclass("org.apache.flink.api.common.typeinfo.TypeInformation")
    ArrayList = jclass("java.util.ArrayList")
    java_seeds = ArrayList()
    for s in seeds:
        java_seeds.add(s)
    if java_type is None:
        if java_seeds.size() == 0:
            raise ValueError("static_seed needs a non-empty list or an explicit java_type")
        java_type = java_seeds.get(0).getClass()
    return SS(java_seeds, TypeInformation.of(java_type))


def kafka(bootstrap_servers: str, topic: str, group_id: str, java_type):
    """Generic Kafka channel of JSON-encoded ``T``. ``java_type`` is a JPype
    :class:`Class` describing the element type."""
    KC = jclass("org.agentic.flink.channel.KafkaChannel")
    return KC(bootstrap_servers, topic, group_id, java_type)


def kafka_context(bootstrap_servers: str, topic: str, group_id: str):
    """Kafka channel of ``KeyedContextItem`` — replaces the old ``KafkaMemoryFeed``."""
    KCC = jclass("org.agentic.flink.channel.KafkaContextChannel")
    return KCC(bootstrap_servers, topic, group_id)


def webhook(port: int, path: str, java_type, host: str = "0.0.0.0"):
    """HTTP-POST webhook channel. ``java_type`` is the JPype class for the
    payload."""
    WC = jclass("org.agentic.flink.channel.WebhookChannel")
    return WC(host, port, path, java_type)


def tool_invocation_side_output(
    tool_id: str,
    java_type,
    mapper: Callable[[dict], Any],
):
    """Side-output transport — recommended default. Tool invocations fire
    via Flink ``OutputTag`` from inside the agent operator.

    ``mapper`` receives a Python dict of the tool's parameters and must
    return a value of the registered Java type.
    """
    Channel = jclass("org.agentic.flink.channel.ToolInvocationChannel")
    Function = jclass("java.util.function.Function")
    import jpype

    @jpype.JImplements(Function)
    class _Mapper:
        @jpype.JOverride
        def apply(self, params):
            return mapper(to_py(params) or {})

    return Channel.sideOutput(tool_id, java_type, _Mapper())


def tool_invocation_in_jvm(
    tool_id: str,
    java_type,
    mapper: Callable[[dict], Any],
):
    """In-JVM BlockingQueue transport — single-process only, test-friendly."""
    Channel = jclass("org.agentic.flink.channel.ToolInvocationChannel")
    Function = jclass("java.util.function.Function")
    import jpype

    @jpype.JImplements(Function)
    class _Mapper:
        @jpype.JOverride
        def apply(self, params):
            return mapper(to_py(params) or {})

    return Channel.inJvm(tool_id, java_type, _Mapper())


__all__ = [
    "static_seed",
    "kafka",
    "kafka_context",
    "webhook",
    "tool_invocation_side_output",
    "tool_invocation_in_jvm",
]
