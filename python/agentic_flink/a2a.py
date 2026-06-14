"""A2A (Agent2Agent) wrappers: call remote agents as a deterministic step on the
Flink graph, with the resilient client (retry + backoff + circuit breaker) and the
non-blocking / keyed-state-correct execution modes — parity with the Java A2AStep.
"""

from __future__ import annotations

from typing import Optional

from ._jvm import jclass


def _duration_ms(ms: int):
    return jclass("java.time.Duration").ofMillis(int(ms))


def _transport(name: str):
    return jclass("org.agentic.flink.a2a.A2ATransport").valueOf(name.upper())


def remote_agent(
    name: str,
    *,
    card_url: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    transport: str = "JSONRPC",
    skill_id: Optional[str] = None,
    streaming: bool = False,
    request_timeout_ms: int = 60_000,
    poll_interval_ms: int = 500,
    description: Optional[str] = None,
    max_retries: int = 2,
    retry_base_backoff_ms: int = 100,
    retry_max_backoff_ms: int = 2000,
    circuit_breaker_threshold: int = 5,
    circuit_breaker_open_ms: int = 30_000,
):
    """Build a :class:`RemoteAgentSpec` for a peer agent — either by Agent Card
    discovery (``card_url``) or a pinned ``endpoint_url`` — including the resilience
    knobs (retry/backoff/circuit breaker) honored by the resilient client.
    """
    if not card_url and not endpoint_url:
        raise ValueError("remote_agent requires either card_url or endpoint_url")
    Spec = jclass("org.agentic.flink.a2a.RemoteAgentSpec")
    b = Spec.builder().withName(name)
    if card_url:
        b = b.withAgentCardUrl(card_url)
    if endpoint_url:
        b = b.withEndpointUrl(endpoint_url).withTransport(_transport(transport))
    if skill_id:
        b = b.withSkillId(skill_id)
    if description:
        b = b.withDescription(description)
    b = (
        b.withStreaming(bool(streaming))
        .withRequestTimeout(_duration_ms(request_timeout_ms))
        .withPollInterval(_duration_ms(poll_interval_ms))
        .withMaxRetries(int(max_retries))
        .withRetryBackoff(_duration_ms(retry_base_backoff_ms), _duration_ms(retry_max_backoff_ms))
        .withCircuitBreakerThreshold(int(circuit_breaker_threshold))
        .withCircuitBreakerOpen(_duration_ms(circuit_breaker_open_ms))
    )
    return b.build()


def discovering_client_factory():
    """The default ServiceLoader-resolved client factory (wraps the SDK client in
    the resilient decorator automatically)."""
    return jclass("org.agentic.flink.a2a.A2AClientFactory").discovering()


def resilient_client_factory(delegate):
    """Wrap any client factory so its clients gain retry + backoff + circuit
    breaker (idempotent)."""
    return jclass("org.agentic.flink.a2a.A2AClientFactory").resilient(delegate)


def a2a_step(
    spec,
    *,
    name: Optional[str] = None,
    input_key: Optional[str] = None,
    output_key: Optional[str] = None,
    fail_on_error: bool = False,
    capacity: int = 100,
    client_factory=None,
):
    """Build an :class:`A2AStep` — an explicit remote-A2A step on the graph.

    Use :func:`apply`, :func:`apply_async`, or :func:`apply_stateful` to wire it
    onto a ``DataStream<AgentEvent>``.
    """
    Step = jclass("org.agentic.flink.a2a.A2AStep")
    b = Step.builder().withSpec(spec).withFailOnError(bool(fail_on_error)).withCapacity(int(capacity))
    if name:
        b = b.withName(name)
    if input_key:
        b = b.withInputKey(input_key)
    if output_key:
        b = b.withOutputKey(output_key)
    if client_factory is not None:
        b = b.withClientFactory(client_factory)
    return b.build()


def apply(step, stream):
    """Blocking keyed step (the remote call runs inside one keyed operator)."""
    return step.applyTo(stream)


def apply_async(step, stream):
    """Non-blocking, stateless step via Flink Async I/O (a slow peer never stalls
    the pipeline). The remote contextId only persists across turns if carried on
    the event — see :func:`apply_stateful` for cross-turn continuity."""
    return step.applyToAsync(stream)


def apply_stateful(step, stream):
    """Non-blocking AND keyed-state-correct: keyed pre-step → stateless async call
    → keyed post-step, with per-conversation continuity through the shared
    ConversationStore."""
    return step.applyToStateful(stream)


__all__ = [
    "remote_agent",
    "discovering_client_factory",
    "resilient_client_factory",
    "a2a_step",
    "apply",
    "apply_async",
    "apply_stateful",
]
