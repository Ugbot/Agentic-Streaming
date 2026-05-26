"""``@tool`` decorator behaviour, including the Java-proxy round-trip."""

from __future__ import annotations

import random


def test_bare_tool_decorator(af):
    from agentic_flink import tool

    @tool
    def hello(name: str) -> str:
        """Friendly greeting."""
        return f"hi {name}"

    assert hello.name == "hello"
    assert "Friendly greeting" in hello.description
    assert hello("alice") == "hi alice"


def test_tool_with_metadata(af):
    from agentic_flink import tool

    @tool(name="add", description="Sum two ints")
    def add(a: int, b: int) -> int:
        return a + b

    assert add.name == "add"
    assert add.description == "Sum two ints"


def test_python_tool_executes_via_java_proxy(af):
    """The killer feature: a Python function invoked by the Java agent."""
    from agentic_flink import tool

    @tool(name="multiply")
    def mul(a: int, b: int) -> int:
        return a * b

    proxy = mul._to_java()
    assert str(proxy.getToolId()) == "multiply"
    assert proxy.validateParameters(_jmap({"a": 1, "b": 2}))

    a = random.randint(2, 100)
    b = random.randint(2, 100)
    result = proxy.execute(_jmap({"a": a, "b": b})).get()
    assert int(result) == a * b


def test_tool_raises_propagate_as_failed_future(af):
    from agentic_flink import tool, jclass

    @tool
    def boom(x: int) -> int:
        raise ValueError(f"no {x}")

    proxy = boom._to_java()
    future = proxy.execute(_jmap({"x": 5}))
    assert future.isCompletedExceptionally()
    # CompletionException wraps the cause; the message survives.
    try:
        future.get()
    except Exception as exc:
        msg = str(exc)
        assert "no 5" in msg or "ValueError" in msg
    else:  # pragma: no cover
        raise AssertionError("expected failure")


def _jmap(d):
    import agentic_flink as af

    HashMap = af.jclass("java.util.HashMap")
    out = HashMap()
    for k, v in d.items():
        out.put(k, v)
    return out
