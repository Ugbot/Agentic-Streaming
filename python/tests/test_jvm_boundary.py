"""No raw Java types leak through the Python API: collections/records/optionals become Python
values, Java exceptions become Python exceptions, ``CompletableFuture`` becomes a Python future,
and Python callables are invoked by the JVM with typed arguments."""

from __future__ import annotations

import asyncio
import concurrent.futures
import random
import string
from typing import List, Optional

import jpype
import pytest

from agentic_flink._proxy import (
    JvmError,
    JvmTimeoutError,
    WorkflowValidationError,
    as_concurrent_future,
    as_future,
    java_calls,
    to_java,
    to_py,
    translate_exception,
)
from agentic_flink.pytools import (
    call_with_typed_args,
    coerce,
    infer_parameters,
    java_function,
    tool,
    tool_spec,
)

pytestmark = pytest.mark.usefixtures("af")


def _rand() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(8))


def test_java_collections_and_records_become_python_values():
    LinkedHashMap = jpype.JClass("java.util.LinkedHashMap")
    ArrayList = jpype.JClass("java.util.ArrayList")
    Optional_ = jpype.JClass("java.util.Optional")
    JEvent = jpype.JClass("org.jagentic.core.Event")

    key, text = _rand(), _rand()
    n = random.randint(1, 10_000)
    m = LinkedHashMap()
    lst = ArrayList()
    lst.add(jpype.JInt(n))
    lst.add(jpype.JDouble(1.5))
    lst.add(jpype.JBoolean(True))
    lst.add(None)
    m.put(key, lst)
    m.put("opt", Optional_.of("x"))
    m.put("none", Optional_.empty())

    py = to_py(m)
    assert type(py) is dict
    assert py == {key: [n, 1.5, True, None], "opt": "x", "none": None}
    assert type(py[key][0]) is int and type(py[key][1]) is float and type(py[key][2]) is bool

    ev = to_py(JEvent.turn("c1", "t1", "u", text))
    assert ev["text"] == text and ev["conversationId"] == "c1" and ev["metadata"] == {}


def test_python_values_become_java_and_round_trip():
    payload = {"n": random.randint(1, 99), "f": 2.5, "s": _rand(), "b": False, "nested": [1, {"k": None}]}
    j = to_java(payload)
    assert isinstance(j, jpype.JClass("java.util.Map"))
    assert isinstance(j.get("nested"), jpype.JClass("java.util.List"))
    assert to_py(j) == payload


def test_java_exceptions_translate_to_python_exceptions():
    Integer = jpype.JClass("java.lang.Integer")
    with pytest.raises(ValueError, match="NumberFormatException"):
        with java_calls():
            Integer.parseInt("not-a-number")

    with pytest.raises(KeyError):
        with java_calls():
            jpype.JClass("java.util.ArrayList")().iterator().next()
    with pytest.raises(AttributeError, match="NullPointerException"):
        with java_calls():
            jpype.JClass("java.util.Objects").requireNonNull(None)

    CompletableFuture = jpype.JClass("java.util.concurrent.CompletableFuture")
    TimeUnit = jpype.JClass("java.util.concurrent.TimeUnit")
    with pytest.raises(JvmTimeoutError) as ti:
        with java_calls():
            CompletableFuture().get(1, TimeUnit.MILLISECONDS)
    assert isinstance(ti.value, TimeoutError)

    failed = CompletableFuture.failedFuture(jpype.JClass("java.lang.IllegalStateException")("boom"))
    with pytest.raises(JvmError, match="boom") as ci:
        with java_calls():
            failed.join()  # CompletionException unwrapped to its cause
    assert ci.value.java_class == "java.lang.IllegalStateException"

    err = translate_exception(jpype.JClass("java.io.IOException")("disk"))
    assert isinstance(err, JvmError) and err.java_class == "java.io.IOException" and "disk" in str(err)
    assert isinstance(translate_exception(KeyError("py")), KeyError)

    Validator = jpype.JClass("org.jagentic.core.pipeline.WorkflowValidator")
    with pytest.raises(WorkflowValidationError):
        with java_calls():
            Validator.validate(to_java({"spec_version": "agentic/v1", "agent": {"id": "a"}}))


def test_completable_future_bridges_to_python_futures():
    CompletableFuture = jpype.JClass("java.util.concurrent.CompletableFuture")
    value = _rand()
    cf = CompletableFuture.completedFuture(to_java({"v": value}))
    f = as_concurrent_future(cf)
    assert isinstance(f, concurrent.futures.Future)
    assert f.result(timeout=5) == {"v": value}

    failing = CompletableFuture.failedFuture(jpype.JClass("java.lang.IllegalArgumentException")("bad"))
    with pytest.raises(ValueError, match="bad"):
        as_concurrent_future(failing).result(timeout=5)

    async def go():
        pending = CompletableFuture()
        af = as_future(pending)
        assert isinstance(af, asyncio.Future) and not af.done()
        pending.complete(to_java([1, 2, 3]))
        return await asyncio.wait_for(af, 5)

    assert asyncio.run(go()) == [1, 2, 3]


def test_typed_argument_coercion_and_inference():
    def fn(amount: float, tags: List[str], note: Optional[str] = None, count: int = 1) -> dict:
        return {"amount": amount, "tags": tags, "note": note, "count": count}

    params = infer_parameters(fn)
    assert params["type"] == "object"
    assert params["properties"]["amount"] == {"type": "number"}
    assert params["properties"]["tags"] == {"type": "array", "items": {"type": "string"}}
    assert params["properties"]["count"] == {"type": "integer"}
    assert params["required"] == ["amount", "tags"]

    out = call_with_typed_args(fn, {"amount": "12.5", "tags": ("a", 1), "count": "3"})
    assert out == {"amount": 12.5, "tags": ["a", "1"], "note": None, "count": 3}
    assert type(out["amount"]) is float and type(out["count"]) is int
    assert coerce("7", int) == 7 and coerce(1, bool) is True and coerce(None, Optional[int]) is None
    with pytest.raises(TypeError, match="missing"):
        call_with_typed_args(fn, {"tags": []})
    with pytest.raises(TypeError, match="unexpected"):
        call_with_typed_args(fn, {"amount": 1, "tags": [], "bogus": 1})


def test_tool_decorator_and_jvm_invocation_with_typed_args():
    tid = f"tool_{_rand()}"
    seen = {}

    @tool(id=tid, description="refund")
    def issue_refund(user: str, amount: float, reason: str = "n/a") -> dict:
        seen["types"] = (type(user), type(amount), type(reason))
        return {"refunded": amount, "user": user, "reason": reason}

    spec = tool_spec(issue_refund)
    assert (spec.id, spec.description) == (tid, "refund")
    assert spec.parameters["required"] == ["user", "amount"]

    jf = java_function(issue_refund)
    amount = random.randint(1, 500)
    result = to_py(jf.apply(to_java({"user": "u1", "amount": str(amount)})))
    assert result == {"refunded": float(amount), "user": "u1", "reason": "n/a"}
    assert seen["types"] == (str, float, str)

    RuntimeException = jpype.JClass("java.lang.RuntimeException")
    with pytest.raises(RuntimeException, match="TypeError"):
        jf.apply(to_java({"user": "u1"}))  # missing arg reported as a real Java exception
