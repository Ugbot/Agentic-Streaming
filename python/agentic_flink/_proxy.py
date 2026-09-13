"""Helpers for crossing the Java ↔ Python boundary.

Three jobs:

1. Convert Java values (``Map``, ``List``, ``Optional``, boxed numbers, enums) to native
   Python containers and back (:func:`to_py`, :func:`to_java`). JPype handles
   ``String`` natively when ``convertStrings=True`` is set on the JVM (the default here).
2. Translate Java exceptions into Python exceptions (:func:`translate_exception`,
   :func:`java_calls`) so callers of the public API never see a raw ``JException``.
3. Bridge :class:`java.util.concurrent.CompletableFuture` to
   :class:`concurrent.futures.Future` (:func:`as_concurrent_future`) and to
   :mod:`asyncio` (:func:`as_future`).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
from typing import Any, Iterator, Mapping

import jpype

from ._jvm import jclass


class JvmError(RuntimeError):
    """A Java exception that crossed into Python. ``java_class`` names the original type."""

    def __init__(self, java_class: str, message: str) -> None:
        super().__init__(f"{java_class}: {message}" if message else java_class)
        self.java_class = java_class
        self.java_message = message


class WorkflowValidationError(ValueError):
    """The JVM core rejected a workflow document (``WorkflowValidator.WorkflowValidationException``)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class JvmTimeoutError(JvmError, TimeoutError):
    pass


_VALUE_ERRORS = (
    "java.lang.IllegalArgumentException",
    "java.lang.NumberFormatException",
    "org.jagentic.core.ToolRegistry$UnknownTool",
)
_KEY_ERRORS = ("java.util.NoSuchElementException",)
_NULLS = ("java.lang.NullPointerException",)
_TIMEOUTS = ("java.util.concurrent.TimeoutException",)
_VALIDATION = ("org.jagentic.core.pipeline.WorkflowValidator$WorkflowValidationException",)


def _java_class_name(exc: jpype.JException) -> str:
    return str(exc.getClass().getName())


def _root_cause(exc: Any) -> Any:
    seen = 0
    while exc is not None and seen < 16:
        cause = exc.getCause()
        if cause is None or cause is exc:
            return exc
        exc = cause
        seen += 1
    return exc


def translate_exception(exc: BaseException) -> BaseException:
    """Map a Java exception to the closest Python exception (unchanged if already Python)."""
    if not isinstance(exc, jpype.JException):
        return exc
    name = _java_class_name(exc)
    message = str(exc.getMessage() or "")
    if name in _VALIDATION:
        return WorkflowValidationError(message)
    if name in ("java.util.concurrent.CompletionException", "java.util.concurrent.ExecutionException"):
        cause = _root_cause(exc)
        if cause is not exc:
            return translate_exception(cause)
    if name in _VALUE_ERRORS:
        return ValueError(f"{name}: {message}")
    if name in _KEY_ERRORS:
        return KeyError(message)
    if name in _NULLS:
        return AttributeError(f"{name}: {message}")
    if name in _TIMEOUTS:
        return JvmTimeoutError(name, message)
    return JvmError(name, message)


@contextlib.contextmanager
def java_calls() -> Iterator[None]:
    """``with java_calls(): ...`` re-raises any Java exception as its Python translation."""
    try:
        yield
    except jpype.JException as e:
        raise translate_exception(e) from None


def to_py(value: Any) -> Any:
    """Recursively convert a Java value to a Python value.

    ``Map`` → :class:`dict`, ``List``/``Set`` → :class:`list`, ``Optional`` → unwrapped
    value or ``None``, boxed numbers → :class:`int`/:class:`float`, ``Boolean`` →
    :class:`bool`, records → :class:`dict` keyed by component name, enums and other
    objects → their string form. Python values pass through.
    """
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str, bytes, dict, list, tuple)) and not isinstance(value, jpype.JObject):
        return value

    JMap = jclass("java.util.Map")
    JCollection = jclass("java.util.Collection")
    JOptional = jclass("java.util.Optional")
    JBoolean = jclass("java.lang.Boolean")
    JNumber = jclass("java.lang.Number")
    JString = jclass("java.lang.String")
    JEnum = jclass("java.lang.Enum")

    if isinstance(value, JMap):
        return {to_py(e.getKey()): to_py(e.getValue()) for e in value.entrySet()}
    if isinstance(value, JCollection):
        return [to_py(v) for v in value]
    if isinstance(value, JOptional):
        return to_py(value.orElse(None))
    if isinstance(value, JBoolean):
        return bool(value.booleanValue())
    if isinstance(value, JNumber):
        name = str(value.getClass().getName())
        if name in ("java.lang.Double", "java.lang.Float", "java.math.BigDecimal"):
            return float(value.doubleValue())
        return int(value.longValue())
    if isinstance(value, JString):
        return str(value)
    if isinstance(value, JEnum):
        return str(value.name())
    if isinstance(value, jpype.JArray):
        return [to_py(v) for v in value]
    cls = value.getClass()
    if cls.isRecord():
        return {str(c.getName()): to_py(c.getAccessor().invoke(value)) for c in cls.getRecordComponents()}
    return str(value)


def to_java(value: Any) -> Any:
    """Recursively convert a Python value to the Java object the core expects.

    ``dict`` → ``LinkedHashMap`` (insertion order matters: router rules are ordered),
    ``list``/``tuple``/``set`` → ``ArrayList``, ``bool`` → ``Boolean``, ``int`` → ``Long``
    (``Integer`` when it fits, so ``(Number) x`` casts in the core all work), ``float`` →
    ``Double``, ``str`` → ``String``, ``None`` → ``null``.
    """
    if value is None:
        return None
    if isinstance(value, jpype.JObject):
        return value
    if isinstance(value, bool):
        return jclass("java.lang.Boolean")(value)
    if isinstance(value, int):
        if -(2**31) <= value < 2**31:
            return jclass("java.lang.Integer")(value)
        return jclass("java.lang.Long")(value)
    if isinstance(value, float):
        return jclass("java.lang.Double")(value)
    if isinstance(value, str):
        return jclass("java.lang.String")(value)
    if isinstance(value, Mapping):
        out = jclass("java.util.LinkedHashMap")()
        for k, v in value.items():
            out.put(to_java(k), to_java(v))
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        out = jclass("java.util.ArrayList")()
        for v in value:
            out.add(to_java(v))
        return out
    raise TypeError(f"cannot convert {type(value).__name__} to a Java value: {value!r}")


def to_java_map(d: Mapping[str, Any]):
    """Convert a Python mapping to a ``java.util.Map`` (deeply)."""
    return to_java(dict(d))


def as_concurrent_future(java_completable_future, convert=to_py) -> concurrent.futures.Future:
    """Wrap a Java ``CompletableFuture`` as a thread-safe :class:`concurrent.futures.Future`.

    Completion arrives on a JVM thread; the Python future is completed from there with
    ``convert(value)`` or the translated exception."""
    fut: concurrent.futures.Future = concurrent.futures.Future()
    fut.set_running_or_notify_cancel()
    BiConsumer = jclass("java.util.function.BiConsumer")

    @jpype.JImplements(BiConsumer)
    class _Callback:
        @jpype.JOverride
        def accept(self, value, throwable):
            if throwable is not None:
                fut.set_exception(translate_exception(_root_cause(throwable)))
            else:
                try:
                    fut.set_result(convert(value))
                except BaseException as e:  # conversion failure must not be swallowed on the JVM thread
                    fut.set_exception(e)

    java_completable_future.whenComplete(_Callback())
    return fut


def as_future(java_completable_future, convert=to_py) -> asyncio.Future:
    """Wrap a Java ``CompletableFuture`` as an :class:`asyncio.Future` on the running loop.

    Usage::

        result = await as_future(runtime.submitAsync(event))
    """
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()

    def _done(cf: concurrent.futures.Future) -> None:
        if fut.cancelled():
            return
        exc = cf.exception()
        if exc is not None:
            loop.call_soon_threadsafe(fut.set_exception, exc)
        else:
            loop.call_soon_threadsafe(fut.set_result, cf.result())

    as_concurrent_future(java_completable_future, convert).add_done_callback(_done)
    return fut


def jpype_implements(interface_class):
    """Local alias around :func:`jpype.JImplements` so wrapper modules don't
    have to import :mod:`jpype` directly. Accepts either a JClass or an FQN
    string."""
    return jpype.JImplements(interface_class)
