"""Helpers for crossing the Java ↔ Python boundary.

Two main jobs:

1. Convert Java values (``Map``, ``List``, ``Optional``) to native Python
   containers and back. JPype handles primitives + ``String`` natively when
   ``convertStrings=True`` is set on the JVM (the default in this package).
2. Bridge Java :class:`java.util.concurrent.CompletableFuture` to
   :mod:`asyncio` so async callers can ``await`` a Java result.
"""

from __future__ import annotations

import asyncio
from typing import Any

from ._jvm import jclass


def to_py(value: Any) -> Any:
    """Recursively convert a Java value to a Python value.

    Handles ``java.util.Map`` → :class:`dict`, ``java.util.List`` →
    :class:`list`, and ``java.util.Optional`` → unwrapped Python object or
    :data:`None`. Leaves primitives and strings alone.
    """
    if value is None:
        return None

    JMap = jclass("java.util.Map")
    JList = jclass("java.util.List")
    JOptional = jclass("java.util.Optional")

    if isinstance(value, JMap):
        return {to_py(k): to_py(value.get(k)) for k in value.keySet()}
    if isinstance(value, JList):
        return [to_py(v) for v in value]
    if isinstance(value, JOptional):
        return to_py(value.orElse(None))
    return value


def to_java_map(d: dict[str, Any]):
    """Convert a Python dict to a ``java.util.HashMap`` for arguments that need
    the Java type explicitly (some interfaces require ``Map`` not ``Map<?,?>``
    that JPype would synthesize)."""
    HashMap = jclass("java.util.HashMap")
    out = HashMap()
    for k, v in d.items():
        out.put(k, v)
    return out


def as_future(java_completable_future) -> asyncio.Future:
    """Wrap a Java :class:`CompletableFuture` as an :class:`asyncio.Future`.

    Usage::

        result = await as_future(corpus.search(vec, 5))

    The Java side calls back from a JVM thread; we marshal the result onto
    the running event loop with :meth:`asyncio.AbstractEventLoop.call_soon_threadsafe`.
    """
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()

    BiConsumer = jclass("java.util.function.BiConsumer")

    @jpype_implements(BiConsumer)
    class _Callback:
        def accept(self, value, throwable):  # noqa: D401
            if throwable is not None:
                err = RuntimeError(str(throwable.getMessage()))
                loop.call_soon_threadsafe(fut.set_exception, err)
            else:
                loop.call_soon_threadsafe(fut.set_result, to_py(value))

    java_completable_future.whenComplete(_Callback())
    return fut


def jpype_implements(interface_class):
    """Local alias around :func:`jpype.JImplements` so wrapper modules don't
    have to import :mod:`jpype` directly. Accepts either a JClass or an FQN
    string."""
    import jpype

    return jpype.JImplements(interface_class)
