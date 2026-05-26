"""Python ``@tool`` decorator + ``ToolExecutor`` proxy.

Decorate any Python function to expose it as a Java
:class:`org.agentic.flink.tools.ToolExecutor`. The agent's LLM can
then call the tool through the regular tool-call path; the Python function
runs on whatever JVM thread invoked it (typically the operator thread), so
there's no process boundary and no serialization.

Two usage patterns::

    @tool
    def get_weather(city: str) -> str:
        return f"sunny in {city}"

    @tool(name="add", description="Add two numbers")
    def add(a: int, b: int) -> int:
        return a + b

The decorator inspects the function's signature; Java arguments arrive as a
``Map<String, Object>`` and are matched to the function's parameters by
name. Extra keys are ignored; missing keys raise :class:`KeyError`.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, Optional

import jpype

from ._jvm import jclass
from ._proxy import to_py


class PythonTool:
    """A Python function exposed as a Java :class:`ToolExecutor`.

    Holds the wrapped function + the Java proxy that delegates to it. Pass
    instances of this class to :meth:`AgentBuilder.with_tools` /
    :meth:`ToolRegistry.builder().register_tool`.
    """

    def __init__(self, func: Callable[..., Any], name: str, description: str):
        self.func = func
        self.name = name
        self.description = description
        self._signature = inspect.signature(func)
        self._java_proxy = None  # built on first _to_java() call (needs JVM)

    def __call__(self, *args, **kwargs):
        """Direct invocation from Python is allowed and skips the Java path."""
        return self.func(*args, **kwargs)

    def _to_java(self):
        if self._java_proxy is None:
            self._java_proxy = _build_proxy(self)
        return self._java_proxy


def tool(
    arg: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
):
    """Decorator that wraps a Python function as a :class:`PythonTool`.

    Used with or without arguments::

        @tool
        def add(a: int, b: int) -> int: ...

        @tool(name="weather", description="Get the weather for a city.")
        def get_weather(city: str) -> str: ...
    """

    def wrap(func: Callable) -> PythonTool:
        tool_name = name or func.__name__
        tool_desc = description or (func.__doc__ or "").strip() or tool_name
        return PythonTool(func, tool_name, tool_desc)

    if callable(arg) and name is None and description is None:
        # Bare ``@tool`` form — ``arg`` is the function.
        return wrap(arg)
    return wrap


def _build_proxy(pytool: PythonTool):
    """Build the Java ``ToolExecutor`` proxy bound to ``pytool``."""

    ToolExecutor = jclass("org.agentic.flink.tools.ToolExecutor")
    CompletableFuture = jclass("java.util.concurrent.CompletableFuture")

    sig = pytool._signature
    param_names = list(sig.parameters.keys())

    @jpype.JImplements(ToolExecutor)
    class _Proxy:  # pragma: no cover -- exercised through Java
        @jpype.JOverride
        def execute(self, parameters):
            params = to_py(parameters) or {}
            try:
                # Try kwarg-style first (matches param names).
                kwargs = {n: params[n] for n in param_names if n in params}
                # If the function has **kwargs, pass all params through.
                if any(
                    p.kind == inspect.Parameter.VAR_KEYWORD
                    for p in sig.parameters.values()
                ):
                    kwargs = dict(params)
                result = pytool.func(**kwargs)
                return CompletableFuture.completedFuture(result)
            except Exception as exc:  # noqa: BLE001
                RuntimeException = jclass("java.lang.RuntimeException")
                return CompletableFuture.failedFuture(
                    RuntimeException(f"{type(exc).__name__}: {exc}")
                )

        @jpype.JOverride
        def getToolId(self):
            return pytool.name

        @jpype.JOverride
        def getDescription(self):
            return pytool.description

        @jpype.JOverride
        def validateParameters(self, parameters):
            return parameters is not None

    return _Proxy()


__all__ = ["PythonTool", "tool"]
