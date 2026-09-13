"""Bridge Python callables into the JVM core's ``ToolRegistry``.

A tool declared as ``kind: function`` in the workflow IR is bound to a Python callable. When
the JVM graph calls the tool, the arguments arrive as a Java ``Map<String,Object>``; this module
converts them to Python, coerces them to the callable's annotated parameter types, invokes it,
and converts the return value back. A Python exception surfaces in the JVM as a tool failure,
so the core's retry policy and ``turn_failed`` handling apply exactly as for a Java tool.

::

    @tool(description="Refund a charge", parameters={"type": "object", ...})
    def issue_refund(amount: float, reason: str = "") -> dict: ...

    Agent("support").use_tool("issue_refund", issue_refund)
"""

from __future__ import annotations

import inspect
import weakref
import typing
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional

import jpype

from ._jvm import jclass
from ._proxy import to_java, to_py

ToolFn = Callable[..., Any]

# Decorated callables -> their ToolSpec (keyed by identity; the callable itself is unchanged).
_TOOL_SPECS: "weakref.WeakKeyDictionary[Any, ToolSpec]" = weakref.WeakKeyDictionary()

_JSON_TYPES: Dict[str, str] = {
    "str": "string", "int": "integer", "float": "number", "bool": "boolean",
    "dict": "object", "list": "array", "Mapping": "object", "Sequence": "array",
}


@dataclass(frozen=True)
class ToolSpec:
    id: str
    description: str
    parameters: Dict[str, Any]
    fn: ToolFn


def _origin(annotation: Any) -> Any:
    return typing.get_origin(annotation) or annotation


def coerce(value: Any, annotation: Any) -> Any:
    """Coerce a JSON-ish value to the annotated Python type (leaves it alone if untyped)."""
    if annotation is inspect.Parameter.empty or value is None:
        return value
    origin = _origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return coerce(value, non_none[0])
        return value
    if origin is bool:
        if isinstance(value, str):
            return value.strip().lower() in ("true", "1", "yes")
        return bool(value)
    if origin is int and not isinstance(value, bool):
        return int(value)
    if origin is float:
        return float(value)
    if origin is str:
        return str(value)
    if origin in (list, tuple, typing.Sequence) and isinstance(value, (list, tuple)):
        inner = args[0] if args else inspect.Parameter.empty
        return origin([coerce(v, inner) for v in value]) if origin in (list, tuple) else [coerce(v, inner) for v in value]
    if origin in (dict, typing.Mapping) and isinstance(value, Mapping):
        vt = args[1] if len(args) == 2 else inspect.Parameter.empty
        return {k: coerce(v, vt) for k, v in value.items()}
    return value


def _json_type(annotation: Any) -> Dict[str, Any]:
    origin = _origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        return _json_type(non_none[0]) if len(non_none) == 1 else {}
    tname = origin.__name__ if isinstance(origin, type) else str(origin)
    prop: Dict[str, Any] = {}
    if tname in _JSON_TYPES:
        prop["type"] = _JSON_TYPES[tname]
    if prop.get("type") == "array" and args:
        items = _json_type(args[0])
        if items:
            prop["items"] = items
    return prop


def infer_parameters(fn: ToolFn) -> Dict[str, Any]:
    """A JSON Schema for ``fn``'s parameters from its signature and annotations."""
    sig = inspect.signature(fn)
    hints = typing.get_type_hints(fn) if hasattr(fn, "__annotations__") else {}
    props: Dict[str, Any] = {}
    required = []
    for name, p in sig.parameters.items():
        if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        props[name] = _json_type(hints.get(name, p.annotation))
        if p.default is inspect.Parameter.empty:
            required.append(name)
    schema: Dict[str, Any] = {"type": "object", "properties": props}
    if required:
        schema["required"] = required
    return schema


def call_with_typed_args(fn: ToolFn, args: Mapping[str, Any]) -> Any:
    """Invoke ``fn`` with ``args`` as keyword arguments coerced to its annotations.

    A callable whose only parameter is untyped or annotated as a mapping, and whose name does
    not match any argument key, receives the whole argument mapping positionally."""
    sig = inspect.signature(fn)
    hints = typing.get_type_hints(fn) if hasattr(fn, "__annotations__") else {}
    params = sig.parameters
    accepts_var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
    named = [p for p in params.values() if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)]
    if len(named) == 1 and not accepts_var_kw and named[0].name not in args:
        ann = hints.get(named[0].name, named[0].annotation)
        if ann is inspect.Parameter.empty or _origin(ann) in (dict, typing.Mapping):
            return fn(dict(args))
    kwargs: Dict[str, Any] = {}
    for key, value in args.items():
        if key in params:
            kwargs[key] = coerce(value, hints.get(key, params[key].annotation))
        elif accepts_var_kw:
            kwargs[key] = value
        else:
            raise TypeError(f"tool {_fn_name(fn)!r} got an unexpected argument {key!r}")
    missing = [p.name for p in named if p.default is inspect.Parameter.empty and p.name not in kwargs]
    if missing:
        raise TypeError(f"tool {_fn_name(fn)!r} is missing arguments {missing}")
    return fn(**kwargs)


def _fn_name(fn: ToolFn) -> str:
    return fn.__name__ if hasattr(fn, "__name__") else type(fn).__name__


def tool(fn: Optional[ToolFn] = None, *, id: Optional[str] = None, description: Optional[str] = None,
         parameters: Optional[Mapping[str, Any]] = None):
    """Decorator registering a :class:`ToolSpec` for a Python function.

    Usable bare (``@tool``) or with options. The decorated function is returned unchanged,
    so it stays an ordinary Python callable; :func:`tool_spec` recovers the spec."""

    def wrap(f: ToolFn) -> ToolFn:
        spec = ToolSpec(
            id=id or f.__name__,
            description=(description or (f.__doc__ or f.__name__).strip().splitlines()[0]),
            parameters=dict(parameters) if parameters is not None else infer_parameters(f),
            fn=f,
        )
        _TOOL_SPECS[f] = spec
        return f

    return wrap(fn) if fn is not None else wrap


def tool_spec(fn: ToolFn, tool_id: Optional[str] = None) -> ToolSpec:
    """The :class:`ToolSpec` for ``fn``: the decorator's if present, otherwise inferred."""
    spec = _TOOL_SPECS.get(fn)
    if spec is not None:
        return spec if tool_id is None or spec.id == tool_id else ToolSpec(tool_id, spec.description, spec.parameters, fn)
    return ToolSpec(
        id=tool_id or _fn_name(fn),
        description=(fn.__doc__ or tool_id or _fn_name(fn)).strip().splitlines()[0],
        parameters=infer_parameters(fn),
        fn=fn,
    )


def java_function(fn: ToolFn):
    """Wrap ``fn`` as a ``java.util.function.Function<Map<String,Object>,Object>`` for the core."""
    Function = jclass("java.util.function.Function")
    RuntimeException = jclass("java.lang.RuntimeException")

    @jpype.JImplements(Function)
    class _PyTool:
        @jpype.JOverride
        def apply(self, params):
            args = to_py(params) or {}
            try:
                return to_java(call_with_typed_args(fn, args))
            except Exception as e:  # surface the Python failure text to the core's tool_failed event
                raise RuntimeException(f"{type(e).__name__}: {e}")

    return _PyTool()


def register_python_tools(registry, bindings: Mapping[str, ToolFn], declared: Mapping[str, Mapping[str, Any]]) -> None:
    """Register every bound callable on a core ``ToolRegistry``.

    ``declared`` carries the document's tool entries by id; their ``description`` and
    ``parameters`` win over what the callable infers."""
    for tool_id, fn in bindings.items():
        spec = tool_spec(fn, tool_id)
        entry = declared.get(tool_id, {})
        description = entry.get("description") or spec.description
        parameters = entry.get("parameters") or spec.parameters
        registry.register(tool_id, description, to_java(parameters), java_function(fn))


__all__ = [
    "ToolSpec",
    "call_with_typed_args",
    "coerce",
    "infer_parameters",
    "java_function",
    "register_python_tools",
    "tool",
    "tool_spec",
]
