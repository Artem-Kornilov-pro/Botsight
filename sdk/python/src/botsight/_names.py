from __future__ import annotations

import functools
from typing import Any

_cache: dict[Any, str] = {}


def handler_name(callback: Any) -> str:
    """``module:qualname`` of a handler; never raises.

    Works for functions, bound methods, ``functools.partial`` and class-based handlers.
    """
    try:
        return _cache[callback]
    except (KeyError, TypeError):
        pass
    target = callback
    while isinstance(target, functools.partial):
        target = target.func
    target = getattr(target, "__func__", target)  # bound method
    module = getattr(target, "__module__", None) or type(target).__module__
    qualname = getattr(target, "__qualname__", None) or type(target).__qualname__
    name = f"{module}:{qualname}"
    try:
        if len(_cache) > 10_000:
            _cache.clear()
        _cache[callback] = name
    except TypeError:  # unhashable callable
        pass
    return name
