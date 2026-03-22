from __future__ import annotations

from typing import Any
from decimal import Decimal

try:
    import orjson as _json_impl
except Exception:  # pragma: no cover
    _json_impl = None

import json as _stdlib_json


def _default_serializer(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError


def loads(value: str | bytes | bytearray | memoryview | None) -> Any:
    if value is None:
        raise ValueError("Cannot load None as JSON")
    if _json_impl is not None:
        return _json_impl.loads(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    return _stdlib_json.loads(value)


def dumps(value: Any, *, ensure_ascii: bool = False, sort_keys: bool = False) -> str:
    if _json_impl is not None:
        option = 0
        if sort_keys:
            option |= _json_impl.OPT_SORT_KEYS
        # orjson.dumps returns bytes, so we decode it for use in most bot contexts
        return _json_impl.dumps(value, default=_default_serializer, option=option).decode("utf-8")
    
    # Stdlib fallback with simple Decimal support
    def std_default(obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        raise TypeError
        
    return _stdlib_json.dumps(value, default=std_default, ensure_ascii=ensure_ascii, sort_keys=sort_keys)


def loads_or_default(value: str | bytes | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return loads(value)
    except Exception:
        return default
