"""Canonical JSON encoding used for every value that enters history.

History is the source of truth, so values are always *round-tripped* through this codec
before workflow code sees them. That keeps the live run and every later replay looking at
byte-identical data (tuples become lists, dict keys become strings, and so on).
"""

from __future__ import annotations

import hashlib
import json
import traceback
from typing import Any

from replayagent.errors import ApplicationError, SerializationError


def dumps(value: Any) -> str:
    """Encode ``value`` as canonical JSON (sorted keys, no whitespace, no NaN)."""
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    except (TypeError, ValueError) as exc:
        raise SerializationError(f"value is not JSON-serialisable: {exc}") from exc


def loads(text: str) -> Any:
    """Decode JSON produced by :func:`dumps`."""
    return json.loads(text)


def roundtrip(value: Any) -> Any:
    """Return ``value`` as it will look after being stored and loaded again."""
    return loads(dumps(value))


def digest(value: Any) -> str:
    """Short stable fingerprint of a JSON value (used for determinism checks)."""
    return hashlib.sha256(dumps(value).encode("utf-8")).hexdigest()[:16]


def encode_error(exc: BaseException, *, include_traceback: bool = True) -> dict[str, Any]:
    """Describe an exception as a JSON object suitable for history."""
    non_retryable = False
    err_type = type(exc).__name__
    if isinstance(exc, ApplicationError):
        non_retryable = exc.non_retryable
        if exc.type:
            err_type = exc.type
    out: dict[str, Any] = {
        "type": err_type,
        "message": str(exc),
        "non_retryable": non_retryable,
    }
    if include_traceback:
        out["traceback"] = "".join(traceback.format_exception(exc)).strip()[-4000:]
    return out


def make_error(err_type: str, message: str, *, non_retryable: bool = False) -> dict[str, Any]:
    """Build an error object for failures that are not Python exceptions (timeouts...)."""
    return {"type": err_type, "message": message, "non_retryable": non_retryable}
