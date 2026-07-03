"""Live pipeline progress events.

A contextvar-scoped callback lets one pipeline run stream stage events to a
listener (e.g. the SSE endpoint) without threading a parameter through every
stage. Emission is strictly best-effort: a broken listener never breaks the
pipeline.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextvars import ContextVar, Token
from typing import Any

import structlog

logger = structlog.get_logger()

ProgressCallback = Callable[[dict[str, Any]], None]

_progress_callback: ContextVar[ProgressCallback | None] = ContextVar("tacit_progress_callback", default=None)

# Cap detail payloads so SSE frames stay small.
_MAX_LIST_ITEMS = 12
_MAX_STR_LEN = 300


def set_progress_callback(callback: ProgressCallback) -> Token:
    """Register *callback* for the current context. Returns a reset token."""
    return _progress_callback.set(callback)


def reset_progress_callback(token: Token) -> None:
    _progress_callback.reset(token)


def _compact(value: Any) -> Any:
    if isinstance(value, str):
        return value[:_MAX_STR_LEN]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_compact(v) for v in list(value)[:_MAX_LIST_ITEMS]]
    if isinstance(value, dict):
        return {str(k)[:_MAX_STR_LEN]: _compact(v) for k, v in list(value.items())[:_MAX_LIST_ITEMS]}
    return str(value)[:_MAX_STR_LEN]


def emit_progress(stage: str, status: str = "info", reason: str = "", **details: Any) -> None:
    """Emit a progress event to the registered listener, if any. Never raises."""
    callback = _progress_callback.get()
    if callback is None:
        return
    try:
        callback(
            {
                "stage": stage,
                "status": status,
                "reason": reason,
                "ts": time.time(),
                "details": {k: _compact(v) for k, v in details.items()},
            }
        )
    except Exception:
        logger.warning("progress_emit_failed", stage=stage, exc_info=True)
