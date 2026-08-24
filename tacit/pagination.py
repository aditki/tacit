"""Shared bounded keyset-pagination primitives for long-lived audit stores."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass

CursorValue = str | int | float | None
_MAX_CURSOR_LENGTH = 1_024
MAX_COMPATIBILITY_OFFSET = 10_000


@dataclass(frozen=True)
class KeysetPage[T]:
    """One stable page ordered from newest to oldest."""

    items: list[T]
    has_more: bool
    next_cursor: str | None


def encode_cursor(*values: CursorValue) -> str:
    payload = json.dumps(values, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_cursor(cursor: str, *, field_count: int) -> tuple[CursorValue, ...]:
    """Decode a small opaque cursor and reject malformed or oversized input."""
    if not cursor or len(cursor) > _MAX_CURSOR_LENGTH:
        raise ValueError("invalid pagination cursor")
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.b64decode(cursor + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid pagination cursor") from exc
    if not isinstance(payload, list) or len(payload) != field_count:
        raise ValueError("invalid pagination cursor")
    if any(not isinstance(value, (str, int, float, type(None))) for value in payload):
        raise ValueError("invalid pagination cursor")
    return tuple(payload)
