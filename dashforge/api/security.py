"""API authentication and request sanitation helpers."""

from __future__ import annotations

import secrets

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from dashforge.config import settings

MAX_PROMPT_LENGTH = 2000

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(request: Request, api_key: str | None = Security(api_key_header)) -> None:
    """Verify API key if auth is enabled. No-op when disabled."""
    runtime_settings = getattr(request.app.state, "settings", settings)
    if not runtime_settings.api_auth_enabled:
        return
    if not api_key or not secrets.compare_digest(api_key, runtime_settings.api_auth_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def sanitize_prompt(prompt: str) -> str:
    """Basic prompt sanitization — length cap and control char removal."""
    cleaned = "".join(c for c in prompt if c == "\n" or (c.isprintable() and ord(c) < 0x10000))
    return cleaned[:MAX_PROMPT_LENGTH].strip()
