"""API authentication and request sanitation helpers."""

from __future__ import annotations

import re
import secrets

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from tacit.config import settings

MAX_PROMPT_LENGTH = 2000
MAX_TENANT_LENGTH = 128

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


def knowledge_tenant(request: Request) -> str:
    """Resolve a tenant without allowing a request to cross the configured boundary."""
    runtime_settings = getattr(request.app.state, "settings", settings)
    return resolve_knowledge_tenant(
        runtime_settings.knowledge_tenant_id,
        request.headers.get("X-Tacit-Tenant"),
    )


def resolve_knowledge_tenant(
    configured_value: str,
    requested_value: str | None,
    *,
    reject_pinned_override: bool = True,
) -> str:
    """Resolve and validate a tenant against a pinned or wildcard boundary."""
    configured = configured_value.strip() or "default"
    requested = (requested_value or "").strip()
    if configured == "*" and not requested:
        raise HTTPException(status_code=400, detail="Knowledge tenant is required")
    if configured != "*":
        if reject_pinned_override and requested and requested != configured:
            raise HTTPException(status_code=403, detail="Tenant access denied")
        requested = configured
    if not requested or len(requested) > MAX_TENANT_LENGTH or re.fullmatch(r"[A-Za-z0-9_.:-]+", requested) is None:
        raise HTTPException(status_code=400, detail="Invalid knowledge tenant")
    return requested


def require_knowledge_permission(permission: str):
    """Build a dependency backed by server-side permission configuration."""

    async def dependency(request: Request) -> None:
        assert_knowledge_permission(request, permission)

    return dependency


def assert_knowledge_permission(request: Request, permission: str) -> None:
    runtime_settings = getattr(request.app.state, "settings", settings)
    permissions = {value.strip() for value in runtime_settings.knowledge_permissions.split(",") if value.strip()}
    if permission not in permissions:
        raise HTTPException(status_code=403, detail=f"Missing permission: {permission}")
