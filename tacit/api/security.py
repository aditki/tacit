"""API authentication and request sanitation helpers."""

from __future__ import annotations

import re
import secrets
from enum import StrEnum
from typing import Final

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from tacit.config import settings

MAX_PROMPT_LENGTH = 2000
MAX_TENANT_LENGTH = 128

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class KnowledgeAction(StrEnum):
    """Product actions mapped to their server-side authorization requirements."""

    READ = "read"
    APPROVE = "approve"
    TRUST = "trust"
    REJECT = "reject"
    CORRECT = "correct"
    EXPORT = "export"
    OVERRIDE = "override"
    TEACH_SIGNALS = "teach_signals"


KNOWLEDGE_ACTION_PERMISSIONS: Final[dict[KnowledgeAction, tuple[str, ...]]] = {
    KnowledgeAction.READ: ("knowledge.read",),
    KnowledgeAction.APPROVE: ("knowledge.review",),
    KnowledgeAction.TRUST: ("knowledge.review", "knowledge.trust"),
    KnowledgeAction.REJECT: ("knowledge.reject",),
    KnowledgeAction.CORRECT: ("knowledge.correct",),
    KnowledgeAction.EXPORT: ("knowledge.export",),
    KnowledgeAction.OVERRIDE: ("knowledge.override",),
    KnowledgeAction.TEACH_SIGNALS: ("knowledge.review", "knowledge.trust"),
}


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


def assert_tenant_access(request: Request, resource_tenant: str) -> str:
    """Require the selected tenant to own a persisted resource."""
    selected_tenant = knowledge_tenant(request)
    if not resource_tenant or selected_tenant != resource_tenant:
        raise HTTPException(status_code=403, detail="Tenant access denied")
    return selected_tenant


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


def require_knowledge_action(action: KnowledgeAction):
    """Build a dependency for a semantic Operational Knowledge action."""

    async def dependency(request: Request) -> None:
        assert_knowledge_action(request, action)

    return dependency


def assert_knowledge_action(request: Request, action: KnowledgeAction) -> None:
    """Authorize every permission required by a semantic product action."""
    for permission in KNOWLEDGE_ACTION_PERMISSIONS[action]:
        assert_knowledge_permission(request, permission)


def assert_knowledge_permission(request: Request, permission: str) -> None:
    runtime_settings = getattr(request.app.state, "settings", settings)
    permissions = {value.strip() for value in runtime_settings.knowledge_permissions.split(",") if value.strip()}
    if permission not in permissions:
        raise HTTPException(status_code=403, detail=f"Missing permission: {permission}")
