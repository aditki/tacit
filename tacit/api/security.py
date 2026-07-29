"""API authentication and request sanitation helpers."""

from __future__ import annotations

import secrets
from enum import StrEnum
from typing import Any, Final

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from tacit.config import settings
from tacit.tenancy import MAX_TENANT_LENGTH as MAX_TENANT_LENGTH
from tacit.tenancy import TenantBoundaryError, resolve_tenant_boundary

MAX_PROMPT_LENGTH = 2000

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class KnowledgeAction(StrEnum):
    """Product actions mapped to their server-side authorization requirements."""

    READ = "read"
    APPROVE = "approve"
    TRUST = "trust"
    REJECT = "reject"
    CORRECT = "correct"
    APPLY = "apply"
    EXPORT = "export"
    OVERRIDE = "override"
    TEACH_SIGNALS = "teach_signals"


KNOWLEDGE_ACTION_PERMISSIONS: Final[dict[KnowledgeAction, tuple[str, ...]]] = {
    KnowledgeAction.READ: ("knowledge.read",),
    KnowledgeAction.APPROVE: ("knowledge.review",),
    KnowledgeAction.TRUST: ("knowledge.review", "knowledge.trust"),
    KnowledgeAction.REJECT: ("knowledge.reject",),
    KnowledgeAction.CORRECT: ("knowledge.correct",),
    KnowledgeAction.APPLY: ("knowledge.apply",),
    KnowledgeAction.EXPORT: ("knowledge.read", "knowledge.export"),
    KnowledgeAction.OVERRIDE: ("knowledge.override",),
    KnowledgeAction.TEACH_SIGNALS: ("knowledge.review", "knowledge.trust"),
}


async def verify_api_key(request: Request, api_key: str | None = Security(api_key_header)) -> None:
    """Verify API key if auth is enabled. No-op when disabled."""
    runtime_settings = getattr(request.app.state, "settings", settings)
    if not runtime_settings.api_auth_enabled:
        return
    configured_tenant = str(runtime_settings.knowledge_tenant_id or "default")
    expected_key = runtime_settings.api_auth_key
    if configured_tenant == "*":
        selected_tenant = resolve_knowledge_tenant(
            configured_tenant,
            request.headers.get("X-Tacit-Tenant"),
        )
        tenant_keys = dict(getattr(runtime_settings, "knowledge_tenant_api_keys", {}) or {})
        expected_key = tenant_keys.get(selected_tenant, "")
    if not api_key or not expected_key or not secrets.compare_digest(api_key, expected_key):
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


def assert_contract_tenant_access(
    request: Request,
    contract: Any,
    *,
    store: Any | None = None,
    runtime_settings: Any | None = None,
) -> str:
    """Authorize a contract using the recorded row tenant for legacy payloads."""
    active_settings = runtime_settings or getattr(request.app.state, "settings", settings)
    configured = str(getattr(active_settings, "knowledge_tenant_id", "default") or "default")
    contract_tenant = str(contract.request.scope.tenant_id or "")
    investigation_id = str(getattr(getattr(contract, "investigation", None), "id", ""))
    selected_tenant = knowledge_tenant(request)
    investigation = (
        store.get(investigation_id, tenant_id=selected_tenant)
        if store is not None and investigation_id and hasattr(store, "get")
        else None
    )
    recorded_tenant = str((investigation or {}).get("tenant_id") or "")
    if not recorded_tenant:
        if contract_tenant not in {"", "default"}:
            recorded_tenant = contract_tenant
        elif configured != "*":
            recorded_tenant = configured
        else:
            recorded_tenant = contract_tenant
    if contract_tenant not in {"", "default", recorded_tenant}:
        raise HTTPException(status_code=403, detail="Tenant access denied")
    if selected_tenant != recorded_tenant:
        raise HTTPException(status_code=403, detail="Tenant access denied")
    return selected_tenant


def resolve_knowledge_tenant(
    configured_value: str,
    requested_value: str | None,
    *,
    reject_pinned_override: bool = True,
) -> str:
    """Resolve and validate a tenant against a pinned or wildcard boundary."""
    try:
        return resolve_tenant_boundary(
            configured_value,
            requested_value,
            reject_pinned_override=reject_pinned_override,
        )
    except TenantBoundaryError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


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
    runtime_settings = getattr(request.app.state, "settings", settings)
    try:
        enforce_knowledge_action(runtime_settings, action)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def enforce_knowledge_action(runtime_settings: Any, action: KnowledgeAction) -> None:
    """Framework-neutral semantic permission check shared by API and CLI."""
    permissions = {value.strip() for value in str(runtime_settings.knowledge_permissions).split(",") if value.strip()}
    for permission in KNOWLEDGE_ACTION_PERMISSIONS[action]:
        if permission not in permissions:
            raise PermissionError(f"Missing permission: {permission}")


def assert_knowledge_permission(request: Request, permission: str) -> None:
    runtime_settings = getattr(request.app.state, "settings", settings)
    permissions = {value.strip() for value in runtime_settings.knowledge_permissions.split(",") if value.strip()}
    if permission not in permissions:
        raise HTTPException(status_code=403, detail=f"Missing permission: {permission}")
