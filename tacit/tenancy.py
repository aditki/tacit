"""Framework-neutral tenant boundary validation."""

from __future__ import annotations

import re

MAX_TENANT_LENGTH = 128


class TenantBoundaryError(ValueError):
    """A tenant selection is invalid or crosses the configured boundary."""

    def __init__(self, detail: str, *, status_code: int):
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


def resolve_tenant_boundary(
    configured_value: str,
    requested_value: str | None,
    *,
    reject_pinned_override: bool = True,
) -> str:
    """Resolve and validate a concrete tenant for pinned or wildcard runtimes."""
    configured = configured_value.strip() or "default"
    requested = (requested_value or "").strip()
    if configured == "*" and not requested:
        raise TenantBoundaryError("Knowledge tenant is required", status_code=400)
    if configured != "*":
        if reject_pinned_override and requested and requested != configured:
            raise TenantBoundaryError("Tenant access denied", status_code=403)
        requested = configured
    if not requested or len(requested) > MAX_TENANT_LENGTH or re.fullmatch(r"[A-Za-z0-9_.:-]+", requested) is None:
        raise TenantBoundaryError("Invalid knowledge tenant", status_code=400)
    return requested
