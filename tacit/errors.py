"""Tacit domain exception taxonomy."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

from tacit.tenancy import TenantBoundaryError


class RuntimeOwnershipError(ValueError):
    """Raised when a runtime owner cannot safely participate in composition."""


class SemanticAuthorizationError(PermissionError):
    """Raised when product policy denies a semantic operation.

    Subclassing ``PermissionError`` preserves existing API and CLI error
    translation without conflating authorization with filesystem access.
    """


AUTHORITY_BOUNDARY_ERRORS = (
    SemanticAuthorizationError,
    RuntimeOwnershipError,
    TenantBoundaryError,
)

_MAX_DIAGNOSTIC_COUNT = 1_000_000


def safe_failure_diagnostics(
    exc: BaseException,
    *,
    reason_code: str,
    counters: Mapping[str, int] | None = None,
) -> dict[str, str | int]:
    """Return bounded diagnostics without messages, tracebacks, or request state."""
    root = exc.__cause__ or exc
    error_class = type(root)
    error_identity = f"{error_class.__module__}.{error_class.__qualname__}"[:512]
    diagnostics: dict[str, str | int] = {
        "error_type": error_class.__name__[:128],
        "failure_fingerprint": hashlib.blake2s(
            f"{reason_code}:{error_identity}".encode(),
            digest_size=6,
        ).hexdigest(),
    }
    for name, value in (counters or {}).items():
        diagnostics[name] = min(max(int(value), 0), _MAX_DIAGNOSTIC_COUNT)
    return diagnostics


def safe_failure_detail(
    exc: BaseException,
    *,
    reason_code: str,
    counters: Mapping[str, int] | None = None,
) -> tuple[str, dict[str, str | int]]:
    """Return a stable durable detail plus its message-free diagnostics."""
    diagnostics = safe_failure_diagnostics(
        exc,
        reason_code=reason_code,
        counters=counters,
    )
    detail = ";".join(
        (
            reason_code,
            f"error_type={diagnostics['error_type']}",
            f"failure_fingerprint={diagnostics['failure_fingerprint']}",
        )
    )
    return detail, diagnostics


class TacitError(Exception):
    """Base class for errors raised by Tacit domain code."""


class RecoverableTacitError(TacitError):
    """A failure that can be recorded and degraded without failing the process."""


class FatalPipelineError(TacitError):
    """A pipeline failure that should stop request processing."""


class PipelineExecutionError(FatalPipelineError):
    """A failed pipeline carrying the non-sensitive lifecycle identity it created."""

    def __init__(
        self,
        message: str,
        *,
        investigation_id: str = "",
        investigation_run_id: str = "",
        investigation_status: str = "failed",
        audit_status: str = "run_created",
    ) -> None:
        super().__init__(message)
        self.investigation_id = investigation_id
        self.investigation_run_id = investigation_run_id
        self.investigation_status = investigation_status
        self.audit_status = audit_status

    def public_payload(self, *, detail: str = "Failed to generate dashboard") -> dict[str, str]:
        """Return the non-sensitive lifecycle envelope safe for API clients."""
        return {
            "detail": detail,
            "investigation_id": self.investigation_id,
            "investigation_run_id": self.investigation_run_id,
            "investigation_status": self.investigation_status,
            "audit_status": self.audit_status,
        }


class BackendUnavailable(RecoverableTacitError):
    """A dashboard backend or datasource is unavailable."""


class HistoryWriteFailed(RecoverableTacitError):
    """A best-effort history write failed."""


class PipelineStageError(RecoverableTacitError):
    """A recoverable stage-level failure."""


class EvidenceResolutionError(PipelineStageError):
    """Evidence resolution failed without invalidating the whole request."""
