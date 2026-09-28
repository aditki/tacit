"""System and static UI routes."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from tacit.api.optional_integrations import optional_integration_degradation
from tacit.config import Settings, canonical_api_allowed_hosts
from tacit.container_healthcheck import probe_container_health
from tacit.models.schemas import HealthResponse
from tacit.pipeline_admission import runtime_admission_health_snapshot

router = APIRouter()
_STATIC_DIR = Path(__file__).resolve().parents[2] / "static"
_CONTAINER_HEALTH_ADDRESS = "127.0.0.1"
_CONTAINER_HEALTH_PORT = 8000
_CONTAINER_HEALTH_PATH = "/healthz"
_CONTAINER_HEALTH_DEADLINE_SECONDS = 3.0
_CONTAINER_HEALTH_IO_TIMEOUT_SECONDS = 1.0
_CONTAINER_HEALTH_MAX_HEADER_BYTES = 8 * 1024
_CONTAINER_HEALTH_MAX_BODY_BYTES = 16 * 1024


class OptionalIntegrationHealth(BaseModel):
    """Disclosure-safe status for one optional integration."""

    status: Literal["starting", "reconnecting", "failed", "stopped"]
    reason_code: str


class RequestBodyAdmissionHealth(BaseModel):
    """Payload-free aggregate ingress utilization and configured capacity."""

    active_requests: int
    reserved_bytes: int
    max_concurrent: int
    max_buffered_bytes: int
    active_tenant_partitions: int
    max_concurrent_per_tenant: int
    max_buffered_bytes_per_tenant: int
    rejections: dict[str, int]


class PipelineAdmissionHealth(BaseModel):
    """Cardinality-bounded aggregate pipeline and provider utilization."""

    capacity: int
    active: int
    queued: int
    retained: int
    blocking_in_flight: int
    cleanup_in_flight: int
    service_owner_in_flight: int
    saturated: bool
    fatal: bool
    degraded: bool
    reason_code: str | None = None


class SystemHealthResponse(HealthResponse):
    """Core API health plus optional-integration degradation."""

    degraded: bool | None = Field(default=None, description="Whether required or optional runtime work is degraded")
    optional_integrations: dict[str, OptionalIntegrationHealth] | None = Field(
        default=None,
        description="Sanitized degradation details for optional integrations",
    )
    request_body_admission: RequestBodyAdmissionHealth | None = Field(
        default=None,
        description="Aggregate authenticated request-body admission utilization",
    )
    pipeline_admission: PipelineAdmissionHealth | None = Field(
        default=None,
        description="Aggregate process-level pipeline and provider admission utilization",
    )


@router.get(
    "/healthz",
    tags=["System"],
    summary="Health check",
    response_model=SystemHealthResponse,
    response_model_exclude_none=True,
    response_description="Server health status",
    responses={
        503: {
            "description": "Runtime is fatally fenced and requires restart",
            "model": SystemHealthResponse,
        }
    },
)
async def healthz(request: Request):
    """Lightweight health check for load balancers and orchestrators."""
    status_code, response = _health_response(request)
    if status_code != 200:
        return JSONResponse(status_code=status_code, content=response)
    return response


@router.head("/healthz", include_in_schema=False)
async def healthz_head(request: Request) -> Response:
    """Return health status without a response body."""
    status_code, _response = _health_response(request)
    return Response(status_code=status_code, media_type="application/json")


def _health_response(request: Request) -> tuple[int, dict[str, object]]:
    optional_degradation = optional_integration_degradation(request.app)
    response: dict[str, object] = {"status": "ok"}
    request_body_admission = _request_body_admission_health(request.app)
    if request_body_admission is not None:
        response["request_body_admission"] = request_body_admission
    pipeline_admission = _pipeline_admission_health(request.app)
    if pipeline_admission is not None:
        response["pipeline_admission"] = pipeline_admission
    if optional_degradation:
        response["optional_integrations"] = optional_degradation
    if optional_degradation or (pipeline_admission is not None and pipeline_admission["degraded"]):
        response["degraded"] = True
    if pipeline_admission is not None and pipeline_admission["fatal"]:
        response["status"] = "failed"
        return 503, response
    return 200, response


def container_healthcheck(*, runtime_settings: Any | None = None) -> None:
    """Probe the fixed loopback API under one bounded, proxy-free deadline."""
    active_settings = runtime_settings if runtime_settings is not None else Settings()
    configured = canonical_api_allowed_hosts(getattr(active_settings, "api_allowed_hosts", ""))
    first_allowed = configured.split(",", maxsplit=1)[0]
    host = f"health.{first_allowed[2:]}" if first_allowed.startswith("*.") else first_allowed
    probe_container_health(
        host=host,
        address=_CONTAINER_HEALTH_ADDRESS,
        port=_CONTAINER_HEALTH_PORT,
        path=_CONTAINER_HEALTH_PATH,
        deadline_seconds=_CONTAINER_HEALTH_DEADLINE_SECONDS,
        io_timeout_seconds=_CONTAINER_HEALTH_IO_TIMEOUT_SECONDS,
        max_header_bytes=_CONTAINER_HEALTH_MAX_HEADER_BYTES,
        max_body_bytes=_CONTAINER_HEALTH_MAX_BODY_BYTES,
    )


def _request_body_admission_health(app: Any) -> dict[str, object] | None:
    controller = getattr(app.state, "request_body_admission", None)
    if controller is None:
        return None
    snapshot = controller.snapshot()
    return {
        "active_requests": snapshot.active_requests,
        "reserved_bytes": snapshot.reserved_bytes,
        "max_concurrent": snapshot.max_concurrent,
        "max_buffered_bytes": snapshot.max_buffered_bytes,
        "active_tenant_partitions": snapshot.active_tenant_partitions,
        "max_concurrent_per_tenant": snapshot.max_concurrent_per_tenant,
        "max_buffered_bytes_per_tenant": snapshot.max_buffered_bytes_per_tenant,
        "rejections": {reason.value: count for reason, count in snapshot.rejections_by_reason},
    }


def _pipeline_admission_health(app: Any) -> dict[str, object] | None:
    runtime_stores = getattr(app.state, "runtime_stores", None)
    if runtime_stores is None:
        return None
    runtime_identity = runtime_stores.runtime_ownership.admission_namespace
    snapshot = runtime_admission_health_snapshot(runtime_identity or "")
    if snapshot is None:
        return None
    response: dict[str, object] = {
        "capacity": snapshot.capacity,
        "active": snapshot.active,
        "queued": snapshot.queued,
        "retained": snapshot.retained,
        "blocking_in_flight": snapshot.blocking_in_flight,
        "cleanup_in_flight": snapshot.cleanup_in_flight,
        "service_owner_in_flight": snapshot.service_owner_in_flight,
        "saturated": snapshot.saturated,
        "fatal": snapshot.fatal,
        "degraded": snapshot.degraded,
    }
    if snapshot.reason_code is not None:
        response["reason_code"] = snapshot.reason_code
    return response


@router.get("/", include_in_schema=False)
@router.head("/", include_in_schema=False)
async def web_ui():
    return FileResponse(_STATIC_DIR / "index.html", media_type="text/html")


if __name__ == "__main__":
    container_healthcheck()
