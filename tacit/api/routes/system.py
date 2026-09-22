"""System and static UI routes."""

from __future__ import annotations

import json
import socket
import time
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from tacit.api.optional_integrations import optional_integration_degradation
from tacit.config import Settings, canonical_api_allowed_hosts
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
_CONTAINER_HEALTH_RECV_BYTES = 4 * 1024


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
    deadline = time.monotonic() + _CONTAINER_HEALTH_DEADLINE_SECONDS
    active_settings = runtime_settings if runtime_settings is not None else Settings()
    configured = canonical_api_allowed_hosts(getattr(active_settings, "api_allowed_hosts", ""))
    first_allowed = configured.split(",", maxsplit=1)[0]
    host = f"health.{first_allowed[2:]}" if first_allowed.startswith("*.") else first_allowed
    request = (
        f"GET {_CONTAINER_HEALTH_PATH} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Accept: application/json\r\n"
        "Accept-Encoding: identity\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")

    try:
        connection = socket.create_connection(
            (_CONTAINER_HEALTH_ADDRESS, _CONTAINER_HEALTH_PORT),
            timeout=_container_health_timeout(deadline),
        )
        with connection:
            _container_health_send(connection, request, deadline=deadline)
            status, headers, body = _container_health_response(connection, deadline=deadline)
    except TimeoutError:
        raise
    except OSError as exc:
        raise RuntimeError("Container health check transport failed") from exc

    if not 200 <= status < 300:
        raise RuntimeError("Container health check returned a non-success HTTP status")
    content_types = headers.get("content-type", ())
    if len(content_types) != 1 or content_types[0].split(";", maxsplit=1)[0].strip().casefold() != "application/json":
        raise RuntimeError("Container health check did not return JSON content")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise RuntimeError("Container health check returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Container health check must return a JSON object")
    if payload.get("status") != "ok":
        raise RuntimeError("Container health check status is not ok")


def _container_health_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Container health check absolute deadline exceeded")
    return min(remaining, _CONTAINER_HEALTH_IO_TIMEOUT_SECONDS)


def _container_health_send(connection: socket.socket, payload: bytes, *, deadline: float) -> None:
    remaining = memoryview(payload)
    while remaining:
        connection.settimeout(_container_health_timeout(deadline))
        sent = connection.send(remaining)
        if sent <= 0:
            raise RuntimeError("Container health check request could not be sent")
        remaining = remaining[sent:]


def _container_health_recv(connection: socket.socket, size: int, *, deadline: float) -> bytes:
    connection.settimeout(_container_health_timeout(deadline))
    try:
        return connection.recv(size)
    except TimeoutError as exc:
        raise TimeoutError("Container health check absolute deadline exceeded") from exc


def _container_health_response(
    connection: socket.socket,
    *,
    deadline: float,
) -> tuple[int, dict[str, tuple[str, ...]], bytes]:
    response = bytearray()
    header_end = -1
    while header_end < 0:
        chunk = _container_health_recv(connection, _CONTAINER_HEALTH_RECV_BYTES, deadline=deadline)
        if not chunk:
            raise RuntimeError("Container health check returned incomplete HTTP headers")
        response.extend(chunk)
        header_end = response.find(b"\r\n\r\n")
        if header_end < 0 and len(response) > _CONTAINER_HEALTH_MAX_HEADER_BYTES:
            raise RuntimeError("Container health check response exceeded the header size limit")
    if header_end > _CONTAINER_HEALTH_MAX_HEADER_BYTES:
        raise RuntimeError("Container health check response exceeded the header size limit")

    header_bytes = bytes(response[:header_end])
    body_prefix = bytes(response[header_end + 4 :])
    status, headers = _container_health_headers(header_bytes)
    if not 200 <= status < 300:
        return status, headers, b""
    if headers.get("transfer-encoding"):
        raise RuntimeError("Container health check returned unsupported transfer encoding")
    content_encodings = headers.get("content-encoding", ())
    if content_encodings and any(value.strip().casefold() != "identity" for value in content_encodings):
        raise RuntimeError("Container health check returned unsupported content encoding")

    content_lengths = headers.get("content-length", ())
    if len(content_lengths) > 1:
        raise RuntimeError("Container health check returned ambiguous content length")
    if content_lengths:
        raw_length = content_lengths[0]
        if not raw_length.isascii() or not raw_length.isdigit():
            raise RuntimeError("Container health check returned invalid content length")
        expected_length = int(raw_length)
        if expected_length > _CONTAINER_HEALTH_MAX_BODY_BYTES:
            raise RuntimeError("Container health check response exceeded the body size limit")
        if len(body_prefix) > expected_length:
            raise RuntimeError("Container health check returned excess response bytes")
        body = bytearray(body_prefix)
        while len(body) < expected_length:
            chunk = _container_health_recv(
                connection,
                min(_CONTAINER_HEALTH_RECV_BYTES, expected_length - len(body)),
                deadline=deadline,
            )
            if not chunk:
                raise RuntimeError("Container health check returned an incomplete response body")
            body.extend(chunk)
        return status, headers, bytes(body)

    body = bytearray(body_prefix)
    if len(body) > _CONTAINER_HEALTH_MAX_BODY_BYTES:
        raise RuntimeError("Container health check response exceeded the body size limit")
    while True:
        chunk = _container_health_recv(
            connection,
            min(_CONTAINER_HEALTH_RECV_BYTES, _CONTAINER_HEALTH_MAX_BODY_BYTES + 1 - len(body)),
            deadline=deadline,
        )
        if not chunk:
            return status, headers, bytes(body)
        body.extend(chunk)
        if len(body) > _CONTAINER_HEALTH_MAX_BODY_BYTES:
            raise RuntimeError("Container health check response exceeded the body size limit")


def _container_health_headers(header_bytes: bytes) -> tuple[int, dict[str, tuple[str, ...]]]:
    lines = header_bytes.split(b"\r\n")
    try:
        status_line = lines[0].decode("ascii")
    except (IndexError, UnicodeDecodeError) as exc:
        raise RuntimeError("Container health check returned an invalid HTTP status") from exc
    status_parts = status_line.split(" ", maxsplit=2)
    if (
        len(status_parts) < 2
        or status_parts[0] not in {"HTTP/1.0", "HTTP/1.1"}
        or len(status_parts[1]) != 3
        or not status_parts[1].isdigit()
    ):
        raise RuntimeError("Container health check returned an invalid HTTP status")

    parsed: dict[str, list[str]] = {}
    for line in lines[1:]:
        if not line or line[:1] in {b" ", b"\t"} or b":" not in line:
            raise RuntimeError("Container health check returned invalid HTTP headers")
        raw_name, raw_value = line.split(b":", maxsplit=1)
        try:
            name = raw_name.decode("ascii").casefold()
            value = raw_value.decode("latin-1").strip()
        except UnicodeDecodeError as exc:
            raise RuntimeError("Container health check returned invalid HTTP headers") from exc
        if (
            not name
            or any(not (character.isalnum() or character == "-") for character in name)
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        ):
            raise RuntimeError("Container health check returned invalid HTTP headers")
        parsed.setdefault(name, []).append(value)
    return int(status_parts[1]), {name: tuple(values) for name, values in parsed.items()}


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
