"""FastAPI app factory and OpenAPI metadata."""

from __future__ import annotations

import math
from typing import Any

import structlog
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from tacit import __version__
from tacit.api.request_body_limit import RequestBodyAdmissionController, RequestBodyLimitMiddleware
from tacit.config import (
    API_MAX_REQUEST_BODY_BYTES_MAX,
    API_MAX_REQUEST_BODY_BYTES_MIN,
    API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR,
    API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX,
    API_REQUEST_BODY_MAX_CONCURRENT_MAX,
    API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MAX,
    API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MIN,
    API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MAX,
    API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
    API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MAX,
    API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MIN,
    DEFAULT_API_MAX_REQUEST_BODY_BYTES,
    DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES,
    DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT,
    DEFAULT_API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR,
    DEFAULT_API_REQUEST_BODY_MEMORY_FLOOR_BYTES,
    DEFAULT_API_REQUEST_BODY_READ_TIMEOUT_SECONDS,
    DEFAULT_API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES,
    DEFAULT_API_REQUEST_BODY_TENANT_MAX_CONCURRENT,
    Settings,
    api_host_is_allowed,
    canonical_api_allowed_hosts,
    canonical_api_host_name,
    canonical_cors_allowed_origins,
)
from tacit.config import settings as default_settings
from tacit.runtime_stores import RuntimeStores

LifespanFactory = Any
_DEFAULT_LIFESPAN = object()

_PRIVATE_NO_STORE = b"private, no-store"
_FRAME_DENY = b"DENY"
_FRAME_ANCESTORS_NONE = b"frame-ancestors 'none'"
_VALIDATION_ERROR_LIMIT = 16
_VALIDATION_LOCATION_DEPTH_LIMIT = 8
_VALIDATION_TYPE_MAX_LENGTH = 64
_VALIDATION_LOCATION_SOURCES = frozenset({"body", "cookie", "header", "path", "query"})
_STATE_CHANGING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

logger = structlog.get_logger()


def _bounded_validation_type(raw_type: object) -> str:
    if not isinstance(raw_type, str) or not 1 <= len(raw_type) <= _VALIDATION_TYPE_MAX_LENGTH:
        return "request_invalid"
    if not all(character.isascii() and (character.isalnum() or character in "_.-") for character in raw_type):
        return "request_invalid"
    return raw_type


def _bounded_validation_location(raw_location: object) -> list[str]:
    if not isinstance(raw_location, (list, tuple)) or not raw_location:
        return ["request"]
    source = raw_location[0]
    location = [source if isinstance(source, str) and source in _VALIDATION_LOCATION_SOURCES else "request"]
    remaining = raw_location[1:_VALIDATION_LOCATION_DEPTH_LIMIT]
    location.extend("item" if isinstance(component, int) else "field" for component in remaining)
    if len(raw_location) > _VALIDATION_LOCATION_DEPTH_LIMIT:
        location.append("nested")
    return location


def _bounded_validation_message(error_type: str) -> str:
    messages = {
        "extra_forbidden": "Unexpected field",
        "json_invalid": "Request body contains invalid JSON",
        "list_type": "Value must be a list",
        "literal_error": "Value is not an allowed choice",
        "missing": "Field required",
        "model_attributes_type": "Request body must be an object",
        "string_too_long": "String exceeds the maximum allowed length",
        "string_too_short": "String is shorter than the minimum allowed length",
    }
    if error_type in messages:
        return messages[error_type]
    if error_type.endswith("_parsing") or error_type.endswith("_type"):
        return "Value has an invalid type or format"
    return "Invalid request value"


async def _bounded_request_validation_error(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    raw_errors = exc.errors() if isinstance(exc, RequestValidationError) else []
    errors: list[dict[str, object]] = []
    for raw_error in raw_errors[:_VALIDATION_ERROR_LIMIT]:
        error = raw_error if isinstance(raw_error, dict) else {}
        error_type = _bounded_validation_type(error.get("type"))
        errors.append(
            {
                "type": error_type,
                "loc": _bounded_validation_location(error.get("loc")),
                "msg": _bounded_validation_message(error_type),
            }
        )
    if not errors:
        errors.append(
            {
                "type": "request_invalid",
                "loc": ["request"],
                "msg": "Invalid request value",
            }
        )
    logger.info(
        "request_validation_rejected",
        reason_code="request_validation_failed",
        error_count=len(errors),
        errors_truncated=len(raw_errors) > _VALIDATION_ERROR_LIMIT,
    )
    return JSONResponse(
        {"detail": errors},
        status_code=422,
        headers={"Connection": "close"},
    )


class ResponseSecurityHeadersMiddleware:
    """Keep browser credentials and tenant responses out of shared contexts."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        response_started = False

        async def secure_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                headers = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower()
                    not in {
                        b"cache-control",
                        b"content-security-policy",
                        b"x-frame-options",
                    }
                ]
                headers.extend(
                    (
                        (b"cache-control", _PRIVATE_NO_STORE),
                        (b"content-security-policy", _FRAME_ANCESTORS_NONE),
                        (b"x-frame-options", _FRAME_DENY),
                    )
                )
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, secure_send)
        except Exception:
            if not response_started:
                response = PlainTextResponse(
                    "Internal Server Error",
                    status_code=500,
                    headers={"Connection": "close"},
                )
                await response(scope, receive, secure_send)
            raise


class PreBodyTerminalCORSMiddleware(CORSMiddleware):
    """Authorize browser mutations and close pre-body CORS responses."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_headers = Headers(scope=scope)
        origin_values = request_headers.getlist("origin")
        method = str(scope.get("method", "")).upper()
        is_preflight = method == "OPTIONS" and "access-control-request-method" in request_headers
        if (
            method in _STATE_CHANGING_METHODS
            and origin_values
            and not is_preflight
            and not self._mutation_origin_is_allowed(scope, origin_values)
        ):
            logger.info(
                "cross_origin_mutation_rejected",
                reason_code="cors_origin_not_allowed",
                method=method,
            )
            response_headers = {"Connection": "close"} if str(scope.get("http_version", "")).startswith("1.") else None
            response = JSONResponse(
                {"detail": "Cross-origin mutation is not allowed"},
                status_code=403,
                headers=response_headers,
            )
            await response(scope, receive, send)
            return

        await super().__call__(scope, receive, send)

    def _mutation_origin_is_allowed(self, scope: Scope, origin_values: list[str]) -> bool:
        if self.allow_all_origins:
            return True
        if len(origin_values) != 1:
            return False
        origin = _canonical_single_origin(origin_values[0])
        if origin is None:
            return False
        if self.is_allowed_origin(origin=origin):
            return True
        request_origin = _canonical_request_origin(scope)
        return request_origin is not None and origin == request_origin

    def preflight_response(self, request_headers: Headers) -> Response:
        response = super().preflight_response(request_headers)
        response.headers["Connection"] = "close"
        return response


def _canonical_single_origin(value: str) -> str | None:
    if "," in value:
        return None
    try:
        canonical = canonical_cors_allowed_origins(value)
    except ValueError:
        return None
    if not canonical or canonical == "*" or "," in canonical:
        return None
    return canonical


def _canonical_request_origin(scope: Scope) -> str | None:
    scheme = str(scope.get("scheme", "")).casefold()
    if scheme not in {"http", "https"}:
        return None
    host_values = [value for name, value in scope.get("headers", ()) if bytes(name).lower() == b"host"]
    if len(host_values) != 1:
        return None
    try:
        authority = bytes(host_values[0]).decode("ascii")
    except UnicodeDecodeError:
        return None
    return _canonical_single_origin(f"{scheme}://{authority}")


OPENAPI_TAGS = [
    {
        "name": "Investigation Generation",
        "description": "Generate evidence-grounded observability investigations from natural-language prompts. "
        "The pipeline: Intent Classification → Metric Discovery → Query Building → Artifact Publishing.",
    },
    {
        "name": "Feedback",
        "description": "Submit and retrieve human evaluation feedback for generated dashboards. "
        "Raw feedback is assessment and governed-candidate input; it never changes runtime ranking directly.",
    },
    {
        "name": "Insights",
        "description": "Analyze collected feedback to surface actionable improvement signals: "
        "per-archetype quality, noisy dashboards, metric quality, archetype gaps, and recommendations.",
    },
    {
        "name": "Archetypes",
        "description": "View and manage investigation archetype templates. "
        "Curated archetypes are loaded from packaged data or `TACIT_ARCHETYPES_PATH` "
        "and can be hot-reloaded without restart.",
    },
    {
        "name": "Signals",
        "description": "Semantic signal taxonomy — maps canonical observability concepts "
        "(e.g. 'request_latency', 'error_rate') to environment-specific metrics. "
        "Signals decouple archetypes from raw metric names for portability.",
    },
    {
        "name": "Learning",
        "description": "Learn operational patterns from trusted dashboards and alerts. "
        "Ingests dashboards, extracts metric co-occurrence, panel groupings, "
        "and aggregation patterns, then proposes governed signal mappings. "
        "Generated archetype output is quarantined and disabled by default.",
    },
    {
        "name": "Operational Knowledge",
        "description": "Review, promote, explain, revise, and audit governed operational knowledge.",
    },
    {
        "name": "System",
        "description": "Health checks and system status.",
    },
]

DESCRIPTION = (
    "## Evidence-Grounded Incident Investigation\n\n"
    "Tacit is a multi-agent pipeline that turns plain-English incident descriptions "
    "and trusted operational context into validated observability investigations. "
    "It supports Grafana and SignalFx outputs, works across common datasource types "
    "(Prometheus, CloudWatch, Loki, Elasticsearch, Graphite, InfluxDB, etc.), and uses "
    "LLM-powered intent classification, cross-datasource metric discovery, and "
    "deterministic query building.\n\n"
    "### Key capabilities\n"
    "- **Investigation generation** — describe the incident, get validated evidence artifacts\n"
    "- **Feedback and assessment** — rate dashboards to measure usefulness and identify governed learning candidates\n"
    "- **Curated archetype management** — edit operator-authored templates via YAML and hot-reload without restart; "
    "generated output remains quarantined\n\n"
    "### Authentication\n"
    "When `API_AUTH_ENABLED=true`, pass your key via the `X-API-Key` header. "
    "When disabled (default for development), all endpoints are open. Wildcard multi-tenant "
    "operation requires API authentication and tenant-specific keys.\n\n"
    "### Interactive docs\n"
    "- **Swagger UI** — you are here (`/docs`)\n"
    "- **ReDoc** — alternative view at [`/redoc`](/redoc)\n"
    "- **Web UI** — interactive investigation workspace at [`/`](/)\n"
)


class ExplicitHostPolicyMiddleware:
    """Reject requests whose canonical Host is outside the configured policy."""

    def __init__(self, app: Any, *, allowed_hosts: list[str]) -> None:
        self.app = app
        self.allowed_hosts = tuple(allowed_hosts)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        host_values = [value for name, value in scope.get("headers", ()) if bytes(name).lower() == b"host"]
        host = _canonical_request_host(host_values[0]) if len(host_values) == 1 else None
        if host is not None and api_host_is_allowed(host, self.allowed_hosts):
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        response = PlainTextResponse(
            "Invalid host header",
            status_code=400,
            headers={"Connection": "close"},
        )
        await response(scope, receive, send)


def _canonical_request_host(value: bytes) -> str | None:
    try:
        raw = value.decode("ascii")
    except UnicodeDecodeError:
        return None
    if (
        not raw
        or raw != raw.strip()
        or any(character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F for character in raw)
        or any(character in raw for character in ("/", "\\", "@", ","))
    ):
        return None
    if raw.startswith("["):
        closing = raw.find("]")
        if closing < 0:
            return None
        host_value = raw[: closing + 1]
        remainder = raw[closing + 1 :]
        if remainder and not _valid_host_port(remainder):
            return None
    else:
        if raw.count(":") > 1:
            return None
        host_value, separator, port = raw.partition(":")
        if separator and not _valid_host_port(f":{port}"):
            return None
    try:
        return canonical_api_host_name(host_value)
    except ValueError:
        return None


def _valid_host_port(value: str) -> bool:
    if not value.startswith(":") or not value[1:].isdigit():
        return False
    port = int(value[1:])
    return 1 <= port <= 65_535


def _validate_api_runtime_settings(
    runtime_settings: Any,
) -> tuple[list[str], list[str], int, int, int, int, int, int, int, float]:
    configured_tenant = str(getattr(runtime_settings, "knowledge_tenant_id", "default") or "default")
    wildcard_tenancy = configured_tenant == "*"
    if wildcard_tenancy and not bool(getattr(runtime_settings, "api_auth_enabled", False)):
        raise ValueError("Wildcard knowledge tenancy requires API authentication")
    max_request_body_bytes = _request_body_limit(runtime_settings)
    (
        max_concurrent,
        max_buffered_bytes,
        tenant_max_concurrent,
        tenant_max_buffered_bytes,
        memory_amplification_factor,
        memory_floor_bytes,
        read_timeout_seconds,
    ) = _request_body_admission_settings(
        runtime_settings,
        max_request_body_bytes=max_request_body_bytes,
        wildcard_tenancy=wildcard_tenancy,
    )
    return (
        _allowed_hosts(runtime_settings),
        _cors_origins(runtime_settings),
        max_request_body_bytes,
        max_concurrent,
        max_buffered_bytes,
        tenant_max_concurrent,
        tenant_max_buffered_bytes,
        memory_amplification_factor,
        memory_floor_bytes,
        read_timeout_seconds,
    )


def _allowed_hosts(runtime_settings: Any) -> list[str]:
    configured = canonical_api_allowed_hosts(getattr(runtime_settings, "api_allowed_hosts", ""))
    return configured.split(",")


def _cors_origins(runtime_settings: Any) -> list[str]:
    configured_value = canonical_cors_allowed_origins(getattr(runtime_settings, "api_cors_allowed_origins", ""))
    configured = [origin for origin in configured_value.split(",") if origin]
    auth_enabled = bool(getattr(runtime_settings, "api_auth_enabled", False))
    if auth_enabled and "*" in configured:
        raise ValueError("Authenticated API deployments cannot use wildcard CORS")
    if configured:
        return configured
    return []


def _request_body_limit(runtime_settings: Any) -> int:
    value = getattr(
        runtime_settings,
        "api_max_request_body_bytes",
        DEFAULT_API_MAX_REQUEST_BODY_BYTES,
    )
    if type(value) is not int or value < API_MAX_REQUEST_BODY_BYTES_MIN or value > API_MAX_REQUEST_BODY_BYTES_MAX:
        raise ValueError(
            "api_max_request_body_bytes must be an integer between "
            f"{API_MAX_REQUEST_BODY_BYTES_MIN} and {API_MAX_REQUEST_BODY_BYTES_MAX}"
        )
    return value


def _request_body_admission_settings(
    runtime_settings: Any,
    *,
    max_request_body_bytes: int,
    wildcard_tenancy: bool,
) -> tuple[int, int, int, int, int, int, float]:
    max_concurrent = getattr(
        runtime_settings,
        "api_request_body_max_concurrent",
        DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT,
    )
    if type(max_concurrent) is not int or not 1 <= max_concurrent <= API_REQUEST_BODY_MAX_CONCURRENT_MAX:
        raise ValueError(
            "api_request_body_max_concurrent must be an integer between " f"1 and {API_REQUEST_BODY_MAX_CONCURRENT_MAX}"
        )

    max_buffered_bytes = getattr(
        runtime_settings,
        "api_request_body_max_buffered_bytes",
        DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES,
    )
    if type(max_buffered_bytes) is not int or not 1 <= max_buffered_bytes <= API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX:
        raise ValueError(
            "api_request_body_max_buffered_bytes must be an integer between "
            f"1 and {API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX}"
        )

    tenant_max_concurrent = getattr(
        runtime_settings,
        "api_request_body_tenant_max_concurrent",
        DEFAULT_API_REQUEST_BODY_TENANT_MAX_CONCURRENT,
    )
    if type(tenant_max_concurrent) is not int or not 1 <= tenant_max_concurrent <= max_concurrent:
        raise ValueError(
            "api_request_body_tenant_max_concurrent must be an integer between " "1 and api_request_body_max_concurrent"
        )
    if wildcard_tenancy and tenant_max_concurrent >= max_concurrent:
        raise ValueError(
            "api_request_body_tenant_max_concurrent must be lower than "
            "api_request_body_max_concurrent for wildcard tenancy"
        )

    tenant_max_buffered_bytes = getattr(
        runtime_settings,
        "api_request_body_tenant_max_buffered_bytes",
        DEFAULT_API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES,
    )
    if type(tenant_max_buffered_bytes) is not int or not 1 <= tenant_max_buffered_bytes <= max_buffered_bytes:
        raise ValueError(
            "api_request_body_tenant_max_buffered_bytes must be an integer between "
            "1 and api_request_body_max_buffered_bytes"
        )
    if wildcard_tenancy and tenant_max_buffered_bytes >= max_buffered_bytes:
        raise ValueError(
            "api_request_body_tenant_max_buffered_bytes must be lower than "
            "api_request_body_max_buffered_bytes for wildcard tenancy"
        )

    memory_amplification_factor = getattr(
        runtime_settings,
        "api_request_body_memory_amplification_factor",
        DEFAULT_API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR,
    )
    if (
        type(memory_amplification_factor) is not int
        or not API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MIN
        <= memory_amplification_factor
        <= API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MAX
    ):
        raise ValueError(
            "api_request_body_memory_amplification_factor must be an integer between "
            f"{API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MIN} and "
            f"{API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MAX}"
        )

    memory_floor_bytes = getattr(
        runtime_settings,
        "api_request_body_memory_floor_bytes",
        DEFAULT_API_REQUEST_BODY_MEMORY_FLOOR_BYTES,
    )
    if (
        type(memory_floor_bytes) is not int
        or not API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN <= memory_floor_bytes <= API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MAX
    ):
        raise ValueError(
            "api_request_body_memory_floor_bytes must be an integer between "
            f"{API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN} and {API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MAX}"
        )

    effective_memory_amplification_factor = max(
        memory_amplification_factor,
        API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR,
    )
    required_memory = max(
        memory_floor_bytes,
        max_request_body_bytes * effective_memory_amplification_factor,
    )
    if max_buffered_bytes < required_memory:
        raise ValueError(
            "api_request_body_max_buffered_bytes must admit the configured maximum request memory envelope"
        )
    if tenant_max_buffered_bytes < required_memory:
        raise ValueError(
            "api_request_body_tenant_max_buffered_bytes must admit the configured maximum request memory envelope"
        )

    read_timeout_seconds = getattr(
        runtime_settings,
        "api_request_body_read_timeout_seconds",
        DEFAULT_API_REQUEST_BODY_READ_TIMEOUT_SECONDS,
    )
    if (
        isinstance(read_timeout_seconds, bool)
        or not isinstance(read_timeout_seconds, (int, float))
        or not math.isfinite(float(read_timeout_seconds))
        or not API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MIN
        <= float(read_timeout_seconds)
        <= API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MAX
    ):
        raise ValueError(
            "api_request_body_read_timeout_seconds must be a finite number between "
            f"{API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MIN} and {API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MAX}"
        )
    return (
        max_concurrent,
        max_buffered_bytes,
        tenant_max_concurrent,
        tenant_max_buffered_bytes,
        memory_amplification_factor,
        memory_floor_bytes,
        float(read_timeout_seconds),
    )


def create_app(
    *,
    runtime_settings: Settings = default_settings,
    lifespan: LifespanFactory | None | object = _DEFAULT_LIFESPAN,
    include_default_routes: bool = True,
) -> FastAPI:
    """Create the FastAPI app shell.

    Route modules can attach handlers to the returned app. Keeping app
    construction here separates app metadata/middleware from route business
    logic and gives tests a small factory to exercise.
    """
    (
        allowed_hosts,
        cors_origins,
        max_request_body_bytes,
        max_request_bodies,
        max_buffered_request_body_bytes,
        max_tenant_request_bodies,
        max_tenant_buffered_request_body_bytes,
        request_body_memory_amplification_factor,
        request_body_memory_floor_bytes,
        request_body_read_timeout_seconds,
    ) = _validate_api_runtime_settings(runtime_settings)
    if lifespan is _DEFAULT_LIFESPAN:
        from tacit.api.lifespan import create_lifespan

        selected_lifespan: LifespanFactory | None = create_lifespan(runtime_settings)
    else:
        selected_lifespan = lifespan
    authenticated = bool(runtime_settings.api_auth_enabled)
    app = FastAPI(
        title="Tacit",
        description=DESCRIPTION,
        version=__version__,
        lifespan=selected_lifespan,
        openapi_tags=OPENAPI_TAGS,
        docs_url=None if authenticated else "/docs",
        redoc_url=None if authenticated else "/redoc",
        openapi_url=None if authenticated else "/openapi.json",
    )
    app.state.settings = runtime_settings
    app.state.runtime_stores = RuntimeStores(runtime_settings)
    wildcard_tenancy = str(runtime_settings.knowledge_tenant_id or "default") == "*"
    effective_tenant_max_concurrent = max_tenant_request_bodies if wildcard_tenancy else max_request_bodies
    effective_tenant_max_buffered_bytes = (
        max_tenant_buffered_request_body_bytes if wildcard_tenancy else max_buffered_request_body_bytes
    )
    request_body_admission = RequestBodyAdmissionController(
        max_concurrent=max_request_bodies,
        max_buffered_bytes=max_buffered_request_body_bytes,
        max_concurrent_per_tenant=effective_tenant_max_concurrent,
        max_buffered_bytes_per_tenant=effective_tenant_max_buffered_bytes,
        memory_amplification_factor=request_body_memory_amplification_factor,
        memory_floor_bytes=request_body_memory_floor_bytes,
    )
    app.state.request_body_admission = request_body_admission
    app.add_exception_handler(RequestValidationError, _bounded_request_validation_error)
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_bytes=max_request_body_bytes,
        admission_controller=request_body_admission,
        read_timeout_seconds=request_body_read_timeout_seconds,
        runtime_settings=runtime_settings,
    )
    app.add_middleware(
        PreBodyTerminalCORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "X-API-Key", "X-Tacit-Tenant"],
    )
    app.add_middleware(
        ExplicitHostPolicyMiddleware,
        allowed_hosts=allowed_hosts,
    )
    app.add_middleware(ResponseSecurityHeadersMiddleware)
    if include_default_routes:
        from tacit.api.routes import include_routes

        include_routes(app)
    return app
