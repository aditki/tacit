from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

import tacit.api.routes.dashboard as dashboard_routes
import tacit.pipeline.runner as pipeline_runner
from tacit.api.app import create_app
from tacit.config import (
    API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX,
    Settings,
)
from tacit.models.request_limits import (
    DASH_REQUEST_CHANNEL_ID_MAX_LENGTH,
    DASH_REQUEST_MAX_RETAINED_BYTES,
    DASH_REQUEST_PROMPT_MAX_LENGTH,
    DASH_REQUEST_THREAD_TS_MAX_LENGTH,
    DASH_REQUEST_USER_ID_MAX_LENGTH,
    pipeline_retained_request_memory_bound,
)
from tacit.models.schemas import DashRequest, DashResponse
from tacit.pipeline_admission import pipeline_admission_limits
from tacit.tenancy import MAX_TENANT_LENGTH
from tests.http_client import TestClient

_FIELD_LIMITS = {
    "prompt": DASH_REQUEST_PROMPT_MAX_LENGTH,
    "channel_id": DASH_REQUEST_CHANNEL_ID_MAX_LENGTH,
    "user_id": DASH_REQUEST_USER_ID_MAX_LENGTH,
    "thread_ts": DASH_REQUEST_THREAD_TS_MAX_LENGTH,
    "tenant_id": MAX_TENANT_LENGTH,
}


def _request_payload(**updates: str) -> dict[str, str]:
    return {
        "prompt": "checkout latency",
        "channel_id": "C0123456789",
        "user_id": "U0123456789",
        "thread_ts": "1712345678.123456",
        "tenant_id": "tenant-a",
        **updates,
    }


@pytest.mark.parametrize(("field", "limit"), _FIELD_LIMITS.items())
def test_dash_request_retained_fields_accept_their_limit(field: str, limit: int) -> None:
    request = DashRequest.model_validate(_request_payload(**{field: "x" * limit}))

    assert len(getattr(request, field)) == limit


@pytest.mark.parametrize(("field", "limit"), _FIELD_LIMITS.items())
def test_dash_request_retained_fields_reject_limit_plus_one(field: str, limit: int) -> None:
    with pytest.raises(ValidationError, match="String should have at most"):
        DashRequest.model_validate(_request_payload(**{field: "x" * (limit + 1)}))


@pytest.mark.parametrize(("field", "limit"), _FIELD_LIMITS.items())
def test_streaming_chart_accepts_limit_and_rejects_limit_plus_one(
    field: str,
    limit: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant = "t" * MAX_TENANT_LENGTH
    runtime_settings = Settings(
        _env_file=None,
        api_auth_enabled=True,
        knowledge_tenant_id="*",
        knowledge_tenant_api_keys={tenant: "tenant-secret"},
    )
    app = create_app(runtime_settings=runtime_settings, lifespan=None)
    pipeline_calls = 0

    async def fake_run_pipeline(_request: DashRequest, _dependencies: Any) -> DashResponse:
        nonlocal pipeline_calls
        pipeline_calls += 1
        return DashResponse(
            dashboard_url="http://grafana.test/d/bounded",
            dashboard_uid="bounded",
            panel_count=1,
            summary="bounded",
        )

    monkeypatch.setattr(dashboard_routes, "run_pipeline", fake_run_pipeline)
    client = TestClient(app)
    headers = {"X-API-Key": "tenant-secret", "X-Tacit-Tenant": tenant}
    accepted_payload = _request_payload(tenant_id=tenant)
    accepted_payload[field] = tenant if field == "tenant_id" else "x" * limit
    accepted = client.post(
        "/api/v1/chart/stream",
        json=accepted_payload,
        headers=headers,
    )
    rejected_payload = _request_payload(tenant_id=tenant)
    rejected_payload[field] = "x" * (limit + 1)
    rejected = client.post(
        "/api/v1/chart/stream",
        json=rejected_payload,
        headers=headers,
    )

    assert accepted.status_code == 200
    assert rejected.status_code == 422
    assert rejected.json()["detail"][0]["type"] == "string_too_long"
    assert pipeline_calls == 1


def test_pipeline_queue_and_active_request_memory_fit_the_declared_envelope() -> None:
    runtime_settings = Settings(
        _env_file=None,
        pipeline_max_concurrent=1_000,
        pipeline_max_queued=1_000,
        api_request_body_max_buffered_bytes=API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX,
    )
    limits = pipeline_admission_limits(runtime_settings)

    queued_bound = limits.queued * DASH_REQUEST_MAX_RETAINED_BYTES
    aggregate_bound = pipeline_retained_request_memory_bound(
        max_concurrent=limits.concurrent,
        max_queued=limits.queued,
    )

    assert queued_bound == 27_200_000
    assert aggregate_bound == 54_400_000
    assert aggregate_bound <= runtime_settings.api_request_body_max_buffered_bytes


def test_pipeline_request_memory_bound_rejects_limit_plus_one_saturation() -> None:
    aggregate_bound = pipeline_retained_request_memory_bound(
        max_concurrent=1_000,
        max_queued=1_000,
    )

    with pytest.raises(ValidationError, match="pipeline retained request memory"):
        Settings(
            _env_file=None,
            pipeline_max_concurrent=1_000,
            pipeline_max_queued=1_000,
            api_max_request_body_bytes=1_024,
            api_request_body_max_buffered_bytes=aggregate_bound - 1,
            api_request_body_tenant_max_buffered_bytes=4 * 1_024 * 1_024,
        )

    accepted = Settings(
        _env_file=None,
        pipeline_max_concurrent=1_000,
        pipeline_max_queued=1_000,
        api_max_request_body_bytes=1_024,
        api_request_body_max_buffered_bytes=aggregate_bound,
        api_request_body_tenant_max_buffered_bytes=4 * 1_024 * 1_024,
    )
    unvalidated = accepted.model_copy(update={"api_request_body_max_buffered_bytes": aggregate_bound - 1})
    with pytest.raises(ValueError, match="pipeline retained request memory"):
        pipeline_admission_limits(unvalidated)


@pytest.mark.parametrize(("field", "limit"), tuple(_FIELD_LIMITS.items())[:4])
@pytest.mark.parametrize("construction", ["model_copy", "model_construct"])
async def test_direct_pipeline_revalidates_retained_fields_before_runtime_side_effects(
    field: str,
    limit: int,
    construction: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _request_payload(**{field: "x" * (limit + 1)})
    if construction == "model_copy":
        request = DashRequest.model_validate(_request_payload()).model_copy(
            update={field: payload[field]},
        )
    else:
        request = DashRequest.model_construct(**payload)

    effects = {"root": 0, "admission": 0, "inner": 0}

    class AdmissionProbe:
        retained = 0

        def slot(self, **_kwargs: Any) -> None:
            effects["admission"] += 1
            raise AssertionError("invalid request reached pipeline admission")

    class DependencyProbe(SimpleNamespace):
        def start_runtime_root(self) -> None:
            effects["root"] += 1
            return None

    dependencies = DependencyProbe(
        settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        pipeline_admission=AdmissionProbe(),
    )

    async def inner_probe(*_args: Any, **_kwargs: Any) -> DashResponse:
        effects["inner"] += 1
        return DashResponse(
            dashboard_url="https://dashboards.example/invalid",
            dashboard_uid="invalid",
            panel_count=0,
            summary="invalid",
        )

    monkeypatch.setattr(pipeline_runner, "_run_pipeline_with_dependencies", inner_probe)

    with pytest.raises(ValidationError, match="String should have at most"):
        await pipeline_runner.run_pipeline(request, dependencies)

    assert effects == {"root": 0, "admission": 0, "inner": 0}


async def test_direct_pipeline_revalidates_tenant_adjustment_before_runtime_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effects = {"root": 0, "admission": 0, "inner": 0}

    class AdmissionProbe:
        retained = 0

        def slot(self, **_kwargs: Any) -> None:
            effects["admission"] += 1
            raise AssertionError("invalid request reached pipeline admission")

    class DependencyProbe(SimpleNamespace):
        def start_runtime_root(self) -> None:
            effects["root"] += 1
            return None

    dependencies = DependencyProbe(
        settings=Settings(_env_file=None),
        pipeline_admission=AdmissionProbe(),
    )

    async def inner_probe(*_args: Any, **_kwargs: Any) -> DashResponse:
        effects["inner"] += 1
        return DashResponse(
            dashboard_url="https://dashboards.example/invalid",
            dashboard_uid="invalid",
            panel_count=0,
            summary="invalid",
        )

    monkeypatch.setattr(
        "tacit.tenancy.resolve_tenant_boundary",
        lambda *_args, **_kwargs: "t" * (MAX_TENANT_LENGTH + 1),
    )
    monkeypatch.setattr(pipeline_runner, "_run_pipeline_with_dependencies", inner_probe)

    with pytest.raises(ValidationError, match="String should have at most"):
        await pipeline_runner.run_pipeline(DashRequest(prompt="checkout latency"), dependencies)

    assert effects == {"root": 0, "admission": 0, "inner": 0}
