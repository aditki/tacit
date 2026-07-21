from __future__ import annotations

import httpx
import pytest
import respx

from tacit.config import settings
from tests.validate import TestCase as ValidationCase
from tests.validate import grafana_request_headers, run_pipeline_validation


def _case() -> ValidationCase:
    return ValidationCase(
        prompt_id="DF-001",
        prompt="Investigate checkout latency",
        expected_archetype="latency_investigation",
        expected_metrics=["http_request_duration_seconds"],
        expected_datasources=["Prometheus"],
        difficulty="easy",
        validation_goal="metric retrieval",
        critical_metrics=["http_request_duration_seconds"],
    )


def test_grafana_request_headers_omit_empty_bearer_token():
    assert grafana_request_headers("", 1) == {"X-Grafana-Org-Id": "1"}
    assert grafana_request_headers("secret", 2) == {
        "Authorization": "Bearer secret",
        "X-Grafana-Org-Id": "2",
    }


@pytest.mark.asyncio
@respx.mock
async def test_pipeline_harness_surfaces_grafana_fetch_failures(monkeypatch):
    monkeypatch.setattr(settings, "grafana_api_key", "")
    respx.post("http://tacit.test/api/v1/chart").mock(
        return_value=httpx.Response(
            200,
            json={
                "dashboard_uid": "dash-1",
                "dashboard_url": "http://grafana.test/d/dash-1",
                "panel_count": 1,
            },
        )
    )
    grafana = respx.get("http://grafana.test/api/dashboards/uid/dash-1").mock(
        return_value=httpx.Response(503, text="unavailable")
    )

    results = await run_pipeline_validation([_case()], "http://tacit.test", "http://grafana.test")

    assert "Authorization" not in grafana.calls[0].request.headers
    assert results[0].error == "Grafana dashboard fetch HTTP 503: unavailable"
