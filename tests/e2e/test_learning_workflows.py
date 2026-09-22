from __future__ import annotations

import pytest
from click.testing import CliRunner

import tacit.backends as backends_mod
import tacit.cli as cli_mod
import tacit.signals as signals_mod
from tacit.backends.base import AlertFeatures, DashboardFeatures
from tacit.cli import cli
from tacit.config import Settings, settings
from tacit.main import app
from tacit.runtime_stores import RuntimeStores
from tests.http_client import TestClient


@pytest.fixture
def isolated_learning_store(tmp_path, monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="default",
        knowledge_permissions=(
            "knowledge.read,knowledge.review,knowledge.trust,knowledge.reject,knowledge.correct,"
            "knowledge.apply,knowledge.export,knowledge.override"
        ),
        api_auth_enabled=False,
        signals_db_path=str(tmp_path / "learning_e2e.db"),
        history_db_path=str(tmp_path / "history_e2e.db"),
        feedback_db_path=str(tmp_path / "feedback_e2e.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    store = runtime_stores.signals()
    monkeypatch.setattr(signals_mod, "get_signal_store", lambda: store)
    monkeypatch.setattr(cli_mod, "_cli_runtime_stores", lambda: runtime_stores)
    for field in (
        "knowledge_tenant_id",
        "knowledge_permissions",
        "api_auth_enabled",
        "signals_db_path",
        "history_db_path",
        "feedback_db_path",
    ):
        monkeypatch.setattr(settings, field, getattr(runtime_settings, field))
    monkeypatch.setattr(app.state, "settings", runtime_settings)
    monkeypatch.setattr(app.state, "runtime_stores", runtime_stores)
    yield store


@pytest.fixture
def client(isolated_learning_store):
    return TestClient(app)


def _checkout_dashboard_upload() -> dict:
    return {
        "vendor": "grafana",
        "source_name": "checkout-service.json",
        "auto_approve": False,
        "dashboard": {
            "dashboard": {
                "uid": "checkout-service-e2e",
                "title": "Checkout Service Health",
                "tags": ["service:checkout", "tier:edge"],
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "Checkout p95 latency",
                        "targets": [
                            {
                                "expr": (
                                    "histogram_quantile(0.95, "
                                    'sum(rate(checkout_custom_latency_ms{service="checkout"}[5m])) by (le))'
                                ),
                                "datasource": {"type": "prometheus", "uid": "prom"},
                            }
                        ],
                    },
                    {
                        "type": "timeseries",
                        "title": "Checkout 5xx errors",
                        "targets": [
                            {
                                "expr": 'sum(rate(checkout_5xx_count{service="checkout"}[5m]))',
                                "datasource": {"type": "prometheus", "uid": "prom"},
                            }
                        ],
                    },
                ],
            }
        },
    }


def _corroborating_dashboard_upload(uid: str, panel_title: str, expression: str) -> dict:
    return {
        "vendor": "grafana",
        "source_name": f"{uid}.json",
        "auto_approve": True,
        "dashboard": {
            "dashboard": {
                "uid": uid,
                "title": panel_title,
                "tags": ["service:checkout"],
                "panels": [
                    {
                        "type": "timeseries",
                        "title": panel_title,
                        "targets": [
                            {
                                "expr": expression,
                                "datasource": {"type": "prometheus", "uid": "prom"},
                            }
                        ],
                    }
                ],
            }
        },
    }


def test_dashboard_upload_approval_search_and_service_question_e2e(client, isolated_learning_store):
    if not isolated_learning_store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")

    ingest = client.post("/api/v1/learn/dashboard/json", json=_checkout_dashboard_upload())

    assert ingest.status_code == 200
    body = ingest.json()
    assert body["dashboard_uid"] == "checkout-service-e2e"
    assert body["status"] == "pending"
    assert body["indexed_context_rows"] >= 2
    assert body["signal_quality"]["metrics_mapped"] == 2

    candidate_search = client.get(
        "/api/v1/learning/search",
        params={"q": "checkout latency", "service": "checkout"},
    )
    assert candidate_search.status_code == 200
    assert candidate_search.json()["count"] >= 1
    assert candidate_search.json()["results"][0]["review_state"] == "candidate"

    approved_only_before = client.get(
        "/api/v1/learning/search",
        params={"q": "checkout latency", "service": "checkout", "include_candidates": "false"},
    )
    assert approved_only_before.status_code == 200
    assert approved_only_before.json()["count"] == 0

    runner = CliRunner()
    approve_cli = runner.invoke(cli, ["learn", "approve", "checkout-service-e2e", "--backend", "grafana_json"])
    assert approve_cli.exit_code == 0
    assert "Dashboard approved" in approve_cli.output

    approved_search = client.get(
        "/api/v1/learning/search",
        params={"q": "checkout latency", "service": "checkout", "include_candidates": "false"},
    )
    assert approved_search.status_code == 200
    assert approved_search.json()["count"] == 0

    governed_search = client.get(
        "/api/v1/learning/search",
        params={"q": "checkout latency", "service": "checkout"},
    )
    assert governed_search.status_code == 200
    assert governed_search.json()["count"] >= 1
    assert governed_search.json()["results"][0]["review_state"] == "candidate"

    service = client.get("/api/v1/services/checkout")
    assert service.status_code == 200
    service_body = service.json()
    assert service_body["trusted_context_rows"] == 0
    assert service_body["candidate_context_rows"] >= 1
    assert any(metric["metric"] == "checkout_custom_latency_ms" for metric in service_body["top_metrics"])

    service_cli = runner.invoke(cli, ["learn", "service", "checkout"])
    assert service_cli.exit_code == 0
    assert "Checkout Service Health" in service_cli.output
    assert "checkout_custom_latency_ms" in service_cli.output

    search_cli = runner.invoke(
        cli,
        ["learn", "search", "checkout latency", "--service", "checkout"],
    )
    assert search_cli.exit_code == 0
    assert "checkout_custom_latency_ms" in search_cli.output


@pytest.mark.parametrize(("action", "terminal_status"), [("reject", "rejected"), ("ignore", "ignored")])
def test_dashboard_terminal_review_retires_governed_source_support_e2e(
    client,
    isolated_learning_store,
    action,
    terminal_status,
):
    first = client.post(
        "/api/v1/learn/dashboard/json",
        json=_corroborating_dashboard_upload(
            "checkout-latency-source-a",
            "Checkout latency p95",
            "checkout_custom_latency_ms",
        ),
    )
    second = client.post(
        "/api/v1/learn/dashboard/json",
        json=_corroborating_dashboard_upload(
            "checkout-latency-source-b",
            "Checkout latency p99",
            "avg(checkout_custom_latency_ms)",
        ),
    )

    assert first.status_code == second.status_code == 200
    assert first.json()["status"] == second.json()["status"] == "approved"
    active_patterns = {
        mapping["metric_pattern"]
        for mapping in isolated_learning_store.get_mappings_for_signal(
            "request_latency",
            context_datasource_type="prometheus",
            include_decayed=True,
        )
    }
    assert "checkout_custom_latency_ms" in active_patterns

    reviewed = client.post(
        f"/api/v1/learn/dashboards/checkout-latency-source-b/{action}",
        params={"backend": "grafana_json"},
    )

    assert reviewed.status_code == 200
    assert reviewed.json()["status"] == terminal_status
    remaining_patterns = {
        mapping["metric_pattern"]
        for mapping in isolated_learning_store.get_mappings_for_signal(
            "request_latency",
            include_decayed=True,
        )
    }
    assert "checkout_custom_latency_ms" not in remaining_patterns


def test_runbook_artifact_learning_cli_and_api_e2e(client, isolated_learning_store, tmp_path):
    runbook = tmp_path / "checkout.md"
    runbook.write_text(
        "\n".join(
            [
                "# Checkout Runbook",
                "## Checks",
                "- check redis_cache_misses_total",
                "- restart Redis",
                "## Dependencies",
                "- checkout-api depends on redis-cart",
                "## Escalation",
                "- escalate to Payments",
            ]
        )
    )

    runner = CliRunner()
    dry_run = runner.invoke(cli, ["learn", "runbooks", "--file", str(runbook), "--dry-run"])

    assert dry_run.exit_code == 0
    assert "Previewed checkout" in dry_run.output
    assert isolated_learning_store.list_learned_artifacts(artifact_type="runbook") == []

    learned = runner.invoke(cli, ["learn", "runbooks", "--file", str(runbook)])

    assert learned.exit_code == 0
    artifacts = isolated_learning_store.list_learned_artifacts(artifact_type="runbook")
    assert len(artifacts) == 1
    extractions = isolated_learning_store.list_artifact_extractions(artifacts[0]["artifact_id"])
    assert len(extractions["evidence_requirements"]) == 1
    assert len(extractions["dependency_hints"]) == 1
    assert len(extractions["ownership_hints"]) == 1

    api = client.post(
        "/api/v1/learn/runbooks",
        json={
            "title": "Checkout API Runbook",
            "body_text": "## Checks\n- check checkout_latency_seconds\n## Escalation\n- contact Payments",
            "external_id": "api-checkout-runbook",
            "dry_run": True,
        },
    )

    assert api.status_code == 200
    assert api.json()["dry_run"] is True
    assert api.json()["summary"]["evidence_requirements"] == 1

    listing = client.get("/api/v1/learn/runbooks")

    assert listing.status_code == 200
    assert listing.json()["count"] == 1


def test_api_artifacts_without_external_id_do_not_collide(client, isolated_learning_store):
    first = client.post(
        "/api/v1/learn/runbooks",
        json={
            "title": "Checkout API Runbook",
            "body_text": "## Checks\n- check checkout_latency_seconds",
        },
    )
    second = client.post(
        "/api/v1/learn/runbooks",
        json={
            "title": "Checkout API Runbook",
            "body_text": "## Checks\n- check redis_cache_misses_total",
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    listing = client.get("/api/v1/learn/runbooks")
    assert listing.status_code == 200
    runbooks = listing.json()["runbooks"]
    assert listing.json()["count"] == 2
    assert len({runbook["artifact_id"] for runbook in runbooks}) == 2


def test_artifact_list_limits_are_bounded(client, isolated_learning_store):
    runbooks = client.get("/api/v1/learn/runbooks", params={"limit": -1})
    incidents = client.get("/api/v1/learn/incidents", params={"limit": -1})

    assert runbooks.status_code == 422
    assert incidents.status_code == 422


def test_incident_artifact_learning_cli_and_api_e2e(client, isolated_learning_store, tmp_path):
    incident = tmp_path / "inc-482.md"
    incident.write_text(
        "\n".join(
            [
                "# INC-482 Checkout Latency",
                "## Symptoms",
                "- observed redis_cache_misses_total above normal",
                "## Dependencies",
                "- checkout-api depends on redis-cart",
                "## Escalation",
                "- contact Payments",
                "## Resolution",
                "- Root cause: redis-cart",
            ]
        )
    )

    runner = CliRunner()
    dry_run = runner.invoke(cli, ["learn", "incidents", "--file", str(incident), "--dry-run"])

    assert dry_run.exit_code == 0
    assert "Previewed inc 482" in dry_run.output
    assert isolated_learning_store.list_learned_artifacts(artifact_type="incident") == []

    learned = runner.invoke(cli, ["learn", "incidents", "--file", str(incident)])

    assert learned.exit_code == 0
    artifacts = isolated_learning_store.list_learned_artifacts(artifact_type="incident")
    assert len(artifacts) == 1
    extractions = isolated_learning_store.list_artifact_extractions(artifacts[0]["artifact_id"])
    assert len(extractions["evidence_requirements"]) == 1
    assert extractions["evidence_requirements"][0]["observation_state"] == "observed"
    assert len(extractions["dependency_hints"]) == 1
    assert len(extractions["ownership_hints"]) == 1

    api = client.post(
        "/api/v1/learn/incidents",
        json={
            "title": "INC-483 Checkout Errors",
            "body_text": "## Evidence\n- detected checkout_errors_total spike\n## Resolution\n- Culprit: checkout-api",
            "external_id": "INC-483",
            "dry_run": True,
        },
    )

    assert api.status_code == 200
    assert api.json()["dry_run"] is True
    assert api.json()["summary"]["evidence_requirements"] == 1
    assert api.json()["warnings"] == ["ignored_causal_claim:Culprit: checkout-api"]

    listing = client.get("/api/v1/learn/incidents")

    assert listing.status_code == 200
    assert listing.json()["count"] == 1


def test_bulk_grafana_learning_cli_indexes_backend_dashboards_e2e(isolated_learning_store, monkeypatch):
    if not isolated_learning_store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")

    class FakeGrafanaBackend:
        name = "grafana"
        query_language = "promql"

        async def list_dashboards(self, limit: int = 500):
            assert limit == 25
            return [{"uid": "checkout-bulk", "title": "Checkout Bulk Ops", "backend": "grafana"}]

        async def ingest_dashboard(self, uid: str):
            assert uid == "checkout-bulk"
            return DashboardFeatures(
                dashboard_uid=uid,
                dashboard_title="Checkout Bulk Ops",
                dashboard_tags=["service:checkout"],
                backend_name="grafana",
                query_language="promql",
                metrics_found=["checkout_custom_latency_ms", "checkout_5xx_count"],
                panel_count=2,
                panel_titles=["Checkout Latency", "Checkout Errors"],
                panels=[
                    {
                        "title": "Checkout Latency",
                        "queries": ['checkout_custom_latency_ms{service="checkout"}'],
                        "metrics": ["checkout_custom_latency_ms"],
                    },
                    {
                        "title": "Checkout Errors",
                        "queries": ['checkout_5xx_count{service="checkout"}'],
                        "metrics": ["checkout_5xx_count"],
                    },
                ],
            )

        async def close(self):
            return None

    monkeypatch.setattr(backends_mod, "get_active_backends", lambda *_args, **_kwargs: [FakeGrafanaBackend()])

    runner = CliRunner()
    result = runner.invoke(cli, ["learn", "grafana", "--auto-approve", "--limit", "25"])

    assert result.exit_code == 0
    assert "Learned from 1 grafana dashboards" in result.output
    assert "Indexed context rows: 2" in result.output
    assert "Mappings created: 0" in result.output

    summary = isolated_learning_store.describe_service("checkout", include_candidates=True)
    assert summary["trusted_context_rows"] == 0
    assert summary["candidate_context_rows"] == 2
    assert {metric["metric"] for metric in summary["top_metrics"]} == {
        "checkout_custom_latency_ms",
        "checkout_5xx_count",
    }


def test_bulk_grafana_alert_learning_indexes_alert_context_e2e(client, isolated_learning_store, monkeypatch):
    if not isolated_learning_store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")

    class FakeGrafanaBackend:
        name = "grafana"
        query_language = "promql"

        async def list_alerts(self, limit: int = 500):
            assert limit == 25
            return [{"uid": "checkout-latency-alert", "title": "Checkout latency high", "backend": "grafana"}]

        async def ingest_alert(self, uid: str):
            assert uid == "checkout-latency-alert"
            return AlertFeatures(
                alert_uid=uid,
                alert_title="Checkout latency high",
                alert_tags=["service:checkout", "severity:critical"],
                backend_name="grafana",
                query_language="promql",
                condition="A > 1",
                severity="critical",
                labels={"service": "checkout", "severity": "critical"},
                metrics_found=["checkout_request_duration_seconds"],
                query_transformations=[
                    'histogram_quantile(0.95, checkout_request_duration_seconds{service="checkout"})'
                ],
                service_hints=["checkout"],
            )

        async def close(self):
            return None

    monkeypatch.setattr(backends_mod, "get_active_backends", lambda *_args, **_kwargs: [FakeGrafanaBackend()])

    runner = CliRunner()
    dry_run = runner.invoke(cli, ["learn", "alerts", "--from", "grafana", "--limit", "25", "--dry-run"])
    assert dry_run.exit_code == 0
    assert "Previewed 1 grafana alerts" in dry_run.output

    listed_after_dry_run = client.get("/api/v1/learn/alerts")
    assert listed_after_dry_run.status_code == 200
    assert listed_after_dry_run.json()["count"] == 0

    api_dry_run = client.post(
        "/api/v1/learn/backends/grafana/alerts",
        params={"limit": 25, "dry_run": "true"},
    )
    assert api_dry_run.status_code == 200
    assert api_dry_run.json()["summary"]["source"] == "grafana"
    assert api_dry_run.json()["summary"]["artifact_type"] == "alert_rule"

    listed_after_api_dry_run = client.get("/api/v1/learn/alerts")
    assert listed_after_api_dry_run.status_code == 200
    assert listed_after_api_dry_run.json()["count"] == 0

    result = runner.invoke(cli, ["learn", "alerts", "--from", "grafana", "--limit", "25"])

    assert result.exit_code == 0
    assert "Learned from 1 grafana alerts" in result.output
    assert "Indexed context rows: 1" in result.output

    search = client.get(
        "/api/v1/learning/search",
        params={"q": "checkout latency", "service": "checkout"},
    )
    assert search.status_code == 200
    rows = search.json()["results"]
    assert rows
    assert rows[0]["source_kind"] == "alert_rule"
    assert rows[0]["review_state"] == "candidate"

    listed = client.get("/api/v1/learn/alerts")
    assert listed.status_code == 200
    assert listed.json()["count"] == 1
    assert listed.json()["alerts"][0]["alert_uid"] == "checkout-latency-alert"


def test_cli_reject_records_negative_training_data_e2e(client, isolated_learning_store):
    if not isolated_learning_store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")

    isolated_learning_store.record_ingested_dashboard(
        "checkout-service-e2e",
        backend_name="grafana_json",
        dashboard_title="Checkout Service Health",
        dashboard_tags=["service:checkout"],
        metrics_found=["checkout_custom_failure_ratio"],
        signals_inferred=[
            {
                "signal_type": "checkout_failure_ratio",
                "metric": "checkout_custom_failure_ratio",
                "source": "heuristic",
                "signal_family": "errors",
                "score": 0.91,
                "margin": 0.4,
                "evidence": ["panel title indicates failures"],
                "inference_version": "test",
            }
        ],
        status="pending",
    )
    isolated_learning_store.index_dashboard_context(
        dashboard_uid="checkout-service-e2e",
        backend_name="grafana_json",
        dashboard_title="Checkout Service Health",
        dashboard_tags=["service:checkout"],
        panels=[
            {
                "title": "Checkout failures",
                "queries": ['checkout_custom_failure_ratio{service="checkout"}'],
                "metrics": ["checkout_custom_failure_ratio"],
            }
        ],
        metrics_found=["checkout_custom_failure_ratio"],
        signals_inferred=[
            {
                "signal_type": "checkout_failure_ratio",
                "metric": "checkout_custom_failure_ratio",
                "source": "heuristic",
                "signal_family": "errors",
                "score": 0.91,
                "margin": 0.4,
                "evidence": ["panel title indicates failures"],
                "inference_version": "test",
            }
        ],
        status="pending",
    )

    runner = CliRunner()
    reject_cli = runner.invoke(cli, ["learn", "reject", "checkout-service-e2e", "--backend", "grafana_json"])

    assert reject_cli.exit_code == 0
    assert "Dashboard rejected" in reject_cli.output
    assert "Rejected candidates recorded:" in reject_cli.output

    rejected = isolated_learning_store.list_rejected_candidates()
    assert rejected
    assert {item["why_not"] for item in rejected} == {"dashboard_rejected"}
    assert isolated_learning_store.search_learning_context("checkout failures", service="checkout") == []
