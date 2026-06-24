import pytest

from tacit.alert_ingest import ingest_alert, learn_backend_alerts
from tacit.backends.base import AlertFeatures
from tacit.backends.grafana import GrafanaBackend, _parse_grafana_alert_rule
from tacit.backends.signalfx import SignalFxBackend, _parse_signalfx_detector


def test_grafana_alert_rule_parses_to_common_alert_features():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "A",
            "isPaused": False,
            "labels": {"service": "checkout", "severity": "critical"},
            "annotations": {"__dashboardUid__": "checkout-dashboard", "__panelTitle__": "p95 latency"},
            "data": [
                {
                    "refId": "A",
                    "model": {
                        "datasource": {"type": "prometheus", "uid": "prom"},
                        "expr": (
                            "histogram_quantile(0.95, " 'rate(checkout_latency_seconds_bucket{service="checkout"}[5m]))'
                        ),
                    },
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
    )

    assert features.alert_uid == "checkout-latency"
    assert features.backend_name == "grafana"
    assert features.query_language == "promql"
    assert features.metrics_found == ["checkout_latency_seconds_bucket"]
    assert features.service_hints == ["checkout"]
    assert features.dashboard_uid == "checkout-dashboard"


def test_grafana_alert_rule_skips_expression_ref_ids_and_non_prometheus_queries():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "B",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "datasourceUid": "prom",
                    "model": {
                        "datasource": {"type": "prometheus", "uid": "prom"},
                        "expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])',
                    },
                },
                {
                    "refId": "B",
                    "datasourceUid": "__expr__",
                    "model": {"type": "math", "expression": "$A > 0"},
                },
                {
                    "refId": "C",
                    "datasourceUid": "loki",
                    "model": {"datasource": {"type": "loki", "uid": "loki"}, "expr": '{app="checkout"} |= "error"'},
                },
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
    )

    assert features.metrics_found == ["checkout_latency_seconds_count"]
    assert features.query_transformations == ['rate(checkout_latency_seconds_count{service="checkout"}[5m])']
    assert "$A > 0" in features.condition


def test_grafana_alert_rule_skips_unknown_datasource_uid_queries():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-logs",
            "title": "Checkout logs high",
            "condition": "A",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "datasourceUid": "loki-prod",
                    "model": {"expr": '{app="checkout"} |= "error"'},
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
    )

    assert features.metrics_found == []
    assert features.query_transformations == []


def test_grafana_alert_rule_resolves_datasource_uid_only_prometheus_queries():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "A",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "datasourceUid": "prom-prod",
                    "model": {"expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])'},
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
        datasource_types_by_uid={"prom-prod": "prometheus"},
    )

    assert features.metrics_found == ["checkout_latency_seconds_count"]
    assert features.query_transformations == ['rate(checkout_latency_seconds_count{service="checkout"}[5m])']


def test_grafana_alert_rule_uses_uid_resolution_when_datasource_object_has_name_only():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "A",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "model": {
                        "datasource": {"uid": "prom-prod", "name": "Prod Prometheus"},
                        "expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])',
                    },
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
        datasource_types_by_uid={"prom-prod": "prometheus"},
    )

    assert features.metrics_found == ["checkout_latency_seconds_count"]


def test_grafana_alert_threshold_details_change_condition_for_fingerprint():
    base_rule = {
        "uid": "checkout-latency",
        "title": "Checkout latency high",
        "condition": "C",
        "labels": {"service": "checkout"},
        "data": [
            {
                "refId": "A",
                "model": {
                    "datasource": {"type": "prometheus", "uid": "prom"},
                    "expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])',
                },
            },
            {
                "refId": "C",
                "datasourceUid": "__expr__",
                "model": {"type": "math", "expression": "$A > 10"},
            },
        ],
    }
    changed_rule = {
        **base_rule,
        "data": [
            base_rule["data"][0],
            {
                "refId": "C",
                "datasourceUid": "__expr__",
                "model": {"type": "math", "expression": "$A > 20"},
            },
        ],
    }

    first = _parse_grafana_alert_rule(base_rule, backend_name="grafana", base_url="http://grafana.example")
    changed = _parse_grafana_alert_rule(changed_rule, backend_name="grafana", base_url="http://grafana.example")

    assert first.metrics_found == changed.metrics_found == ["checkout_latency_seconds_count"]
    assert "$A > 10" in first.condition
    assert "$A > 20" in changed.condition
    assert first.condition != changed.condition


@pytest.mark.asyncio
async def test_grafana_backend_resolves_datasource_uid_for_managed_alerts():
    class FakeGrafanaClient:
        base_url = "http://grafana.example"

        async def _get(self, path: str, **_kwargs):
            assert path == "/api/v1/provisioning/alert-rules/checkout-latency"
            return {
                "uid": "checkout-latency",
                "title": "Checkout latency high",
                "condition": "A",
                "data": [
                    {
                        "refId": "A",
                        "datasourceUid": "prom-prod",
                        "model": {"expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])'},
                    }
                ],
            }

        async def list_datasources(self):
            return [{"uid": "prom-prod", "name": "Prometheus", "type": "prometheus"}]

        async def close(self):
            return None

    backend = GrafanaBackend(client=FakeGrafanaClient())

    features = await backend.ingest_alert("checkout-latency")

    assert features.metrics_found == ["checkout_latency_seconds_count"]


def test_signalfx_detector_parses_to_common_alert_features():
    features = _parse_signalfx_detector(
        {
            "id": "detector-1",
            "name": "Checkout errors high",
            "tags": ["service:checkout"],
            "teams": ["payments"],
            "programText": "A = data('checkout.errors').sum().publish(label='A')",
            "rules": [{"detectLabel": "A above threshold", "severity": "Critical"}],
        },
        backend_name="signalfx",
        realm="us1",
    )

    assert features.alert_uid == "detector-1"
    assert features.backend_name == "signalfx"
    assert features.query_language == "signalflow"
    assert features.metrics_found == ["checkout.errors"]
    assert features.condition == "A above threshold"
    assert features.severity == "Critical"
    assert features.labels == {"team": "payments"}


@pytest.mark.asyncio
async def test_invalid_alert_backend_closes_instantiated_clients(monkeypatch):
    closed = []

    class FakeBackend:
        name = "grafana"

        async def close(self):
            closed.append(self.name)

    monkeypatch.setattr("tacit.backends.get_active_backends", lambda *_args, **_kwargs: [FakeBackend()])

    with pytest.raises(ValueError):
        await ingest_alert("checkout-latency", backend_name="grafna")

    assert closed == ["grafana"]


@pytest.mark.asyncio
async def test_limited_alert_crawl_does_not_mark_unseen_alerts_stale(tmp_path, monkeypatch):
    from tacit.signals import SignalStore

    store = SignalStore(db_path=tmp_path / "signals.db")
    store.record_ingested_alert(
        "outside-current-page",
        backend_name="grafana",
        alert_title="Still exists on a later page",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)

    class FakeBackend:
        name = "grafana"
        last_alert_list_complete = False

        async def list_alerts(self, limit: int = 500):
            assert limit == 1
            return [{"uid": "current-page", "title": "Current page"}]

        async def ingest_alert(self, uid: str):
            return AlertFeatures(
                alert_uid=uid,
                alert_title="Current page",
                backend_name="grafana",
                query_language="promql",
                condition="A > 1",
                metrics_found=["checkout_request_duration_seconds"],
                query_transformations=['checkout_request_duration_seconds{service="checkout"}'],
            )

        async def close(self):
            return None

    monkeypatch.setattr("tacit.backends.get_active_backends", lambda *_args, **_kwargs: [FakeBackend()])

    result = await learn_backend_alerts("grafana", limit=1)
    stale_row = store.get_ingested_alert("outside-current-page", "grafana")

    assert result["stale_marked"] == 0
    assert result["stale_reconciliation_skipped"] is True
    assert result["summary"]["warnings"] == ["stale_reconciliation_skipped_partial_crawl"]
    assert stale_row is not None
    assert stale_row["stale"] is False


@pytest.mark.asyncio
async def test_signalfx_detector_crawl_marks_short_page_complete():
    class FakeSignalFxClient:
        realm = "us1"

        async def _get(self, path: str, params=None):
            assert path == "/v2/detector"
            assert params == {"limit": 10}
            return {"results": [{"id": "detector-1", "name": "Checkout"}]}

        async def close(self):
            return None

    backend = SignalFxBackend(client=FakeSignalFxClient())

    alerts = await backend.list_alerts(limit=10)

    assert alerts == [{"uid": "detector-1", "title": "Checkout", "backend": "signalfx"}]
    assert backend.last_alert_list_complete is True


@pytest.mark.asyncio
async def test_signalfx_detector_crawl_keeps_paged_response_incomplete():
    class FakeSignalFxClient:
        realm = "us1"

        async def _get(self, path: str, params=None):
            assert path == "/v2/detector"
            assert params == {"limit": 10}
            return {
                "results": [{"id": f"detector-{idx}", "name": f"Detector {idx}"} for idx in range(10)],
                "nextPageLink": "/v2/detector?offset=10",
            }

        async def close(self):
            return None

    backend = SignalFxBackend(client=FakeSignalFxClient())

    await backend.list_alerts(limit=10)

    assert backend.last_alert_list_complete is False


@pytest.mark.asyncio
async def test_signalfx_detector_crawl_pages_until_limit_or_complete():
    class FakeSignalFxClient:
        realm = "us1"

        async def _get(self, path: str, params=None):
            assert path == "/v2/detector"
            if params == {"limit": 100}:
                return {
                    "results": [{"id": f"detector-{idx}", "name": f"Detector {idx}"} for idx in range(100)],
                    "nextPageLink": "/v2/detector?offset=100",
                }
            assert params == {"limit": 100, "offset": 100}
            return {"results": [{"id": f"detector-{idx}", "name": f"Detector {idx}"} for idx in range(100, 125)]}

        async def close(self):
            return None

    backend = SignalFxBackend(client=FakeSignalFxClient())

    alerts = await backend.list_alerts(limit=200)

    assert len(alerts) == 125
    assert alerts[0]["uid"] == "detector-0"
    assert alerts[-1]["uid"] == "detector-124"
    assert backend.last_alert_list_complete is True
