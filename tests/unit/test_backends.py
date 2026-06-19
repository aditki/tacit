"""TDD tests for the DashboardBackend adapter pattern.

Tests written BEFORE implementation. These define the contract that
GrafanaBackend, SignalFxBackend, and the registry must satisfy.
"""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dashforge.models.schemas import (
    ArchetypeMatch,
    DashboardSpec,
    DatasourceInfo,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    SignalType,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _make_intent(**overrides) -> Intent:
    defaults = dict(
        summary="5xx errors on checkout-service",
        domain="web",
        services=["checkout-service"],
        signals=[SignalType.METRICS],
        keywords=["error", "5xx", "http"],
        timerange="1h",
        problem_type="error_spike",
        archetypes=[ArchetypeMatch(type="error_spike", confidence=0.95)],
    )
    defaults.update(overrides)
    return Intent(**defaults)


def _make_spec(query_lang="promql", ds_type="prometheus") -> DashboardSpec:
    return DashboardSpec(
        title="Test Dashboard",
        timerange="1h",
        panels=[
            PanelSpec(
                title="Request Rate",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr=(
                            "data('http.requests').publish(label='A')"
                            if query_lang == "signalflow"
                            else "rate(http_requests_total[5m])"
                        ),
                        legend_format="rps",
                        datasource_uid="ds1",
                        datasource_type=ds_type,
                    )
                ],
            ),
        ],
    )


# ═══════════════════════════════════════════════════════════════════════════
# 1. base.py — PublishResult dataclass
# ═══════════════════════════════════════════════════════════════════════════


def test_publish_result_defaults():
    from dashforge.backends.base import PublishResult

    r = PublishResult()
    assert r.url == ""
    assert r.uid == ""
    assert r.backend_name == ""
    print("[PASS] test_publish_result_defaults")


def test_publish_result_with_values():
    from dashforge.backends.base import PublishResult

    r = PublishResult(url="http://grafana/d/abc", uid="abc", backend_name="grafana")
    assert r.url == "http://grafana/d/abc"
    assert r.uid == "abc"
    assert r.backend_name == "grafana"
    print("[PASS] test_publish_result_with_values")


# ═══════════════════════════════════════════════════════════════════════════
# 2. base.py — DashboardBackend Protocol shape
# ═══════════════════════════════════════════════════════════════════════════


def test_backend_protocol_attributes():
    import inspect

    from dashforge.backends.base import DashboardBackend

    # Protocol should define these methods
    members = {name for name, _ in inspect.getmembers(DashboardBackend)}
    assert "discover_metrics" in members
    assert "validate_queries" in members
    assert "publish" in members
    assert "close" in members
    print("[PASS] test_backend_protocol_attributes")


# ═══════════════════════════════════════════════════════════════════════════
# 3. GrafanaBackend — properties
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_properties():
    from dashforge.backends.grafana import GrafanaBackend

    backend = GrafanaBackend.__new__(GrafanaBackend)
    assert backend.name == "grafana"
    assert backend.query_language == "promql"
    print("[PASS] test_grafana_backend_properties")


# ═══════════════════════════════════════════════════════════════════════════
# 4. GrafanaBackend — discover_metrics delegates to existing code
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_discover_metrics():
    from dashforge.backends.grafana import GrafanaBackend

    mock_client = AsyncMock()
    backend = GrafanaBackend(client=mock_client)

    fake_entries = [
        MetricEntry(
            name="http_requests_total",
            datasource_uid="prom1",
            datasource_name="Prom",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]

    with (
        patch("dashforge.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list,
        patch("dashforge.backends.grafana.filter_datasources_by_signal") as mock_filter,
        patch("dashforge.backends.grafana.filter_searchable_datasources") as mock_searchable,
        patch("dashforge.backends.grafana.discover_all_metrics", new_callable=AsyncMock) as mock_discover,
    ):

        mock_list.return_value = [MagicMock(type="prometheus")]
        mock_filter.return_value = [MagicMock(type="prometheus")]
        mock_searchable.return_value = [MagicMock(type="prometheus")]
        mock_discover.return_value = fake_entries

        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))

        assert len(result) == 1
        assert result[0].name == "http_requests_total"
        mock_discover.assert_called_once()

    print("[PASS] test_grafana_backend_discover_metrics")


# ═══════════════════════════════════════════════════════════════════════════
# 5. GrafanaBackend — validate_queries delegates to existing code
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_validate_queries():
    from dashforge.backends.grafana import GrafanaBackend

    mock_client = AsyncMock()
    backend = GrafanaBackend(client=mock_client)

    spec = _make_spec()

    with patch("dashforge.backends.grafana.validate_dashboard_queries", new_callable=AsyncMock) as mock_val:
        mock_val.return_value = (spec, [])
        result_spec, warnings = asyncio.run(backend.validate_queries(spec))
        assert len(result_spec.panels) == 1
        assert warnings == []
        mock_val.assert_called_once_with(mock_client, spec, None)

    print("[PASS] test_grafana_backend_validate_queries")


# ═══════════════════════════════════════════════════════════════════════════
# 6. GrafanaBackend — publish delegates to existing code
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_publish():
    from dashforge.backends.base import PublishResult
    from dashforge.backends.grafana import GrafanaBackend

    mock_client = AsyncMock()
    backend = GrafanaBackend(client=mock_client)

    spec = _make_spec()

    with patch("dashforge.backends.grafana.publish_dashboard_fn", new_callable=AsyncMock) as mock_pub:
        mock_pub.return_value = ("http://grafana/d/abc", "abc")
        result = asyncio.run(backend.publish(spec))
        assert isinstance(result, PublishResult)
        assert result.url == "http://grafana/d/abc"
        assert result.uid == "abc"
        assert result.backend_name == "grafana"

    print("[PASS] test_grafana_backend_publish")


# ═══════════════════════════════════════════════════════════════════════════
# 7. SignalFxBackend — properties
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_properties():
    from dashforge.backends.signalfx import SignalFxBackend

    backend = SignalFxBackend.__new__(SignalFxBackend)
    assert backend.name == "signalfx"
    assert backend.query_language == "signalflow"
    print("[PASS] test_signalfx_backend_properties")


# ═══════════════════════════════════════════════════════════════════════════
# 8. SignalFxBackend — discover_metrics delegates to signalfx.discovery
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_discover_metrics():
    from dashforge.backends.signalfx import SignalFxBackend

    mock_client = AsyncMock()
    backend = SignalFxBackend(client=mock_client)

    fake_entries = [
        MetricEntry(
            name="http.server.request.count",
            datasource_uid="signalfx-direct",
            datasource_name="SignalFx Direct",
            datasource_type="signalfx",
            query_language="signalflow",
        ),
    ]

    with patch("dashforge.backends.signalfx.sfx_discover", new_callable=AsyncMock) as mock_disc:
        mock_disc.return_value = fake_entries
        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))
        assert len(result) == 1
        assert result[0].datasource_type == "signalfx"
        mock_disc.assert_called_once_with(mock_client, intent.keywords)

    print("[PASS] test_signalfx_backend_discover_metrics")


# ═══════════════════════════════════════════════════════════════════════════
# 9. SignalFxBackend — validate_queries delegates to validate_signalflow_queries
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_validate_queries():
    from dashforge.backends.signalfx import SignalFxBackend

    mock_client = AsyncMock()
    backend = SignalFxBackend(client=mock_client)

    spec = _make_spec(query_lang="signalflow", ds_type="signalfx")

    with patch("dashforge.backends.signalfx.validate_signalflow_queries", new_callable=AsyncMock) as mock_val:
        mock_val.return_value = (spec, [])
        result_spec, warnings = asyncio.run(backend.validate_queries(spec))
        assert len(result_spec.panels) == 1
        assert warnings == []
        mock_val.assert_called_once_with(mock_client, spec)

    print("[PASS] test_signalfx_backend_validate_queries")


# ═══════════════════════════════════════════════════════════════════════════
# 10. SignalFxBackend — publish delegates to signalfx.publisher
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_publish():
    from dashforge.backends.base import PublishResult
    from dashforge.backends.signalfx import SignalFxBackend

    mock_client = AsyncMock()
    backend = SignalFxBackend(client=mock_client)

    spec = _make_spec(query_lang="signalflow", ds_type="signalfx")

    with patch("dashforge.backends.signalfx.sfx_publish", new_callable=AsyncMock) as mock_pub:
        mock_pub.return_value = ("https://app.us1.signalfx.com/#/dashboard/D123", "D123")
        result = asyncio.run(backend.publish(spec))
        assert isinstance(result, PublishResult)
        assert "signalfx.com" in result.url
        assert result.uid == "D123"
        assert result.backend_name == "signalfx"

    print("[PASS] test_signalfx_backend_publish")


# ═══════════════════════════════════════════════════════════════════════════
# 11. Registry — get_active_backends reads config
# ═══════════════════════════════════════════════════════════════════════════


def test_registry_grafana_only():
    from dashforge.backends import get_active_backends

    with patch("dashforge.backends.settings") as mock_settings:
        mock_settings.grafana_enabled = True
        mock_settings.signalfx_enabled = False
        mock_settings.signalfx_api_token = ""
        backends = get_active_backends()
        assert len(backends) == 1
        assert backends[0].name == "grafana"

    print("[PASS] test_registry_grafana_only")


def test_registry_signalfx_only():
    from dashforge.backends import get_active_backends

    with patch("dashforge.backends.settings") as mock_settings:
        mock_settings.grafana_enabled = False
        mock_settings.signalfx_enabled = True
        mock_settings.signalfx_api_token = "test-token"
        backends = get_active_backends()
        assert len(backends) == 1
        assert backends[0].name == "signalfx"

    print("[PASS] test_registry_signalfx_only")


def test_registry_both_enabled():
    from dashforge.backends import get_active_backends

    with patch("dashforge.backends.settings") as mock_settings:
        mock_settings.grafana_enabled = True
        mock_settings.signalfx_enabled = True
        mock_settings.signalfx_api_token = "test-token"
        backends = get_active_backends()
        assert len(backends) == 2
        names = {b.name for b in backends}
        assert names == {"grafana", "signalfx"}

    print("[PASS] test_registry_both_enabled")


def test_registry_none_enabled():
    from dashforge.backends import get_active_backends

    with patch("dashforge.backends.settings") as mock_settings:
        mock_settings.grafana_enabled = False
        mock_settings.signalfx_enabled = False
        mock_settings.signalfx_api_token = ""
        backends = get_active_backends()
        assert len(backends) == 0

    print("[PASS] test_registry_none_enabled")


# ═══════════════════════════════════════════════════════════════════════════
# 12. Registry — primary backend is first in list
# ═══════════════════════════════════════════════════════════════════════════


def test_registry_primary_is_first():
    """When both enabled, the primary backend (first) determines query language."""
    from dashforge.backends import get_active_backends

    with patch("dashforge.backends.settings") as mock_settings:
        mock_settings.grafana_enabled = True
        mock_settings.signalfx_enabled = True
        mock_settings.signalfx_api_token = "tok"
        backends = get_active_backends()
        primary = backends[0]
        # When Grafana is enabled, it should be primary (PromQL is the standard)
        assert primary.name == "grafana"
        assert primary.query_language == "promql"

    print("[PASS] test_registry_primary_is_first")


# ═══════════════════════════════════════════════════════════════════════════
# 13. Close — backends clean up resources
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_close():
    from dashforge.backends.grafana import GrafanaBackend

    mock_client = AsyncMock()
    backend = GrafanaBackend(client=mock_client)
    asyncio.run(backend.close())
    mock_client.close.assert_called_once()
    print("[PASS] test_grafana_backend_close")


def test_signalfx_backend_close():
    from dashforge.backends.signalfx import SignalFxBackend

    mock_client = AsyncMock()
    backend = SignalFxBackend(client=mock_client)
    asyncio.run(backend.close())
    mock_client.close.assert_called_once()
    print("[PASS] test_signalfx_backend_close")


def test_signalfx_backend_list_dashboards_reads_dashboard_configs():
    from dashforge.backends.signalfx import SignalFxBackend

    mock_client = AsyncMock()
    mock_client.list_dashboard_groups.return_value = {
        "results": [
            {
                "name": "Checkout Group",
                "dashboardConfigs": [
                    {"dashboardId": "dash-1", "name": "Checkout Health"},
                    {"dashboardId": "dash-2", "dashboardName": "Checkout Errors"},
                ],
            }
        ]
    }
    backend = SignalFxBackend(client=mock_client)

    dashboards = asyncio.run(backend.list_dashboards(limit=10))

    assert dashboards == [
        {"uid": "dash-1", "title": "Checkout Health", "folder": "Checkout Group", "backend": "signalfx"},
        {"uid": "dash-2", "title": "Checkout Errors", "folder": "Checkout Group", "backend": "signalfx"},
    ]
    print("[PASS] test_signalfx_backend_list_dashboards_reads_dashboard_configs")


# ═══════════════════════════════════════════════════════════════════════════
# 14. GrafanaBackend — discover returns empty when no searchable datasources
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_discover_no_datasources():
    from dashforge.backends.grafana import GrafanaBackend

    mock_client = AsyncMock()
    backend = GrafanaBackend(client=mock_client)

    with (
        patch("dashforge.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list,
        patch("dashforge.backends.grafana.filter_datasources_by_signal") as mock_filter,
        patch("dashforge.backends.grafana.filter_searchable_datasources") as mock_searchable,
    ):

        mock_list.return_value = []
        mock_filter.return_value = []
        mock_searchable.return_value = []

        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))
        assert result == []

    print("[PASS] test_grafana_backend_discover_no_datasources")


def test_grafana_backend_datasource_targets_when_metrics_absent():
    from dashforge.backends.grafana import GrafanaBackend

    mock_client = AsyncMock()
    backend = GrafanaBackend(client=mock_client)
    prom_ds = DatasourceInfo(
        uid="prom1",
        name="Prometheus",
        type="prometheus",
    )

    with (
        patch("dashforge.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list,
        patch("dashforge.backends.grafana.filter_datasources_by_signal") as mock_filter,
        patch("dashforge.backends.grafana.filter_searchable_datasources") as mock_searchable,
    ):
        mock_list.return_value = [prom_ds]
        mock_filter.return_value = [prom_ds]
        mock_searchable.return_value = [prom_ds]

        intent = _make_intent()
        result = asyncio.run(backend.discover_datasource_targets(intent.keywords, intent))

        assert len(result) == 1
        assert result[0].name == ""
        assert result[0].datasource_uid == "prom1"
        assert result[0].datasource_type == "prometheus"
        assert result[0].query_language == "promql"
        assert backend.last_discovery_status.available is True
        assert backend.last_discovery_status.searchable_datasource_count == 1

    print("[PASS] test_grafana_backend_datasource_targets_when_metrics_absent")


def test_grafana_backend_marks_connection_failure_unavailable():
    from dashforge.backends.grafana import GrafanaBackend

    mock_client = AsyncMock()
    backend = GrafanaBackend(client=mock_client)

    with patch("dashforge.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list:
        mock_list.side_effect = RuntimeError("connection refused")
        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))

        assert result == []
        assert backend.last_discovery_status.available is False
        assert "connection refused" in backend.last_discovery_status.error

    print("[PASS] test_grafana_backend_marks_connection_failure_unavailable")


# ═══════════════════════════════════════════════════════════════════════════
# 15. SignalFxBackend — discover handles errors gracefully
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_discover_error():
    from dashforge.backends.signalfx import SignalFxBackend

    mock_client = AsyncMock()
    backend = SignalFxBackend(client=mock_client)

    with patch("dashforge.backends.signalfx.sfx_discover", new_callable=AsyncMock) as mock_disc:
        mock_disc.side_effect = Exception("Connection refused")
        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))
        assert result == []

    print("[PASS] test_signalfx_backend_discover_error")


# ── Bug 8: ingest_dashboard must close all backends ────────────────────


def test_ingest_dashboard_closes_all_backends():
    """When get_active_backends() returns multiple backends and one is
    selected by name, ALL backends must be closed — not just the selected
    one.  Otherwise, unused HTTP clients leak."""
    from dashforge.backends.base import DashboardFeatures

    grafana_backend = AsyncMock()
    grafana_backend.name = "grafana"
    grafana_backend.ingest_dashboard = AsyncMock(
        return_value=DashboardFeatures(
            dashboard_uid="test-uid",
            dashboard_title="Test",
            backend_name="grafana",
            query_language="promql",
            metrics_found=["up"],
            panel_count=1,
            panels=[],
        )
    )

    signalfx_backend = AsyncMock()
    signalfx_backend.name = "signalfx"

    with patch(
        "dashforge.backends.get_active_backends",
        return_value=[grafana_backend, signalfx_backend],
    ):
        from dashforge.dashboard_ingest import ingest_dashboard

        asyncio.run(
            ingest_dashboard(
                dashboard_uid="test-uid",
                backend_name="grafana",
                auto_approve=False,
            )
        )

    # Both backends must have close() called
    grafana_backend.close.assert_awaited_once()
    signalfx_backend.close.assert_awaited_once()


print("[PASS] test_ingest_dashboard_closes_all_backends")


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # 1-2. Base
    test_publish_result_defaults()
    test_publish_result_with_values()
    test_backend_protocol_attributes()

    # 3-6. GrafanaBackend
    test_grafana_backend_properties()
    test_grafana_backend_discover_metrics()
    test_grafana_backend_validate_queries()
    test_grafana_backend_publish()
    test_grafana_backend_close()
    test_grafana_backend_discover_no_datasources()

    # 7-10. SignalFxBackend
    test_signalfx_backend_properties()
    test_signalfx_backend_discover_metrics()
    test_signalfx_backend_validate_queries()
    test_signalfx_backend_publish()
    test_signalfx_backend_close()
    test_signalfx_backend_discover_error()

    # 11-12. Registry
    test_registry_grafana_only()
    test_registry_signalfx_only()
    test_registry_both_enabled()
    test_registry_none_enabled()
    test_registry_primary_is_first()

    print("\n=== All backend adapter tests passed ===")
