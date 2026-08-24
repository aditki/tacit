"""TDD tests for the DashboardBackend adapter pattern.

Tests written BEFORE implementation. These define the contract that
GrafanaBackend, SignalFxBackend, and the registry must satisfy.
"""

import asyncio
import os
import sys
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tacit.models.schemas import (
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


def _backend_settings(*, grafana: bool, signalfx: bool, token: str = ""):
    from tacit.config import Settings

    return Settings(
        _env_file=None,
        grafana_enabled=grafana,
        grafana_url="http://grafana.test",
        grafana_api_key="",
        grafana_org_id=1,
        signalfx_enabled=signalfx,
        signalfx_api_token=token,
        signalfx_realm="us1",
    )


def _owned_mock_client(provider: str) -> AsyncMock:
    from tacit.config import Settings
    from tacit.runtime_ownership import (
        RuntimeRemoteIdentity,
        adapt_third_party_runtime_owner,
        credential_fingerprint,
    )

    runtime_settings = Settings(
        _env_file=None,
        grafana_url="https://grafana.test",
        grafana_api_key="",
        grafana_org_id=1,
        signalfx_realm="us1",
        signalfx_api_token="",
    )
    if provider == "grafana":
        remote = RuntimeRemoteIdentity(
            provider="grafana",
            endpoint=runtime_settings.grafana_url,
            account=str(runtime_settings.grafana_org_id),
            credential_fingerprint=credential_fingerprint(runtime_settings.grafana_api_key),
        )
    else:
        remote = RuntimeRemoteIdentity(
            provider="signalfx",
            endpoint="https://api.us1.signalfx.com",
            account="us1",
            credential_fingerprint=credential_fingerprint(runtime_settings.signalfx_api_token),
        )
    client = AsyncMock()
    client.runtime_settings = runtime_settings
    client.runtime_ownership = adapt_third_party_runtime_owner(
        component=f"test_{provider}_client",
        owner=client,
        runtime_settings=runtime_settings,
        remote=remote,
    )
    return client


@pytest.mark.parametrize(
    ("backend_path", "provider", "other_provider"),
    [
        ("tacit.backends.grafana.GrafanaBackend", "grafana", "signalfx"),
        ("tacit.backends.signalfx.SignalFxBackend", "signalfx", "grafana"),
    ],
)
def test_backend_rejects_mixed_provider_descriptor(
    backend_path,
    provider,
    other_provider,
):
    from importlib import import_module

    from tacit.runtime_ownership import RuntimeOwnershipMismatchError, RuntimeRemoteIdentity

    client = _owned_mock_client(provider)
    expected = client.runtime_ownership.remotes[0]
    client.runtime_ownership = replace(
        client.runtime_ownership,
        remotes=(
            expected,
            RuntimeRemoteIdentity(
                provider=other_provider,
                endpoint=(
                    "https://api.us1.signalfx.com" if other_provider == "signalfx" else "https://grafana.other.test"
                ),
            ),
        ),
    )
    module_name, class_name = backend_path.rsplit(".", 1)
    backend_type = getattr(import_module(module_name), class_name)

    with pytest.raises(RuntimeOwnershipMismatchError, match="sole provider"):
        backend_type(client=client)


@pytest.mark.parametrize(
    ("backend_path", "provider", "discovery_patch"),
    [
        (
            "tacit.backends.grafana.GrafanaBackend",
            "grafana",
            "tacit.backends.grafana.list_datasources",
        ),
        (
            "tacit.backends.signalfx.SignalFxBackend",
            "signalfx",
            "tacit.backends.signalfx.sfx_discover",
        ),
    ],
)
def test_backend_discovery_propagates_authority_failures(
    backend_path,
    provider,
    discovery_patch,
):
    from importlib import import_module

    from tacit.errors import RuntimeOwnershipError

    module_name, class_name = backend_path.rsplit(".", 1)
    backend_type = getattr(import_module(module_name), class_name)
    backend = backend_type(client=_owned_mock_client(provider))

    with patch(discovery_patch, new_callable=AsyncMock) as discovery:
        discovery.side_effect = RuntimeOwnershipError("sensitive owner detail")
        with pytest.raises(RuntimeOwnershipError, match="sensitive owner detail"):
            asyncio.run(backend.discover_metrics([], _make_intent()))


def test_grafana_backend_passes_app_scoped_discovery_settings():
    from tacit.backends.grafana import GrafanaBackend

    client = _owned_mock_client("grafana")
    runtime_settings = client.runtime_settings.model_copy(update={"max_metric_catalog_size": 1})
    client.runtime_settings = runtime_settings
    from tacit.runtime_ownership import runtime_descriptor_for_remote

    client.runtime_ownership = runtime_descriptor_for_remote(
        component="test_grafana_client",
        runtime_settings=runtime_settings,
        remote=client.runtime_ownership.remotes[0],
    )
    backend = GrafanaBackend(client=client)

    with (
        patch.object(backend, "_select_searchable_datasources", new_callable=AsyncMock) as select,
        patch("tacit.backends.grafana.discover_all_metrics", new_callable=AsyncMock) as discover,
    ):
        select.return_value = ([MagicMock()], [MagicMock()])
        discover.return_value = []
        asyncio.run(backend.discover_metrics([], _make_intent()))

    assert discover.call_args.kwargs["runtime_settings"].max_metric_catalog_size == 1


@pytest.mark.parametrize(
    ("backend_path", "client_method"),
    [
        ("tacit.backends.grafana.GrafanaBackend", "_get"),
        ("tacit.backends.signalfx.SignalFxBackend", "search_metrics"),
    ],
)
def test_injected_backend_rejects_ownerless_client_before_transport_use(backend_path, client_method):
    from importlib import import_module

    from tacit.runtime_ownership import RuntimeOwnershipError

    class OwnerlessClient:
        calls = 0

        def __getattr__(self, name):
            if name == client_method:
                self.calls += 1
            raise AttributeError(name)

    module_name, class_name = backend_path.rsplit(".", 1)
    backend_type = getattr(import_module(module_name), class_name)
    client = OwnerlessClient()

    with pytest.raises(RuntimeOwnershipError, match="public runtime ownership descriptor"):
        backend_type(client=client)

    assert client.calls == 0


@pytest.mark.parametrize(
    "backend_path",
    [
        "tacit.backends.grafana.GrafanaBackend",
        "tacit.backends.signalfx.SignalFxBackend",
    ],
)
def test_injected_backend_rejects_explicitly_unavailable_client_before_transport_use(backend_path):
    from importlib import import_module

    from tacit.runtime_ownership import RuntimeOwnershipDescriptor, RuntimeOwnershipError

    class UnavailableClient:
        runtime_ownership = RuntimeOwnershipDescriptor.unavailable(
            component="third_party_client",
            reason="adapter_unavailable",
        )
        calls = 0

        def __getattr__(self, name):
            self.calls += 1
            raise AttributeError(name)

    module_name, class_name = backend_path.rsplit(".", 1)
    backend_type = getattr(import_module(module_name), class_name)
    client = UnavailableClient()

    with pytest.raises(RuntimeOwnershipError, match="explicitly unavailable"):
        backend_type(client=client)

    assert client.calls == 0


@pytest.mark.parametrize(
    ("backend_path", "provider"),
    [
        ("tacit.backends.grafana.GrafanaBackend", "grafana"),
        ("tacit.backends.signalfx.SignalFxBackend", "signalfx"),
    ],
)
def test_injected_backend_rejects_descriptor_without_expected_remote(backend_path, provider):
    from importlib import import_module

    from tacit.runtime_ownership import RuntimeOwnershipDescriptor, RuntimeOwnershipMismatchError

    class WrongRemoteClient:
        runtime_ownership = RuntimeOwnershipDescriptor(
            component=f"third_party_{provider}_client",
            cache_namespace="unrelated-cache",
        )

    module_name, class_name = backend_path.rsplit(".", 1)
    backend_type = getattr(import_module(module_name), class_name)

    with pytest.raises(RuntimeOwnershipMismatchError, match="expected remote identity"):
        backend_type(client=WrongRemoteClient())


@pytest.mark.parametrize("provider", ["grafana", "signalfx"])
def test_injected_backend_accepts_explicit_third_party_ownership_adapter(provider):
    from tacit.backends.grafana import GrafanaBackend
    from tacit.backends.signalfx import SignalFxBackend
    from tacit.config import Settings
    from tacit.runtime_ownership import (
        RuntimeRemoteIdentity,
        adapt_third_party_runtime_owner,
        credential_fingerprint,
    )

    runtime_settings = Settings(
        _env_file=None,
        grafana_url="https://grafana.example.test",
        grafana_api_key="grafana-secret",
        grafana_org_id=7,
        signalfx_realm="us1",
        signalfx_api_token="signalfx-secret",
    )
    if provider == "grafana":
        remote = RuntimeRemoteIdentity(
            provider="grafana",
            endpoint=runtime_settings.grafana_url,
            account=str(runtime_settings.grafana_org_id),
            credential_fingerprint=credential_fingerprint(runtime_settings.grafana_api_key),
        )
        backend_type = GrafanaBackend
    else:
        remote = RuntimeRemoteIdentity(
            provider="signalfx",
            endpoint="https://api.us1.signalfx.com",
            account="us1",
            credential_fingerprint=credential_fingerprint(runtime_settings.signalfx_api_token),
        )
        backend_type = SignalFxBackend

    class AdaptedClient:
        runtime_ownership = adapt_third_party_runtime_owner(
            component=f"third_party_{provider}_client",
            owner=object(),
            runtime_settings=runtime_settings,
            remote=remote,
        )

    backend = backend_type(client=AdaptedClient(), runtime_settings=runtime_settings)  # type: ignore[arg-type]

    assert backend.runtime_ownership.remotes == (remote,)


@pytest.mark.parametrize(
    ("base_url", "error"),
    [
        ("", "remote endpoint is invalid"),
        ("   ", "remote endpoint is invalid"),
        ("grafana.example", "remote endpoint is invalid"),
        (
            "https://operator:secret@grafana.example",
            "remote endpoint credentials are not allowed",
        ),
    ],
)
def test_grafana_client_rejects_explicit_invalid_base_url_before_http_client_construction(base_url, error):
    from tacit.config import Settings
    from tacit.grafana.client import GrafanaClient
    from tacit.runtime_ownership import RuntimeOwnershipError

    runtime_settings = Settings(_env_file=None, grafana_url="https://configured.grafana.example")

    with patch("tacit.grafana.client.httpx.AsyncClient") as http_client:
        with pytest.raises(RuntimeOwnershipError, match=error):
            GrafanaClient(base_url=base_url, runtime_settings=runtime_settings)

    http_client.assert_not_called()


def test_grafana_client_uses_configured_base_url_when_override_is_none():
    from tacit.config import Settings
    from tacit.grafana.client import GrafanaClient

    runtime_settings = Settings(_env_file=None, grafana_url="https://Configured.Grafana.Example:443/api/")

    with patch("tacit.grafana.client.httpx.AsyncClient") as http_client:
        client = GrafanaClient(base_url=None, runtime_settings=runtime_settings)
        transport = client._client

    assert client.base_url == "https://configured.grafana.example/api"
    assert transport is http_client.return_value
    assert http_client.call_args.kwargs["base_url"] == client.base_url


def test_grafana_client_override_is_the_effective_sole_remote_owner():
    from tacit.backends.grafana import GrafanaBackend
    from tacit.config import Settings
    from tacit.grafana.client import GrafanaClient
    from tacit.runtime_ownership import credential_fingerprint

    configured = Settings(
        _env_file=None,
        grafana_url="https://configured.grafana.example",
        grafana_api_key="configured-key",
        grafana_org_id=1,
    )

    with patch("tacit.grafana.client.httpx.AsyncClient") as http_client:
        client = GrafanaClient(
            base_url="https://Override.Grafana.Example:443/api/",
            api_key="override-key",
            org_id=42,
            runtime_settings=configured,
        )
        backend = GrafanaBackend(client=client)

    remote = backend.runtime_ownership.remotes[0]
    assert remote.provider == "grafana"
    assert remote.endpoint == "https://override.grafana.example/api"
    assert remote.account == "42"
    assert remote.credential_fingerprint == credential_fingerprint("override-key")
    assert client.runtime_settings.grafana_url == remote.endpoint
    assert client.runtime_settings.grafana_api_key == "override-key"
    assert client.runtime_settings.grafana_org_id == 42
    assert backend.runtime_settings == client.runtime_settings
    http_client.assert_not_called()


def test_grafana_backend_accepts_equivalent_explicit_second_owner():
    from tacit.backends.grafana import GrafanaBackend
    from tacit.config import Settings
    from tacit.grafana.client import GrafanaClient

    configured = Settings(
        _env_file=None,
        grafana_url="https://configured.grafana.example",
        grafana_api_key="configured-key",
        grafana_org_id=1,
    )

    with patch("tacit.grafana.client.httpx.AsyncClient"):
        client = GrafanaClient(
            base_url="https://effective.grafana.example",
            api_key="effective-key",
            org_id=7,
            runtime_settings=configured,
        )
        backend = GrafanaBackend(client=client, runtime_settings=client.runtime_settings)

    assert backend.runtime_ownership.remotes == client.runtime_ownership.remotes


@pytest.mark.parametrize(
    "settings_update",
    [
        {"grafana_url": "https://other.grafana.example"},
        {"grafana_org_id": 99},
        {"grafana_api_key": "other-key"},
    ],
)
def test_grafana_backend_rejects_disagreeing_explicit_owner_before_network_io(settings_update):
    from tacit.backends.grafana import GrafanaBackend
    from tacit.config import Settings
    from tacit.grafana.client import GrafanaClient
    from tacit.runtime_ownership import RuntimeOwnershipMismatchError

    effective = Settings(
        _env_file=None,
        grafana_url="https://configured.grafana.example",
        grafana_api_key="configured-key",
        grafana_org_id=1,
    )

    with patch("tacit.grafana.client.httpx.AsyncClient") as http_client:
        client = GrafanaClient(
            base_url="https://effective.grafana.example",
            api_key="effective-key",
            org_id=7,
            runtime_settings=effective,
        )
        transport = http_client.return_value
        with pytest.raises(RuntimeOwnershipMismatchError):
            GrafanaBackend(
                client=client,
                runtime_settings=client.runtime_settings.model_copy(deep=True, update=settings_update),
            )

    transport.get.assert_not_called()
    transport.post.assert_not_called()
    http_client.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# 1. base.py — PublishResult dataclass
# ═══════════════════════════════════════════════════════════════════════════


def test_publish_result_defaults():
    from tacit.backends.base import PublishResult

    r = PublishResult()
    assert r.url == ""
    assert r.uid == ""
    assert r.backend_name == ""
    print("[PASS] test_publish_result_defaults")


def test_publish_result_with_values():
    from tacit.backends.base import PublishResult

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

    from tacit.backends.base import DashboardBackend

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
    from tacit.backends.grafana import GrafanaBackend

    backend = GrafanaBackend.__new__(GrafanaBackend)
    assert backend.name == "grafana"
    assert backend.query_language == "promql"
    print("[PASS] test_grafana_backend_properties")


# ═══════════════════════════════════════════════════════════════════════════
# 4. GrafanaBackend — discover_metrics delegates to existing code
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_discover_metrics():
    from tacit.backends.grafana import GrafanaBackend

    mock_client = _owned_mock_client("grafana")
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
        patch("tacit.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list,
        patch("tacit.backends.grafana.filter_datasources_by_signal") as mock_filter,
        patch("tacit.backends.grafana.filter_searchable_datasources") as mock_searchable,
        patch("tacit.backends.grafana.discover_all_metrics", new_callable=AsyncMock) as mock_discover,
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
    from tacit.backends.grafana import GrafanaBackend

    mock_client = _owned_mock_client("grafana")
    backend = GrafanaBackend(client=mock_client)

    spec = _make_spec()

    with patch("tacit.backends.grafana.validate_dashboard_queries", new_callable=AsyncMock) as mock_val:
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
    from tacit.backends.base import PublishResult
    from tacit.backends.grafana import GrafanaBackend
    from tacit.config import Settings
    from tacit.runtime_ownership import (
        RuntimeRemoteIdentity,
        credential_fingerprint,
        runtime_descriptor_for_remote,
    )

    mock_client = _owned_mock_client("grafana")
    runtime_settings = Settings(grafana_url="http://runtime-grafana.test", tacit_dashboard_folder="Runtime")
    mock_client.runtime_settings = runtime_settings
    mock_client.runtime_ownership = runtime_descriptor_for_remote(
        component="test_grafana_client",
        runtime_settings=runtime_settings,
        remote=RuntimeRemoteIdentity(
            provider="grafana",
            endpoint=runtime_settings.grafana_url,
            account=str(runtime_settings.grafana_org_id),
            credential_fingerprint=credential_fingerprint(runtime_settings.grafana_api_key),
        ),
    )
    backend = GrafanaBackend(client=mock_client, runtime_settings=runtime_settings)

    spec = _make_spec()

    with patch("tacit.backends.grafana.publish_dashboard_fn", new_callable=AsyncMock) as mock_pub:
        mock_pub.return_value = ("http://grafana/d/abc", "abc")
        result = asyncio.run(backend.publish(spec))
        assert isinstance(result, PublishResult)
        assert result.url == "http://grafana/d/abc"
        assert result.uid == "abc"
        assert result.backend_name == "grafana"
        mock_pub.assert_called_once_with(mock_client, spec, runtime_settings=backend.runtime_settings)

    print("[PASS] test_grafana_backend_publish")


# ═══════════════════════════════════════════════════════════════════════════
# 7. SignalFxBackend — properties
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_properties():
    from tacit.backends.signalfx import SignalFxBackend

    backend = SignalFxBackend.__new__(SignalFxBackend)
    assert backend.name == "signalfx"
    assert backend.query_language == "signalflow"
    print("[PASS] test_signalfx_backend_properties")


# ═══════════════════════════════════════════════════════════════════════════
# 8. SignalFxBackend — discover_metrics delegates to signalfx.discovery
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_discover_metrics():
    from tacit.backends.signalfx import SignalFxBackend

    mock_client = _owned_mock_client("signalfx")
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

    with patch("tacit.backends.signalfx.sfx_discover", new_callable=AsyncMock) as mock_disc:
        mock_disc.return_value = fake_entries
        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))
        assert len(result) == 1
        assert result[0].datasource_type == "signalfx"
        mock_disc.assert_called_once_with(
            mock_client,
            intent.keywords,
            runtime_settings=backend.runtime_settings,
        )

    print("[PASS] test_signalfx_backend_discover_metrics")


# ═══════════════════════════════════════════════════════════════════════════
# 9. SignalFxBackend — validate_queries delegates to validate_signalflow_queries
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_validate_queries():
    from tacit.backends.signalfx import SignalFxBackend

    mock_client = _owned_mock_client("signalfx")
    backend = SignalFxBackend(client=mock_client)

    spec = _make_spec(query_lang="signalflow", ds_type="signalfx")

    with patch("tacit.backends.signalfx.validate_signalflow_queries", new_callable=AsyncMock) as mock_val:
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
    from tacit.backends.base import PublishResult
    from tacit.backends.signalfx import SignalFxBackend
    from tacit.config import Settings
    from tacit.runtime_ownership import (
        RuntimeRemoteIdentity,
        credential_fingerprint,
        runtime_descriptor_for_remote,
    )

    mock_client = _owned_mock_client("signalfx")
    runtime_settings = Settings(signalfx_dashboard_group="Runtime Group")
    mock_client.runtime_settings = runtime_settings
    mock_client.runtime_ownership = runtime_descriptor_for_remote(
        component="test_signalfx_client",
        runtime_settings=runtime_settings,
        remote=RuntimeRemoteIdentity(
            provider="signalfx",
            endpoint=f"https://api.{runtime_settings.signalfx_realm}.signalfx.com",
            account=runtime_settings.signalfx_realm,
            credential_fingerprint=credential_fingerprint(runtime_settings.signalfx_api_token),
        ),
    )
    backend = SignalFxBackend(client=mock_client, runtime_settings=runtime_settings)

    spec = _make_spec(query_lang="signalflow", ds_type="signalfx")

    with patch("tacit.backends.signalfx.sfx_publish", new_callable=AsyncMock) as mock_pub:
        mock_pub.return_value = ("https://app.us1.signalfx.com/#/dashboard/D123", "D123")
        result = asyncio.run(backend.publish(spec))
        assert isinstance(result, PublishResult)
        assert "signalfx.com" in result.url
        assert result.uid == "D123"
        assert result.backend_name == "signalfx"
        mock_pub.assert_called_once_with(
            mock_client,
            spec,
            group_name="Runtime Group",
            runtime_settings=backend.runtime_settings,
        )

    print("[PASS] test_signalfx_backend_publish")


# ═══════════════════════════════════════════════════════════════════════════
# 11. Registry — get_active_backends reads config
# ═══════════════════════════════════════════════════════════════════════════


def test_registry_grafana_only():
    from tacit.backends import get_active_backends

    with patch("tacit.backends.settings", _backend_settings(grafana=True, signalfx=False)):
        backends = get_active_backends()
        try:
            assert len(backends) == 1
            assert backends[0].name == "grafana"
        finally:
            for backend in backends:
                asyncio.run(backend.close())

    print("[PASS] test_registry_grafana_only")


def test_registry_signalfx_only():
    from tacit.backends import get_active_backends

    with patch(
        "tacit.backends.settings",
        _backend_settings(grafana=False, signalfx=True, token="test-token"),
    ):
        backends = get_active_backends()
        try:
            assert len(backends) == 1
            assert backends[0].name == "signalfx"
        finally:
            for backend in backends:
                asyncio.run(backend.close())

    print("[PASS] test_registry_signalfx_only")


def test_registry_both_enabled():
    from tacit.backends import get_active_backends

    with patch(
        "tacit.backends.settings",
        _backend_settings(grafana=True, signalfx=True, token="test-token"),
    ):
        backends = get_active_backends()
        try:
            assert len(backends) == 2
            names = {b.name for b in backends}
            assert names == {"grafana", "signalfx"}
        finally:
            for backend in backends:
                asyncio.run(backend.close())

    print("[PASS] test_registry_both_enabled")


def test_registry_none_enabled():
    from tacit.backends import get_active_backends

    with patch("tacit.backends.settings", _backend_settings(grafana=False, signalfx=False)):
        backends = get_active_backends()
        assert len(backends) == 0

    print("[PASS] test_registry_none_enabled")


def test_registry_uses_explicit_runtime_settings():
    from tacit.backends import get_active_backends
    from tacit.config import Settings

    runtime_settings = Settings(
        grafana_enabled=False,
        signalfx_enabled=True,
        signalfx_api_token="runtime-token",
        signalfx_realm="eu0",
    )

    backends = get_active_backends(runtime_settings)

    try:
        assert len(backends) == 1
        assert backends[0].name == "signalfx"
        assert backends[0]._client.api_token == "runtime-token"
        assert backends[0]._client.realm == "eu0"
    finally:
        for backend in backends:
            asyncio.run(backend.close())


# ═══════════════════════════════════════════════════════════════════════════
# 12. Registry — primary backend is first in list
# ═══════════════════════════════════════════════════════════════════════════


def test_registry_primary_is_first():
    """When both enabled, the primary backend (first) determines query language."""
    from tacit.backends import get_active_backends

    with patch(
        "tacit.backends.settings",
        _backend_settings(grafana=True, signalfx=True, token="tok"),
    ):
        backends = get_active_backends()
        try:
            primary = backends[0]
            # When Grafana is enabled, it should be primary (PromQL is the standard)
            assert primary.name == "grafana"
            assert primary.query_language == "promql"
        finally:
            for backend in backends:
                asyncio.run(backend.close())

    print("[PASS] test_registry_primary_is_first")


# ═══════════════════════════════════════════════════════════════════════════
# 13. Close — backends clean up resources
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_close():
    from tacit.backends.grafana import GrafanaBackend

    mock_client = _owned_mock_client("grafana")
    backend = GrafanaBackend(client=mock_client)
    asyncio.run(backend.close())
    mock_client.close.assert_called_once()
    print("[PASS] test_grafana_backend_close")


def test_signalfx_backend_close():
    from tacit.backends.signalfx import SignalFxBackend

    mock_client = _owned_mock_client("signalfx")
    backend = SignalFxBackend(client=mock_client)
    asyncio.run(backend.close())
    mock_client.close.assert_called_once()
    print("[PASS] test_signalfx_backend_close")


def test_signalfx_backend_list_dashboards_reads_dashboard_configs():
    from tacit.backends.signalfx import SignalFxBackend

    mock_client = _owned_mock_client("signalfx")
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


def test_signalfx_backend_list_dashboards_paginates_dashboard_groups():
    from tacit.backends.signalfx import SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE, SignalFxBackend

    first_page = {
        "results": [
            {
                "name": f"Group {i}",
                "dashboardConfigs": [{"dashboardId": f"dash-{i}", "name": f"Dashboard {i}"}],
            }
            for i in range(SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE)
        ],
        "nextPageLink": f"/v2/dashboardgroup?offset={SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE}",
    }
    second_page = {
        "results": [
            {
                "name": "Final Group",
                "dashboardConfigs": [{"dashboardId": "dash-final", "name": "Dashboard Final"}],
            }
        ]
    }

    mock_client = _owned_mock_client("signalfx")
    mock_client.list_dashboard_groups.side_effect = [first_page, second_page]
    backend = SignalFxBackend(client=mock_client)

    dashboards = asyncio.run(backend.list_dashboards(limit=SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE + 1))

    assert len(dashboards) == SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE + 1
    assert dashboards[0] == {
        "uid": "dash-0",
        "title": "Dashboard 0",
        "folder": "Group 0",
        "backend": "signalfx",
    }
    assert dashboards[-1] == {
        "uid": "dash-final",
        "title": "Dashboard Final",
        "folder": "Final Group",
        "backend": "signalfx",
    }
    assert mock_client.list_dashboard_groups.call_args_list[0].kwargs == {
        "limit": SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE,
        "offset": 0,
    }
    assert mock_client.list_dashboard_groups.call_args_list[1].kwargs == {
        "limit": 1,
        "offset": SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE,
    }
    print("[PASS] test_signalfx_backend_list_dashboards_paginates_dashboard_groups")


def test_grafana_backend_list_dashboards_paginates_search_results():
    from tacit.backends.grafana import GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE, GrafanaBackend

    first_page = [
        {
            "uid": f"dash-{i}",
            "title": f"Dashboard {i}",
            "folderTitle": "Ops",
            "url": f"/d/dash-{i}",
        }
        for i in range(GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE)
    ]
    second_page = [
        {
            "uid": "dash-final",
            "title": "Dashboard Final",
            "folderTitle": "Ops",
            "url": "/d/dash-final",
        }
    ]

    mock_client = _owned_mock_client("grafana")
    mock_client._get.side_effect = [first_page, second_page]
    backend = GrafanaBackend(client=mock_client)

    dashboards = asyncio.run(backend.list_dashboards(limit=GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE + 1))

    assert len(dashboards) == GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE + 1
    assert dashboards[0] == {
        "uid": "dash-0",
        "title": "Dashboard 0",
        "folder": "Ops",
        "url": "/d/dash-0",
        "backend": "grafana",
    }
    assert dashboards[-1] == {
        "uid": "dash-final",
        "title": "Dashboard Final",
        "folder": "Ops",
        "url": "/d/dash-final",
        "backend": "grafana",
    }
    assert mock_client._get.call_args_list[0].kwargs["params"] == {
        "type": "dash-db",
        "limit": GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE,
        "page": 1,
    }
    assert mock_client._get.call_args_list[1].kwargs["params"] == {
        "type": "dash-db",
        "limit": GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE,
        "page": 2,
    }
    print("[PASS] test_grafana_backend_list_dashboards_paginates_search_results")


def test_grafana_dashboard_limit_does_not_claim_a_complete_crawl():
    from tacit.backends.grafana import GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE, GrafanaBackend

    first_page = [{"uid": f"dash-{index}"} for index in range(GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE)]
    second_page = [{"uid": f"dash-{GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE + index}"} for index in range(200)]
    mock_client = _owned_mock_client("grafana")
    mock_client._get.side_effect = [first_page, second_page]
    backend = GrafanaBackend(client=mock_client)

    dashboards = asyncio.run(backend.list_dashboards(limit=600))

    assert len(dashboards) == 600
    assert backend.last_dashboard_list_complete is False
    assert mock_client._get.call_args_list[1].kwargs["params"] == {
        "type": "dash-db",
        "limit": GRAFANA_DASHBOARD_SEARCH_PAGE_SIZE,
        "page": 2,
    }


# ═══════════════════════════════════════════════════════════════════════════
# 14. GrafanaBackend — discover returns empty when no searchable datasources
# ═══════════════════════════════════════════════════════════════════════════


def test_grafana_backend_discover_no_datasources():
    from tacit.backends.grafana import GrafanaBackend

    mock_client = _owned_mock_client("grafana")
    backend = GrafanaBackend(client=mock_client)

    with (
        patch("tacit.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list,
        patch("tacit.backends.grafana.filter_datasources_by_signal") as mock_filter,
        patch("tacit.backends.grafana.filter_searchable_datasources") as mock_searchable,
    ):

        mock_list.return_value = []
        mock_filter.return_value = []
        mock_searchable.return_value = []

        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))
        assert result == []

    print("[PASS] test_grafana_backend_discover_no_datasources")


def test_grafana_backend_datasource_targets_when_metrics_absent():
    from tacit.backends.grafana import GrafanaBackend

    mock_client = _owned_mock_client("grafana")
    backend = GrafanaBackend(client=mock_client)
    prom_ds = DatasourceInfo(
        uid="prom1",
        name="Prometheus",
        type="prometheus",
    )

    with (
        patch("tacit.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list,
        patch("tacit.backends.grafana.filter_datasources_by_signal") as mock_filter,
        patch("tacit.backends.grafana.filter_searchable_datasources") as mock_searchable,
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
    from tacit.backends.grafana import GrafanaBackend

    mock_client = _owned_mock_client("grafana")
    backend = GrafanaBackend(client=mock_client)

    with patch("tacit.backends.grafana.list_datasources", new_callable=AsyncMock) as mock_list:
        mock_list.side_effect = RuntimeError("connection refused")
        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))

        assert result == []
        assert backend.last_discovery_status.available is False
        assert backend.last_discovery_status.error == "grafana_discover_failed"

    print("[PASS] test_grafana_backend_marks_connection_failure_unavailable")


# ═══════════════════════════════════════════════════════════════════════════
# 15. SignalFxBackend — discover handles errors gracefully
# ═══════════════════════════════════════════════════════════════════════════


def test_signalfx_backend_discover_error():
    from tacit.backends.signalfx import SignalFxBackend

    mock_client = _owned_mock_client("signalfx")
    backend = SignalFxBackend(client=mock_client)

    with patch("tacit.backends.signalfx.sfx_discover", new_callable=AsyncMock) as mock_disc:
        mock_disc.side_effect = Exception("Connection refused")
        intent = _make_intent()
        result = asyncio.run(backend.discover_metrics(intent.keywords, intent))
        assert result == []

    print("[PASS] test_signalfx_backend_discover_error")


# ── Bug 8: ingest_dashboard must close all backends ────────────────────


def test_ingest_dashboard_closes_all_backends(tmp_path):
    """When get_active_backends() returns multiple backends and one is
    selected by name, ALL backends must be closed — not just the selected
    one.  Otherwise, unused HTTP clients leak."""
    from tacit.backends.base import DashboardFeatures
    from tacit.signals import SignalStore

    signal_store = SignalStore(db_path=tmp_path / "signals.db")

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
        "tacit.backends.get_active_backends",
        return_value=[grafana_backend, signalfx_backend],
    ):
        from tacit.dashboard_ingest import ingest_dashboard

        asyncio.run(
            ingest_dashboard(
                dashboard_uid="test-uid",
                backend_name="grafana",
                auto_approve=False,
                store=signal_store,
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
