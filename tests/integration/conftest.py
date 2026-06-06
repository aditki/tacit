"""Shared fixtures/helpers for hermetic vendor integration tests.

No live network: every test mounts RESPX over httpx and feeds responses built
by the contract factories. Everything under tests/integration is auto-marked
``integration`` so CI can run `-m "not integration"` (unit) first, then
`-m integration`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo

GRAFANA_BASE = "https://grafana.test"


def pytest_collection_modifyitems(config, items):
    integration_root = Path(__file__).parent
    for item in items:
        if Path(str(item.fspath)).is_relative_to(integration_root):
            item.add_marker(pytest.mark.integration)


def make_grafana_client() -> GrafanaClient:
    return GrafanaClient(base_url=GRAFANA_BASE, api_key="test-token", org_id=1)


def datasource(uid: str, name: str, ds_type: str, **json_data) -> DatasourceInfo:
    return DatasourceInfo(uid=uid, name=name, type=ds_type, json_data=json_data)


def proxy_url(uid: str, path: str) -> str:
    return f"{GRAFANA_BASE}/api/datasources/proxy/uid/{uid}/{path}"


def resource_url(uid: str, resource: str) -> str:
    return f"{GRAFANA_BASE}/api/datasources/uid/{uid}/resources/{resource}"
