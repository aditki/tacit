from __future__ import annotations

from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.grafana.adapters.registry import (
    get_adapter_for_type,
    register_adapter_factory,
    reset_adapters_for_tests,
    supported_datasource_types,
)
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry


class DummyAdapter(DatasourceAdapter):
    @property
    def query_language(self) -> str:
        return "dummyql"

    @property
    def supported_types(self) -> set[str]:
        return {"dummy-datasource"}

    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        return []


def test_register_adapter_factory_and_reset():
    calls = 0

    def factory() -> DatasourceAdapter:
        nonlocal calls
        calls += 1
        return DummyAdapter()

    try:
        register_adapter_factory("dummy", factory)

        first = get_adapter_for_type("dummy-datasource")
        second = get_adapter_for_type("dummy-datasource")

        assert isinstance(first, DummyAdapter)
        assert first is second
        assert calls == 1
        assert "dummy-datasource" in supported_datasource_types()

        reset_adapters_for_tests()
        third = get_adapter_for_type("dummy-datasource")
        assert isinstance(third, DummyAdapter)
        assert third is not first
        assert calls == 2
    finally:
        reset_adapters_for_tests()
