"""Registry that maps Grafana datasource types to adapters."""

from __future__ import annotations

from collections.abc import Callable

import structlog

from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.models.schemas import DatasourceInfo

logger = structlog.get_logger()

AdapterFactory = Callable[[], DatasourceAdapter]


def _prometheus_adapter() -> DatasourceAdapter:
    from dashforge.grafana.adapters.prometheus import PrometheusAdapter

    return PrometheusAdapter()


def _cloudwatch_adapter() -> DatasourceAdapter:
    from dashforge.grafana.adapters.cloudwatch import CloudWatchAdapter

    return CloudWatchAdapter()


def _loki_adapter() -> DatasourceAdapter:
    from dashforge.grafana.adapters.loki import LokiAdapter

    return LokiAdapter()


def _elasticsearch_adapter() -> DatasourceAdapter:
    from dashforge.grafana.adapters.elasticsearch import ElasticsearchAdapter

    return ElasticsearchAdapter()


def _graphite_adapter() -> DatasourceAdapter:
    from dashforge.grafana.adapters.graphite import GraphiteAdapter

    return GraphiteAdapter()


def _influxdb_adapter() -> DatasourceAdapter:
    from dashforge.grafana.adapters.influxdb import InfluxDBAdapter

    return InfluxDBAdapter()


def _signalfx_adapter() -> DatasourceAdapter:
    from dashforge.grafana.adapters.signalfx import SignalFxAdapter

    return SignalFxAdapter()


_ADAPTER_FACTORIES: dict[str, AdapterFactory] = {
    "prometheus": _prometheus_adapter,
    "cloudwatch": _cloudwatch_adapter,
    "loki": _loki_adapter,
    "elasticsearch": _elasticsearch_adapter,
    "graphite": _graphite_adapter,
    "influxdb": _influxdb_adapter,
    "signalfx": _signalfx_adapter,
}
_TYPE_MAP: dict[str, DatasourceAdapter] | None = None


def register_adapter_factory(name: str, factory: AdapterFactory) -> None:
    """Register or override a Grafana datasource adapter factory."""
    _ADAPTER_FACTORIES[name.lower()] = factory
    reset_adapters_for_tests()


def reset_adapters_for_tests() -> None:
    """Clear cached adapter instances."""
    global _TYPE_MAP
    _TYPE_MAP = None


def _type_map() -> dict[str, DatasourceAdapter]:
    global _TYPE_MAP
    if _TYPE_MAP is not None:
        return _TYPE_MAP

    type_map: dict[str, DatasourceAdapter] = {}
    for name, factory in _ADAPTER_FACTORIES.items():
        adapter = factory()
        for ds_type in adapter.supported_types:
            type_map[ds_type] = adapter
        logger.debug("datasource_adapter_registered", adapter=name, supported_types=sorted(adapter.supported_types))
    _TYPE_MAP = type_map
    return type_map


def get_adapter(datasource: DatasourceInfo) -> DatasourceAdapter | None:
    """Return the adapter for a given datasource, or None if unsupported."""
    return _type_map().get(datasource.type)


def get_adapter_for_type(ds_type: str) -> DatasourceAdapter | None:
    """Return the adapter for a datasource type string."""
    return _type_map().get(ds_type)


def supported_datasource_types() -> set[str]:
    """Return all datasource types we can search."""
    return set(_type_map().keys())
