"""Registry that maps Grafana datasource types to adapters."""
from __future__ import annotations

import structlog

from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.grafana.adapters.cloudwatch import CloudWatchAdapter
from dashforge.grafana.adapters.elasticsearch import ElasticsearchAdapter
from dashforge.grafana.adapters.graphite import GraphiteAdapter
from dashforge.grafana.adapters.influxdb import InfluxDBAdapter
from dashforge.grafana.adapters.loki import LokiAdapter
from dashforge.grafana.adapters.prometheus import PrometheusAdapter
from dashforge.models.schemas import DatasourceInfo

logger = structlog.get_logger()

_ALL_ADAPTERS: list[DatasourceAdapter] = [
    PrometheusAdapter(),
    CloudWatchAdapter(),
    LokiAdapter(),
    ElasticsearchAdapter(),
    GraphiteAdapter(),
    InfluxDBAdapter(),
]

_TYPE_MAP: dict[str, DatasourceAdapter] = {}
for _adapter in _ALL_ADAPTERS:
    for _ds_type in _adapter.supported_types:
        _TYPE_MAP[_ds_type] = _adapter


def get_adapter(datasource: DatasourceInfo) -> DatasourceAdapter | None:
    """Return the adapter for a given datasource, or None if unsupported."""
    return _TYPE_MAP.get(datasource.type)


def get_adapter_for_type(ds_type: str) -> DatasourceAdapter | None:
    """Return the adapter for a datasource type string."""
    return _TYPE_MAP.get(ds_type)


def supported_datasource_types() -> set[str]:
    """Return all datasource types we can search."""
    return set(_TYPE_MAP.keys())
