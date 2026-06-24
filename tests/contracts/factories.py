"""Factories that build vendor payloads *through* the contract models.

Every mock body in the integration tests is produced here, so a payload can
never drift from its schema: if a vendor renames a field we change the model
once and these factories (and the tests that use them) fail loudly.

Each factory returns plain JSON (``model_dump(mode="json")`` / lists) ready to
hand to ``httpx.Response(json=...)`` under RESPX.
"""

from __future__ import annotations

from typing import Any

from tests.contracts import (
    cloudwatch_models as cw,
)
from tests.contracts import (
    grafana_models as gf,
)
from tests.contracts import (
    signalfx_models as sfx,
)
from tests.contracts.elasticsearch_models import (
    ESFieldType,
    ESIndexMapping,
    ESMappingResponse,
    ESMappings,
)
from tests.contracts.graphite_models import GraphiteFindResponse, GraphiteNode
from tests.contracts.influxdb_models import InfluxQueryResponse, InfluxResult, InfluxSeries
from tests.contracts.loki_models import LokiLabelsResponse
from tests.contracts.prometheus_models import PrometheusLabelValuesResponse, PrometheusSeriesResponse

# ── Grafana ──────────────────────────────────────────────────────────────────


def grafana_datasource(uid: str, name: str, ds_type: str, **extra: Any) -> dict[str, Any]:
    return gf.GrafanaDatasource(uid=uid, name=name, type=ds_type, **extra).model_dump(by_alias=True)


def grafana_datasources(*datasources: dict[str, Any]) -> list[dict[str, Any]]:
    return list(datasources)


def grafana_dashboard_envelope(
    *, uid: str = "dash-1", title: str = "Service Health", panels: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    model = gf.GrafanaDashboardEnvelope(
        meta=gf.GrafanaDashboardMeta(slug=title.lower().replace(" ", "-"), url=f"/d/{uid}"),
        dashboard=gf.GrafanaDashboardModel(uid=uid, title=title, panels=panels or []),
    )
    return model.model_dump(by_alias=True)


def grafana_folder(uid: str = "folder-1", title: str = "Tacit") -> dict[str, Any]:
    return gf.GrafanaFolder(uid=uid, title=title, url=f"/dashboards/f/{uid}").model_dump(by_alias=True)


def grafana_save_response(uid: str = "abc123", *, dash_id: int = 42) -> dict[str, Any]:
    return gf.GrafanaDashboardSaveResponse(
        id=dash_id, uid=uid, url=f"/d/{uid}/tacit", slug="tacit", version=1
    ).model_dump(by_alias=True)


# ── Prometheus ───────────────────────────────────────────────────────────────


def prometheus_label_values(*names: str) -> dict[str, Any]:
    return PrometheusLabelValuesResponse(data=list(names)).model_dump()


def prometheus_series(*series: dict[str, str]) -> dict[str, Any]:
    return PrometheusSeriesResponse(data=list(series)).model_dump()


# ── Loki ─────────────────────────────────────────────────────────────────────


def loki_labels(*labels: str) -> dict[str, Any]:
    return LokiLabelsResponse(data=list(labels)).model_dump()


# ── CloudWatch ───────────────────────────────────────────────────────────────


def cloudwatch_namespaces(*namespaces: str) -> list[str]:
    return cw.CloudWatchNamespacesResponse(namespaces)  # type: ignore[call-arg]


def cloudwatch_metrics(*metrics: str) -> list[str]:
    return list(metrics)


def cloudwatch_dimension_keys(*dimensions: str) -> list[dict[str, str]]:
    return [{"value": dim} for dim in dimensions]


# ── Elasticsearch / OpenSearch ───────────────────────────────────────────────


def elasticsearch_mapping(index: str, fields: dict[str, str]) -> dict[str, Any]:
    model = ESMappingResponse(
        {index: ESIndexMapping(mappings=ESMappings(properties={f: ESFieldType(type=t) for f, t in fields.items()}))}
    )
    return model.model_dump()


# ── Graphite ─────────────────────────────────────────────────────────────────


def graphite_find(*metric_paths: str) -> list[dict[str, Any]]:
    nodes = [GraphiteNode(id=p, text=p.split(".")[-1], leaf=1) for p in metric_paths]
    return GraphiteFindResponse(nodes).model_dump()


# ── InfluxDB ─────────────────────────────────────────────────────────────────


def influx_measurements(*measurements: str) -> dict[str, Any]:
    series = InfluxSeries(name="measurements", columns=["name"], values=[[m] for m in measurements])
    return InfluxQueryResponse(results=[InfluxResult(series=[series])]).model_dump()


# ── SignalFx ─────────────────────────────────────────────────────────────────


def signalfx_metric_search(*names: str, type_: str = "GAUGE") -> dict[str, Any]:
    metrics = [sfx.SignalFxMetric(name=n, type=type_) for n in names]
    return sfx.SignalFxMetricSearchResponse(count=len(metrics), results=metrics).model_dump()


def signalfx_chart_response(chart_id: str = "CHART1", name: str = "CPU") -> dict[str, Any]:
    return sfx.SignalFxChartResponse(id=chart_id, name=name).model_dump()


def signalfx_dashboard_response(dash_id: str = "DASH1", name: str = "Tacit") -> dict[str, Any]:
    return sfx.SignalFxDashboardResponse(id=dash_id, name=name).model_dump()
