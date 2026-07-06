import asyncio

from tacit.grafana.datasource import _cap_metric_results, discover_all_metrics
from tacit.models.schemas import DatasourceInfo, MetricEntry


def _metric(name: str, datasource_uid: str) -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid=datasource_uid,
        datasource_name=datasource_uid,
        datasource_type="prometheus",
        query_language="promql",
    )


def test_metric_catalog_cap_interleaves_datasources():
    results = [
        [_metric(f"synthetic_{index}", "synthetic") for index in range(4)],
        [_metric(f"real_{index}", "real") for index in range(4)],
    ]

    capped = _cap_metric_results(results, 4)

    assert [(entry.datasource_uid, entry.name) for entry in capped] == [
        ("synthetic", "synthetic_0"),
        ("real", "real_0"),
        ("synthetic", "synthetic_1"),
        ("real", "real_1"),
    ]


def test_metric_catalog_cap_preserves_small_catalog_order():
    results = [
        [_metric("synthetic", "synthetic")],
        [_metric("real", "real")],
    ]

    capped = _cap_metric_results(results, 4)

    assert [entry.name for entry in capped] == ["synthetic", "real"]


def test_discover_all_metrics_marks_default_datasource(monkeypatch):
    class Adapter:
        async def discover_metrics(self, client, datasource, keywords):
            return [
                MetricEntry(
                    name=f"{datasource.uid}_metric",
                    datasource_uid=datasource.uid,
                    datasource_name=datasource.name,
                    datasource_type=datasource.type,
                    query_language="promql",
                )
            ]

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda datasource: Adapter())
    datasources = [
        DatasourceInfo(uid="classic-prom", name="Classic Prometheus", type="prometheus"),
        DatasourceInfo(uid="default-prom", name="Default Prometheus", type="prometheus", is_default=True),
    ]

    entries = asyncio.run(discover_all_metrics(client=object(), datasources=datasources, keywords=[]))

    defaults_by_uid = {entry.datasource_uid: entry.datasource_is_default for entry in entries}
    assert defaults_by_uid == {"classic-prom": False, "default-prom": True}
