from dashforge.grafana.datasource import _cap_metric_results
from dashforge.models.schemas import MetricEntry


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
