from __future__ import annotations

from dashforge.signals.learning_index import build_learning_context_rows, infer_services_for_learning


def test_infer_services_preserves_job_and_pod_labels():
    services = infer_services_for_learning(
        metric="http_requests_total",
        query_text='sum(rate(http_requests_total{job="checkout", pod="checkout-api-7f9d"}[5m]))',
        dashboard_title="Checkout",
        panel_title="Traffic",
        tags=[],
    )

    assert "checkout" in services
    assert "checkout-api-7f9d" in services


def test_infer_services_does_not_index_generic_infra_metric_prefixes_as_services():
    for metric in (
        "cpu_usage_seconds_total",
        "memory_working_set_bytes",
        "redis_connected_clients",
        "kafka_consumer_lag",
        "kube_pod_container_status_restarts_total",
    ):
        services = infer_services_for_learning(
            metric=metric,
            query_text=f"sum({metric})",
            dashboard_title="Infrastructure",
            panel_title="Saturation",
            tags=[],
        )

        first_token = metric.split("_", 1)[0]
        assert first_token not in services


def test_learning_context_rows_include_job_and_pod_services():
    rows = build_learning_context_rows(
        dashboard_uid="dash-1",
        backend_name="grafana",
        dashboard_title="Checkout",
        dashboard_tags=[],
        panels=[
            {
                "title": "Traffic",
                "queries": ['sum(rate(http_requests_total{job="checkout", pod="checkout-api-7f9d"}[5m]))'],
                "metrics": ["http_requests_total"],
            }
        ],
        metrics_found=["http_requests_total"],
        signals_inferred=[{"metric": "http_requests_total", "signal_type": "request_rate", "confidence": 0.8}],
        status="approved",
        activated_pairs=None,
    )

    service_column = rows[0][9]
    assert "checkout" in service_column
    assert "checkout-api-7f9d" in service_column
